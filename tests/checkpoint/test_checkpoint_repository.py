# tests/checkpoint/test_checkpoint_repository.py

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from enterprise_api_ingestion_platform.checkpoint.checkpoint_repository import (
    CheckpointRepository,
)


def test_table_name_uses_environment_configuration() -> None:
    spark = MagicMock()

    repository = CheckpointRepository(spark)

    assert repository.table_name == "enterprise_api_dev.config.api_checkpoint"


def test_get_last_success_checkpoint_returns_latest_checkpoint() -> None:
    spark = MagicMock()

    recorded_at = datetime(2026, 9, 7, 10, 30, tzinfo=timezone.utc)

    row = {
        "api_id": "orders",
        "state_type": "CURSOR",
        "state_value": "cursor-123",
        "recorded_at": recorded_at,
        "run_id": "run-123",
    }

    (
        spark.table.return_value
        .filter.return_value
        .orderBy.return_value
        .limit.return_value
        .collect.return_value
    ) = [row]

    repository = CheckpointRepository(spark)

    checkpoint = repository.get_last_success_checkpoint("orders")

    assert checkpoint is not None
    assert checkpoint.api_id == "orders"
    assert checkpoint.state_type == "CURSOR"
    assert checkpoint.state_value == "cursor-123"
    assert checkpoint.recorded_at == recorded_at
    assert checkpoint.run_id == "run-123"


def test_get_last_success_checkpoint_returns_none_when_missing() -> None:
    spark = MagicMock()

    (
        spark.table.return_value
        .filter.return_value
        .orderBy.return_value
        .limit.return_value
        .collect.return_value
    ) = []

    repository = CheckpointRepository(spark)

    checkpoint = repository.get_last_success_checkpoint("orders")

    assert checkpoint is None


def test_get_last_success_checkpoint_wraps_spark_error() -> None:
    spark = MagicMock()

    spark.table.side_effect = RuntimeError("table unavailable")

    repository = CheckpointRepository(spark)

    with pytest.raises(
        RuntimeError,
        match="Failed to retrieve checkpoint for API 'orders'",
    ):
        repository.get_last_success_checkpoint("orders")


def test_save_checkpoint_appends_to_delta() -> None:
    spark = MagicMock()

    dataframe = MagicMock()
    spark.createDataFrame.return_value = dataframe

    recorded_at = datetime(2026, 9, 7, 10, 30, tzinfo=timezone.utc)

    repository = CheckpointRepository(spark)

    repository.save_checkpoint(
        api_id="orders",
        state_type="CURSOR",
        state_value="cursor-123",
        recorded_at=recorded_at,
        run_id="run-123",
    )

    spark.createDataFrame.assert_called_once()

    dataframe.write.format.assert_called_once_with("delta")
    dataframe.write.format.return_value.mode.assert_called_once_with(
        "append"
    )
    (
        dataframe.write.format.return_value.mode.return_value.saveAsTable
    ).assert_called_once_with(
        "enterprise_api_dev.config.api_checkpoint"
    )


def test_save_checkpoint_wraps_spark_error() -> None:
    spark = MagicMock()

    spark.createDataFrame.side_effect = RuntimeError("write failed")

    repository = CheckpointRepository(spark)

    with pytest.raises(
        RuntimeError,
        match="Failed to save checkpoint for API 'orders'",
    ):
        repository.save_checkpoint(
            api_id="orders",
            state_type="CURSOR",
            state_value="cursor-123",
            recorded_at=datetime(
                2026,
                9,
                7,
                10,
                30,
                tzinfo=timezone.utc,
            ),
            run_id="run-123",
        )