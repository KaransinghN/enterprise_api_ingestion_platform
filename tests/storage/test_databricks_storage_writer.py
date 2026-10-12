from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata
from enterprise_api_ingestion_platform.storage.databricks_storage_writer import (
    DatabricksManagedStorageWriter,
)


def create_metadata() -> ApiMetadata:
    return ApiMetadata(
        api_id="orders-api",
        api_name="orders",
        endpoint="https://example.com/orders",
        load_type="FULL",
    )


def create_writer() -> DatabricksManagedStorageWriter:
    return DatabricksManagedStorageWriter(
        spark=MagicMock(),
        landing_catalog="enterprise_api_dev",
        landing_schema="dev_karansingh_n_landing",
        landing_volume="landing_volume",
    )


def test_writer_builds_expected_volume_root() -> None:
    writer = create_writer()

    assert writer.raw_volume_root == (
        "/Volumes/enterprise_api_dev/"
        "dev_karansingh_n_landing/landing_volume"
    )


def test_target_file_uses_api_directory_and_unique_json_filename() -> None:
    writer = create_writer()
    metadata = create_metadata()

    first_file = writer._target_file(metadata)
    second_file = writer._target_file(metadata)

    assert first_file.startswith(
        "/Volumes/enterprise_api_dev/"
        "dev_karansingh_n_landing/landing_volume/orders-api/"
    )
    assert first_file.endswith(".json")
    assert first_file != second_file


def test_write_serializes_payload_and_writes_to_volume() -> None:
    writer = create_writer()
    metadata = create_metadata()
    payload = [{"id": 1, "status": "COMPLETE"}]
    dbutils_mock = MagicMock()

    with (
        patch(
            "enterprise_api_ingestion_platform.storage."
            "databricks_storage_writer._get_dbutils",
            return_value=dbutils_mock,
        ),
        patch.object(
            writer,
            "get_file_size",
            return_value=123,
        ) as size_mock,
    ):
        result = writer.write(metadata, payload)

    dbutils_mock.fs.put.assert_called_once()
    args = dbutils_mock.fs.put.call_args.args

    assert args[0] == result
    assert args[1] == json.dumps(
        payload,
        ensure_ascii=False,
        default=str,
    )
    assert args[2] is True
    size_mock.assert_called_once_with(file_path=result)


def test_get_file_size_returns_file_size() -> None:
    writer = create_writer()
    file_info = MagicMock()
    file_info.isDir.return_value = False
    file_info.size = 123
    dbutils_mock = MagicMock()
    dbutils_mock.fs.ls.return_value = [file_info]

    with patch(
        "enterprise_api_ingestion_platform.storage."
        "databricks_storage_writer._get_dbutils",
        return_value=dbutils_mock,
    ):
        size = writer.get_file_size("/Volumes/catalog/schema/volume/file.json")

    assert size == 123
    dbutils_mock.fs.ls.assert_called_once_with(
        "/Volumes/catalog/schema/volume/file.json",
    )


def test_get_file_size_raises_when_file_is_missing() -> None:
    writer = create_writer()
    dbutils_mock = MagicMock()
    dbutils_mock.fs.ls.return_value = []

    with (
        patch(
            "enterprise_api_ingestion_platform.storage."
            "databricks_storage_writer._get_dbutils",
            return_value=dbutils_mock,
        ),
        pytest.raises(
            FileNotFoundError,
            match="Landing file was not found",
        ),
    ):
        writer.get_file_size("/Volumes/catalog/schema/volume/missing.json")


def test_get_file_size_raises_for_directory() -> None:
    writer = create_writer()
    directory_info = MagicMock()
    directory_info.isDir.return_value = True
    dbutils_mock = MagicMock()
    dbutils_mock.fs.ls.return_value = [directory_info]

    with (
        patch(
            "enterprise_api_ingestion_platform.storage."
            "databricks_storage_writer._get_dbutils",
            return_value=dbutils_mock,
        ),
        pytest.raises(
            IsADirectoryError,
            match="Landing path is a directory",
        ),
    ):
        writer.get_file_size("/Volumes/catalog/schema/volume/directory")
