# tests/conftest.py

"""Configure pytest and provide Databricks Connect Spark fixtures."""

from __future__ import annotations

import csv
import json
import os
import pathlib
import sys
from contextlib import contextmanager

try:
    import pytest
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
except ImportError as exc:
    raise ImportError(
        "Test dependencies not found.\n\n"
        "Run tests using 'uv run pytest'. "
        "See https://docs.astral.sh/uv for more information."
    ) from exc


@pytest.fixture()
def spark() -> SparkSession:
    """Provide a Databricks Connect SparkSession for tests."""
    if os.environ.get("RUN_DATABRICKS_TESTS") != "1":
        pytest.skip("Databricks integration tests are disabled")

    session = DatabricksSession.builder.getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    return session


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Provide a callable for loading JSON or CSV test fixtures."""

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()

        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)

        if suffix == ".csv":
            with path.open(newline="") as file:
                rows = list(csv.DictReader(file))
            return spark.createDataFrame(rows)

        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


def _enable_fallback_compute() -> None:
    """Enable serverless compute when no compute is configured."""
    conf = WorkspaceClient().config

    if (
        conf.serverless_compute_id
        or conf.cluster_id
        or os.environ.get("SPARK_REMOTE")
    ):
        return

    url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"

    print(
        "☁️ no compute specified, falling back to serverless compute",
        file=sys.stderr,
    )
    print(
        f"  see {url} for manual configuration",
        file=sys.stdout,
    )

    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")

    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config) -> None:
    """Configure the Databricks test session."""
    if os.environ.get("RUN_DATABRICKS_TESTS") != "1":
        return

    with _allow_stderr_output(config):
        _enable_fallback_compute()

        if hasattr(DatabricksSession.builder, "validateSession"):
            DatabricksSession.builder.validateSession().getOrCreate()
        else:
            DatabricksSession.builder.getOrCreate()