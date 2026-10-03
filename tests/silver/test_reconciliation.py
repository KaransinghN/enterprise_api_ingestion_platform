# tests/silver/test_reconciliation.py

from __future__ import annotations

import os

from pyspark.sql import SparkSession


CATALOG = os.environ.get("DATABRICKS_TEST_CATALOG", "enterprise_api_dev")
BRONZE_SCHEMA = os.environ.get("DATABRICKS_TEST_BRONZE_SCHEMA", "bronze")
SILVER_SCHEMA = os.environ.get("DATABRICKS_TEST_SILVER_SCHEMA", "silver")

BRONZE_ORDER_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.openapi_orders"
SILVER_ORDER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.fact_order"


def _latest_bronze_order_count(spark: SparkSession) -> int:
    """Return the number of latest Bronze order snapshots."""

    result = spark.sql(
        f"""
        WITH latest_orders AS (
            SELECT
                id,
                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY ingestion_timestamp DESC, _source_file DESC
                ) AS rn
            FROM {BRONZE_ORDER_TABLE}
        )
        SELECT COUNT(*) AS order_count
        FROM latest_orders
        WHERE rn = 1
        """
    )

    return result.first()["order_count"]


def _silver_distinct_order_count(spark: SparkSession) -> int:
    """Return the number of distinct orders in Silver."""

    result = spark.sql(
        f"""
        SELECT COUNT(DISTINCT order_id) AS order_count
        FROM {SILVER_ORDER_TABLE}
        """
    )

    return result.first()["order_count"]


def _silver_duplicate_order_line_count(spark: SparkSession) -> int:
    """Return the number of duplicate Silver order-line keys."""

    result = spark.sql(
        f"""
        SELECT COUNT(*) AS duplicate_count
        FROM (
            SELECT
                order_id,
                line_number
            FROM {SILVER_ORDER_TABLE}
            GROUP BY order_id, line_number
            HAVING COUNT(*) > 1
        )
        """
    )

    return result.first()["duplicate_count"]


def test_bronze_to_silver_order_reconciliation(spark: SparkSession) -> None:
    """Verify latest Bronze orders reconcile with distinct Silver orders."""

    if os.environ.get("RUN_DATABRICKS_TESTS") != "1":
        return

    latest_bronze_orders = _latest_bronze_order_count(spark)
    silver_orders = _silver_distinct_order_count(spark)

    assert latest_bronze_orders == silver_orders


def test_silver_order_line_keys_are_unique(spark: SparkSession) -> None:
    """Verify Silver order-line business keys are unique."""

    if os.environ.get("RUN_DATABRICKS_TESTS") != "1":
        return

    duplicate_order_lines = _silver_duplicate_order_line_count(spark)

    assert duplicate_order_lines == 0