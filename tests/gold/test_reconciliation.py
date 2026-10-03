# tests/gold/test_reconciliation.py

from __future__ import annotations

import os

from pyspark.sql import SparkSession


CATALOG = os.environ.get("DATABRICKS_TEST_CATALOG", "enterprise_api_dev")
SILVER_SCHEMA = os.environ.get("DATABRICKS_TEST_SILVER_SCHEMA", "silver")
GOLD_SCHEMA = os.environ.get("DATABRICKS_TEST_GOLD_SCHEMA", "gold")

SILVER_ORDER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.fact_order"
GOLD_ORDER_KPI_TABLE = f"{CATALOG}.{GOLD_SCHEMA}.gold_order_kpis"


def _silver_order_metrics(spark: SparkSession) -> tuple[int, int, int]:
    """Return Silver order count, units sold, and order-level sales."""

    result = spark.sql(
        f"""
        WITH silver_order_totals AS (
            SELECT
                order_id,
                MAX(order_total) AS order_total
            FROM {SILVER_ORDER_TABLE}
            GROUP BY order_id
        ),
        silver_units AS (
            SELECT
                SUM(quantity) AS units_sold
            FROM {SILVER_ORDER_TABLE}
        )
        SELECT
            (SELECT COUNT(*) FROM silver_order_totals) AS order_count,
            (SELECT units_sold FROM silver_units) AS units_sold,
            (SELECT SUM(order_total) FROM silver_order_totals) AS sales_amount
        """
    )

    row = result.first()

    return (
        row["order_count"],
        row["units_sold"],
        row["sales_amount"],
    )


def _gold_order_metrics(spark: SparkSession) -> tuple[int, int, int]:
    """Return Gold order count, units sold, and sales."""

    result = spark.sql(
        f"""
        SELECT
            COUNT(*) AS order_count,
            SUM(units_sold) AS units_sold,
            SUM(order_total) AS sales_amount
        FROM {GOLD_ORDER_KPI_TABLE}
        """
    )

    row = result.first()

    return (
        row["order_count"],
        row["units_sold"],
        row["sales_amount"],
    )


def _gold_duplicate_order_count(spark: SparkSession) -> int:
    """Return the number of duplicate Gold order keys."""

    result = spark.sql(
        f"""
        SELECT COUNT(*) AS duplicate_count
        FROM (
            SELECT
                order_id
            FROM {GOLD_ORDER_KPI_TABLE}
            GROUP BY order_id
            HAVING COUNT(*) > 1
        )
        """
    )

    return result.first()["duplicate_count"]


def test_silver_to_gold_order_reconciliation(spark: SparkSession) -> None:
    """Verify Silver order metrics reconcile with Gold order KPIs."""

    if os.environ.get("RUN_DATABRICKS_TESTS") != "1":
        return

    silver_orders, silver_units, silver_sales = _silver_order_metrics(spark)
    gold_orders, gold_units, gold_sales = _gold_order_metrics(spark)

    assert silver_orders == gold_orders
    assert silver_units == gold_units
    assert silver_sales == gold_sales


def test_gold_order_kpi_keys_are_unique(spark: SparkSession) -> None:
    """Verify Gold order KPI rows are unique by order_id."""

    if os.environ.get("RUN_DATABRICKS_TESTS") != "1":
        return

    duplicate_orders = _gold_duplicate_order_count(spark)

    assert duplicate_orders == 0