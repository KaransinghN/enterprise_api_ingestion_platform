# src/enterprise_api_ingestion_platform/gold/gold_pipeline.py

from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from transformations import (
    transform_gold_customer_sales,
    transform_gold_order_kpis,
    transform_gold_product_sales,
)


spark = SparkSession.getActiveSession()

SILVER_CATALOG = spark.conf.get("SILVER_CATALOG")
SILVER_SCHEMA = spark.conf.get("SILVER_SCHEMA")

SILVER_FACT_ORDER_TABLE = (
    f"{SILVER_CATALOG}.{SILVER_SCHEMA}.fact_order"
)

SILVER_DIM_CUSTOMER_TABLE = (
    f"{SILVER_CATALOG}.{SILVER_SCHEMA}.dim_customer"
)

SILVER_DIM_PRODUCT_TABLE = (
    f"{SILVER_CATALOG}.{SILVER_SCHEMA}.dim_product"
)


def _require_spark() -> SparkSession:
    """
    Returns the active Spark session required by the Gold pipeline.
    """

    if spark is None:
        raise RuntimeError(
            "No active SparkSession is available for the Gold pipeline."
        )

    return spark


@dp.materialized_view(
    name="gold_order_kpis",
    comment=(
        "Gold order-level KPIs derived from the Silver fact_order "
        "order-line fact."
    ),
)
def gold_order_kpis() -> DataFrame:
    """
    Creates one Gold row per order from Silver fact_order.
    """

    session = _require_spark()

    fact_order_df = session.read.table(
        SILVER_FACT_ORDER_TABLE,
    )

    return transform_gold_order_kpis(
        fact_order_df,
    )


@dp.materialized_view(
    name="gold_customer_sales",
    comment=(
        "Gold customer sales metrics derived from gold_order_kpis "
        "and dim_customer."
    ),
)
def gold_customer_sales() -> DataFrame:
    """
    Creates one Gold row per customer.
    """

    session = _require_spark()

    gold_order_kpis_df = session.read.table(
        "gold_order_kpis",
    )

    dim_customer_df = session.read.table(
        SILVER_DIM_CUSTOMER_TABLE,
    )

    return transform_gold_customer_sales(
        gold_order_kpis_df=gold_order_kpis_df,
        dim_customer_df=dim_customer_df,
    )


@dp.materialized_view(
    name="gold_product_sales",
    comment=(
        "Gold product sales metrics derived from the Silver fact_order "
        "order-line fact and dim_product."
    ),
)
def gold_product_sales() -> DataFrame:
    """
    Creates one Gold row per product.
    """

    session = _require_spark()

    fact_order_df = session.read.table(
        SILVER_FACT_ORDER_TABLE,
    )

    dim_product_df = session.read.table(
        SILVER_DIM_PRODUCT_TABLE,
    )

    return transform_gold_product_sales(
        fact_order_df=fact_order_df,
        dim_product_df=dim_product_df,
    )
