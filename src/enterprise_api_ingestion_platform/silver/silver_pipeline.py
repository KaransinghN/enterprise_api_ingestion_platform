#src/enterprise_api_ingestion_platform/silver/silver_pipeline.py

from dataclasses import dataclass

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from transformations import (
    transform_dim_customer,
    transform_dim_product,
    transform_fact_order,
)

def _get_spark() -> SparkSession:
    """
    Returns the active Spark session.
    """
    spark_session = SparkSession.getActiveSession()

    if spark_session is None:
        raise RuntimeError(
            "No active Spark session is available."
        )

    return spark_session


spark = _get_spark()

METADATA_CATALOG = spark.conf.get("METADATA_CATALOG")
METADATA_SCHEMA = spark.conf.get("METADATA_SCHEMA")

METADATA_TABLE = (
    f"{METADATA_CATALOG}."
    f"{METADATA_SCHEMA}."
    "api_metadata"
)


OPENAPI_USERS_API_ID = "openapi_users"
OPENAPI_PRODUCTS_API_ID = "openapi_products"
OPENAPI_ORDERS_API_ID = "openapi_orders"

CUSTOMER_TABLE = "dim_customer"
PRODUCT_TABLE = "dim_product"
ORDER_TABLE = "fact_order"


@dataclass(frozen=True, slots=True)
class SilverRoute:
    """
    Metadata-driven routing configuration for one Silver API model.
    """

    api_id: str
    bronze_catalog: str
    bronze_schema: str
    bronze_table: str

    @property
    def bronze_fqn(self) -> str:
        """
        Returns the fully qualified Bronze table name.
        """
        return (
            f"{self.bronze_catalog}."
            f"{self.bronze_schema}."
            f"{self.bronze_table}"
        )


def _validate_identifier(
    value: str | None,
    field_name: str,
) -> str:
    """
    Validates one Unity Catalog identifier component.
    """

    if value is None:
        raise ValueError(
            f"{field_name} cannot be null."
        )

    normalized = value.strip()

    if not normalized:
        raise ValueError(
            f"{field_name} cannot be empty."
        )

    if any(
        character in normalized
        for character in (".", "/", "\\")
    ):
        raise ValueError(
            f"{field_name} contains an invalid character: "
            f"{value!r}"
        )

    return normalized


def _load_silver_routes() -> list[SilverRoute]:
    """
    Loads all enabled Silver source routes from API metadata.
    """

    spark_session = _get_spark()

    rows = (
        spark_session.table(METADATA_TABLE)
        .filter(col("enabled"))
        .select(
            "api_id",
            "bronze_catalog",
            "bronze_schema",
            "bronze_table",
        )
        .orderBy(col("api_id"))
        .collect()
    )

    routes: list[SilverRoute] = []

    for row in rows:
        routes.append(
            SilverRoute(
                api_id=_validate_identifier(
                    row["api_id"],
                    "api_id",
                ),
                bronze_catalog=_validate_identifier(
                    row["bronze_catalog"],
                    "bronze_catalog",
                ),
                bronze_schema=_validate_identifier(
                    row["bronze_schema"],
                    "bronze_schema",
                ),
                bronze_table=_validate_identifier(
                    row["bronze_table"],
                    "bronze_table",
                ),
            )
        )

    return routes


def _get_route(
    routes: list[SilverRoute],
    api_id: str,
) -> SilverRoute:
    """
    Returns the unique enabled Silver route for an API.
    """

    matching_routes = [
        route
        for route in routes
        if route.api_id == api_id
    ]

    if not matching_routes:
        raise ValueError(
            f"No enabled API metadata found for api_id={api_id!r}."
        )

    if len(matching_routes) > 1:
        raise ValueError(
            f"Multiple enabled API metadata rows found "
            f"for api_id={api_id!r}."
        )

    return matching_routes[0]


SILVER_ROUTES = _load_silver_routes()

OPENAPI_USERS_ROUTE = _get_route(
    routes=SILVER_ROUTES,
    api_id=OPENAPI_USERS_API_ID,
)

OPENAPI_PRODUCTS_ROUTE = _get_route(
    routes=SILVER_ROUTES,
    api_id=OPENAPI_PRODUCTS_API_ID,
)

OPENAPI_ORDERS_ROUTE = _get_route(
    routes=SILVER_ROUTES,
    api_id=OPENAPI_ORDERS_API_ID,
)


@dp.materialized_view(
    name=CUSTOMER_TABLE,
    cluster_by_auto=True,
    comment="Silver customer dimension derived from OpenAPI Users.",
)
def dim_customer() -> DataFrame:
    """
    Produces the customer dimension.
    """

    bronze_df = _get_spark().read.table(
        OPENAPI_USERS_ROUTE.bronze_fqn
    )

    return transform_dim_customer(bronze_df)


@dp.materialized_view(
    name=PRODUCT_TABLE,
    cluster_by_auto=True,
    comment="Silver product dimension derived from OpenAPI Products.",
)
def dim_product() -> DataFrame:
    """
    Produces the product dimension.
    """

    bronze_df = _get_spark().read.table(
        OPENAPI_PRODUCTS_ROUTE.bronze_fqn
    )

    return transform_dim_product(bronze_df)


def _read_fact_order_source() -> DataFrame:
    """
    Reads the Bronze orders source.
    """

    return _get_spark().read.table(
        OPENAPI_ORDERS_ROUTE.bronze_fqn
    )

@dp.materialized_view(
    name=ORDER_TABLE,
    cluster_by_auto=True,
    comment="Silver order-line fact derived from OpenAPI Orders.",
)
@dp.expect_or_fail(
    "valid_order_id",
    "order_id IS NOT NULL",
)
@dp.expect_or_fail(
    "valid_line_number",
    "line_number IS NOT NULL",
)
@dp.expect_or_fail(
    "valid_customer_id",
    "customer_id IS NOT NULL",
)
@dp.expect_or_fail(
    "valid_product_id",
    "product_id IS NOT NULL",
)
@dp.expect_or_fail(
    "valid_quantity",
    "quantity IS NOT NULL AND quantity > 0",
)
def fact_order() -> DataFrame:
    """
    Produces one Silver fact row per product line within an order.
    """

    spark_session = _get_spark()

    bronze_df = _read_fact_order_source()

    customer_df = spark_session.read.table(
        CUSTOMER_TABLE
    )

    product_df = spark_session.read.table(
        PRODUCT_TABLE
    )

    return transform_fact_order(
        bronze_df=bronze_df,
        customer_df=customer_df,
        product_df=product_df,
    )