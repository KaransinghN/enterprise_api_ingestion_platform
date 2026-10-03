#src/enterprise_api_ingestion_platform/metadata/metadata_initializer.py
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialize managed Unity Catalog metadata tables."
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--metadata_schema", required=True)
    parser.add_argument("--audit_schema", required=True)
    return parser.parse_args()


def api_metadata_schema() -> StructType:
    return StructType(
        [
            StructField("api_id", StringType(), False),
            StructField("api_name", StringType(), False),
            StructField("endpoint", StringType(), False),
            StructField("http_method", StringType(), False),
            StructField("auth_type", StringType(), False),
            StructField("secret_scope", StringType(), True),
            StructField("secret_key", StringType(), True),
            StructField("auth_header_name", StringType(), True),
            StructField("auth_header_prefix", StringType(), True),
            StructField("headers_json", StringType(), True),
            StructField("query_parameters", StringType(), True),
            StructField("landing_container", StringType(), False),
            StructField("landing_path", StringType(), False),
            StructField("bronze_catalog", StringType(), True),
            StructField("bronze_schema", StringType(), True),
            StructField("bronze_table", StringType(), True),
            StructField("record_path", StringType(), True),
            StructField("pagination_type", StringType(), True),
            StructField("page_size", IntegerType(), True),
            StructField("cursor_field", StringType(), False),
            StructField("cursor_parameter", StringType(), False),
            StructField("load_type", StringType(), False),
            StructField("incremental_column", StringType(), True),
            StructField("incremental_parameter", StringType(), True),
            StructField("incremental_format", StringType(), True),
            StructField("overlap_minutes", IntegerType(), True),
            StructField("initial_load_value", StringType(), True),
            StructField("download_mode", StringType(), False),
            StructField("schedule", StringType(), True),
            StructField("execution_group", StringType(), True),
            StructField("enabled", BooleanType(), False),
            StructField("schema_version", IntegerType(), False),
            StructField("created_by", StringType(), True),
            StructField("created_ts", TimestampType(), True),
            StructField("updated_ts", TimestampType(), True),
        ]
    )


def api_checkpoint_schema() -> StructType:
    return StructType(
        [
            StructField("api_id", StringType(), False),
            StructField("state_type", StringType(), False),
            StructField("state_value", StringType(), False),
            StructField("recorded_at", TimestampType(), False),
            StructField("run_id", StringType(), False),
        ]
    )


def job_execution_schema() -> StructType:
    return StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("job_run_id", StringType(), True),
            StructField("api_name", StringType(), False),
            StructField("job_name", StringType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), False),
            StructField("status", StringType(), False),
            StructField("records_read", IntegerType(), True),
            StructField("records_written", IntegerType(), True),
            StructField("pages_read", IntegerType(), True),
            StructField("schema_version", IntegerType(), True),
            StructField("duration_seconds", DoubleType(), False),
            StructField("landing_file", StringType(), True),
            StructField("landing_file_size_bytes", LongType(), True),
            StructField("failure_stage", StringType(), True),
            StructField("failure_type", StringType(), True),
            StructField("http_status", IntegerType(), True),
            StructField("retry_count", IntegerType(), True),
            StructField("total_attempts", IntegerType(), True),
            StructField("error_message", StringType(), True),
        ]
    )


def create_managed_table(
    spark: SparkSession,
    table_name: str,
    schema: StructType,
) -> None:
    if spark.catalog.tableExists(table_name):
        return

    empty_dataframe = spark.createDataFrame([], schema=schema)

    (
        empty_dataframe.write
        .format("delta")
        .mode("ignore")
        .saveAsTable(table_name)
    )


def apply_not_null_constraints(
    spark: SparkSession,
    table_name: str,
    columns: tuple[str, ...],
) -> None:
    for column in columns:
        spark.sql(
            f"ALTER TABLE {table_name} "
            f"ALTER COLUMN `{column}` SET NOT NULL"
        )


def initialize_tables(
    spark: SparkSession,
    catalog: str,
    metadata_schema: str,
    audit_schema: str,
) -> None:
    api_metadata_table = f"{catalog}.{metadata_schema}.api_metadata"
    api_checkpoint_table = f"{catalog}.{metadata_schema}.api_checkpoint"
    job_execution_table = f"{catalog}.{audit_schema}.job_execution"

    create_managed_table(
        spark=spark,
        table_name=api_metadata_table,
        schema=api_metadata_schema(),
    )
    apply_not_null_constraints(
        spark=spark,
        table_name=api_metadata_table,
        columns=(
            "api_id",
            "api_name",
            "endpoint",
            "http_method",
            "auth_type",
            "bronze_catalog",
            "bronze_schema",
            "bronze_table",
        ),
    )

    create_managed_table(
        spark=spark,
        table_name=api_checkpoint_table,
        schema=api_checkpoint_schema(),
    )
    apply_not_null_constraints(
        spark=spark,
        table_name=api_checkpoint_table,
        columns=(
            "api_id",
            "state_type",
            "state_value",
            "recorded_at",
            "run_id",
        ),
    )

    create_managed_table(
        spark=spark,
        table_name=job_execution_table,
        schema=job_execution_schema(),
    )
    apply_not_null_constraints(
        spark=spark,
        table_name=job_execution_table,
        columns=(
            "run_id",
            "api_name",
            "job_name",
            "start_time",
            "end_time",
            "status",
            "duration_seconds",
        ),
    )


def main() -> None:
    args = parse_arguments()

    spark = (
        SparkSession.builder
        .appName("enterprise-api-metadata-initializer")
        .getOrCreate()
    )

    try:
        initialize_tables(
            spark=spark,
            catalog=args.catalog,
            metadata_schema=args.metadata_schema,
            audit_schema=args.audit_schema,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
