#src/enterprise_api_ingestion_platform/logging/audit.logger.py
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from enterprise_api_ingestion_platform.common.exceptions import (
    AuditLoggingException,
)
from enterprise_api_ingestion_platform.config.environment import environment


class AuditLogger:
    """
    Writes application-level execution audit records
    to the audit Delta table.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def log_execution(
        self,
        run_id: str,
        job_run_id: str | None,
        api_name: str,
        job_name: str,
        start_time,
        end_time,
        status: str,
        records_read: int | None,
        records_written: int | None,
        pages_read: int | None,
        schema_version: int,
        duration_seconds: float,
        landing_file: str | None,
        landing_file_size_bytes: int | None,
        failure_stage: str | None,
        failure_type: str | None,
        http_status: int | None,
        retry_count: int | None,
        total_attempts: int | None,
        error_message: str | None,
    ) -> None:
        """
        Writes one execution audit record.

        This class is intentionally responsible only for
        persisting the audit record. Metrics and failure
        information are collected by the execution components
        and passed into this method.
        """

        schema = StructType(
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

        try:
            dataframe = self.spark.createDataFrame(
                [
                    (
                        run_id,
                        job_run_id,
                        api_name,
                        job_name,
                        start_time,
                        end_time,
                        status,
                        records_read,
                        records_written,
                        pages_read,
                        schema_version,
                        duration_seconds,
                        landing_file,
                        landing_file_size_bytes,
                        failure_stage,
                        failure_type,
                        http_status,
                        retry_count,
                        total_attempts,
                        error_message,
                    )
                ],
                schema=schema,
            )

            dataframe.write.format("delta").mode("append").saveAsTable(
                f"{environment.catalog}."
                f"{environment.audit_schema}."
                "job_execution"
            )

        except Exception as exc:
            raise AuditLoggingException(
                f"Failed to write audit log: {exc}"
            ) from exc