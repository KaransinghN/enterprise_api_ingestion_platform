# src/enterprise_api_ingestion_platform/metadata/metadata_reader.py

import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from enterprise_api_ingestion_platform.common.exceptions import (
    DuplicateMetadataException,
    MetadataNotFoundException,
)
from enterprise_api_ingestion_platform.config.environment import environment
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata



class MetadataReader:
    """
    Reads API metadata from the configuration Delta table.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @property
    def metadata_table(self) -> str:
        return (
            f"{environment.catalog}."
            f"{environment.metadata_schema}."
            "api_metadata"
        )

    def get_metadata(self, api_name: str) -> ApiMetadata:
        """
        Return enabled metadata for a single API.
        """
        dataframe = (
            self.spark.table(self.metadata_table)
            .filter(col("api_name") == api_name)
            .filter(col("enabled"))
        )

        rows = dataframe.collect()

        if len(rows) == 0:
            raise MetadataNotFoundException(
                f"Metadata not found for API: {api_name}"
            )

        if len(rows) > 1:
            raise DuplicateMetadataException(
                f"Duplicate metadata found for API: {api_name}"
            )

        return self._to_api_metadata(rows[0])

    def get_enabled_by_schedule(
        self,
        schedule: str,
    ) -> list[ApiMetadata]:
        """
        Return all enabled APIs configured for a schedule.
        """
        rows = (
            self.spark.table(self.metadata_table)
            .filter(col("enabled"))
            .filter(col("schedule") == schedule)
            .collect()
        )

        return [self._to_api_metadata(row) for row in rows]

    def _to_api_metadata(self, row) -> ApiMetadata:
        """
        Convert a metadata table row into the domain model.
        """
        return ApiMetadata(
            api_id=getattr(row, "api_id", None),
            api_name=row.api_name,
            endpoint=row.endpoint,
            http_method=row.http_method,
            auth_type=row.auth_type,
            secret_scope=getattr(row, "secret_scope", None),
            secret_key=getattr(row, "secret_key", None),
            auth_header_name=getattr(
                row,
                "auth_header_name",
                None,
            ),
            auth_header_prefix=getattr(
                row,
                "auth_header_prefix",
                None,
            ),
            headers_json=(
                json.loads(row.headers_json)
                if getattr(row, "headers_json", None)
                else {}
            ),
            query_parameters=(
                json.loads(row.query_parameters)
                if getattr(row, "query_parameters", None)
                else {}
            ),
            landing_container=getattr(
                row,
                "landing_container",
                "landing",
            ) or "landing",
            landing_path=getattr(
                row,
                "landing_path",
                "",
            ) or "",
            bronze_catalog=getattr(
                row,
                "bronze_catalog",
                None,
            ),
            bronze_schema=getattr(
                row,
                "bronze_schema",
                None,
            ),
            bronze_table=getattr(
                row,
                "bronze_table",
                None,
            ),
            record_path=getattr(
                row,
                "record_path",
                None,
            ),
            pagination_type=getattr(
                row,
                "pagination_type",
                None,
            ),
            page_size=getattr(
                row,
                "page_size",
                None,
            ),
            cursor_field=getattr(
                row,
                "cursor_field",
                None,
            ) or "next_cursor",
            cursor_parameter=getattr(
                row,
                "cursor_parameter",
                None,
            ) or "cursor",
            load_type=getattr(
                row,
                "load_type",
                "FULL",
            ) or "FULL",
            incremental_column=getattr(
                row,
                "incremental_column",
                None,
            ),
            incremental_parameter=getattr(
                row,
                "incremental_parameter",
                None,
            ),
            incremental_format=getattr(
                row,
                "incremental_format",
                None,
            ),
            overlap_minutes=getattr(
                row,
                "overlap_minutes",
                None,
            ),
            initial_load_value=getattr(
                row,
                "initial_load_value",
                None,
            ),
            download_mode=getattr(
                row,
                "download_mode",
                "BUFFER",
            ) or "BUFFER",
            schedule=getattr(
                row,
                "schedule",
                None,
            ),
            execution_group=getattr(
                row,
                "execution_group",
                None,
            ),
            enabled=getattr(
                row,
                "enabled",
                True,
            ),
            created_by=getattr(
                row,
                "created_by",
                None,
            ),
            created_ts=getattr(
                row,
                "created_ts",
                None,
            ),
            updated_ts=getattr(
                row,
                "updated_ts",
                None,
            ),
        )