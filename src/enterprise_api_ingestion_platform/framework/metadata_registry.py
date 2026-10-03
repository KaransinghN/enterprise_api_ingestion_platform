#src\enterprise_api_ingestion_platform\framework\metadata_registry.py

from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata


class MetadataRegistry:
    """Central registry for API metadata."""

    @staticmethod
    def graph_users() -> ApiMetadata:
        return ApiMetadata(
            api_name="graph_users",
            endpoint="https://graph.microsoft.com/v1.0/users",
            http_method="GET",
            auth_type="CLIENT_CREDENTIAL",
            landing_container="landing",
            landing_path="graph/users",
            bronze_catalog="adb_api_ingestion_dev",
            bronze_schema="karan_vsk",
            bronze_table="graph_users_bronze",
            load_type="FULL",
            enabled=True,
        )