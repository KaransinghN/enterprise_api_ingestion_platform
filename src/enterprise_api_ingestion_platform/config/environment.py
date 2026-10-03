# src/enterprise_api_ingestion_platform/config/environment.py

from dataclasses import dataclass
import os


@dataclass(frozen=True, slots=True)
class Environment:
    """
    Environment-specific configuration.
    """

    name: str
    catalog: str
    audit_schema: str
    metadata_schema: str
    secret_scope: str

    @classmethod
    def load(cls) -> "Environment":
        """
        Load environment-specific configuration.
        """

        environment = os.getenv(
            "ENVIRONMENT",
            "dev",
        ).lower()

        catalog_mapping = {
            "dev": "enterprise_api_dev",
            "qa": "adb_api_ingestion_qa",
            "prod": "adb_api_ingestion_prod",
        }

        return cls(
            name=environment,
            catalog=os.getenv(
                "CATALOG",
                catalog_mapping.get(
                    environment,
                    "enterprise_api_dev",
                ),
            ),
            audit_schema=os.getenv(
                "AUDIT_SCHEMA",
                "audit",
            ),
            metadata_schema=os.getenv(
                "METADATA_SCHEMA",
                "config",
            ),
            secret_scope=os.getenv(
                "SECRET_SCOPE",
                "api-ingestion-scope",
            ),
        )


environment = Environment.load()