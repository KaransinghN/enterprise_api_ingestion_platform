# src/enterprise_api_ingestion_platform/main.py

import argparse
import os
from importlib.metadata import PackageNotFoundError, version

from pyspark.sql import SparkSession


ALL_APIS = "ALL"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Metadata-driven OpenAPI ingestion entry point."
    )

    parser.add_argument(
        "--api_name",
        required=True,
        help="API name to ingest, or ALL for all enabled APIs.",
    )

    parser.add_argument(
        "--catalog",
        required=True,
        help="Unity Catalog catalog containing application metadata and audit tables.",
    )

    parser.add_argument(
        "--landing_catalog",
        required=True,
        help="Unity Catalog catalog containing the landing volume.",
    )

    parser.add_argument(
        "--landing_schema",
        required=True,
        help="Unity Catalog schema containing the landing volume.",
    )

    parser.add_argument(
        "--landing_volume",
        required=True,
        help="Unity Catalog managed landing volume.",
    )

    parser.add_argument(
        "--metadata_schema",
        required=True,
        help="Unity Catalog schema containing API metadata and checkpoints.",
    )

    parser.add_argument(
        "--audit_schema",
        required=True,
        help="Unity Catalog schema containing execution audit tables.",
    )

    parser.add_argument(
        "--environment",
        required=True,
        help="Deployment environment, for example dev, qa, or prod.",
    )

    parser.add_argument(
        "--job_run_id",
        required=False,
        default=None,
        help="Databricks job run ID.",
    )

    return parser.parse_args()


def configure_runtime_environment(
    args: argparse.Namespace,
) -> None:
    """
    Configure runtime environment variables before importing
    application services that depend on Environment configuration.
    """
    os.environ["ENVIRONMENT"] = args.environment
    os.environ["CATALOG"] = args.catalog
    os.environ["METADATA_SCHEMA"] = args.metadata_schema
    os.environ["AUDIT_SCHEMA"] = args.audit_schema


def main() -> None:
    args = parse_arguments()

    # Configure the runtime environment before importing services
    # that resolve Environment configuration.
    configure_runtime_environment(args)

    # Import application services after runtime configuration.
    from enterprise_api_ingestion_platform.checkpoint.checkpoint_repository import (
        CheckpointRepository,
    )
    from enterprise_api_ingestion_platform.checkpoint.checkpoint_service import (
        CheckpointService,
    )
    from enterprise_api_ingestion_platform.execution.execution_service import (
    ExecutionService,
    )
    from enterprise_api_ingestion_platform.ingestion.ingestion_service import (
        IngestionService,
    )
    from enterprise_api_ingestion_platform.landing.landing_service import (
        LandingService,
    )
    from enterprise_api_ingestion_platform.landing.landing_writer import (
        LandingWriter,
    )
    from enterprise_api_ingestion_platform.logging.audit_logger import (
        AuditLogger,
    )
    from enterprise_api_ingestion_platform.logging.logger import (
        get_logger,
    )
    from enterprise_api_ingestion_platform.metadata.metadata_service import (
        MetadataService,
    )
    from enterprise_api_ingestion_platform.storage.storage_writer_factory import (
        StorageWriterFactory,
    )
    from enterprise_api_ingestion_platform.utils.http_client import (
        HttpClient,
    )

    spark = (
        SparkSession.builder
        .appName("enterprise-api-ingestion")
        .getOrCreate()
    )

    logger = get_logger(__name__)

    try:
        package_version = version(
            "enterprise_api_ingestion_platform"
        )
    except PackageNotFoundError:
        package_version = "unknown"

    logger.info(
        "Starting OpenAPI ingestion: "
        "api_name=%s, environment=%s, catalog=%s, "
        "metadata_schema=%s, audit_schema=%s, "
        "landing_catalog=%s, landing_schema=%s, "
        "landing_volume=%s, job_run_id=%s, "
        "package_version=%s",
        args.api_name,
        args.environment,
        args.catalog,
        args.metadata_schema,
        args.audit_schema,
        args.landing_catalog,
        args.landing_schema,
        args.landing_volume,
        args.job_run_id,
        package_version,
    )

    metadata_service = MetadataService()

    http_client = HttpClient()

    storage_writer = StorageWriterFactory.create(
        spark=spark,
        landing_catalog=args.landing_catalog,
        landing_schema=args.landing_schema,
        landing_volume=args.landing_volume,
    )

    landing_writer = LandingWriter(
        storage_writer=storage_writer,
    )

    landing_service = LandingService(
        http_client=http_client,
        landing_writer=landing_writer,
    )

    audit_logger = AuditLogger(spark)

    checkpoint_repository = CheckpointRepository(spark)

    checkpoint_service = CheckpointService(
        checkpoint_repository=checkpoint_repository,
    )

    ingestion_service = IngestionService(
        landing_service=landing_service,
        audit_logger=audit_logger,
        checkpoint_service=checkpoint_service,
    )

    execution_service = ExecutionService(
        metadata_service=metadata_service,
        ingestion_service=ingestion_service,
    )

    if args.api_name.upper() == ALL_APIS:
        logger.info(
            "Executing ingestion for all enabled APIs: job_run_id=%s",
            args.job_run_id,
        )

        execution_service.execute_all(
            job_run_id=args.job_run_id,
        )

    else:
        logger.info(
            "Executing ingestion for API '%s': job_run_id=%s",
            args.api_name,
            args.job_run_id,
        )

        metadata = metadata_service.get_metadata(
            args.api_name,
        )

        execution_service.execute_api(
            metadata=metadata,
            job_run_id=args.job_run_id,
        )

        logger.info(
            "Completed ingestion for API '%s': job_run_id=%s",
            args.api_name,
            args.job_run_id,
        )


if __name__ == "__main__":
    main()