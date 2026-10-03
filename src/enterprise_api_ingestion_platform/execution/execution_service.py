# src/enterprise_api_ingestion_platform/execution_service.py

from enterprise_api_ingestion_platform.ingestion.ingestion_service import (
    IngestionService,
)
from enterprise_api_ingestion_platform.logging.logger import (
    get_logger,
)
from enterprise_api_ingestion_platform.metadata.metadata_service import (
    MetadataService,
)
from enterprise_api_ingestion_platform.models.api_metadata import (
    ApiMetadata,
)


class ExecutionService:
    """
    Orchestrates execution of API ingestions.
    """

    def __init__(
        self,
        metadata_service: MetadataService,
        ingestion_service: IngestionService,
    ) -> None:
        self._metadata_service = metadata_service
        self._ingestion_service = ingestion_service
        self._logger = get_logger(__name__)

    def execute_api(
           self,
           metadata: ApiMetadata,
           job_run_id: str | None = None,
    ) -> None:
        """
        Executes ingestion for a single API.
        """
        self._logger.info(
            "Executing API: %s job_run_id=%s",
            metadata.api_name,
            job_run_id,
        )

        self._ingestion_service.run(
            metadata,
            job_run_id=job_run_id,
        )

    def execute_all(
        self,
        job_run_id: str | None = None,
    ) -> None:
        
        """
        Executes all enabled APIs.
        """

        metadata_list = (
            self._metadata_service.get_enabled_metadata()
        )

        self._logger.info(
            "Found %d enabled API(s).",
            len(metadata_list),
        )

        for index, metadata in enumerate(
            metadata_list,
            start=1,
        ):
            self._logger.info(
                "[%d/%d] Executing API: %s",
                index,
                len(metadata_list),
                metadata.api_name,
            )

            self.execute_api(
                metadata,
                job_run_id=job_run_id,
            )

        self._logger.info(
            "Completed execution of all enabled APIs."
        )

    def execute_schedule(
        self,
        schedule: str,
        execution_group: str | None = None,
    ) -> None:
        """
        Executes all enabled APIs for the specified schedule.

        Parameters
        ----------
        schedule
            Schedule identifier.

        execution_group
            Optional execution group filter.
        """

        metadata_list = (
            self._metadata_service.get_enabled_metadata_by_schedule(
                schedule=schedule,
                execution_group=execution_group,
            )
        )

        self._logger.info(
            "Found %d enabled API(s) for schedule '%s'.",
            len(metadata_list),
            schedule,
        )

        for index, metadata in enumerate(
            metadata_list,
            start=1,
        ):
            self._logger.info(
                "[%d/%d] Executing API: %s",
                index,
                len(metadata_list),
                metadata.api_name,
            )

            self.execute_api(metadata)

        self._logger.info(
            "Completed schedule '%s'.",
            schedule,
        )

    def execute_group(
        self,
        execution_group: str,
        schedule: str,
    ) -> None:
        """
        Executes all enabled APIs for the specified execution
        group and schedule.

        Parameters
        ----------
        execution_group
            Metadata execution group.

        schedule
            Schedule identifier.
        """

        self.execute_schedule(
            schedule=schedule,
            execution_group=execution_group,
        )