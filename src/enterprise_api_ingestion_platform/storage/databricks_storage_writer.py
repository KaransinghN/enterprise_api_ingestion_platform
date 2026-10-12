# src/enterprise_api_ingestion_platform/storage/databricks_storage_writer.py
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any, Iterable
from uuid import uuid4

from pyspark.sql import SparkSession

from enterprise_api_ingestion_platform.logging.logger import get_logger
from enterprise_api_ingestion_platform.models.api_metadata import ApiMetadata
from enterprise_api_ingestion_platform.storage.storage_writer import StorageWriter

logger = get_logger(__name__)


def _get_dbutils() -> Any:
    """Load Databricks runtime utilities only when filesystem access is needed."""
    from databricks.sdk.runtime import dbutils

    return dbutils


class DatabricksManagedStorageWriter(StorageWriter):
    """
    Writes raw API responses to a Unity Catalog managed volume.

    Bronze Delta tables are created downstream by the Lakeflow Bronze
    pipeline. This writer deliberately preserves the source JSON so
    Bronze owns schema inference and schema evolution.
    """

    def __init__(
        self,
        spark: SparkSession,
        landing_catalog: str,
        landing_schema: str,
        landing_volume: str,
    ) -> None:
        self._spark = spark

        self._raw_volume_root = self._build_volume_root(
            landing_catalog=landing_catalog,
            landing_schema=landing_schema,
            landing_volume=landing_volume,
        )

    @property
    def raw_volume_root(self) -> str:
        """
        Returns the configured Unity Catalog Volume root.
        """

        return self._raw_volume_root

    def write(
        self,
        metadata: ApiMetadata,
        payload: Any,
    ) -> str:
        """
        Writes a buffered API response as a raw JSON file.

        Returns
        -------
        str
            Fully qualified path of the written JSON file.
        """

        target_file = self._target_file(metadata)

        serialized_payload = json.dumps(
            payload,
            ensure_ascii=False,
            default=str,
        )

        self._write_json(
            target_file=target_file,
            content=serialized_payload,
        )

        landing_file_size_bytes = self.get_file_size(
            file_path=target_file,
        )

        logger.info(
            "Raw landing write completed. "
            "api=%s target=%s size_bytes=%s",
            metadata.api_name,
            target_file,
            landing_file_size_bytes,
        )

        return target_file

    def write_stream(
        self,
        metadata: ApiMetadata,
        chunks: Iterable[bytes],
    ) -> str:
        """
        Writes a streamed JSON response as a raw JSON file.

        Returns
        -------
        str
            Fully qualified path of the written JSON file.
        """

        payload = b"".join(
            chunk
            for chunk in chunks
            if chunk
        )

        if not payload:
            return self.write(
                metadata=metadata,
                payload={},
            )

        try:
            decoded_payload = json.loads(
                payload.decode("utf-8"),
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "Streaming response is not valid UTF-8 JSON."
            ) from exc

        return self.write(
            metadata=metadata,
            payload=decoded_payload,
        )

    def get_file_size(
        self,
        file_path: str,
    ) -> int:
        """
        Returns the size of a previously written file in bytes.

        Unity Catalog Volume file metadata is obtained through
        Databricks file-system utilities.
        """

        file_info = _get_dbutils().fs.ls(
            file_path,
        )

        if not file_info:
            raise FileNotFoundError(
                f"Landing file was not found: {file_path}"
            )

        entry = file_info[0]

        if entry.isDir():
            raise IsADirectoryError(
                f"Landing path is a directory, not a file: {file_path}"
            )

        return int(entry.size)

    def _target_directory(
        self,
        metadata: ApiMetadata,
    ) -> str:
        """
        Builds the deterministic raw landing directory for an API.
        """

        api_id = self._sanitize_path_component(
            metadata.api_id,
        )

        return str(
            PurePosixPath(
                self.raw_volume_root,
                api_id,
            )
        )

    def _target_file(
        self,
        metadata: ApiMetadata,
    ) -> str:
        """
        Builds a unique JSON file path for one ingestion run.
        """

        timestamp = datetime.now(
            timezone.utc,
        ).strftime(
            "%Y%m%dT%H%M%S%fZ",
        )

        run_id = uuid4().hex

        return str(
            PurePosixPath(
                self._target_directory(metadata),
                f"{timestamp}_{run_id}.json",
            )
        )

    @staticmethod
    def _write_json(
        target_file: str,
        content: str,
    ) -> None:
        """
        Writes JSON directly as a file in the Unity Catalog Volume.
        """

        _get_dbutils().fs.put(
            target_file,
            content,
            True,
        )

    @staticmethod
    def _build_volume_root(
        landing_catalog: str,
        landing_schema: str,
        landing_volume: str,
    ) -> str:
        """
        Builds and validates the fully qualified Unity Catalog Volume path.
        """

        components = {
            "landing_catalog": landing_catalog,
            "landing_schema": landing_schema,
            "landing_volume": landing_volume,
        }

        missing = [
            name
            for name, value in components.items()
            if not value or not value.strip()
        ]

        if missing:
            raise ValueError(
                "Missing required landing volume configuration: "
                f"{', '.join(missing)}"
            )

        sanitized_components = [
            DatabricksManagedStorageWriter._sanitize_path_component(
                value,
            )
            for value in components.values()
        ]

        return str(
            PurePosixPath(
                "/Volumes",
                *sanitized_components,
            )
        )

    @staticmethod
    def _sanitize_path_component(
        value: str | None,
    ) -> str:
        """
        Prevents configuration or metadata values from escaping
        the expected Volume path.
        """

        if not value:
            raise ValueError(
                "Path component is required."
            )

        sanitized = value.strip()

        if (
            sanitized in {".", ".."}
            or "/" in sanitized
            or "\\" in sanitized
        ):
            raise ValueError(
                f"Invalid path component: {value!r}"
            )

        return sanitized