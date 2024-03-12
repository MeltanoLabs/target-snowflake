"""Snowflake target sink class, which handles writing streams."""

from __future__ import annotations

import os
import typing as t
from urllib.parse import urlparse
from uuid import uuid4

from singer_sdk.batch import JSONLinesBatcher
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
    BatchFileFormat,
)
from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.sinks import SQLSink
from snowflake.sqlalchemy.base import SnowflakeIdentifierPreparer
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

from target_snowflake.connector import SnowflakeConnector

if t.TYPE_CHECKING:
    from singer_sdk import PluginBase, SQLConnector

DEFAULT_BATCH_CONFIG = {
    "encoding": {"format": "jsonl", "compression": "gzip"},
    "storage": {"root": "file://"},
}


class SnowflakeSink(SQLSink):
    """Snowflake target sink class."""

    connector_class = SnowflakeConnector

    def __init__(  # noqa: PLR0913
        self,
        target: PluginBase,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
        connector: SQLConnector | None = None,
    ) -> None:
        """Initialize Snowflake Sink."""
        self.target = target
        super().__init__(
            target=target,
            stream_name=stream_name,
            schema=schema,
            key_properties=key_properties,
            connector=connector,
        )

    @property
    def schema_name(self) -> str | None:
        schema = super().schema_name or self.config.get("schema")
        return schema.upper() if schema else None

    @property
    def database_name(self) -> str | None:
        db = super().database_name or self.config.get("database")
        return db.upper() if db else None

    @property
    def table_name(self) -> str:
        return super().table_name.upper()

    def setup(self) -> None:
        """Set up Sink.

        This method is called on Sink creation, and creates the required Schema and
        Table entities in the target database.
        """
        if self.schema_name:
            # Needed to conform schema name
            self.connector.prepare_schema(
                self.conform_name(self.schema_name, object_type="schema"),
            )
        try:
            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=self.conform_schema(self.schema),
                primary_keys=self.key_properties,
                as_temp_table=False,
            )
        except Exception:
            (
                self.logger.exception(
                    "Error creating %s %s",
                    self.full_table_name,
                    self.conform_schema(self.schema),
                ),
            )
            raise

    def conform_name(
        self,
        name: str,
        object_type: str | None = None,
    ) -> str:
        if object_type and object_type != "column":
            return super().conform_name(name=name, object_type=object_type)
        formatter = SnowflakeIdentifierPreparer(SnowflakeDialect())
        if '"' not in formatter.format_collation(name.lower()):
            name = name.lower()
        return name

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: t.Iterable[dict[str, t.Any]],
    ) -> int | None:
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        # prepare records for serialization
        processed_records = (
            conform_record_data_types(
                stream_name=self.stream_name,
                record=rcd,
                schema=schema,
                level="RECURSIVE",
                logger=self.logger,
            )
            for rcd in records
        )

        # serialize to batch files and upload
        # TODO: support other batchers
        batcher = JSONLinesBatcher(
            tap_name=self.target.name,
            stream_name=self.stream_name,
            batch_config=self.batch_config,
        )
        batches = batcher.get_batches(records=processed_records)
        for files in batches:
            self.insert_batch_files_via_internal_stage(
                full_table_name=full_table_name,
                files=files,
            )
        # if records list, we can quickly return record count.
        return len(records) if isinstance(records, list) else None

    # Custom methods to process batch files

    @property
    def batch_config(self) -> BatchConfig | None:
        """Get batch configuration.

        Returns:
            A frozen (read-only) config dictionary map.
        """
        raw = self.config.get("batch_config", DEFAULT_BATCH_CONFIG)
        return BatchConfig.from_dict(raw)

    def insert_batch_files_via_internal_stage(
        self,
        full_table_name: str,
        files: t.Sequence[str],
    ) -> None:
        """Process a batch file with the given batch context.

        Args:
            encoding: The batch file encoding.
            files: The batch files to process.
        """
        self.logger.info("Processing batch of files.")
        try:
            sync_id = f"{self.stream_name}-{uuid4()}"
            file_format = f'{self.database_name}.{self.schema_name}."{sync_id}"'
            self.connector.put_batches_to_stage(sync_id=sync_id, files=files)
            self.connector.prepare_schema(
                self.conform_name(self.schema_name, object_type="schema"),  # type: ignore[arg-type]
            )
            self.connector.create_file_format(file_format=file_format)

            if self.key_properties:
                # merge into destination table
                self.connector.merge_from_stage(
                    full_table_name=full_table_name,
                    schema=self.schema,
                    sync_id=sync_id,
                    file_format=file_format,
                    key_properties=self.key_properties,
                )

            else:
                self.connector.copy_from_stage(
                    full_table_name=full_table_name,
                    schema=self.schema,
                    sync_id=sync_id,
                    file_format=file_format,
                )

        finally:
            self.logger.debug("Cleaning up after batch processing")
            self.connector.drop_file_format(file_format=file_format)
            self.connector.remove_staged_files(sync_id=sync_id)
            # clean up local files
            if self.config.get("clean_up_batch_files"):
                for file_url in files:
                    file_path = urlparse(file_url).path
                    if os.path.exists(file_path):  # noqa: PTH110
                        os.remove(file_path)  # noqa: PTH107

    def process_batch_files(
        self,
        encoding: BaseBatchFileEncoding,
        files: t.Sequence[str],
    ) -> None:
        """Process a batch file with the given batch context.

        Args:
            encoding: The batch file encoding.
            files: The batch files to process.

        Raises:
            NotImplementedError: If the batch file encoding is not supported.
        """
        if encoding.format == BatchFileFormat.JSONL:
            self.insert_batch_files_via_internal_stage(
                full_table_name=self.full_table_name,
                files=files,
            )
        else:
            msg = f"Unsupported batch file encoding: {encoding.format}"
            raise NotImplementedError(
                msg,
            )

    # TODO: remove after https://github.com/meltano/sdk/issues/1819 is fixed
    def _singer_validate_message(self, record: dict) -> None:
        """Ensure record conforms to Singer Spec.

        Args:
            record: Record (after parsing, schema validations and transformations).

        Raises:
            MissingKeyPropertiesError: If record is missing one or more key properties.
        """
