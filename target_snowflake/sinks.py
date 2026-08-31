# Copyright (C) 2026 Meltano.

"""Snowflake target sink class, which handles writing streams."""

from __future__ import annotations

import os
import typing as t
from urllib.parse import urlparse
from uuid import uuid4

from singer_sdk.contrib.batch_encoder_jsonl import JSONLinesBatcher
from singer_sdk.helpers._batch import (
    BaseBatchFileEncoding,
    BatchConfig,
    BatchFileFormat,
)
from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.helpers.conform import TypeConformanceLevel
from singer_sdk.sql.sink import SQLSink

from target_snowflake.connector import SnowflakeConnector

if t.TYPE_CHECKING:
    from singer_sdk import Target
    from singer_sdk.sql.connector import FullyQualifiedName

DEFAULT_BATCH_CONFIG = {
    "encoding": {"format": "jsonl", "compression": "gzip"},
    "storage": {"root": "file://"},
}


class SnowflakeSink(SQLSink[SnowflakeConnector]):
    """Snowflake target sink class."""

    connector_class = SnowflakeConnector

    def __init__(
        self,
        target: Target,
        stream_name: str,
        schema: dict,
        key_properties: list[str] | None,
        connector: SnowflakeConnector | None = None,
    ) -> None:
        """Initialize Snowflake Sink."""
        self.target = target
        self._file_formats: dict[str, str] = {}
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
        if self.config.get("use_raw_stream_names", False):
            return self.conform_name(self.stream_name, "table").upper()

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

        self.connector.invalidate_table_cache(self.full_table_name)

        try:
            self.connector.prepare_table(
                full_table_name=self.full_table_name,
                schema=self.conform_schema(self.schema),
                primary_keys=self.key_properties,
                as_temp_table=False,
            )
        except Exception:
            self.logger.exception("Error creating %s %s", self.full_table_name, self.conform_schema(self.schema))
            raise

        self.connector.invalidate_table_cache(self.full_table_name)

    def _get_file_format_name(self, file_type: str) -> str:
        """Get (creating on first use) the file format name for a given file type.

        Args:
            file_type: The Snowflake file format type, e.g. ``"JSON"`` or ``"PARQUET"``.

        Returns:
            The name of the file format, unique to this sink instance and file type.
        """
        if file_type not in self._file_formats:
            # Use a unique name per sink instance. A static, stream-derived name is
            # shared across overlapping sinks (e.g. when a SCHEMA message archives
            # the old sink and creates a new one) and across concurrent target
            # processes loading the same stream into the same schema. In those
            # cases one owner's CREATE OR REPLACE / DROP FILE FORMAT clobbers a
            # file format another sink is actively using, causing "File format
            # ... does not exist" during COPY/MERGE. The uuid keeps each sink's
            # file format isolated while still creating/dropping it only once per
            # sink (not per batch) -- and only for file types actually used
            # during this sync, since a stream's batches are all of one encoding.
            file_format = f'{self.database_name}.{self.schema_name}."tf-{self.stream_name}-{uuid4()}"'
            self.connector.create_file_format(file_format=file_format, file_type=file_type)
            self._file_formats[file_type] = file_format

        return self._file_formats[file_type]

    def clean_up(self) -> None:
        # The base Sink.clean_up() calls this too, but overriding here (rather than
        # super().clean_up()) drops it entirely -- force-flushing whatever record_count
        # hasn't been logged yet (the Counter only auto-logs periodically, see
        # singer_sdk.metrics.Counter) so it isn't silently lost when the sink shuts down.
        self.record_counter_metric.exit()
        for file_format in self._file_formats.values():
            self.connector.drop_file_format(file_format=file_format)

    def conform_name(
        self,
        name: str,
        object_type: str | None = None,
    ) -> str:
        if object_type and object_type != "column":
            return super().conform_name(name=name, object_type=object_type)
        return self.connector.format_identifier(name)

    def bulk_insert_records(
        self,
        full_table_name: str | FullyQualifiedName,
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
                level=TypeConformanceLevel.RECURSIVE,
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
    def batch_config(self) -> BatchConfig:
        """Get batch configuration.

        Returns:
            A frozen (read-only) config dictionary map.
        """
        raw = self.config.get("batch_config", DEFAULT_BATCH_CONFIG)

        if self.batch_size_rows:
            raw["batch_size"] = raw.get("batch_size", self.batch_size_rows)

        return BatchConfig.from_dict(raw)

    def insert_batch_files_via_internal_stage(
        self,
        full_table_name: str | FullyQualifiedName,
        files: t.Sequence[str],
        file_type: str = "JSON",
    ) -> int:
        """Process a batch file with the given batch context.

        Args:
            full_table_name: The target table name.
            files: The batch files to process.
            file_type: The Snowflake file format type to stage/load with, e.g.
                ``"JSON"`` or ``"PARQUET"``. The underlying file format is
                created lazily and reused for the rest of this sink's sync.
        """
        file_format = self._get_file_format_name(file_type)
        self.logger.info("Processing batch of files.")
        sync_id = f"{self.stream_name}-{uuid4()}"
        try:
            self.connector.put_batches_to_stage(sync_id=sync_id, files=files)
            if self.key_properties:
                # merge into destination table
                record_count = self.connector.merge_from_stage(
                    full_table_name=full_table_name,
                    schema=self.schema,
                    sync_id=sync_id,
                    file_format=file_format,
                    key_properties=self.key_properties,
                )

            else:
                record_count = self.connector.copy_from_stage(
                    full_table_name=full_table_name,
                    schema=self.schema,
                    sync_id=sync_id,
                    file_format=file_format,
                )

        finally:
            self.logger.debug("Cleaning up after batch processing")
            self.connector.remove_staged_files(sync_id=sync_id)
            # clean up local files
            if self.config.get("clean_up_batch_files"):
                for file_url in files:
                    file_path = urlparse(file_url).path
                    if os.path.exists(file_path):  # noqa: PTH110
                        os.remove(file_path)  # noqa: PTH107

        return record_count

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
            record_count = self.insert_batch_files_via_internal_stage(
                full_table_name=self.full_table_name,
                files=files,
            )
        else:
            msg = f"Unsupported batch file encoding: {encoding.format}"
            raise NotImplementedError(
                msg,
            )

        with self.record_counter_metric as counter:
            counter.increment(record_count)

    # TODO: remove after https://github.com/meltano/sdk/issues/1819 is fixed
    def _singer_validate_message(self, record: dict) -> None:
        """Ensure record conforms to Singer Spec.

        Args:
            record: Record (after parsing, schema validations and transformations).

        Raises:
            MissingKeyPropertiesError: If record is missing one or more key properties.
        """
