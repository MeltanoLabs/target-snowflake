"""Snowflake target sink class, which handles writing streams."""
from __future__ import annotations

import gzip
import json
import os
from operator import contains, eq, truth
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Tuple, cast
from urllib.parse import urlparse
from uuid import uuid4

import snowflake.sqlalchemy.custom_types as sct
import sqlalchemy
from singer_sdk import typing as th
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchConfig
from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.sinks import SQLConnector, SQLSink
from singer_sdk.streams.core import lazy_chunked_generator
from snowflake.sqlalchemy import URL
from sqlalchemy.sql import text


class SnowflakeSink(SQLSink):
    """Snowflake target sink class."""

    connector_class = SnowflakeConnector

    @property
    def schema_name(self) -> Optional[str]:
        schema = super().schema_name or self.config.get("schema")
        return schema.upper() if schema else None

    @property
    def database_name(self) -> Optional[str]:
        db = super().database_name or self.config.get("database")
        return db.upper() if db else None

    @property
    def table_name(self) -> str:
        return super().table_name.upper()

    def process_batch(self, context: dict) -> None:
        """Process a batch with the given batch context.

        Writes a batch to the SQL target. Developers may override this method
        in order to provide a more efficient upload/upsert process.

        Args:
            context: Stream partition or context dictionary.
        """
        # If duplicates are merged, these can be tracked via
        # :meth:`~singer_sdk.Sink.tally_duplicate_merged()`.
        # serialize to batch files
        encoding, files = self.create_batch_files(
            batch_config=self.batch_config, records=context["records"]
        )
        self.process_batch_files(encoding=encoding, files=files)
        # if records list, we can quickly return record count.
        # return len(records) if isinstance(records, list) else None

    def process_batch_files(
        self,
        encoding: BaseBatchFileEncoding,
        files: Sequence[str],
    ) -> None:
        """Process a batch file with the given batch context.

        Args:
            encoding: The batch file encoding.
            files: The batch files to process.
        """
        self.logger.info(f"Processing batch of {len(files)} files")
        self.connector.load_records_from_files(
            full_table_name=self.full_table_name,
            file_encoding=encoding,
            file_paths=files,
            schema=self.schema,
            primary_keys=self.primary_keys,
        )
        try:
            sync_id = f"{self.stream_name}-{uuid4()}"
            file_format = f'{self.database_name}.{self.schema_name}."{sync_id}"'

            self.connector.create_schema(f"{self.database_name}.{self.schema_name}")
            self._create_json_file_format(file_format)
            for file_uri in files:
                self._upload_file_to_stage(sync_id=sync_id, file_uri=file_uri)

            # merge into destination table
            merge_statement, kwargs = self.connector._get_merge_statement(
                full_table_name=self.full_table_name,
                schema=self.conform_schema(self.schema),
                sync_id=sync_id,
                file_format=file_format,
            )
            self.logger.info(f"Merging batch with SQL: {merge_statement}")
            self.connector.connection.execute(merge_statement, **kwargs)
        finally:
            self.logger.debug("Cleaning up after batch processing")
            self._drop_json_file_format(file_format)
            self._delete_files_from_stage(sync_id)

            if self.config.get("clean_up_batch_files"):
                # clean up local files
                for file_url in files:
                    file_path = urlparse(file_url).path
                    if os.path.exists(file_path):
                        os.remove(file_path)

    def bulk_insert_records(
        self,
        full_table_name: str,
        schema: dict,
        records: Iterable[Dict[str, Any]],
    ) -> Optional[int]:
        """Deprecated in favor of `process_batch()`.

        Should not be invoked.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
        raise NotImplementedError( # This point should not be reached.
            "Deprecated in favor of `process_batch()"
        )

    # Copied and modified from `singer_sdk.streams.core.Stream`
    def create_batch_files(
        self,
        batch_config: BatchConfig,
        records: Iterable[Dict[str, Any]],
    ) -> tuple[BaseBatchFileEncoding, list[str]]:
        """This method creates batch files from a record stream.

        Args:
            batch_config: Batch config for this stream.
            context: Stream partition or context dictionary.

        Yields:
            A tuple of (encoding, manifest).
        """
        sync_id = f"target-snowflake--{self.stream_name}-{uuid4()}"
        prefix = batch_config.storage.prefix or ""
        # prepare records for serialization
        processed_records = (
            conform_record_data_types(
                stream_name=self.stream_name,
                record=rcd,
                schema=schema,
                logger=self.logger,
            )
            for rcd in records
        )
        file_urls = []
        for i, chunk in enumerate(
            lazy_chunked_generator(
                records,
                self.MAX_SIZE_DEFAULT,  # no point being larger than the sink max size, as thats the max number of records that will arrive
            ),
            start=1,
        ):
            filename = f"{prefix}{sync_id}-{i}.json.gz"
            with batch_config.storage.fs() as fs:
                with fs.open(filename, "wb") as f:
                    # TODO: Determine compression from config.
                    with gzip.GzipFile(fileobj=f, mode="wb") as gz:
                        gz.writelines(
                            (json.dumps(record) + "\n").encode() for record in chunk
                        )
                file_urls.append(fs.geturl(filename))

        return batch_config.encoding, file_urls
