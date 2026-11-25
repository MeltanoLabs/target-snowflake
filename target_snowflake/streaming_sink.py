# Copyright (C) 2026 Meltano.

"""Snowflake target sink class for Snowpipe Streaming ingestion."""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
import typing as t
from uuid import uuid4

from singer_sdk.helpers._typing import conform_record_data_types
from singer_sdk.helpers.conform import TypeConformanceLevel
from singer_sdk.sql.sink import SQLSink

from target_snowflake.connector import SnowflakeConnector

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


if t.TYPE_CHECKING:
    from collections.abc import MutableMapping

    from snowflake.ingest.streaming import (  # type: ignore[import-not-found]
        ErrorDetail,
        StreamingIngestClient,
        StreamingIngestElasticChannel,
        SuccessDetail,
    )

MISSING_DEPENDENCY_MESSAGE = (
    "ingestion_method: snowpipe_streaming requires the 'snowpipe-streaming' package. "
    "Install it with: pip install 'meltanolabs-target-snowflake[snowpipe]'"
)


def setup_streaming_sdk_logger(*, environ: MutableMapping[str, str], logger: logging.Logger):
    # The Rust core underlying this SDK initializes its own logger -- independent
    # of Python's `logging` -- the moment this module is imported, and defaults to
    # writing it to stdout. For a Singer target, stdout is reserved exclusively for
    # STATE messages read by the orchestrator, so anything else on it corrupts that
    # protocol channel. Must be set before the import below; setdefault() so an
    # operator can still override it (e.g. to a file) via their own env var.
    environ.setdefault("SS_LOG_TARGET", "stderr")

    match logger.getEffectiveLevel():
        case level if logging.NOTSET < level <= logging.DEBUG:
            sp_level = "debug"
        case level if level < logging.INFO:  # 'info' is the Sink's default, but it's a bit chatty for Snowpipe
            sp_level = "info"
        case _:
            sp_level = "warn"

    environ.setdefault("SS_LOG_LEVEL", sp_level)


def get_streaming_client(
    *,
    stream_name: str,
    table_name: str,
    schema_name: str,
    database_name: str,
    properties: dict[str, str],
) -> StreamingIngestClient:

    try:
        from snowflake.ingest.streaming import (  # noqa: PLC0415
            StreamingIngestClient,  # type: ignore[import-not-found]
        )
    except ImportError as e:
        raise ImportError(MISSING_DEPENDENCY_MESSAGE) from e

    # A unique client name per sink instance avoids collisions with a
    # prior (possibly still-open, see target_base.py archived-sink lifecycle)
    # sink instance for the same stream, mirroring the uuid-per-instance pattern
    # already used for file formats in SnowflakeSink._get_file_format_name.
    return StreamingIngestClient.from_table(
        client_name=f"target-snowflake-{stream_name}-{uuid4()}",
        db_name=database_name,
        schema_name=schema_name,
        table_name=table_name,
        properties=properties,
    )


class SnowpipeStreamingSink(SQLSink[SnowflakeConnector]):
    """Snowflake target sink that ingests rows directly via Snowpipe Streaming.

    Unlike `SnowflakeSink` (which buffers records to local batch files and loads
    them with COPY INTO/MERGE INTO), this sink writes each record directly to an
    open Snowpipe Streaming channel as it arrives -- there is no local buffering,
    file staging, or warehouse involved. `load_method: append-only` and
    `load_method: overwrite` (truncate via SQL, then stream) are both supported.
    `load_method: upsert` is only supported for streams with no key properties
    (checked per-stream in `setup()`, since Snowpipe Streaming has no MERGE
    capability); `TargetSnowflake._validate_config` rejects `hard_delete` and
    non-key-pair auth up front, before any sink is created.
    """

    connector_class = SnowflakeConnector

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize Snowpipe Streaming Sink."""
        self._streaming_client: StreamingIngestClient | None = None
        self._channel: StreamingIngestElasticChannel | None = None
        self._rows_appended = 0
        self._rows_acknowledged = 0
        self._append_error: Exception | None = None
        self._acknowledgements = threading.Condition()
        super().__init__(*args, **kwargs)

    # The following four members mirror SnowflakeSink's identifier-formatting
    # overrides. They're duplicated rather than shared via a common base class,
    # since they're the only overlap between the two sink types.

    @property
    def streaming_client(self) -> StreamingIngestClient:
        if self._streaming_client is None:
            # This needs to be called before we get a hold of the client
            setup_streaming_sdk_logger(logger=self.logger, environ=os.environ)

            self._streaming_client = get_streaming_client(
                stream_name=self.stream_name,
                table_name=self.table_name,
                schema_name=self.schema_name,  # type: ignore[arg-type] # ty: ignore[invalid-argument-type]
                database_name=self.database_name,  # type: ignore[arg-type] # ty: ignore[invalid-argument-type]
                properties=self.connector.get_streaming_client_properties(),
            )

        return self._streaming_client

    @override
    @property
    def schema_name(self) -> str | None:
        schema = super().schema_name or self.config.get("schema")
        return schema.upper() if schema else None

    @override
    @property
    def database_name(self) -> str | None:
        db = super().database_name or self.config.get("database")
        return db.upper() if db else None

    @override
    @property
    def table_name(self) -> str:
        if self.config.get("use_raw_stream_names", False):
            return self.conform_name(self.stream_name, "table").upper()

        return super().table_name.upper()

    @override
    def conform_name(
        self,
        name: str,
        object_type: str | None = None,
    ) -> str:
        if object_type and object_type != "column":
            return super().conform_name(name=name, object_type=object_type)
        return self.connector.format_identifier(name)

    @override
    def setup(self) -> None:
        """Prepare the schema/table via DDL, then open a Snowpipe Streaming channel.

        Snowpipe Streaming has no DDL capability of its own, so schema/table
        structure is still prepared through the regular SQLAlchemy connection,
        exactly as `SnowflakeSink.setup()` does.

        `TargetSnowflake.create_sink()` never instantiates this sink for a stream
        with `load_method: upsert` and key properties (it falls back to
        `SnowflakeSink` for just that stream instead, since Snowpipe Streaming has
        no MERGE capability) -- so that combination can't reach here.
        """
        if self.schema_name:
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

        if self.config.get("load_method", "upsert") == "overwrite":
            # TRUNCATE is plain DDL through the same SQLAlchemy connection used
            # for schema/table prep above -- Snowpipe Streaming's insert-only
            # limitation only rules out MERGE (load_method: upsert with key
            # properties), not this.
            self.logger.info("load_method=overwrite: truncating %s", self.full_table_name)
            self.connector.truncate_table(self.full_table_name)

        self._channel = self.streaming_client.get_elastic_channel()
        self._channel.set_success_handler(self._count_acknowledged_rows)
        self._channel.set_error_handler(self._keep_append_error)

    # The SDK calls both handlers on its own thread, and they must return at once.
    def _count_acknowledged_rows(self, detail: SuccessDetail) -> None:
        with self._acknowledgements:
            self._rows_acknowledged += len(detail.append_tokens)
            self._acknowledgements.notify()

    def _keep_append_error(self, detail: ErrorDetail) -> None:
        with self._acknowledgements:
            self._append_error = self._append_error or detail.error
            self._acknowledgements.notify()

    @override
    def process_record(self, record: dict, context: dict) -> None:
        """Conform and append a single row directly to the open channel.

        Unlike `SnowflakeSink` (which relies on `BatchSink`'s default `process_record`
        to buffer into `context["records"]`), this permanently writes the row before
        returning -- there is no batch to flush later in `process_batch`.

        Args:
            record: Individual record in the stream.
            context: Stream partition or context dictionary (unused).
        """
        conformed_record = conform_record_data_types(
            stream_name=self.stream_name,
            record=record,
            schema=self.schema,
            level=TypeConformanceLevel.RECURSIVE,
            logger=self.logger,
        )
        # `setup()` has already imported the package, after it set up the SDK logger.
        from snowflake.ingest.streaming import (  # noqa: PLC0415
            StreamingIngestError,  # type: ignore[import-not-found]
            StreamingIngestErrorCode,
        )

        assert self._channel is not None  # noqa: S101 -- set in setup(), which always runs first
        self._rows_appended += 1
        while True:
            try:
                self._channel.append_row(conformed_record, append_token=self._rows_appended)
            except StreamingIngestError as e:  # noqa: PERF203 -- the loop only repeats after an error
                # The SDK rejects an append while the inflight buffer of the channel is full, for
                # example while Snowflake throttles the channel. It keeps uploading the buffered
                # rows in the background, so wait for space and append the same row again.
                if e.error_code is not StreamingIngestErrorCode.RECEIVER_SATURATED:
                    raise
                self.logger.info("%s Retrying the append in 1 second.", e)
                time.sleep(1)
            else:
                return

    # The sink holds no rows, and a drain waits for the acknowledgement. Only a drain of all sinks
    # emits `STATE`, so drain then, and not at `batch_size_rows`.
    @override
    @property
    def is_full(self) -> bool:
        return False

    @override
    def process_batch(self, context: dict) -> None:
        """Append the rows of a `BATCH` file, then wait until Snowflake acknowledges every row.

        `process_record` already wrote each `RECORD` row directly to the channel, so
        only a context from `process_batch_files` holds rows. The channel uploads the
        rows in the background. Snowflake acknowledges a row once it stores the row
        durably, and makes it queryable some seconds later. The target emits `STATE`
        after a drain, so the rows must be stored durably first.

        Args:
            context: Stream partition or context dictionary.
        """
        for record in context.get("records", []):
            self.process_record(record, context)
        assert self._channel is not None  # noqa: S101 -- set in setup(), which always runs first
        self._channel.initiate_flush()
        with self._acknowledgements:
            self._acknowledgements.wait_for(
                lambda: self._append_error is not None or self._rows_acknowledged >= self._rows_appended,
            )
            if self._append_error is not None:
                raise self._append_error

    @override
    def activate_version(self, new_version: int) -> None:
        """Log and ignore: Snowpipe Streaming is insert-only, with no UPDATE/DELETE.

        Args:
            new_version: The version number to activate (unused).
        """
        self.logger.warning(
            "ACTIVATE_VERSION message received for stream '%s', but "
            "ingestion_method: snowpipe_streaming does not support it (the "
            "Snowpipe Streaming API is insert-only). Ignoring.",
            self.stream_name,
        )

    @override
    def clean_up(self) -> None:
        """Close the client opened in `setup()`, which also closes its elastic channel."""
        self._channel = None
        if self._streaming_client is not None:
            self._streaming_client.close()
            self._streaming_client = None

    # TODO: remove after https://github.com/meltano/sdk/issues/1819 is fixed
    @override
    def _singer_validate_message(self, record: dict) -> None:
        """Ensure record conforms to Singer Spec.

        Args:
            record: Record (after parsing, schema validations and transformations).

        Raises:
            MissingKeyPropertiesError: If record is missing one or more key properties.
        """
