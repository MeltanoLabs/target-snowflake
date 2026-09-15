# Copyright (C) 2026 Meltano.

"""Snowflake target sink class for Snowpipe Streaming ingestion."""

from __future__ import annotations

import sys
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
    from snowflake.ingest.streaming import (  # type: ignore[import-not-found]
        StreamingIngestChannel,
        StreamingIngestClient,
    )

MISSING_DEPENDENCY_MESSAGE = (
    "ingestion_method: snowpipe_streaming requires the 'snowpipe-streaming' package. "
    "Install it with: pip install 'meltanolabs-target-snowflake[snowpipe]'"
)


class SnowpipeStreamingSink(SQLSink[SnowflakeConnector]):
    """Snowflake target sink that ingests rows directly via Snowpipe Streaming.

    Unlike `SnowflakeSink` (which buffers records to local batch files and loads
    them with COPY INTO/MERGE INTO), this sink writes each record directly to an
    open Snowpipe Streaming channel as it arrives -- there is no local buffering,
    file staging, or warehouse involved. Only append-only semantics are supported;
    `TargetSnowflake._validate_config` rejects incompatible `load_method`/
    `hard_delete` combinations before any sink is created.
    """

    connector_class = SnowflakeConnector

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize Snowpipe Streaming Sink."""
        self._streaming_client: StreamingIngestClient | None = None
        self._channel: StreamingIngestChannel | None = None
        super().__init__(*args, **kwargs)

    # The following four members mirror SnowflakeSink's identifier-formatting
    # overrides. They're duplicated rather than shared via a common base class,
    # since they're the only overlap between the two sink types.

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

        try:
            from snowflake.ingest.streaming import (  # noqa: PLC0415
                StreamingIngestClient,  # type: ignore[import-not-found]
            )
        except ImportError as e:
            raise ImportError(MISSING_DEPENDENCY_MESSAGE) from e

        # A unique client/channel name per sink instance avoids collisions with a
        # prior (possibly still-open, see target_base.py archived-sink lifecycle)
        # sink instance for the same stream, mirroring the uuid-per-instance pattern
        # already used for file formats in SnowflakeSink._get_file_format_name.
        self._streaming_client = StreamingIngestClient.from_table(
            client_name=f"target-snowflake-{self.stream_name}-{uuid4()}",
            db_name=self.database_name,  # ty: ignore[invalid-argument-type]
            schema_name=self.schema_name,  # ty: ignore[invalid-argument-type]
            table_name=self.table_name,
            properties=self.connector.get_streaming_client_properties(),
        )
        assert self._streaming_client is not None  # noqa: S101 -- just assigned above
        channel, _status = self._streaming_client.open_channel(
            channel_name=f"{self.stream_name}-{uuid4()}",
        )
        self._channel = channel

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
        assert self._channel is not None  # noqa: S101 -- set in setup(), which always runs first
        self._channel.append_row(conformed_record)

    @override
    def process_batch(self, context: dict) -> None:
        """No-op: `process_record` already wrote each row directly to the channel.

        Args:
            context: Stream partition or context dictionary (unused).
        """
        return

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
        """Close the channel and client opened in `setup()`."""
        self.record_counter_metric.exit()
        if self._channel is not None:
            self._channel.close()
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
