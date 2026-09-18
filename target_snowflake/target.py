# Copyright (C) 2026 Meltano.

"""Snowflake target class."""

from __future__ import annotations

import logging.config
import sys
import typing as t

import click
from singer_sdk import typing as th
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.helpers.capabilities import CapabilitiesEnum, PluginCapabilities
from singer_sdk.sql.target import SQLTarget

from target_snowflake.connector import (
    DEFAULT_TIMESTAMP_TYPE,
    SnowflakeAuthMethod,
    SnowflakeConnector,
    SnowflakeTimestampType,
)
from target_snowflake.initializer import initializer
from target_snowflake.sinks import SnowflakeSink
from target_snowflake.streaming_sink import SnowpipeStreamingSink

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    from singer_sdk.singerlib.types import KeyProperties
    from singer_sdk.sinks import Sink
    from singer_sdk.sql.sink import SQLSink

logging.config.dictConfig(
    {
        "version": 1,
        "disable_existing_loggers": False,
        "loggers": {"snowflake.connector": {"level": "WARNING"}},
    },
)


class TargetSnowflake(SQLTarget):
    """Target for Snowflake."""

    name = "target-snowflake"
    package_name = "meltanolabs_target_snowflake"

    # From https://docs.snowflake.com/en/user-guide/sqlalchemy.html#connection-parameters
    config_jsonschema = th.PropertiesList(
        th.Property(
            "user",
            th.StringType,
            required=True,
            description="The login name for your Snowflake user.",
        ),
        th.Property(
            "password",
            th.StringType,
            required=False,
            description="The password for your Snowflake user.",
        ),
        th.Property(
            "private_key",
            th.StringType,
            required=False,
            secret=True,
            description=(
                "The private key contents, in PEM or base64-encoding format. "
                "For KeyPair authentication either `private_key` or `private_key_path` "
                "must be provided."
            ),
        ),
        th.Property(
            "private_key_path",
            th.StringType,
            required=False,
            description=(
                "Path to file containing private key. For KeyPair authentication either "
                "private_key or private_key_path must be provided."
            ),
        ),
        th.Property(
            "private_key_passphrase",
            th.StringType,
            required=False,
            description="Passphrase to decrypt private key if encrypted.",
        ),
        th.Property(
            "account",
            th.StringType,
            required=True,
            description="Your account identifier. See [Account Identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier.html).",
        ),
        th.Property(
            "database",
            th.StringType,
            required=True,
            description="The initial database for the Snowflake session.",
        ),
        th.Property(
            "schema",
            th.StringType,
            description="The initial schema for the Snowflake session.",
        ),
        th.Property(
            "warehouse",
            th.StringType,
            description="The initial warehouse for the session.",
        ),
        th.Property(
            "role",
            th.StringType,
            description="The initial role for the session.",
        ),
        th.Property(
            "add_record_metadata",
            th.BooleanType,
            default=True,
            description="Whether to add metadata columns.",
        ),
        th.Property(
            "clean_up_batch_files",
            th.BooleanType,
            default=True,
            description="Whether to remove batch files after processing.",
        ),
        th.Property(
            "use_browser_authentication",
            th.BooleanType,
            default=False,
            description="Whether to use SSO authentication using an external browser.",
        ),
        th.Property(
            "oauth_access_token",
            th.StringType,
            required=False,
            secret=True,
            description="OAuth access token for authentication. Token should be valid and not expired.",
        ),
        th.Property(
            "timestamp_type",
            th.StringType,
            allowed_values=[t.name for t in SnowflakeTimestampType],
            default=DEFAULT_TIMESTAMP_TYPE.name,
            description="Snowflake timestamp type to use for date-time properties.",
        ),
        th.Property(
            "uuid_format",
            th.StringType,
            allowed_values=["native", "string"],
            default="native",
            description=(
                "Snowflake column type/value format for `format: uuid` string properties. "
                "'native' (default) uses SQLAlchemy's native UUID type, which compiles to CHAR(32) on Snowflake "
                "and strips dashes from the value. "
                "'string' uses a STRING(36) column and writes the value as-is, preserving dashes."
            ),
        ),
        th.Property(
            "quoted_identifiers_ignore_case",
            th.BooleanType,
            default=True,
            description=(
                "Whether letters in double-quoted object identifiers are stored and resolved as uppercase letters."
            ),
        ),
        th.Property(
            "normalise_casing",
            th.BooleanType,
            default=False,
            description="Whether to normalise identifiers into snake_case.",
        ),
        th.Property(
            "use_raw_stream_names",
            th.BooleanType,
            default=False,
            description=(
                "Whether to use raw stream names as table names instead of informal Singer convention as last "
                "hyphen-separated part of stream name."
            ),
        ),
        th.Property(
            "load_method",
            th.StringType,
            allowed_values=["append-only", "upsert", "overwrite"],
            default="upsert",
            description=(
                "Controls how records are written to the destination table. "
                "'append-only' always uses COPY INTO, writing every input record as a new row "
                "regardless of key properties, without merging or truncating. "
                "'upsert' (default) uses MERGE INTO, matching on key properties to update "
                "existing rows and insert new ones. "
                "'overwrite' truncates the table then uses COPY INTO, replacing all existing "
                "rows — faster for initial loads but destructive if run on a populated table."
            ),
        ),
        th.Property(
            "ingestion_method",
            th.StringType,
            allowed_values=["file_staging", "snowpipe_streaming"],
            default="file_staging",
            description=(
                "How records are loaded into Snowflake. "
                "'file_staging' (default) stages local batch files and loads them with "
                "COPY INTO/MERGE INTO, per `load_method`; this requires a running warehouse. "
                "'snowpipe_streaming' ingests rows directly via the Snowpipe Streaming API, "
                "with no warehouse required and billing based on data ingested. Only "
                "key-pair authentication is supported in this mode, and `hard_delete` is "
                "not supported. `load_method: overwrite` truncates via a regular SQL "
                "connection before streaming begins, same as with `file_staging`. "
                "`load_method: upsert` needs MERGE, which streaming doesn't support, so "
                "any individual stream with key properties falls back to `file_staging` "
                "for that stream only (with a logged warning) rather than failing the "
                "whole sync -- other streams keep streaming."
            ),
        ),
    ).to_dict()

    default_sink_class = SnowflakeSink

    #: A list of capabilities supported by this target.
    capabilities: t.ClassVar[list[CapabilitiesEnum]] = [
        *SQLTarget.capabilities,
        PluginCapabilities.BATCH,
    ]

    @property
    @override
    def target_connector(self) -> SnowflakeConnector:
        """The connector object, narrowed from `SQLConnector` to `SnowflakeConnector`.

        `default_sink_class.connector_class` is always `SnowflakeConnector` for this
        target, but the base `SQLTarget.target_connector` types it generically --
        narrowing it here lets `create_sink()` pass it directly to `SnowflakeSink`.

        Returns:
            The connector object.
        """
        connector = super().target_connector
        assert isinstance(connector, SnowflakeConnector)  # noqa: S101
        return connector

    @override
    def _validate_config(self, *, raise_errors: bool = True) -> list[str]:
        """Validate config, including cross-setting constraints for `ingestion_method`.

        Args:
            raise_errors: Flag to throw an exception if any validation errors are found.

        Returns:
            A list of validation errors.

        Raises:
            ConfigValidationError: If raise_errors is True and validation fails.
        """
        errors = super()._validate_config(raise_errors=False)

        if self.config.get("ingestion_method") == "snowpipe_streaming":
            if self.config.get("hard_delete"):
                errors.append(
                    "ingestion_method: snowpipe_streaming does not support hard_delete.",
                )
            if self._streaming_auth_method() != SnowflakeAuthMethod.KEY_PAIR:
                errors.append(
                    "ingestion_method: snowpipe_streaming requires key-pair authentication "
                    "(private_key or private_key_path).",
                )

        if errors and raise_errors:
            summary = "Config validation failed"
            raise ConfigValidationError(summary, errors=errors)

        return errors

    def _streaming_auth_method(self) -> SnowflakeAuthMethod | None:
        """Best-effort auth method lookup for config validation, without a live connection."""
        try:
            return SnowflakeConnector(config=self.config).auth_method
        except ConfigValidationError:
            return None

    @override
    def get_sink_class(self, stream_name: str) -> type[SQLSink]:
        """Return the sink class to use for a given stream.

        Args:
            stream_name: Name of the stream.

        Returns:
            `SnowpipeStreamingSink` when `ingestion_method: snowpipe_streaming` is
            configured, otherwise the default file-staging `SnowflakeSink`.
        """
        if self.config.get("ingestion_method") == "snowpipe_streaming":
            return SnowpipeStreamingSink

        return super().get_sink_class(stream_name)

    @override
    def create_sink(
        self,
        *,
        stream_name: str,
        schema: dict,
        key_properties: KeyProperties | None = None,
    ) -> Sink:
        """Create a sink, falling back to file-staging for one stream if it needs MERGE.

        `get_sink_class()` alone can't make this call: it only receives
        `stream_name`, but whether Snowpipe Streaming can handle a stream depends on
        its key properties too, which aren't known until here. This lets a fleet of
        streams use `ingestion_method: snowpipe_streaming` even if one or two of them
        have key properties and `load_method: upsert` (which needs MERGE, which
        Snowpipe Streaming doesn't support) -- instead of failing the whole sync.

        Args:
            stream_name: Name of the stream.
            schema: Schema of the stream.
            key_properties: The primary key columns.

        Returns:
            A new sink instance for the stream.
        """
        if (
            self.config.get("ingestion_method") == "snowpipe_streaming"
            and self.config.get("load_method", "upsert") == "upsert"
            and key_properties
        ):
            self.logger.warning(
                "Stream '%s' has key properties %s. ingestion_method: snowpipe_streaming "
                "has no MERGE capability, so load_method: upsert isn't supported for it. "
                "Falling back to ingestion_method: file_staging for this stream (it "
                "will use a warehouse).",
                stream_name,
                key_properties,
            )
            return SnowflakeSink(
                target=self,
                stream_name=stream_name,
                schema=schema,
                key_properties=list(key_properties),
                connector=self.target_connector,
            )

        return super().create_sink(stream_name=stream_name, schema=schema, key_properties=key_properties)

    @classmethod
    def cb_initialize(
        cls: type[TargetSnowflake],
        ctx: click.Context,
        param: click.Option,  # noqa: ARG003
        value: bool,  # noqa: FBT001
    ) -> None:
        if value:
            initializer()
            ctx.exit()

    @override
    @classmethod
    def get_singer_command(cls: type[TargetSnowflake]) -> click.Command:
        """Execute standard CLI handler for targets.

        Returns:
            A click.Command object.
        """
        command = super().get_singer_command()
        command.params.extend(
            [
                click.Option(
                    ["--initialize"],
                    is_flag=True,
                    help="Interactive Snowflake account initialization.",
                    callback=cls.cb_initialize,
                    expose_value=False,
                ),
            ],
        )

        return command


if __name__ == "__main__":
    TargetSnowflake.cli()
