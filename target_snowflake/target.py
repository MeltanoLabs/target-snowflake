"""Snowflake target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_snowflake.sinks import SnowflakeSink
from singer_sdk.sinks import Sink

class TargetSnowflake(SQLTarget):
    """Target for Snowflake."""

    name = "target-snowflake"
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
            required=True,
            description="The password for your Snowflake user.",
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
    ).to_dict()

    default_sink_class = SnowflakeSink

    def get_sink(
        self,
        stream_name: str,
        *,
        record: dict | None = None,
        schema: dict | None = None,
        key_properties: list[str] | None = None,
    ) -> Sink:
        _ = record  # Custom implementations may use record in sink selection.
        if schema is None:
            self._assert_sink_exists(stream_name)
            return self._sinks_active[stream_name]

        existing_sink = self._sinks_active.get(stream_name, None)
        if not existing_sink:
            return self.add_sink(stream_name, schema, key_properties)

        # Diffing was not accounting for metadata columns added by the target.
        # The existing schema has the columns but the original from the SCHEMA
        # message does not.
        clean_existing_schema = existing_sink.schema.copy()
        if self.config.get("add_record_metadata", True):
            existing_props_copy = existing_sink.schema["properties"].copy()
            for col in {
                "_sdc_extracted_at",
                "_sdc_received_at",
                "_sdc_batched_at",
                "_sdc_deleted_at",
                "_sdc_sequence",
                "_sdc_table_version",
            }:
                existing_props_copy.pop(col, None)
            clean_existing_schema["properties"] = existing_props_copy
        if (
            clean_existing_schema != schema
            or existing_sink.key_properties != key_properties
        ):
            self.logger.info(
                "Schema or key properties for '%s' stream have changed. "
                "Initializing a new '%s' sink...",
                stream_name,
                stream_name,
            )
            self._sinks_to_clear.append(self._sinks_active.pop(stream_name))
            return self.add_sink(stream_name, schema, key_properties)

        return existing_sink

if __name__ == "__main__":
    TargetSnowflake.cli()
