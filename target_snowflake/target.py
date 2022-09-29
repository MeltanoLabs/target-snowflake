"""Snowflake target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_snowflake.sinks import SnowflakeSink


class TargetSnowflake(SQLTarget):
    """Sample target for Snowflake."""

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


if __name__ == "__main__":
    TargetSnowflake.cli()
