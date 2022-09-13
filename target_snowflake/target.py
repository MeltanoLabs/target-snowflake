"""Snowflake target class."""

from __future__ import annotations

from singer_sdk.target_base import SQLTarget
from singer_sdk import typing as th

from target_snowflake.sinks import (
    SnowflakeSink,
)


class TargetSnowflake(SQLTarget):
    """Sample target for Snowflake."""

    name = "target-snowflake"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            description="SQLAlchemy connection string",
        ),
    ).to_dict()

    default_sink_class = SnowflakeSink
