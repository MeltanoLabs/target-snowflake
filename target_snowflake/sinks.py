"""Snowflake target sink class, which handles writing streams."""

from __future__ import annotations

from singer_sdk.sinks import SQLConnector, SQLSink


class SnowflakeConnector(SQLConnector):
    """The connector for Snowflake.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = False  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for Snowflake.

        Args:
            config: The configuration for the connector.
        """
        return super().get_sqlalchemy_url(config)


class SnowflakeSink(SQLSink):
    """Snowflake target sink class."""

    connector_class = SnowflakeConnector
