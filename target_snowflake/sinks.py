"""Snowflake target sink class, which handles writing streams."""

from __future__ import annotations

from typing import Optional, Sequence, Tuple, cast
from uuid import uuid4

import snowflake.sqlalchemy.custom_types as sct
import sqlalchemy
from singer_sdk import typing as th
from singer_sdk.helpers._batch import BaseBatchFileEncoding
from singer_sdk.sinks import SQLConnector, SQLSink
from snowflake.sqlalchemy import URL
from sqlalchemy.sql import text


class SnowflakeConnector(SQLConnector):
    """The connector for Snowflake.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for Snowflake.

        Args:
            config: The configuration for the connector.
        """
        params = {
            "account": config["account"],
            "user": config["user"],
            "password": config["password"],
            "schema": config["schema"],
        }

        for option in ["database", "warehouse", "role"]:
            if config.get(option):
                params[option] = config.get(option)

        return URL(**params)

    # Overridden here as Snowflake ALTER syntax for columns is different than that implemented in the SDK
    # https://docs.snowflake.com/en/sql-reference/sql/alter-table-column.html
    def _adapt_column_type(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.

        Args:
            full_table_name: The target table name.
            column_name: The target column name.
            sql_type: The new SQLAlchemy type.

        Raises:
            NotImplementedError: if altering columns is not supported.
        """
        current_type: sqlalchemy.types.TypeEngine = self._get_column_type(
            full_table_name, column_name
        )

        # Check if the existing column type and the sql type are the same
        if str(sql_type) == str(current_type):
            # The current column and sql type are the same
            # Nothing to do
            return

        # Not the same type, generic type or compatible types
        # calling merge_sql_types for assistnace
        compatible_sql_type = self.merge_sql_types([current_type, sql_type])

        if str(compatible_sql_type) == str(current_type):
            # Nothing to do
            return

        if not self.allow_column_alter:
            raise NotImplementedError(
                "Altering columns is not supported. "
                f"Could not convert column '{full_table_name}.{column_name}' "
                f"from '{current_type}' to '{compatible_sql_type}'."
            )

        self.connection.execute(
            sqlalchemy.DDL(
                "ALTER TABLE %(table)s ALTER COLUMN %(col_name)s SET DATA TYPE %(col_type)s",
                {
                    "table": full_table_name,
                    "col_name": column_name,
                    "col_type": compatible_sql_type,
                },
            )
        )

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Return a JSON Schema representation of the provided type.

        By default will call `typing.to_sql_type()`.

        Developers may override this method to accept additional input argument types,
        to support non-standard types, or to provide custom typing logic.

        If overriding this method, developers should call the default implementation
        from the base class for all unhandled cases.

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        if th._jsonschema_type_check(jsonschema_type, ("string",)):
            datelike_type = th.get_datelike_property_type(jsonschema_type)
            if datelike_type:
                if datelike_type == "date-time":
                    return cast(sqlalchemy.types.TypeEngine, sct.TIMESTAMP_NTZ())
                if datelike_type in "time":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.TIME())
                if datelike_type == "date":
                    return cast(sqlalchemy.types.TypeEngine, sqlalchemy.types.DATE())

            # snowflake max and default varchar length
            # https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
            maxlength = jsonschema_type.get("maxLength", 16777216)
            return cast(
                sqlalchemy.types.TypeEngine, sqlalchemy.types.VARCHAR(maxlength)
            )

        if th._jsonschema_type_check(jsonschema_type, ("integer",)):
            return cast(sqlalchemy.types.TypeEngine, sct.NUMBER())
        if th._jsonschema_type_check(jsonschema_type, ("object",)):
            return cast(sqlalchemy.types.TypeEngine, sct.VARIANT())
        if th._jsonschema_type_check(jsonschema_type, ("array",)):
            return cast(sqlalchemy.types.TypeEngine, sct.VARIANT())

        # fall back on default implementation
        return th.to_sql_type(jsonschema_type)


class SnowflakeSink(SQLSink):
    """Snowflake target sink class."""

    connector_class = SnowflakeConnector

    @property
    def full_table_name(self) -> str:
        return self.table_name

    @property
    def table_name(self) -> str:
        """Returns the table name, with no schema or database part.

        Returns:
            The target table name.
        """
        table = super().table_name
        if table:
            return table.upper()

    @property
    def schema_name(self) -> Optional[str]:
        """Returns the schema name or `None` if using names with no schema part.

        Returns:
            The target schema name.
        """
        schema = self.config.get("schema")
        if schema:
            return schema.upper()

    def _get_put_statement(self, sync_id: str, file_uri: str) -> Tuple[text, dict]:
        """Get Snowflake PUT statement."""
        return (text(f"put '{file_uri}' '@~/target-snowflake/{sync_id}'"), {})

    def _get_merge_statement(self, table_name, sync_id, file_format):
        """Get Snowflake MERGE statement."""
        column_selections = [
            f"$1:{property_name}::{self.connector.to_sql_type(property_def)} as {property_name}"
            for property_name, property_def in self.schema["properties"].items()
        ]
        join_expr = " and ".join([f"d.{key} = s.{key}" for key in self.key_properties])
        matched_clause = ", ".join(
            [f"d.{col} = s.{col}" for col in self.schema["properties"].keys()]
        )
        not_matched_insert_cols = ", ".join(self.schema["properties"].keys())
        not_matched_insert_values = ", ".join(
            [f"s.{col}" for col in self.schema["properties"].keys()]
        )
        return (
            text(
                f'merge into "{table_name}" d using '
                + f"(select {', '.join(column_selections)} from '@~/target-snowflake/{sync_id}'"
                + f'(file_format => "{file_format}")) s '
                + f"on {join_expr} "
                + f"when matched then update set {matched_clause} "
                + f"when not matched then insert ({not_matched_insert_cols}) "
                + f"values ({not_matched_insert_values})"
            ),
            {},
        )

    def _get_file_format_statement(self, sync_id):
        return (
            text(
                f'create or replace file format "{sync_id}" '
                + "type = 'JSON' compression = 'GZIP'"
            ),
            {},
        )

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
        try:
            sync_id = f"{self.stream_name}-{uuid4()}"
            # PUT batches to remote stage
            for file_uri in files:
                put_statement, kwargs = self._get_put_statement(
                    sync_id=sync_id, file_uri=file_uri
                )
                self.connector.connection.execute(put_statement, **kwargs)
            # create schema
            self.connector.connection.execute(
                text(f'create schema if not exists "{self.schema_name}"')
            )
            self.connector.connection.execute(text(f'use schema "{self.schema_name}"'))
            # create file format
            file_format_statement, kwargs = self._get_file_format_statement(
                sync_id=sync_id
            )
            self.connector.connection.execute(file_format_statement, **kwargs)
            # merge into destination table
            merge_statement, kwargs = self._get_merge_statement(
                table_name=self.table_name,  # database and schema come from the Connection
                sync_id=sync_id,
                file_format=sync_id,
            )
            self.connector.prepare_table(
                full_table_name=self.table_name,  # database and schema come from the Connection
                schema=self.schema,
                primary_keys=self.key_properties,
                as_temp_table=False,
            )
            self.connector.connection.execute(merge_statement, **kwargs)
        finally:
            # clean up file format
            self.connector.connection.execute(
                text(f'drop file format if exists "{sync_id}"')
            )
            # clean up staged files
            self.connector.connection.execute(
                text(f"remove '@~/target-snowflake/{sync_id}/'")
            )
            # clean up local files
            # TODO
