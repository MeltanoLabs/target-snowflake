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


class TypeMap:
    def __init__(self, operator, map_value, match_value=None):
        self.operator = operator
        self.map_value = map_value
        self.match_value = match_value

    def match(self, compare_value):
        try:
            if self.match_value:
                return self.operator(compare_value, self.match_value)
            return self.operator(compare_value)
        except TypeError:
            return False


def evaluate_typemaps(type_maps, compare_value, unmatched_value):
    for type_map in type_maps:
        if type_map.match(compare_value):
            return type_map.map_value
    return unmatched_value


class SnowflakeConnector(SQLConnector):
    """The connector for Snowflake.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = True  # Whether MERGE UPSERT is supported.
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
            "database": config["database"],
        }

        for option in ["warehouse", "role"]:
            if config.get(option):
                params[option] = config.get(option)

        return URL(**params)

    def create_sqlalchemy_engine(self) -> sqlalchemy.engine.Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
            A newly created SQLAlchemy engine object.
        """
        return sqlalchemy.create_engine(
            url=self.sqlalchemy_url,
            connect_args={
                "session_parameters": {
                    "QUOTED_IDENTIFIERS_IGNORE_CASE": "TRUE",
                }
            },
            echo=False,
        )

    def _adapt_column_type(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        """Adapt table column type to support the new JSON schema type.

        Overridden here as Snowflake ALTER syntax for columns is different than that implemented in the SDK
        https://docs.snowflake.com/en/sql-reference/sql/alter-table-column.html
        TODO: update once https://github.com/meltano/sdk/pull/1114 merges

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

        Uses custom Snowflake types from [snowflake-sqlalchemy](https://github.com/snowflakedb/snowflake-sqlalchemy/blob/main/src/snowflake/sqlalchemy/custom_types.py)

        Args:
            jsonschema_type: The JSON Schema representation of the source type.

        Returns:
            The SQLAlchemy type representation of the data type.
        """
        # start with default implementation
        target_type = SQLConnector.to_sql_type(jsonschema_type)
        # snowflake max and default varchar length
        # https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
        maxlength = jsonschema_type.get("maxLength", 16777216)
        # define type maps
        string_submaps = [
            TypeMap(eq, sct.TIMESTAMP_NTZ(), "date-time"),
            TypeMap(contains, sqlalchemy.types.TIME(), "time"),
            TypeMap(eq, sqlalchemy.types.DATE(), "date"),
            TypeMap(eq, sqlalchemy.types.VARCHAR(maxlength), None),
        ]
        type_maps = [
            TypeMap(th._jsonschema_type_check, sct.NUMBER(), ("integer",)),
            TypeMap(th._jsonschema_type_check, sct.VARIANT(), ("object",)),
            TypeMap(th._jsonschema_type_check, sct.VARIANT(), ("array",)),
        ]
        # apply type maps
        if th._jsonschema_type_check(jsonschema_type, ("string",)):
            datelike_type = th.get_datelike_property_type(jsonschema_type)
            target_type = evaluate_typemaps(string_submaps, datelike_type, target_type)
        else:
            target_type = evaluate_typemaps(type_maps, jsonschema_type, target_type)

        return cast(sqlalchemy.types.TypeEngine, target_type)

    def merge_upsert_from_table(
        self, target_table_name: str, from_table_name: str, join_keys: List[str]
    ) -> Optional[int]:
        """Merge upsert data from one table to another.

        Args:
            target_table_name: The destination table name.
            from_table_name: The source table name.
            join_keys: The merge upsert keys, or `None` to append.

        Return:
            The number of records copied, if detectable, or `None` if the API does not
            report number of records affected/inserted.

        Raises:
            NotImplementedError: if the merge upsert capability does not exist or is
                undefined.
        """
        property_names = schema["properties"].keys()
        join_expr = " and ".join(
            [f'd."{key.upper()}" = s."{key.upper()}"' for key in join_keys]
        )
        matched_clause = ", ".join(
            [
                f'd."{col.upper()}" = s."{col.upper()}"' for col in property_names
                if col not in join_keys
            ]
        )
        not_matched_insert_cols = ", ".join(property_names)
        not_matched_insert_values = ", ".join([f's."{col}"' for col in property_names])
        self.connection.execute(
            text(
                f"merge into {target_table_name} d using {from_table_name} s"
                + f"\non {join_expr} "
                + f"\nwhen matched then update set {matched_clause} "
                + f"\nwhen not matched then insert ({not_matched_insert_cols}) "
                + f"\nvalues ({not_matched_insert_values})"
            )
        )

    def merge_upsert_from_stage(
            self,
            full_table_name,
            join_keys,
            schema,
            sync_id,
            file_format
        ):
        """Get Snowflake MERGE statement for loading directly from staged files."""
        # convert from case in JSON to UPPER column name
        column_selections = [
            f"$1:{property_name}::{self.connector.to_sql_type(property_def)} as {property_name.upper()}"
            for property_name, property_def in schema["properties"].items()
        ]
        # use UPPER from here onwards
        upper_properties = [col.upper() for col in schema["properties"].keys()]
        upper_key_properties = [col.upper() for col in join_keys]
        join_expr = " and ".join(
            [f'd."{key}" = s."{key}"' for key in upper_key_properties]
        )
        matched_clause = ", ".join(
            [f'd."{col}" = s."{col}"' for col in upper_properties]
        )
        not_matched_insert_cols = ", ".join(upper_properties)
        not_matched_insert_values = ", ".join(
            [f's."{col}"' for col in upper_properties]
        )
        merge_statement = text(
            f"merge into {full_table_name} d using "
            + f"(select {', '.join(column_selections)} from '{self.stage_path}{sync_id}'"
            + f"(file_format => {file_format})) s "
            + f"on {join_expr} "
            + f"when matched then update set {matched_clause} "
            + f"when not matched then insert ({not_matched_insert_cols}) "
            + f"values ({not_matched_insert_values})"
        )
        self.logger.info(f"Merging batch with SQL: {merge_statement}")
        self.connector.connection.execute(merge_statement)

    @property
    def stage_path(self) -> str:
        return "@~/target-snowflake/"

    def upload_file_to_stage(
        sync_id: str,
        file_uri: str,
    ):
        self.connection.execute(
            text(f"PUT '{file_uri}' '{self.stage_path}{sync_id}'")
        )

    def delete_files_from_stage(
        sync_id: str,
        file_uri: str,
    ):
        self.connection.execute(
            text(f"REMOVE '{self.stage_path}{sync_id}/'")
        )

    def create_json_file_format(self, file_format: str, /):
        file_format_statement = text(
            f"CREATE OR REPLACE FILE FORMAT {file_format}"
            + "TYPE = 'JSON' COMPRESSION = 'GZIP'"
        )
        self.logger.debug(f"Creating file format with SQL: {file_format_statement}")
        self.connector.connection.execute(file_format_statement)

    def drop_json_file_format(self, file_format: str, /):
        file_format_statement = text(
            f"DROP FILE FORMAT IF EXISTS {file_format}"
        )
        self.logger.debug(f"Dropping file format with SQL: {file_format_statement}")
        self.connector.connection.execute(file_format_statement)

    def create_table_from_files(
        self,
        full_table_name: str,
        file_encoding: BaseBatchFileEncoding,
        file_paths: Sequence[str],
        schema: dict,
        primary_keys: list[str],
        partition_keys: list[str],
        as_temp_table: bool,
    ) -> None:
        """Process a batch file with the given batch context.

        Args:
            encoding: The batch file encoding.
            files: The batch files to process.
        """
        if not file_encoding is BaseBatchFileEncoding.JSONL:
            raise RuntimeError(
                f"File encoding '{file_encoding}' not supported. Expected JSONL."
            )

        self.logger.info(f"Processing batch of {len(file_paths)} files")

        for file_uri in file_paths:
            self._upload_file_to_stage(sync_id=sync_id, file_uri=file_uri)

        self.connector.connections.execute(
            text(
                f"COPY INTO {full_table_name}" +
                f"\nFROM '{self.stage_path}{sync_id}/'" +
                "\nFILE_FORMAT = (TYPE = JSON);"
            )
        )


    def load_records_from_files(
        self,
        full_table_name: str,
        file_encoding: BaseBatchFileEncoding,
        file_paths: Sequence[str],
        schema: dict,
        primary_keys: list[str],
        skip_staging_table: bool=False,
        append_mode: bool=False,
    )
        sync_id = f"{self.stream_name}-{uuid4()}"

        if not self.table_exists(full_table_name):
            self.create_table_from_files(
                full_table_name=temp_table,
                file_encoding=file_encoding,
                file_paths=file_paths,
                schema=schema,
                as_temp_table=True,
                primary_keys=primary_keys,
                partition_keys=None,
            )

        elif self.allow_temp_tables and not skip_staging_table:
            temp_table = self.full_table_name & "__temp_{sync_id}"
            self.create_table_from_files(
                full_table_name=temp_table,
                file_encoding=file_encoding,
                file_paths=file_paths,
                schema=schema,
                as_temp_table=True,
                primary_keys=None,
                partition_keys=None,
            )
            if append_mode:
                self.merge_upsert_from_table(
                    full_table_name=full_table_name,
                    schema=self.conform_schema(self.schema),
                    sync_id=sync_id,
                    join_keys=primary_keys
                    file_format=file_format,
                )
            else:
                self.insert_records_from_table(
                    full_table_name=full_table_name,
                    schema=self.conform_schema(self.schema),
                    sync_id=sync_id,
                    join_keys=primary_keys
                    file_format=file_format,
                )

        else:
            self.connector.prepare_schema(f"{self.database_name}.{self.schema_name}")
            file_format = f'{self.database_name}.{self.schema_name}."{sync_id}"'
            self._create_json_file_format(file_format)

            # merge directly into destination table
            if append_mode:
                self.insert_records_from_stage(
                    full_table_name=self.full_table_name,
                    schema=self.conform_schema(self.schema),
                    join_keys=primary_keys
                    sync_id=sync_id,
                    file_format=file_format,
                )
            else:
                self.merge_upsert_from_stage(
                    full_table_name=self.full_table_name,
                    schema=self.conform_schema(self.schema),
                    join_keys=primary_keys
                    sync_id=sync_id,
                    file_format=file_format,
                )

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
