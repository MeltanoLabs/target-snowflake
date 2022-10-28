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

    def _get_put_statement(self, sync_id: str, file_uri: str) -> Tuple[text, dict]:
        """Get Snowflake PUT statement."""
        return (text(f"put '{file_uri}' '@~/target-snowflake/{sync_id}'"), {})

    def _get_merge_statement(self, full_table_name, schema, sync_id, file_format):
        """Get Snowflake MERGE statement."""
        # convert from case in JSON to UPPER column name
        column_selections = [
            f"$1:{property_name}::{self.connector.to_sql_type(property_def)} as {property_name.upper()}"
            for property_name, property_def in schema["properties"].items()
        ]
        # use UPPER from here onwards
        upper_properties = [col.upper() for col in schema["properties"].keys()]
        upper_key_properties = [col.upper() for col in self.key_properties]
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
        return (
            text(
                f"merge into {full_table_name} d using "
                + f"(select {', '.join(column_selections)} from '@~/target-snowflake/{sync_id}'"
                + f"(file_format => {file_format})) s "
                + f"on {join_expr} "
                + f"when matched then update set {matched_clause} "
                + f"when not matched then insert ({not_matched_insert_cols}) "
                + f"values ({not_matched_insert_values})"
            ),
            {},
        )

    def _get_file_format_statement(self, file_format):
        return (
            text(
                f"create or replace file format {file_format}"
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
        self.logger.info(f"Processing batch of {len(files)} files")
        try:
            sync_id = f"{self.stream_name}-{uuid4()}"
            file_format = f'{self.database_name}.{self.schema_name}."{sync_id}"'
            # PUT batches to remote stage
            for file_uri in files:
                put_statement, kwargs = self._get_put_statement(
                    sync_id=sync_id, file_uri=file_uri
                )
                self.connector.connection.execute(put_statement, **kwargs)
            # create schema
            self.logger.debug("Preparing target schema")
            self.connector.connection.execute(
                text(
                    f"create schema if not exists {self.database_name}.{self.schema_name}"
                )
            )
            # create file format in new schema
            file_format_statement, kwargs = self._get_file_format_statement(
                file_format=file_format
            )
            self.logger.debug(f"Creating file format with SQL: {file_format_statement}")
            self.connector.connection.execute(file_format_statement, **kwargs)
            # merge into destination table
            merge_statement, kwargs = self._get_merge_statement(
                full_table_name=self.full_table_name,
                schema=self.conform_schema(self.schema),
                sync_id=sync_id,
                file_format=file_format,
            )
            self.logger.info(f"Merging batch with SQL: {merge_statement}")
            self.connector.connection.execute(merge_statement, **kwargs)
        finally:
            # clean up file format
            self.logger.debug("Cleaning up after batch processing")
            self.connector.connection.execute(
                text(f"drop file format if exists {file_format}")
            )
            # clean up staged files
            self.connector.connection.execute(
                text(f"remove '@~/target-snowflake/{sync_id}/'")
            )
            # clean up local files
            if self.config.get("clean_up_batch_files"):
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
        """Bulk insert records to an existing destination table.

        The default implementation uses a generic SQLAlchemy bulk insert operation.
        This method may optionally be overridden by developers in order to provide
        faster, native bulk uploads.

        Args:
            full_table_name: the target table name.
            schema: the JSON schema for the new table, to be used when inferring column
                names.
            records: the input records.

        Returns:
            True if table exists, False if not, None if unsure or undetectable.
        """
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
        # serialize to batch files
        encoding, files = self.get_batches(
            batch_config=self.batch_config, records=processed_records
        )
        self.process_batch_files(encoding=encoding, files=files)
        # if records list, we can quickly return record count.
        return len(records) if isinstance(records, list) else None

    # Copied and modified from `singer_sdk.streams.core.Stream`
    def get_batches(
        self,
        batch_config: BatchConfig,
        records: Iterable[Dict[str, Any]],
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Batch generator function.

        Developers are encouraged to override this method to customize batching
        behavior for databases, bulk APIs, etc.

        Args:
            batch_config: Batch config for this stream.
            context: Stream partition or context dictionary.

        Yields:
            A tuple of (encoding, manifest) for each batch.
        """
        sync_id = f"target-snowflake--{self.stream_name}-{uuid4()}"
        prefix = batch_config.storage.prefix or ""
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
