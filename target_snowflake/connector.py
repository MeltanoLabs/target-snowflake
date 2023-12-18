from __future__ import annotations

from operator import contains, eq
from typing import TYPE_CHECKING, Any, Iterable, Sequence, cast

import snowflake.sqlalchemy.custom_types as sct
import sqlalchemy
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from singer_sdk import typing as th
from singer_sdk.connectors import SQLConnector
from snowflake.sqlalchemy import URL
from snowflake.sqlalchemy.base import SnowflakeIdentifierPreparer
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy.sql import text

from target_snowflake.snowflake_types import NUMBER, TIMESTAMP_NTZ, VARIANT

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

SNOWFLAKE_MAX_STRING_LENGTH = 16777216


class TypeMap:
    def __init__(self, operator, map_value, match_value=None) -> None:  # noqa: ANN001
        self.operator = operator
        self.map_value = map_value
        self.match_value = match_value

    def match(self, compare_value):  # noqa: ANN001
        try:
            if self.match_value:
                return self.operator(compare_value, self.match_value)
            return self.operator(compare_value)
        except TypeError:
            return False


def evaluate_typemaps(type_maps, compare_value, unmatched_value):  # noqa: ANN001
    for type_map in type_maps:
        if type_map.match(compare_value):
            return type_map.map_value
    return unmatched_value


class SnowflakeConnector(SQLConnector):
    """Snowflake Target Connector.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.table_cache: dict = {}
        self.schema_cache: dict = {}
        super().__init__(*args, **kwargs)

    def get_table_columns(
        self,
        full_table_name: str,
        column_names: list[str] | None = None,
    ) -> dict[str, sqlalchemy.Column]:
        """Return a list of table columns.

        Args:
            full_table_name: Fully qualified table name.
            column_names: A list of column names to filter to.

        Returns:
            An ordered list of column objects.
        """
        if full_table_name in self.table_cache:
            return self.table_cache[full_table_name]
        _, schema_name, table_name = self.parse_full_table_name(full_table_name)
        inspector = sqlalchemy.inspect(self._engine)
        columns = inspector.get_columns(table_name, schema_name)

        parsed_columns = {
            col_meta["name"]: sqlalchemy.Column(
                col_meta["name"],
                self._convert_type(col_meta["type"]),
                nullable=col_meta.get("nullable", False),
            )
            for col_meta in columns
            if not column_names or col_meta["name"].casefold() in {col.casefold() for col in column_names}
        }
        self.table_cache[full_table_name] = parsed_columns
        return parsed_columns

    @staticmethod
    def _convert_type(sql_type):  # noqa: ANN205, ANN001
        if isinstance(sql_type, sct.TIMESTAMP_NTZ):
            return TIMESTAMP_NTZ

        if isinstance(sql_type, sct.NUMBER):
            return NUMBER

        if isinstance(sql_type, sct.VARIANT):
            return VARIANT

        return sql_type

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates a SQLAlchemy URL for Snowflake.

        Args:
            config: The configuration for the connector.
        """
        params = {
            "account": config["account"],
            "user": config["user"],
            "database": config["database"],
        }

        if "password" in config:
            params["password"] = config["password"]
        elif "private_key_path" not in config:
            msg = "Neither password nor private_key_path was provided for authentication."
            raise Exception(msg)  # noqa: TRY002

        for option in ["warehouse", "role"]:
            if config.get(option):
                params[option] = config.get(option)

        return URL(**params)

    def create_engine(self) -> Engine:
        """Creates and returns a new engine. Do not call outside of _engine.

        NOTE: Do not call this method. The only place that this method should
        be called is inside the self._engine method. If you'd like to access
        the engine on a connector, use self._engine.

        This method exists solely so that tap/target developers can override it
        on their subclass of SQLConnector to perform custom engine creation
        logic.

        Returns:
            A new SQLAlchemy Engine.
        """
        connect_args = {
            "session_parameters": {
                "QUOTED_IDENTIFIERS_IGNORE_CASE": "TRUE",
            },
        }
        if "private_key_path" in self.config:
            with open(self.config["private_key_path"], "rb") as private_key_file:  # noqa: PTH123
                private_key = serialization.load_pem_private_key(
                    private_key_file.read(),
                    password=self.config["private_key_passphrase"].encode()
                    if "private_key_passphrase" in self.config
                    else None,
                    backend=default_backend(),
                )
                connect_args["private_key"] = private_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
        engine = sqlalchemy.create_engine(
            self.sqlalchemy_url,
            connect_args=connect_args,
            echo=False,
        )
        connection = engine.connect()
        db_names = [db[1] for db in connection.execute(text("SHOW DATABASES;")).fetchall()]
        if self.config["database"] not in db_names:
            msg = f"Database '{self.config['database']}' does not exist or the user/role doesn't have access to it."
            raise Exception(msg)  # noqa: TRY002
        return engine

    def prepare_column(
        self,
        full_table_name: str,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        formatter = SnowflakeIdentifierPreparer(SnowflakeDialect())
        # Make quoted column names upper case because we create them that way
        # and the metadata that SQLAlchemy returns is case insensitive only for non-quoted
        # column names so these will look like they dont exist yet.
        if '"' in formatter.format_collation(column_name):
            column_name = column_name.upper()

        try:
            super().prepare_column(
                full_table_name,
                column_name,
                sql_type,
            )
        except Exception:
            self.logger.exception(
                "Error preparing column for '%s.%s'",
                full_table_name,
                column_name,
            )
            raise

    @staticmethod
    def get_column_rename_ddl(
        table_name: str,
        column_name: str,
        new_column_name: str,
    ) -> sqlalchemy.DDL:
        formatter = SnowflakeIdentifierPreparer(SnowflakeDialect())
        # Since we build the ddl manually we can't rely on SQLAlchemy to
        # quote column names automatically.
        return SQLConnector.get_column_rename_ddl(
            table_name,
            formatter.format_collation(column_name),
            formatter.format_collation(new_column_name),
        )

    @staticmethod
    def get_column_alter_ddl(
        table_name: str,
        column_name: str,
        column_type: sqlalchemy.types.TypeEngine,
    ) -> sqlalchemy.DDL:
        """Get the alter column DDL statement.

        Override this if your database uses a different syntax for altering columns.

        Args:
            table_name: Fully qualified table name of column to alter.
            column_name: Column name to alter.
            column_type: New column type string.

        Returns:
            A sqlalchemy DDL instance.
        """
        formatter = SnowflakeIdentifierPreparer(SnowflakeDialect())
        # Since we build the ddl manually we can't rely on SQLAlchemy to
        # quote column names automatically.
        return sqlalchemy.DDL(
            "ALTER TABLE %(table_name)s ALTER COLUMN %(column_name)s SET DATA TYPE %(column_type)s",
            {
                "table_name": table_name,
                "column_name": formatter.format_collation(column_name),
                "column_type": column_type,
            },
        )

    @staticmethod
    def _conform_max_length(jsonschema_type):  # noqa: ANN205, ANN001
        """Alter jsonschema representations to limit max length to Snowflake's VARCHAR length."""
        max_length = jsonschema_type.get("maxLength")
        if max_length and max_length > SNOWFLAKE_MAX_STRING_LENGTH:
            jsonschema_type["maxLength"] = SNOWFLAKE_MAX_STRING_LENGTH
        return jsonschema_type

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
        jsonschema_type = SnowflakeConnector._conform_max_length(jsonschema_type)
        target_type = SQLConnector.to_sql_type(jsonschema_type)
        # snowflake max and default varchar length
        # https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
        maxlength = jsonschema_type.get("maxLength", SNOWFLAKE_MAX_STRING_LENGTH)
        # define type maps
        string_submaps = [
            TypeMap(eq, TIMESTAMP_NTZ(), "date-time"),
            TypeMap(contains, sqlalchemy.types.TIME(), "time"),
            TypeMap(eq, sqlalchemy.types.DATE(), "date"),
            TypeMap(eq, sqlalchemy.types.VARCHAR(maxlength), None),
        ]
        type_maps = [
            TypeMap(th._jsonschema_type_check, NUMBER(), ("integer",)),  # noqa: SLF001
            TypeMap(th._jsonschema_type_check, VARIANT(), ("object",)),  # noqa: SLF001
            TypeMap(th._jsonschema_type_check, VARIANT(), ("array",)),  # noqa: SLF001
            TypeMap(th._jsonschema_type_check, sct.DOUBLE(), ("number",)),  # noqa: SLF001
        ]
        # apply type maps
        if th._jsonschema_type_check(jsonschema_type, ("string",)):  # noqa: SLF001
            datelike_type = th.get_datelike_property_type(jsonschema_type)
            target_type = evaluate_typemaps(string_submaps, datelike_type, target_type)
        else:
            target_type = evaluate_typemaps(type_maps, jsonschema_type, target_type)

        return cast(sqlalchemy.types.TypeEngine, target_type)

    def schema_exists(self, schema_name: str) -> bool:
        if schema_name in self.schema_cache:
            return True
        schema_names = sqlalchemy.inspect(self._engine).get_schema_names()
        self.schema_cache = schema_names
        formatter = SnowflakeIdentifierPreparer(SnowflakeDialect())
        # Make quoted schema names upper case because we create them that way
        # and the metadata that SQLAlchemy returns is case insensitive only for
        # non-quoted schema names so these will look like they dont exist yet.
        if '"' in formatter.format_collation(schema_name):
            schema_name = schema_name.upper()
        return schema_name in schema_names

    # Custom SQL get methods

    def _get_put_statement(self, sync_id: str, file_uri: str) -> tuple[text, dict]:  # noqa: ARG002
        """Get Snowflake PUT statement."""
        return (text(f"put :file_uri '@~/target-snowflake/{sync_id}'"), {})

    @staticmethod
    def _format_column_selections(column_selections: list, format: str) -> str:  # noqa: A002
        if format == "json_casting":
            return ", ".join(
                [
                    f"$1:{col['clean_property_name']}::{col['sql_type']} as {col['clean_alias']}"
                    for col in column_selections
                ],
            )
        if format == "col_alias":
            return f"({', '.join([col['clean_alias'] for col in column_selections])})"

        error_message = f"Column format not implemented: {format}"
        raise NotImplementedError(error_message)

    def _get_column_selections(
        self,
        schema: dict,
        formatter: SnowflakeIdentifierPreparer,
    ) -> list:
        column_selections = []
        for property_name, property_def in schema["properties"].items():
            clean_property_name = formatter.format_collation(property_name)
            clean_alias = clean_property_name
            if '"' in clean_property_name:
                clean_alias = clean_property_name.upper()
            column_selections.append(
                {
                    "clean_property_name": clean_property_name,
                    "sql_type": self.to_sql_type(property_def),
                    "clean_alias": clean_alias,
                },
            )
        return column_selections

    def _get_merge_from_stage_statement(  # noqa: ANN202, PLR0913
        self,
        full_table_name: str,
        schema: dict,
        sync_id: str,
        file_format: str,
        key_properties: Iterable[str],
    ):
        """Get Snowflake MERGE statement."""
        formatter = SnowflakeIdentifierPreparer(SnowflakeDialect())
        column_selections = self._get_column_selections(schema, formatter)
        json_casting_selects = self._format_column_selections(
            column_selections,
            "json_casting",
        )

        # use UPPER from here onwards
        formatted_properties = [formatter.format_collation(col) for col in schema["properties"]]
        formatted_key_properties = [formatter.format_collation(col) for col in key_properties]
        join_expr = " and ".join(
            [f"d.{key} = s.{key}" for key in formatted_key_properties],
        )
        matched_clause = ", ".join(
            [f"d.{col} = s.{col}" for col in formatted_properties],
        )
        not_matched_insert_cols = ", ".join(formatted_properties)
        not_matched_insert_values = ", ".join(
            [f"s.{col}" for col in formatted_properties],
        )
        dedup_cols = ", ".join(list(formatted_key_properties))
        dedup = f"QUALIFY ROW_NUMBER() OVER (PARTITION BY {dedup_cols} ORDER BY SEQ8() DESC) = 1"
        return (
            text(
                f"merge into {full_table_name} d using "  # noqa: ISC003
                + f"(select {json_casting_selects} from '@~/target-snowflake/{sync_id}'"  # noqa: S608
                + f"(file_format => {file_format}) {dedup}) s "
                + f"on {join_expr} "
                + f"when matched then update set {matched_clause} "
                + f"when not matched then insert ({not_matched_insert_cols}) "
                + f"values ({not_matched_insert_values})",
            ),
            {},
        )

    def _get_copy_statement(self, full_table_name, schema, sync_id, file_format):  # noqa: ANN202, ANN001
        """Get Snowflake COPY statement."""
        formatter = SnowflakeIdentifierPreparer(SnowflakeDialect())
        column_selections = self._get_column_selections(schema, formatter)
        json_casting_selects = self._format_column_selections(
            column_selections,
            "json_casting",
        )
        col_alias_selects = self._format_column_selections(
            column_selections,
            "col_alias",
        )
        return (
            text(
                f"copy into {full_table_name} {col_alias_selects} from "  # noqa: ISC003
                + f"(select {json_casting_selects} from "  # noqa: S608
                + f"'@~/target-snowflake/{sync_id}')"
                + f"file_format = (format_name='{file_format}')",
            ),
            {},
        )

    def _get_file_format_statement(self, file_format):  # noqa: ANN202, ANN001
        """Get Snowflake CREATE FILE FORMAT statement."""
        return (
            text(f"create or replace file format {file_format}type = 'JSON' compression = 'AUTO'"),
            {},
        )

    def _get_drop_file_format_statement(self, file_format):  # noqa: ANN202, ANN001
        """Get Snowflake DROP FILE FORMAT statement."""
        return (
            text(f"drop file format if exists {file_format}"),
            {},
        )

    def _get_stage_files_remove_statement(self, sync_id):  # noqa: ANN202, ANN001
        """Get Snowflake REMOVE statement."""
        return (
            text(f"remove '@~/target-snowflake/{sync_id}/'"),
            {},
        )

    # Custom connector methods

    def put_batches_to_stage(self, sync_id: str, files: Sequence[str]) -> None:
        """Upload a batch of records to Snowflake.

        Args:
            sync_id: The sync ID for the batch.
            files: The files containing records to upload.
        """
        with self._connect() as conn:
            for file_uri in files:
                put_statement, kwargs = self._get_put_statement(
                    sync_id=sync_id,
                    file_uri=file_uri,
                )
                # sqlalchemy.text stripped a slash, which caused windows to fail so we used bound parameters instead
                # See https://github.com/MeltanoLabs/target-snowflake/issues/87 for more information about this error
                conn.execute(put_statement, file_uri=file_uri, **kwargs)

    def create_file_format(self, file_format: str) -> None:
        """Create a file format in the schema.

        Args:
            file_format: The name of the file format.
        """
        with self._connect() as conn:
            file_format_statement, kwargs = self._get_file_format_statement(
                file_format=file_format,
            )
            self.logger.debug(
                "Creating file format with SQL: %s",
                file_format_statement,
            )
            conn.execute(file_format_statement, **kwargs)

    def merge_from_stage(  # noqa: PLR0913
        self,
        full_table_name: str,
        schema: dict,
        sync_id: str,
        file_format: str,
        key_properties: Sequence[str],
    ):
        """Merge data from a stage into a table.

        Args:
            sync_id: The sync ID for the batch.
            schema: The schema of the data.
            key_properties: The primary key properties of the data.
        """
        with self._connect() as conn:
            merge_statement, kwargs = self._get_merge_from_stage_statement(
                full_table_name=full_table_name,
                schema=schema,
                sync_id=sync_id,
                file_format=file_format,
                key_properties=key_properties,
            )
            self.logger.debug("Merging with SQL: %s", merge_statement)
            conn.execute(merge_statement, **kwargs)

    def copy_from_stage(
        self,
        full_table_name: str,
        schema: dict,
        sync_id: str,
        file_format: str,
    ):
        """Copy data from a stage into a table.

        Args:
            full_table_name: The fully-qualified name of the table.
            schema: The schema of the data.
            sync_id: The sync ID for the batch.
            file_format: The name of the file format.
        """
        with self._connect() as conn:
            copy_statement, kwargs = self._get_copy_statement(
                full_table_name=full_table_name,
                schema=schema,
                sync_id=sync_id,
                file_format=file_format,
            )
            self.logger.debug("Copying with SQL: %s", copy_statement)
            conn.execute(copy_statement, **kwargs)

    def drop_file_format(self, file_format: str) -> None:
        """Drop a file format in the schema.

        Args:
            file_format: The name of the file format.
        """
        with self._connect() as conn:
            drop_statement, kwargs = self._get_drop_file_format_statement(
                file_format=file_format,
            )
            self.logger.debug("Dropping file format with SQL: %s", drop_statement)
            conn.execute(drop_statement, **kwargs)

    def remove_staged_files(self, sync_id: str) -> None:
        """Remove staged files.

        Args:
            sync_id: The sync ID for the batch.
        """
        with self._connect() as conn:
            remove_statement, kwargs = self._get_stage_files_remove_statement(
                sync_id=sync_id,
            )
            self.logger.debug("Removing staged files with SQL: %s", remove_statement)
            conn.execute(remove_statement, **kwargs)

    @staticmethod
    def get_initialize_script(role, user, password, warehouse, database) -> str:  # noqa: ANN001
        # https://fivetran.com/docs/destinations/snowflake/setup-guide
        return f"""
            begin;

            -- change role to securityadmin for user / role steps
            use role securityadmin;

            -- create role
            create role if not exists {role};
            grant role {role} to role SYSADMIN;

            -- create a user
            create user if not exists {user}
            password = '{password}'
            default_role = {role}
            default_warehouse = {warehouse};

            grant role {role} to user {user};

            -- change role to sysadmin for warehouse / database steps
            use role sysadmin;

            -- create a warehouse
            create warehouse if not exists {warehouse}
            warehouse_size = xsmall
            warehouse_type = standard
            auto_suspend = 60
            auto_resume = true
            initially_suspended = true;

            -- create database
            create database if not exists {database};

            -- grant role access to warehouse
            grant USAGE
            on warehouse {warehouse}
            to role {role};

            -- grant access to database
            grant CREATE SCHEMA, MONITOR, USAGE
            on database {database}
            to role {role};

            commit;

        """

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
        try:
            super()._adapt_column_type(full_table_name, column_name, sql_type)
        except Exception:
            current_type: sqlalchemy.types.TypeEngine = self._get_column_type(
                full_table_name,
                column_name,
            )
            self.logger.exception(
                "Error adapting column type for '%s.%s', '%s' to '%s' (new sql type)",
                full_table_name,
                column_name,
                current_type,
                sql_type,
            )
            raise
