from __future__ import annotations

import base64
import binascii
import urllib.parse
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any
from warnings import warn

import snowflake.sqlalchemy.custom_types as sct
import sqlalchemy
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from singer_sdk.connectors import SQLConnector
from singer_sdk.connectors.sql import FullyQualifiedName, JSONSchemaToSQL
from singer_sdk.exceptions import ConfigValidationError
from snowflake.sqlalchemy import URL
from snowflake.sqlalchemy.base import SnowflakeIdentifierPreparer
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy.sql import text

from target_snowflake.snowflake_types import NUMBER, TIMESTAMP_NTZ, VARIANT

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from sqlalchemy.engine import Engine


class SnowflakeFullyQualifiedName(FullyQualifiedName):
    def __init__(
        self,
        *,
        table: str | None = None,
        schema: str | None = None,
        database: str | None = None,
        delimiter: str = ".",
        dialect: SnowflakeDialect,
    ) -> None:
        self.dialect = dialect
        super().__init__(table=table, schema=schema, database=database, delimiter=delimiter)

    def prepare_part(self, part: str) -> str:
        return self.dialect.identifier_preparer.quote(part)


class JSONSchemaToSnowflake(JSONSchemaToSQL):
    def handle_multiple_types(self, types: Sequence[str]) -> sqlalchemy.types.TypeEngine:
        if "object" in types or "array" in types:
            return VARIANT()

        return super().handle_multiple_types(types)


class SnowflakeAuthMethod(Enum):
    """Supported methods to authenticate to snowflake"""

    BROWSER = 1
    PASSWORD = 2
    KEY_PAIR = 3


class SnowflakeConnector(SQLConnector):
    """Snowflake Target Connector.

    This class handles all DDL and type conversions.
    """

    allow_column_add: bool = True  # Whether ADD COLUMN is supported.
    allow_column_rename: bool = True  # Whether RENAME COLUMN is supported.
    allow_column_alter: bool = True  # Whether altering column types is supported.
    allow_merge_upsert: bool = False  # Whether MERGE UPSERT is supported.
    allow_temp_tables: bool = True  # Whether temp tables are supported.

    max_varchar_length = 16_777_216
    jsonschema_to_sql_converter = JSONSchemaToSnowflake

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

    def get_private_key(self):
        """Get private key from the right location."""
        phrase = self.config.get("private_key_passphrase")
        encoded_passphrase = phrase.encode() if phrase else None
        if "private_key_path" in self.config:
            self.logger.debug("Reading private key from file: %s", self.config["private_key_path"])
            key_path = Path(self.config["private_key_path"])
            if not key_path.is_file():
                error_message = f"Private key file not found: {key_path}"
                raise FileNotFoundError(error_message)
            with key_path.open("rb") as key_file:
                key_content = key_file.read()
        else:
            private_key = self.config["private_key"]
            self.logger.debug("Reading private key from config")
            if "-----BEGIN " in private_key:
                warn(
                    "Use base64 encoded private key instead of PEM format",
                    DeprecationWarning,
                    stacklevel=2,
                )
                self.logger.info("Private key is in PEM format")
                key_content = private_key.encode()
            else:
                try:
                    self.logger.debug("Private key is in base64 format")
                    key_content = base64.b64decode(private_key)
                except binascii.Error as e:
                    error_message = f"Invalid private key format: {e}"
                    raise ValueError(error_message) from e
        p_key = serialization.load_pem_private_key(
            key_content,
            password=encoded_passphrase,
            backend=default_backend(),
        )

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @cached_property
    def auth_method(self) -> SnowflakeAuthMethod:
        """Validate & return the authentication method based on config."""
        if self.config.get("use_browser_authentication"):
            return SnowflakeAuthMethod.BROWSER

        valid_auth_methods = {"private_key", "private_key_path", "password"}
        config_auth_methods = [x for x in self.config if x in valid_auth_methods]
        if len(config_auth_methods) != 1:
            msg = (
                "Neither password nor private key was provided for "
                "authentication. For password-less browser authentication via SSO, "
                "set use_browser_authentication config option to True."
            )
            raise ConfigValidationError(msg)
        if config_auth_methods[0] in ["private_key", "private_key_path"]:
            return SnowflakeAuthMethod.KEY_PAIR
        return SnowflakeAuthMethod.PASSWORD

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

        if self.auth_method == SnowflakeAuthMethod.BROWSER:
            params["authenticator"] = "externalbrowser"
        elif self.auth_method == SnowflakeAuthMethod.PASSWORD:
            params["password"] = urllib.parse.quote(config["password"])

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
            "client_session_keep_alive": True,  # See https://github.com/snowflakedb/snowflake-connector-python/issues/218
        }
        if self.auth_method == SnowflakeAuthMethod.KEY_PAIR:
            connect_args["private_key"] = self.get_private_key()
        engine = sqlalchemy.create_engine(
            self.sqlalchemy_url,
            connect_args=connect_args,
            echo=False,
        )
        with engine.connect() as conn:
            db_names = [db[1] for db in conn.execute(text("SHOW DATABASES;")).fetchall()]
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

    @cached_property
    def jsonschema_to_sql(self) -> JSONSchemaToSQL:
        # https://docs.snowflake.com/en/sql-reference/intro-summary-data-types.html
        to_sql = super().jsonschema_to_sql
        to_sql.register_type_handler("integer", NUMBER)
        to_sql.register_type_handler("object", VARIANT)
        to_sql.register_type_handler("array", VARIANT)
        to_sql.register_type_handler("number", sct.DOUBLE)
        to_sql.register_format_handler("date-time", TIMESTAMP_NTZ)
        return to_sql

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

    def _get_merge_from_stage_statement(  # noqa: ANN202
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
                f"merge into {full_table_name} d using "  # noqa: ISC003, S608
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
                f"copy into {full_table_name} {col_alias_selects} from "  # noqa: ISC003, S608
                + f"(select {json_casting_selects} from "  # noqa: S608
                + f"'@~/target-snowflake/{sync_id}')"
                + f"file_format = (format_name='{file_format}')",
            ),
            {},
        )

    def _get_file_format_statement(self, file_format: str) -> tuple[sqlalchemy.TextClause, dict]:
        """Get Snowflake CREATE FILE FORMAT statement."""
        return (
            text(f"create or replace file format {file_format} type = 'JSON' compression = 'AUTO'"),
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
        with self._connect() as conn, conn.begin():
            for file_uri in files:
                put_statement, kwargs = self._get_put_statement(
                    sync_id=sync_id,
                    file_uri=file_uri,
                )
                # sqlalchemy.text stripped a slash, which caused windows to fail so we used bound parameters instead
                # See https://github.com/MeltanoLabs/target-snowflake/issues/87 for more information about this error
                conn.execute(put_statement, {"file_uri": file_uri, **kwargs})

    def create_file_format(self, file_format: str) -> None:
        """Create a file format in the schema.

        Args:
            file_format: The name of the file format.
        """
        with self._connect() as conn, conn.begin():
            file_format_statement, kwargs = self._get_file_format_statement(
                file_format=file_format,
            )
            self.logger.debug(
                "Creating file format with SQL: %s",
                file_format_statement,
            )
            conn.execute(file_format_statement, **kwargs)

    def merge_from_stage(
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
        with self._connect() as conn, conn.begin():
            merge_statement, kwargs = self._get_merge_from_stage_statement(
                full_table_name=full_table_name,
                schema=schema,
                sync_id=sync_id,
                file_format=file_format,
                key_properties=key_properties,
            )
            self.logger.debug("Merging with SQL: %s", merge_statement)
            result = conn.execute(merge_statement, **kwargs)
            return result.rowcount

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
        with self._connect() as conn, conn.begin():
            copy_statement, kwargs = self._get_copy_statement(
                full_table_name=full_table_name,
                schema=schema,
                sync_id=sync_id,
                file_format=file_format,
            )
            self.logger.debug("Copying with SQL: %s", copy_statement)
            result = conn.execute(copy_statement, **kwargs)
            return result.rowcount

    def drop_file_format(self, file_format: str) -> None:
        """Drop a file format in the schema.

        Args:
            file_format: The name of the file format.
        """
        with self._connect() as conn, conn.begin():
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
        with self._connect() as conn, conn.begin():
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

    def get_fully_qualified_name(
        self,
        table_name: str | None = None,
        schema_name: str | None = None,
        db_name: str | None = None,
        delimiter: str = ".",
    ) -> SnowflakeFullyQualifiedName:
        return SnowflakeFullyQualifiedName(
            table=table_name,
            schema=schema_name,
            database=db_name,
            delimiter=delimiter,
            dialect=self._dialect,
        )
