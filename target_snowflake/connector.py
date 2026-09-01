# Copyright (C) 2026 Meltano.

from __future__ import annotations

import base64
import binascii
import re
import urllib.parse
import uuid
from contextlib import contextmanager
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any
from warnings import warn

import humps
import snowflake.sqlalchemy.custom_types as sct
import sqlalchemy
import sqlalchemy.sql.type_api
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from singer_sdk.exceptions import ConfigValidationError
from singer_sdk.sql.connector import FullyQualifiedName, JSONSchemaToSQL, SQLConnector
from snowflake.sqlalchemy import URL
from snowflake.sqlalchemy.base import SnowflakeIdentifierPreparer
from snowflake.sqlalchemy.snowdialect import SnowflakeDialect
from sqlalchemy.sql import text
from sqlalchemy.sql.compiler import RESERVED_WORDS as DEFAULT_RESERVED_WORDS

from target_snowflake.snowflake_types import (
    NUMBER,
    TIMESTAMP_LTZ,
    TIMESTAMP_NTZ,
    TIMESTAMP_TZ,
    VARIANT,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Sequence

    import sqlalchemy as sa
    from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
    from sqlalchemy.engine import Engine
    from sqlalchemy.sql.compiler import IdentifierPreparer


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
    OAUTH = 4


class SnowflakeTimestampType(str, Enum):
    """Supported Snowflake timestamp types."""

    TIMESTAMP_TZ = "TIMESTAMP_TZ"
    TIMESTAMP_LTZ = "TIMESTAMP_LTZ"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"


DEFAULT_TIMESTAMP_TYPE = SnowflakeTimestampType.TIMESTAMP_NTZ
TIMESTAMP_TYPES: dict[SnowflakeTimestampType, type[sqlalchemy.sql.type_api.TypeEngine]] = {
    SnowflakeTimestampType.TIMESTAMP_TZ: TIMESTAMP_TZ,
    SnowflakeTimestampType.TIMESTAMP_LTZ: TIMESTAMP_LTZ,
    SnowflakeTimestampType.TIMESTAMP_NTZ: TIMESTAMP_NTZ,
}


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
        self.schema_cache: list[str] = []
        self._inspector: sqlalchemy.Inspector | None = None
        super().__init__(*args, **kwargs)

    @contextmanager
    def connect(self) -> Generator[sa.Connection, None, None]:
        """Return a SQLAlchemy connection context manager."""
        with self._connect() as conn:
            yield conn

    @property
    def inspector(self) -> sqlalchemy.Inspector:
        """Return a cached Inspector instance for schema reflection."""
        if self._inspector is None:
            self._inspector = sqlalchemy.inspect(self._engine)
        return self._inspector

    def invalidate_table_cache(self, full_table_name: str | FullyQualifiedName) -> None:
        """Discard cached reflection state for a table after DDL.

        Dropping the `table_cache` entry alone is not enough. The Inspector
        memoises reflection for its lifetime, and snowflake-sqlalchemy caches
        columns per *schema*, so a table created after the first reflection of
        that schema stays invisible and reflecting it raises NoSuchTableError.
        The Inspector is dropped so the next reflection queries Snowflake.

        Args:
            full_table_name: the table whose cached state is now stale.
        """
        self.table_cache.pop(full_table_name, None)
        self._inspector = None

    def get_table_columns(
        self,
        full_table_name: str | FullyQualifiedName,
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
        inspector = self.inspector
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
        if isinstance(sql_type, sct.TIMESTAMP_TZ):
            return TIMESTAMP_TZ

        if isinstance(sql_type, sct.TIMESTAMP_NTZ):
            return TIMESTAMP_NTZ

        if isinstance(sql_type, sct.NUMBER):
            return NUMBER

        if isinstance(sql_type, sct.VARIANT):
            return VARIANT

        return sql_type

    def _get_private_key_content(self) -> bytes:
        """Get private key from the right location."""
        if "private_key_path" in self.config:
            self.logger.debug("Reading private key from file: %s", self.config["private_key_path"])
            key_path = Path(self.config["private_key_path"])
            if not key_path.is_file():
                error_message = f"Private key file not found: {key_path}"
                raise FileNotFoundError(error_message)

            return key_path.read_bytes()

        private_key: str = self.config["private_key"]
        self.logger.debug("Reading private key from config")
        if "-----BEGIN " in private_key:
            warn(
                "Use base64 encoded private key instead of PEM format",
                DeprecationWarning,
                stacklevel=2,
            )
            self.logger.info("Private key is in PEM format")
            return private_key.encode()

        try:
            self.logger.debug("Private key is in base64 format")
            key_content = base64.b64decode(private_key)
        except binascii.Error as e:
            error_message = f"Invalid private key format: {e}"
            raise ValueError(error_message) from e

        return key_content

    def _load_private_key(self) -> PrivateKeyTypes:
        phrase = self.config.get("private_key_passphrase")
        encoded_passphrase = phrase.encode() if phrase else None
        key_content = self._get_private_key_content()

        try:
            return serialization.load_der_private_key(
                key_content,
                password=encoded_passphrase,
                backend=default_backend(),
            )
        except ValueError:
            self.logger.debug("DER deserialization failed; retrying as PEM")
            return serialization.load_pem_private_key(
                key_content,
                password=encoded_passphrase,
                backend=default_backend(),
            )

    def get_private_key(self):
        return self._load_private_key().private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @cached_property
    def auth_method(self) -> SnowflakeAuthMethod:
        """Validate & return the authentication method based on config."""
        if self.config.get("use_browser_authentication"):
            return SnowflakeAuthMethod.BROWSER

        valid_auth_methods = {"private_key", "private_key_path", "password", "oauth_access_token"}
        config_auth_methods = [x for x in self.config if x in valid_auth_methods]
        if len(config_auth_methods) != 1:
            msg = (
                "No password, private key, or OAuth token was provided for "
                "authentication. For password-less browser authentication via SSO, "
                "set use_browser_authentication config option to True."
            )
            raise ConfigValidationError(msg)
        if config_auth_methods[0] in ["private_key", "private_key_path"]:
            return SnowflakeAuthMethod.KEY_PAIR
        if config_auth_methods[0] == "oauth_access_token":
            return SnowflakeAuthMethod.OAUTH
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

    def get_connect_args(self) -> dict[str, Any]:
        """Get the connect args for the connector."""
        connect_args = {
            "session_parameters": {
                "QUOTED_IDENTIFIERS_IGNORE_CASE": str(self.config.get("quoted_identifiers_ignore_case", True)).upper(),
            },
            "client_session_keep_alive": True,  # See https://github.com/snowflakedb/snowflake-connector-python/issues/218
        }
        if self.auth_method == SnowflakeAuthMethod.KEY_PAIR:
            connect_args["private_key"] = self.get_private_key()
        elif self.auth_method == SnowflakeAuthMethod.OAUTH:
            oauth_token = self.config.get("oauth_access_token", "")
            if not oauth_token:
                msg = "OAuth access token is required but not provided or is empty"
                raise ConfigValidationError(msg)
            connect_args["token"] = oauth_token
            connect_args["authenticator"] = "oauth"

        return connect_args

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
        engine = sqlalchemy.create_engine(
            self.sqlalchemy_url,
            connect_args=self.get_connect_args(),
            echo=False,
        )

        # Snowflake dialect doesn't natively recognise UUID columns returned by reflection
        engine.dialect.ischema_names["UUID"] = sqlalchemy.types.Uuid  # type: ignore[attr-defined] # ty:ignore[unresolved-attribute]
        # Map Python's uuid.UUID to SQLAlchemy's UUID type when writing values
        engine.dialect.colspecs[uuid.UUID] = sqlalchemy.types.Uuid  # type: ignore[index] # ty:ignore[invalid-assignment]

        engine.dialect.identifier_preparer.reserved_words |= DEFAULT_RESERVED_WORDS

        with engine.connect() as conn:
            db_names = [db[1] for db in conn.execute(text("SHOW DATABASES;")).fetchall()]
            if self.config["database"] not in db_names:
                msg = f"Database '{self.config['database']}' does not exist or the user/role doesn't have access to it."
                raise Exception(msg)  # noqa: TRY002
        return engine

    @cached_property
    def formatter(self) -> IdentifierPreparer:
        return self._engine.dialect.identifier_preparer

    def prepare_column(
        self,
        full_table_name: str | FullyQualifiedName,
        column_name: str,
        sql_type: sqlalchemy.types.TypeEngine,
    ) -> None:
        column_name = self.format_identifier(column_name)

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
        table_name: str | FullyQualifiedName,
        column_name: str,
        new_column_name: str,
    ) -> sqlalchemy.DDL:
        formatter = SnowflakeIdentifierPreparer(SnowflakeDialect())
        formatter.reserved_words |= DEFAULT_RESERVED_WORDS
        # Since we build the ddl manually we can't rely on SQLAlchemy to
        # quote column names automatically.
        return SQLConnector.get_column_rename_ddl(
            table_name,
            formatter.format_collation(column_name),
            formatter.format_collation(new_column_name),
        )

    @staticmethod
    def get_column_alter_ddl(
        table_name: str | FullyQualifiedName,
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
        formatter.reserved_words |= DEFAULT_RESERVED_WORDS
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
        to_sql.register_format_handler(
            "date-time",
            TIMESTAMP_TYPES[self.config.get("timestamp_type", DEFAULT_TIMESTAMP_TYPE)],
        )
        if self.config.get("uuid_format", "native") == "string":
            # a standard UUID string is 36 characters long
            to_sql.register_format_handler("uuid", lambda _: sct.STRING(36))
        return to_sql

    def schema_exists(self, schema_name: str) -> bool:
        if schema_name in self.schema_cache:
            return True
        schema_names = self.inspector.get_schema_names()
        self.schema_cache = schema_names
        schema_name = self.format_identifier(schema_name)
        return schema_name in schema_names

    # Custom SQL get methods

    def _get_put_statement(self, sync_id: str, file_uri: str) -> tuple[sqlalchemy.TextClause, dict]:  # noqa: ARG002
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
    ) -> list:
        column_selections = []
        for property_name, property_def in schema["properties"].items():
            clean_property_name = self.formatter.format_collation(property_name)
            clean_alias = self.format_identifier(property_name, safe=True)
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
        full_table_name: str | FullyQualifiedName,
        schema: dict,
        sync_id: str,
        file_format: str,
        key_properties: Iterable[str],
    ):
        """Get Snowflake MERGE statement."""
        column_selections = self._get_column_selections(schema)
        json_casting_selects = self._format_column_selections(
            column_selections,
            "json_casting",
        )

        formatted_properties = [self.format_identifier(k, safe=True) for k in schema["properties"]]
        formatted_key_properties = [self.format_identifier(k, safe=True) for k in key_properties]
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
        column_selections = self._get_column_selections(schema)
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

    def _get_file_format_statement(
        self,
        file_format: str,
        file_type: str = "JSON",
    ) -> tuple[sqlalchemy.TextClause, dict]:
        """Get Snowflake CREATE FILE FORMAT statement."""
        return (
            text(f"create or replace file format {file_format} type = '{file_type}' compression = 'AUTO'"),
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

    def create_file_format(self, file_format: str, file_type: str = "JSON") -> None:
        """Create a file format in the schema.

        Args:
            file_format: The name of the file format.
            file_type: The Snowflake file format type, e.g. ``"JSON"`` or ``"PARQUET"``.
        """
        with self._connect() as conn, conn.begin():
            file_format_statement, kwargs = self._get_file_format_statement(
                file_format=file_format,
                file_type=file_type,
            )
            self.logger.debug(
                "Creating file format with SQL: %s",
                file_format_statement,
            )
            conn.execute(file_format_statement, **kwargs)

    def merge_from_stage(
        self,
        full_table_name: str | FullyQualifiedName,
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
            # MERGE's rowcount, like COPY INTO's below, isn't reliable via the generic
            # DBAPI/SQLAlchemy rowcount attribute - Snowflake's MERGE instead returns a
            # single-row result set: ("number of rows inserted", "number of rows updated",
            # "number of rows deleted"). Sum those instead of trusting result.rowcount,
            # which silently undercounts.
            row = result.fetchone()
            return sum(row) if row is not None else result.rowcount

    def copy_from_stage(
        self,
        full_table_name: str | FullyQualifiedName,
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
            # COPY INTO rowcount = number of files, not rows.
            # Fetch the result set and sum the rows_loaded column instead.
            rows = result.fetchall()
            return sum(r[3] for r in rows) if rows else result.rowcount

    def truncate_table(self, full_table_name: str | FullyQualifiedName) -> None:
        """Truncate a table.

        Args:
            full_table_name: The fully-qualified name of the table to truncate.
        """
        with self._connect() as conn, conn.begin():
            conn.execute(text(f"truncate table {full_table_name}"))

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
        full_table_name: str | FullyQualifiedName,
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

    def format_identifier(self, identifier: str, *, safe: bool = False) -> str:
        """Format an identifier per the `normalise_casing`/`quoted_identifiers_ignore_case` config.

        Args:
            identifier: The raw identifier (column/schema name) to format.
            safe: If True, return the quoted form when the identifier requires quoting
                (e.g. reserved words, mixed casing); otherwise return the unquoted form
                used to compare against SQLAlchemy's case-insensitive representation.

        Returns:
            The formatted identifier.
        """
        if self.config.get("normalise_casing", False):
            # substrings of 2 or more upper-case characters need to be converted to
            # title-case to play nicely with proceeding `humps.decamelise` call and avoid
            # bad formatting
            #
            # without: "TEST_streamName" -> "TES_T_stream_name"
            # with: "TEST_streamName" -> "test_stream_name"
            formatted = re.sub(r"[A-Z]{2,}", lambda match: match.group().lower(), identifier)

            formatted = humps.decamelize(formatted)

            # substitute hyphens
            formatted = humps.dekebabize(formatted)

            # the following should only quote reserved keywords e.g. `desc` at this point
            # as name should not contain mixed casing due to snake_case transformation (no
            # need to quote)
        else:
            formatted = identifier

        safe_formatted = self.formatter.format_collation(formatted)

        if '"' not in safe_formatted or self.config.get("quoted_identifiers_ignore_case", True):
            # Lowercase column names that are created in a case-insensitive manner, either instrinsically or when
            # QUOTED_IDENTIFIERS_IGNORE_CASE is set to FALSE, to match their SQLAlchemy representation.
            #
            # > Snowflake stores all case-insensitive object names in uppercase text. In contrast, SQLAlchemy considers
            # > all lowercase object names to be case-insensitive.
            #
            # https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy#object-name-case-handling
            #
            # > Unquoted identifiers are stored and resolved in uppercase. Therefore, an unquoted identifier is
            # > equivalent to a capitalized double-quoted identifier with the same name.
            #
            # https://docs.snowflake.com/en/sql-reference/identifiers-syntax#unquoted-identifiers
            #
            # > To configure Snowflake to treat alphabetic characters in double-quoted identifiers as uppercase for the
            # > session, set the parameter to TRUE for the session. With this setting, all alphabetical characters in
            # > identifiers are stored and resolved as uppercase characters.
            #
            # https://docs.snowflake.com/en/sql-reference/identifiers-syntax#controlling-case-using-the-quoted-identifiers-ignore-case-parameter

            formatted = formatted.lower()

            # ...but only when SQLAlchemy actually hands the name back lowercased.
            #
            # `SnowflakeDialect.normalize_name` lowercases a stored (upper-case) identifier
            # only if the lower-case form needs no quoting. Anything that *does* require
            # quoting - reserved words such as `user`, and names containing spaces, colons,
            # brackets, etc. - is returned exactly as Snowflake stored it, which under
            # QUOTED_IDENTIFIERS_IGNORE_CASE is upper case. Those must be compared in upper
            # case or an existing column looks missing and gets re-added via ALTER TABLE.
            if '"' in self.formatter.format_collation(formatted):
                formatted = formatted.upper()
                safe_formatted = safe_formatted.upper()

        # Identifiers that require quoting should be returned as-is when QUOTED_IDENTIFIERS_IGNORE_CASE is set to FALSE.
        return safe_formatted if safe else formatted
