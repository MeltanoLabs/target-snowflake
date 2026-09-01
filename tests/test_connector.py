# Copyright (C) 2026 Meltano.

"""Unit tests for the Snowflake connector."""

from __future__ import annotations

from unittest import mock

import pytest
import snowflake.sqlalchemy.custom_types as sct
import sqlalchemy
from sqlalchemy import types

from target_snowflake.connector import SnowflakeConnector, SnowflakeTimestampType
from target_snowflake.snowflake_types import NUMBER, VARIANT


@pytest.fixture
def connector():
    return SnowflakeConnector()


@pytest.fixture
def connectable_connector():
    """A connector with enough config to build a real (unconnected) SQLAlchemy engine."""
    config = {
        "user": "test_user",
        "password": "test_password",
        "account": "test_account",
        "database": "TEST_DATABASE",
    }
    return SnowflakeConnector(config=config)


@pytest.fixture
def mock_engine_connect(connectable_connector: SnowflakeConnector):
    with mock.patch("sqlalchemy.engine.Engine.connect") as connect_mock:
        connection = mock.MagicMock()
        connection.execute.return_value.fetchall.return_value = [
            (None, connectable_connector.config["database"]),
        ]

        context_manager = mock.MagicMock()
        context_manager.__enter__.return_value = connection

        connect_mock.return_value = context_manager
        yield connect_mock


@pytest.mark.parametrize(
    ("schema", "expected_type"),
    [
        pytest.param({"type": "object"}, VARIANT, id="object"),
        pytest.param({"type": ["array", "null"]}, VARIANT, id="array"),
        pytest.param({"type": ["array", "object", "string"]}, VARIANT, id="array_object_string"),
        pytest.param({"type": ["integer", "null"]}, NUMBER, id="integer"),
        pytest.param({"type": ["number", "null"]}, sct.DOUBLE, id="number"),
        pytest.param({"type": ["string", "null"], "format": "date-time"}, sct.TIMESTAMP_NTZ, id="date-time"),
        # Upstream types
        pytest.param({"type": ["string", "null"]}, types.VARCHAR, id="string"),
        pytest.param({"type": ["boolean", "null"]}, types.BOOLEAN, id="boolean"),
        pytest.param({"type": "string", "format": "time"}, types.TIME, id="time"),
        pytest.param({"type": "string", "format": "date"}, types.DATE, id="date"),
        pytest.param({"type": "string", "format": "uuid"}, types.UUID, id="uuid"),
    ],
)
def test_jsonschema_to_sql(connector: SnowflakeConnector, schema: dict, expected_type: type[types.TypeEngine]):
    sql_type = connector.to_sql_type(schema)
    assert isinstance(sql_type, expected_type)


@pytest.mark.parametrize(
    ("config", "expected_type"),
    [
        ({"timestamp_type": SnowflakeTimestampType.TIMESTAMP_TZ}, sct.TIMESTAMP_TZ),
        ({"timestamp_type": SnowflakeTimestampType.TIMESTAMP_LTZ}, sct.TIMESTAMP_LTZ),
        ({"timestamp_type": SnowflakeTimestampType.TIMESTAMP_NTZ}, sct.TIMESTAMP_NTZ),
    ],
)
def test_datetime_to_sql(connector: SnowflakeConnector, config: dict, expected_type: type[types.TypeEngine]):
    connector.config.update(config)
    schema = {"type": ["string", "null"], "format": "date-time"}
    sql_type = connector.to_sql_type(schema)
    assert isinstance(sql_type, expected_type)


def test_uuid_to_sql_defaults_to_native(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "uuid"})
    assert isinstance(sql_type, types.UUID)


def test_uuid_to_sql_as_string(connector: SnowflakeConnector):
    connector.config.update({"uuid_format": "string"})
    sql_type = connector.to_sql_type({"type": "string", "format": "uuid"})
    assert isinstance(sql_type, sct.STRING)
    assert sql_type.length == 36


def test_to_sql_type_with_max_varchar_length(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "maxLength": 1_000_000})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 1_000_000

    sql_type = connector.to_sql_type({"type": "string", "maxLength": SnowflakeConnector.max_varchar_length + 1})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == SnowflakeConnector.max_varchar_length


def test_email_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "email"})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 254


def test_uri_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "uri"})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 2083


def test_hostname_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "hostname"})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 253


def test_ipv4_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "ipv4"})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 15


def test_ipv6_format(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type({"type": "string", "format": "ipv6"})
    assert isinstance(sql_type, types.VARCHAR)
    assert sql_type.length == 45


def test_singer_decimal(connector: SnowflakeConnector):
    sql_type = connector.to_sql_type(
        {
            "type": "string",
            "format": "x-singer.decimal",
            "precision": 38,
            "scale": 18,
        },
    )
    assert isinstance(sql_type, types.DECIMAL)
    assert sql_type.precision == 38
    assert sql_type.scale == 18


@pytest.mark.parametrize(
    ("config", "identifier", "expected_formatted"),
    [
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "email", "email"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "email_address", "email_address"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "emailAddress", "emailAddress"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "EmailAddress", "EmailAddress"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "EMAIL_ADDRESS", "EMAIL_ADDRESS"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "EMAILADDRESS", "EMAILADDRESS"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": False}, "user", "user"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "email", "email"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "email_address", "email_address"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "emailAddress", "emailaddress"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "EmailAddress", "emailaddress"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "EMAIL_ADDRESS", "email_address"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "EMAILADDRESS", "emailaddress"),
        ({"normalise_casing": False, "quoted_identifiers_ignore_case": True}, "user", "USER"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "email", "email"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "email_address", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "emailAddress", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "EmailAddress", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "EMAIL_ADDRESS", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "EMAILADDRESS", "emailaddress"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": False}, "user", "user"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "email", "email"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "email_address", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "emailAddress", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "EmailAddress", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "EMAIL_ADDRESS", "email_address"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "EMAILADDRESS", "emailaddress"),
        ({"normalise_casing": True, "quoted_identifiers_ignore_case": True}, "user", "USER"),
    ],
)
@pytest.mark.usefixtures("mock_engine_connect")
def test_format_identifier(
    connectable_connector: SnowflakeConnector,
    config: dict,
    identifier: str,
    expected_formatted: str,
):
    connectable_connector.config.update(config)
    assert connectable_connector.format_identifier(identifier) == expected_formatted


@pytest.mark.parametrize(
    "column_name",
    [
        pytest.param("Order Total [USD]", id="spaces_brackets_mixed_case"),
        pytest.param("customer:id", id="colon"),
        pytest.param("field with spaces", id="spaces"),
        pytest.param("Nested[0]", id="brackets"),
        pytest.param("ns:Attribute [meta]", id="colon_spaces_brackets"),
        pytest.param("hyphenated-column", id="hypthenated"),
        pytest.param("user", id="reserved_word"),
    ],
)
@pytest.mark.usefixtures("mock_engine_connect")
def test_prepare_column_matches_existing_quoted_column(
    connectable_connector: SnowflakeConnector,
    column_name: str,
):
    # Columns needing quoting must not be re-added when they already exist.
    connectable_connector.config.update(
        {"quoted_identifiers_ignore_case": True, "normalise_casing": False},
    )

    # The name Snowflake reports for a column originally created as a quoted identifier.
    stored_name = column_name.upper()
    existing_columns = {stored_name: sqlalchemy.Column(stored_name, types.VARCHAR())}

    with (
        mock.patch.object(connectable_connector, "get_table_columns", return_value=existing_columns),
        mock.patch.object(connectable_connector, "_create_empty_column") as create_column,
        mock.patch.object(connectable_connector, "_adapt_column_type") as adapt_column,
    ):
        connectable_connector.prepare_column(
            "TEST_DATABASE.TEST_SCHEMA.TEST_TABLE",
            column_name,
            types.VARCHAR(),
        )

    create_column.assert_not_called()
    adapt_column.assert_called_once()


@pytest.mark.parametrize(
    ("column_name", "reported_name"),
    [
        pytest.param("plain_column", "plain_column", id="plain"),
        pytest.param("MixedCase", "mixedcase", id="mixed_case"),
        pytest.param("emailAddress", "emailaddress", id="camel_case"),
        pytest.param("EMAIL_ADDRESS", "email_address", id="upper_case"),
    ],
)
@pytest.mark.usefixtures("mock_engine_connect")
def test_prepare_column_matches_existing_unquoted_column(
    connectable_connector: SnowflakeConnector,
    column_name: str,
    reported_name: str,
):
    # Guards the quoted-identifier fix against over-correcting. Snowflake stores these
    # upper case too, but because the lower-case form needs no quoting `normalize_name`
    # hands them back lowercased, so they must still be compared in lower case.
    connectable_connector.config.update(
        {"quoted_identifiers_ignore_case": True, "normalise_casing": False},
    )

    existing_columns = {reported_name: sqlalchemy.Column(reported_name, types.VARCHAR())}

    with (
        mock.patch.object(connectable_connector, "get_table_columns", return_value=existing_columns),
        mock.patch.object(connectable_connector, "_create_empty_column") as create_column,
        mock.patch.object(connectable_connector, "_adapt_column_type") as adapt_column,
    ):
        connectable_connector.prepare_column(
            "TEST_DATABASE.TEST_SCHEMA.TEST_TABLE",
            column_name,
            types.VARCHAR(),
        )

    create_column.assert_not_called()
    adapt_column.assert_called_once()


def test_invalidate_table_cache_drops_entry_and_resets_inspector(connector: SnowflakeConnector):
    connector.table_cache["db.schema.table"] = {"col": object()}
    connector._inspector = mock.Mock(spec=sqlalchemy.Inspector)  # noqa: SLF001

    connector.invalidate_table_cache("db.schema.table")

    assert "db.schema.table" not in connector.table_cache
    assert connector._inspector is None  # noqa: SLF001


def test_invalidate_table_cache_leaves_other_tables_cached(connector: SnowflakeConnector):
    connector.table_cache["db.schema.users"] = {"id": object()}
    connector.table_cache["db.schema.posts"] = {"id": object()}

    connector.invalidate_table_cache("db.schema.users")

    assert "db.schema.users" not in connector.table_cache
    assert "db.schema.posts" in connector.table_cache
