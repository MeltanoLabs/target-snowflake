from __future__ import annotations

from pathlib import Path

import pytest
import snowflake.sqlalchemy.custom_types as sct
import sqlalchemy as sa
from singer_sdk.testing.suites import TestSuite
from singer_sdk.testing.target_tests import (
    TargetArrayData,
    TargetCamelcaseComplexSchema,
    TargetCamelcaseTest,
    TargetCliPrintsTest,
    TargetDuplicateRecords,
    TargetEncodedStringData,
    TargetInvalidSchemaTest,
    TargetNoPrimaryKeys,
    TargetOptionalAttributes,
    TargetRecordBeforeSchemaTest,
    TargetRecordMissingKeyProperty,
    TargetRecordMissingRequiredProperty,
    TargetSchemaNoProperties,
    TargetSchemaUpdates,
    TargetSpecialCharsInAttributes,
)
from singer_sdk.testing.templates import TargetFileTestTemplate


class SnowflakeTargetArrayData(TargetArrayData):
    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = (
            f"{self.target.config['database']}.{self.target.config['default_target_schema']}.test_{self.name}".upper()
        )
        result = connector.connection.execute(
            sa.text(f"select * from {table} order by 1"),
        )
        assert result.rowcount == 4
        row = result.first()
        if self.target.config.get("add_record_metadata", True):
            assert len(row) == 9, f"Row has unexpected length {len(row)}"
        else:
            assert len(row) == 2, f"Row has unexpected length {len(row)}"

        assert row[1] == '[\n  "apple",\n  "orange",\n  "pear"\n]'
        table_schema = connector.get_table(table)
        expected_types = {
            "id": sa.DECIMAL,
            "fruits": sct.VARIANT,
            "_sdc_extracted_at": sct.TIMESTAMP_NTZ,
            "_sdc_batched_at": sct.TIMESTAMP_NTZ,
            "_sdc_received_at": sct.TIMESTAMP_NTZ,
            "_sdc_deleted_at": sct.TIMESTAMP_NTZ,
            "_sdc_sync_started_at": sct.NUMBER,
            "_sdc_table_version": sct.NUMBER,
            "_sdc_sequence": sct.NUMBER,
        }
        for column in table_schema.columns:
            assert column.name in expected_types, f"Column {column.name} not found in expected types"
            assert isinstance(
                column.type,
                expected_types[column.name],
            ), f"Column {column.name} is of unexpected type {column.type}"


class SnowflakeTargetCamelcaseComplexSchema(TargetCamelcaseComplexSchema):
    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.ForecastingTypeToCategory".upper()  # noqa: E501
        table_schema = connector.get_table(table)
        expected_types = {
            "id": sa.VARCHAR,
            "isdeleted": sa.types.BOOLEAN,
            "createddate": sct.TIMESTAMP_NTZ,
            "createdbyid": sct.STRING,
            "lastmodifieddate": sct.TIMESTAMP_NTZ,
            "lastmodifiedbyid": sct.STRING,
            "systemmodstamp": sct.TIMESTAMP_NTZ,
            "forecastingtypeid": sct.STRING,
            "forecastingitemcategory": sct.STRING,
            "displayposition": sct.NUMBER,
            "isadjustable": sa.types.BOOLEAN,
            "isowneradjustable": sa.types.BOOLEAN,
            "age": sct.NUMBER,
            "newcamelcasedattribute": sct.STRING,
            "_attribute_startswith_underscore": sct.STRING,
            "_sdc_extracted_at": sct.TIMESTAMP_NTZ,
            "_sdc_batched_at": sct.TIMESTAMP_NTZ,
            "_sdc_received_at": sct.TIMESTAMP_NTZ,
            "_sdc_deleted_at": sct.TIMESTAMP_NTZ,
            "_sdc_sync_started_at": sct.NUMBER,
            "_sdc_table_version": sct.NUMBER,
            "_sdc_sequence": sct.NUMBER,
        }
        for column in table_schema.columns:
            assert column.name in expected_types, f"Column {column.name} not found in expected types"
            assert isinstance(
                column.type,
                expected_types[column.name],
            ), f"Column {column.name} is of unexpected type {column.type}"


class SnowflakeTargetDuplicateRecords(TargetDuplicateRecords):
    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = (
            f"{self.target.config['database']}.{self.target.config['default_target_schema']}.test_{self.name}".upper()
        )
        result = connector.connection.execute(
            sa.text(f"select * from {table} order by 1"),
        )
        expected_value = {
            1: 100,
            2: 20,
        }
        assert result.rowcount == 2
        for row in result:
            assert len(row) == 9, f"Row has unexpected length {len(row)}"
            assert row[0] in expected_value
            assert expected_value.get(row[0]) == row[1]

        table_schema = connector.get_table(table)
        expected_types = {
            "id": sct.NUMBER,
            "metric": sct.NUMBER,
            "_sdc_extracted_at": sct.TIMESTAMP_NTZ,
            "_sdc_batched_at": sct.TIMESTAMP_NTZ,
            "_sdc_received_at": sct.TIMESTAMP_NTZ,
            "_sdc_deleted_at": sct.TIMESTAMP_NTZ,
            "_sdc_sync_started_at": sct.NUMBER,
            "_sdc_table_version": sct.NUMBER,
            "_sdc_sequence": sct.NUMBER,
        }
        for column in table_schema.columns:
            assert column.name in expected_types, f"Column {column.name} not found in expected types"
            assert isinstance(
                column.type,
                expected_types[column.name],
            ), f"Column {column.name} is of unexpected type {column.type}"


class SnowflakeTargetCamelcaseTest(TargetCamelcaseTest):
    @property
    def stream_name(self) -> str:
        return "TestCamelcase"

    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = (
            f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{self.stream_name}".upper()
        )
        connector.connection.execute(
            sa.text(f"select * from {table} order by 1"),
        )

        table_schema = connector.get_table(table)
        expected_types = {
            "id": sct.STRING,
            "clientname": sct.STRING,
            "_sdc_extracted_at": sct.TIMESTAMP_NTZ,
            "_sdc_batched_at": sct.TIMESTAMP_NTZ,
            "_sdc_received_at": sct.TIMESTAMP_NTZ,
            "_sdc_deleted_at": sct.TIMESTAMP_NTZ,
            "_sdc_sync_started_at": sct.NUMBER,
            "_sdc_table_version": sct.NUMBER,
            "_sdc_sequence": sct.NUMBER,
        }
        for column in table_schema.columns:
            assert column.name in expected_types, f"Column {column.name} not found in expected types"
            assert isinstance(
                column.type,
                expected_types[column.name],
            ), f"Column {column.name} is of unexpected type {column.type}"


class SnowflakeTargetEncodedStringData(TargetEncodedStringData):
    @property
    def stream_names(self) -> list[str]:
        return ["test_strings", "test_strings_in_objects", "test_strings_in_arrays"]

    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        for table_name in self.stream_names:
            table = (
                f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{table_name}".upper()
            )
            connector.connection.execute(
                sa.text(f"select * from {table} order by 1"),
            )
            # TODO: more assertions


class SnowflakeTargetInvalidSchemaTest(TargetInvalidSchemaTest):
    def test(self) -> None:
        with pytest.raises(Exception):  # noqa: B017, PT011
            self.runner.sync_all()


class SnowflakeTargetRecordBeforeSchemaTest(TargetRecordBeforeSchemaTest):
    def test(self) -> None:
        with pytest.raises(Exception):  # noqa: B017, PT011
            self.runner.sync_all()


class SnowflakeTargetRecordMissingKeyProperty(TargetRecordMissingKeyProperty):
    def test(self) -> None:
        # TODO: catch exact exception, currently snowflake throws an integrity error
        with pytest.raises(Exception):  # noqa: B017, PT011
            self.runner.sync_all()


# Snowflake does not support ignoring missing keys when copying/selecting JSON
# data from staged files. We should make a CSV batcher and use
#  `ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE`
class SnowflakeTargetOptionalAttributes(TargetOptionalAttributes):
    def test(self) -> None:
        with pytest.raises(Exception):  # noqa: B017, PT011
            self.runner.sync_all()


class SnowflakeTargetRecordMissingRequiredProperty(TargetRecordMissingRequiredProperty):
    def test(self) -> None:
        with pytest.raises(Exception):  # noqa: B017, PT011
            self.runner.sync_all()


class SnowflakeTargetSchemaNoProperties(TargetSchemaNoProperties):
    @property
    def stream_names(self) -> list[str]:
        return [
            "test_object_schema_with_properties",
            "test_object_schema_no_properties",
        ]

    def validate(self) -> None:
        for table_name in self.stream_names:
            connector = self.target.default_sink_class.connector_class(
                self.target.config,
            )
            table = (
                f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{table_name}".upper()
            )
            result = connector.connection.execute(
                sa.text(f"select * from {table} order by 1"),
            )
            assert result.rowcount == 2
            row = result.first()
            if self.target.config.get("add_record_metadata", True):
                assert len(row) == 8, f"Row has unexpected length {len(row)}"
            else:
                assert len(row) == 1, f"Row has unexpected length {len(row)}"

            table_schema = connector.get_table(table)
            expected_types = {
                "object_store": sct.VARIANT,
                "_sdc_extracted_at": sct.TIMESTAMP_NTZ,
                "_sdc_batched_at": sct.TIMESTAMP_NTZ,
                "_sdc_received_at": sct.TIMESTAMP_NTZ,
                "_sdc_deleted_at": sct.TIMESTAMP_NTZ,
                "_sdc_sync_started_at": sct.NUMBER,
                "_sdc_table_version": sct.NUMBER,
                "_sdc_sequence": sct.NUMBER,
            }
            for column in table_schema.columns:
                assert column.name in expected_types, f"Column {column.name} not found in expected types"
                assert isinstance(
                    column.type,
                    expected_types[column.name],
                ), f"Column {column.name} is of unexpected type {column.type}"


class SnowflakeTargetSchemaUpdates(TargetSchemaUpdates):
    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = (
            f"{self.target.config['database']}.{self.target.config['default_target_schema']}.test_{self.name}".upper()
        )
        result = connector.connection.execute(
            sa.text(f"select * from {table} order by 1"),
        )
        assert result.rowcount == 6
        row = result.first()

        if self.target.config.get("add_record_metadata", True):
            assert len(row) == 13, f"Row has unexpected length {len(row)}"
        else:
            assert len(row) == 7, f"Row has unexpected length {len(row)}"

        table_schema = connector.get_table(table)
        expected_types = {
            "id": sct.NUMBER,
            "a1": sct.DOUBLE,
            "a2": sct.STRING,
            "a3": sa.types.BOOLEAN,
            "a4": sct.VARIANT,
            "a5": sct.VARIANT,
            "a6": sct.NUMBER,
            "_sdc_extracted_at": sct.TIMESTAMP_NTZ,
            "_sdc_batched_at": sct.TIMESTAMP_NTZ,
            "_sdc_received_at": sct.TIMESTAMP_NTZ,
            "_sdc_deleted_at": sct.TIMESTAMP_NTZ,
            "_sdc_table_version": sct.NUMBER,
            "_sdc_sequence": sct.NUMBER,
        }
        for column in table_schema.columns:
            assert column.name in expected_types, f"Column {column.name} not found in expected types"
            assert isinstance(
                column.type,
                expected_types[column.name],
            ), f"Column {column.name} is of unexpected type {column.type}"


class SnowflakeTargetReservedWords(TargetFileTestTemplate):
    # Contains reserved words from
    # https://docs.snowflake.com/en/sql-reference/reserved-keywords
    # Syncs records then alters schema by adding a non-reserved word column.
    name = "reserved_words"

    @property
    def singer_filepath(self) -> Path:
        current_dir = Path(__file__).resolve().parent
        return current_dir / "target_test_streams" / f"{self.name}.singer"

    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{self.name}".upper()
        result = connector.connection.execute(
            sa.text(f"select * from {table}"),
        )
        assert result.rowcount == 2
        row = result.first()
        assert len(row) == 12, f"Row has unexpected length {len(row)}"


class SnowflakeTargetReservedWordsNoKeyProps(TargetFileTestTemplate):
    # Contains reserved words from
    # https://docs.snowflake.com/en/sql-reference/reserved-keywords
    # TODO: Syncs records then alters schema by adding a non-reserved word column.
    name = "reserved_words_no_key_props"

    @property
    def singer_filepath(self) -> Path:
        current_dir = Path(__file__).resolve().parent
        return current_dir / "target_test_streams" / f"{self.name}.singer"

    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{self.name}".upper()
        result = connector.connection.execute(
            sa.text(f"select * from {table}"),
        )
        assert result.rowcount == 1
        row = result.first()
        assert len(row) == 11, f"Row has unexpected length {len(row)}"


class SnowflakeTargetColonsInColName(TargetFileTestTemplate):
    name = "colons_in_col_name"

    @property
    def singer_filepath(self) -> Path:
        current_dir = Path(__file__).resolve().parent
        return current_dir / "target_test_streams" / f"{self.name}.singer"

    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{self.name}".upper()
        result = connector.connection.execute(
            sa.text(f"select * from {table}"),
        )
        assert result.rowcount == 1
        row = result.first()
        assert len(row) == 12, f"Row has unexpected length {len(row)}"
        table_schema = connector.get_table(table)
        assert {column.name for column in table_schema.columns} == {
            "FOO::BAR",
            "interval",
            "enabled",
            "LOWERCASE_VAL::ANOTHER_VAL",
            "event",
            "_sdc_extracted_at",
            "_sdc_batched_at",
            "_sdc_received_at",
            "_sdc_deleted_at",
            "_sdc_sync_started_at",
            "_sdc_table_version",
            "_sdc_sequence",
        }


class SnowflakeTargetExistingTable(TargetFileTestTemplate):
    name = "existing_table"

    @property
    def singer_filepath(self) -> Path:
        current_dir = Path(__file__).resolve().parent
        return current_dir / "target_test_streams" / f"{self.name}.singer"

    def setup(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{self.name}".upper()
        connector.connection.execute(
            sa.text(f"""
            CREATE OR REPLACE TABLE {table} (
                ID VARCHAR(16777216),
                COL_STR VARCHAR(16777216),
                COL_TS TIMESTAMP_NTZ(9),
                COL_INT INTEGER,
                COL_BOOL BOOLEAN,
                COL_VARIANT VARIANT,
                _SDC_BATCHED_AT TIMESTAMP_NTZ(9),
                _SDC_DELETED_AT VARCHAR(16777216),
                _SDC_EXTRACTED_AT TIMESTAMP_NTZ(9),
                _SDC_RECEIVED_AT TIMESTAMP_NTZ(9),
                _SDC_SEQUENCE NUMBER(38,0),
                _SDC_TABLE_VERSION NUMBER(38,0),
                PRIMARY KEY (ID)
            )
            """),
        )

    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{self.name}".upper()
        result = connector.connection.execute(
            sa.text(f"select * from {table}"),
        )
        assert result.rowcount == 1
        row = result.first()
        assert len(row) == 13, f"Row has unexpected length {len(row)}"


class SnowflakeTargetExistingTableAlter(SnowflakeTargetExistingTable):
    name = "existing_table_alter"
    # This sends a schema that will request altering from TIMESTAMP_NTZ to VARCHAR

    def setup(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{self.name}".upper()
        connector.connection.execute(
            sa.text(f"""
            CREATE OR REPLACE TABLE {table} (
                ID VARCHAR(16777216),
                COL_STR VARCHAR(16777216),
                COL_TS TIMESTAMP_NTZ(9),
                COL_INT STRING,
                COL_BOOL BOOLEAN,
                COL_VARIANT VARIANT,
                _SDC_BATCHED_AT TIMESTAMP_NTZ(9),
                _SDC_DELETED_AT VARCHAR(16777216),
                _SDC_EXTRACTED_AT TIMESTAMP_NTZ(9),
                _SDC_RECEIVED_AT TIMESTAMP_NTZ(9),
                _SDC_SEQUENCE NUMBER(38,0),
                _SDC_TABLE_VERSION NUMBER(38,0),
                PRIMARY KEY (ID)
            )
            """),
        )


class SnowflakeTargetExistingReservedNameTableAlter(TargetFileTestTemplate):
    name = "existing_reserved_name_table_alter"
    # This sends a schema that will request altering from TIMESTAMP_NTZ to VARCHAR

    @property
    def singer_filepath(self) -> Path:
        current_dir = Path(__file__).resolve().parent
        return current_dir / "target_test_streams" / "reserved_words_in_table.singer"

    def setup(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f'{self.target.config["database"]}.{self.target.config["default_target_schema"]}."order"'.upper()
        connector.connection.execute(
            sa.text(f"""
            CREATE OR REPLACE TABLE {table} (
                ID VARCHAR(16777216),
                COL_STR VARCHAR(16777216),
                COL_TS TIMESTAMP_NTZ(9),
                COL_INT STRING,
                COL_BOOL BOOLEAN,
                COL_VARIANT VARIANT,
                _SDC_BATCHED_AT TIMESTAMP_NTZ(9),
                _SDC_DELETED_AT VARCHAR(16777216),
                _SDC_EXTRACTED_AT TIMESTAMP_NTZ(9),
                _SDC_RECEIVED_AT TIMESTAMP_NTZ(9),
                _SDC_SEQUENCE NUMBER(38,0),
                _SDC_TABLE_VERSION NUMBER(38,0),
                PRIMARY KEY (ID)
            )
            """),
        )


class SnowflakeTargetReservedWordsInTable(TargetFileTestTemplate):
    # Contains reserved words from
    # https://docs.snowflake.com/en/sql-reference/reserved-keywords
    # Syncs records then alters schema by adding a non-reserved word column.
    name = "reserved_words_in_table"

    @property
    def singer_filepath(self) -> Path:
        current_dir = Path(__file__).resolve().parent
        return current_dir / "target_test_streams" / "reserved_words_in_table.singer"

    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f'{self.target.config["database"]}.{self.target.config["default_target_schema"]}."order"'.upper()
        result = connector.connection.execute(sa.text(f"select * from {table}"))
        assert result.rowcount == 1
        row = result.first()
        assert len(row) == 13, f"Row has unexpected length {len(row)}"


class SnowflakeTargetTypeEdgeCasesTest(TargetFileTestTemplate):
    name = "type_edge_cases"

    @property
    def singer_filepath(self) -> Path:
        current_dir = Path(__file__).resolve().parent
        return current_dir / "target_test_streams" / f"{self.name}.singer"

    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{self.name}".upper()
        table_schema = connector.get_table(table)
        expected_types = {
            "id": sct.NUMBER,
            "col_max_length_str": sct.STRING,
            "col_multiple_of": sct.DOUBLE,
            "col_multiple_of_int": sct.DOUBLE,
            "_sdc_extracted_at": sct.TIMESTAMP_NTZ,
            "_sdc_batched_at": sct.TIMESTAMP_NTZ,
            "_sdc_received_at": sct.TIMESTAMP_NTZ,
            "_sdc_deleted_at": sct.TIMESTAMP_NTZ,
            "_sdc_sync_started_at": sct.NUMBER,
            "_sdc_table_version": sct.NUMBER,
            "_sdc_sequence": sct.NUMBER,
        }
        for column in table_schema.columns:
            assert column.name in expected_types, f"Column {column.name} not found in expected types"
            assert isinstance(
                column.type,
                expected_types[column.name],
            ), f"Column {column.name} is of unexpected type {column.type}"


class SnowflakeTargetColumnOrderMismatch(TargetFileTestTemplate):
    name = "column_order_mismatch"

    def setup(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{self.name}".upper()
        # Seed the 2 columns from tap schema and an unused third column to assert explicit inserts are working
        connector.connection.execute(
            sa.text(f"""
            CREATE OR REPLACE TABLE {table} (
                COL1 VARCHAR(16777216),
                COL3 TIMESTAMP_NTZ(9),
                COL2 BOOLEAN
            )
            """),
        )

    @property
    def singer_filepath(self) -> Path:
        current_dir = Path(__file__).resolve().parent
        return current_dir / "target_test_streams" / f"{self.name}.singer"


target_tests = TestSuite(
    kind="target",
    tests=[
        # Core
        SnowflakeTargetArrayData,
        SnowflakeTargetCamelcaseComplexSchema,
        SnowflakeTargetCamelcaseTest,
        TargetCliPrintsTest,
        SnowflakeTargetDuplicateRecords,
        SnowflakeTargetEncodedStringData,
        SnowflakeTargetInvalidSchemaTest,
        # TODO: Not available in the SDK yet
        # TargetMultipleStateMessages,
        TargetNoPrimaryKeys,  # Implicitly asserts no pk is handled
        TargetOptionalAttributes,  # Implicitly asserts nullable fields handled
        SnowflakeTargetRecordBeforeSchemaTest,
        SnowflakeTargetRecordMissingKeyProperty,
        SnowflakeTargetRecordMissingRequiredProperty,
        SnowflakeTargetSchemaNoProperties,
        # SnowflakeTargetSchemaUpdates,
        TargetSpecialCharsInAttributes,  # Implicitly asserts special chars handled
        SnowflakeTargetReservedWords,
        SnowflakeTargetReservedWordsNoKeyProps,
        SnowflakeTargetColonsInColName,
        SnowflakeTargetExistingTable,
        SnowflakeTargetExistingTableAlter,
        SnowflakeTargetExistingReservedNameTableAlter,
        SnowflakeTargetReservedWordsInTable,
        SnowflakeTargetTypeEdgeCasesTest,
        SnowflakeTargetColumnOrderMismatch,
    ],
)
