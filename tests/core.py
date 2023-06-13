import pytest
import snowflake.sqlalchemy.custom_types as sct
import sqlalchemy
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


class SnowflakeTargetArrayData(TargetArrayData):
    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.test_{self.name}".upper()
        result = connector.connection.execute(
            f"select * from {table}",
        )
        assert result.rowcount == 4
        row = result.first()
        assert len(row) == 8
        assert row[1] == '[\n  "apple",\n  "orange",\n  "pear"\n]'
        table_schema = connector.get_table(table)
        expected_types = {
            "id": sct._CUSTOM_DECIMAL,
            "fruits": sct.VARIANT,
            "_sdc_extracted_at": sct.TIMESTAMP_NTZ,
            "_sdc_batched_at": sct.TIMESTAMP_NTZ,
            "_sdc_received_at": sct.TIMESTAMP_NTZ,
            "_sdc_deleted_at": sct.TIMESTAMP_NTZ,
            "_sdc_table_version": sct.NUMBER,
            "_sdc_sequence": sct.NUMBER,
        }
        for column in table_schema.columns:
            assert column.name in expected_types
            isinstance(column.type, expected_types[column.name])


class SnowflakeTargetCamelcaseComplexSchema(TargetCamelcaseComplexSchema):
    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.ForecastingTypeToCategory".upper()
        table_schema = connector.get_table(table)
        expected_types = {
            "id": sct._CUSTOM_DECIMAL,
            "isdeleted": sqlalchemy.types.BOOLEAN,
            "createddate": sct.TIMESTAMP_NTZ,
            "createdbyid": sct.STRING,
            "lastmodifieddate": sct.TIMESTAMP_NTZ,
            "lastmodifiedbyid": sct.STRING,
            "systemmodstamp": sct.TIMESTAMP_NTZ,
            "forecastingtypeid": sct.STRING,
            "forecastingitemcategory": sct.STRING,
            "displayposition": sct.NUMBER,
            "isadjustable": sqlalchemy.types.BOOLEAN,
            "isowneradjustable": sqlalchemy.types.BOOLEAN,
            "age": sct.NUMBER,
            "newcamelcasedattribute": sct.STRING,
            "_sdc_extracted_at": sct.TIMESTAMP_NTZ,
            "_sdc_batched_at": sct.TIMESTAMP_NTZ,
            "_sdc_received_at": sct.TIMESTAMP_NTZ,
            "_sdc_deleted_at": sct.TIMESTAMP_NTZ,
            "_sdc_table_version": sct.NUMBER,
            "_sdc_sequence": sct.NUMBER,
        }
        for column in table_schema.columns:
            assert column.name in expected_types
            isinstance(column.type, expected_types[column.name])


class SnowflakeTargetDuplicateRecords(TargetDuplicateRecords):
    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.test_duplicate_records".upper()
        result = connector.connection.execute(
            f"select * from {table}",
        )
        expected_value = {
            1: 100,
            2: 20,
        }
        assert result.rowcount == 2
        for row in result:
            assert len(row) == 8
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
            "_sdc_table_version": sct.NUMBER,
            "_sdc_sequence": sct.NUMBER,
        }
        for column in table_schema.columns:
            assert column.name in expected_types
            isinstance(column.type, expected_types[column.name])


class SnowflakeTargetCamelcaseTest(TargetCamelcaseTest):
    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.TestCamelcase".upper()
        connector.connection.execute(
            f"select * from {table}",
        )

        table_schema = connector.get_table(table)
        expected_types = {
            "id": sct.STRING,
            "clientname": sct.STRING,
            "_sdc_extracted_at": sct.TIMESTAMP_NTZ,
            "_sdc_batched_at": sct.TIMESTAMP_NTZ,
            "_sdc_received_at": sct.TIMESTAMP_NTZ,
            "_sdc_deleted_at": sct.TIMESTAMP_NTZ,
            "_sdc_table_version": sct.NUMBER,
            "_sdc_sequence": sct.NUMBER,
        }
        for column in table_schema.columns:
            assert column.name in expected_types
            isinstance(column.type, expected_types[column.name])


class SnowflakeTargetEncodedStringData(TargetEncodedStringData):
    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        for table_name in [
            "test_strings",
            "test_strings_in_objects",
            "test_strings_in_arrays",
        ]:
            table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{table_name}".upper()
            connector.connection.execute(
                f"select * from {table}",
            )
            # TODO: more assertions


class SnowflakeTargetInvalidSchemaTest(TargetInvalidSchemaTest):
    def test(self) -> None:
        with pytest.raises(Exception):
            self.runner.sync_all()


class SnowflakeTargetRecordBeforeSchemaTest(TargetRecordBeforeSchemaTest):
    def test(self) -> None:
        with pytest.raises(Exception):
            self.runner.sync_all()


class SnowflakeTargetRecordMissingKeyProperty(TargetRecordMissingKeyProperty):
    def test(self) -> None:
        # TODO: try to catch exact exception, currently snowflake throws an integrity error
        with pytest.raises(Exception):
            self.runner.sync_all()


class SnowflakeTargetRecordMissingRequiredProperty(TargetRecordMissingRequiredProperty):
    def test(self) -> None:
        with pytest.raises(Exception):
            self.runner.sync_all()


class SnowflakeTargetSchemaNoProperties(TargetSchemaNoProperties):
    def validate(self) -> None:
        for table_name in [
            "test_object_schema_with_properties",
            "test_object_schema_no_properties",
        ]:
            connector = self.target.default_sink_class.connector_class(
                self.target.config
            )
            table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.{table_name}".upper()
            result = connector.connection.execute(
                f"select * from {table}",
            )
            assert result.rowcount == 2
            row = result.first()
            assert len(row) == 7
            table_schema = connector.get_table(table)
            expected_types = {
                "object_store": sct.VARIANT,
                "_sdc_extracted_at": sct.TIMESTAMP_NTZ,
                "_sdc_batched_at": sct.TIMESTAMP_NTZ,
                "_sdc_received_at": sct.TIMESTAMP_NTZ,
                "_sdc_deleted_at": sct.TIMESTAMP_NTZ,
                "_sdc_table_version": sct.NUMBER,
                "_sdc_sequence": sct.NUMBER,
            }
            for column in table_schema.columns:
                assert column.name in expected_types
                isinstance(column.type, expected_types[column.name])


class SnowflakeTargetSchemaUpdates(TargetSchemaUpdates):
    def validate(self) -> None:
        connector = self.target.default_sink_class.connector_class(self.target.config)
        table = f"{self.target.config['database']}.{self.target.config['default_target_schema']}.test_schema_updates".upper()
        result = connector.connection.execute(
            f"select * from {table}",
        )
        assert result.rowcount == 6
        row = result.first()
        assert len(row) == 13
        table_schema = connector.get_table(table)
        expected_types = {
            "id": sct.NUMBER,
            "a1": sct.NUMBER,
            "a2": sct.STRING,
            "a3": sqlalchemy.types.BOOLEAN,
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
            assert column.name in expected_types
            isinstance(column.type, expected_types[column.name])


target_tests = TestSuite(
    kind="target",
    tests=[
        # Core
        SnowflakeTargetArrayData,
        SnowflakeTargetCamelcaseComplexSchema,
        SnowflakeTargetCamelcaseTest,
        TargetCliPrintsTest,
        # TODO: bug https://github.com/MeltanoLabs/target-snowflake/issues/41
        # SnowflakeTargetDuplicateRecords,
        SnowflakeTargetEncodedStringData,
        SnowflakeTargetInvalidSchemaTest,
        # Not available in the SDK yet
        # TargetMultipleStateMessages,
        TargetNoPrimaryKeys,  # Implicitly asserts no pk is handled
        TargetOptionalAttributes,  # Implicitly asserts that nullable fields are handled
        SnowflakeTargetRecordBeforeSchemaTest,
        SnowflakeTargetRecordMissingKeyProperty,
        SnowflakeTargetRecordMissingRequiredProperty,
        SnowflakeTargetSchemaNoProperties,
        SnowflakeTargetSchemaUpdates,
        TargetSpecialCharsInAttributes,  # Implicitly asserts that special chars are handled
    ],
)
