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
            "_sdc_sequence": sct.NUMBER
        }
        for column in table_schema.columns:
            assert column.name in expected_types
            isinstance(column.type, expected_types[column.name])

target_tests = TestSuite(
    kind="target",
    tests=[
        SnowflakeTargetArrayData,
        SnowflakeTargetCamelcaseComplexSchema,
        # TargetCamelcaseTest,
        TargetCliPrintsTest,
        TargetDuplicateRecords,
        TargetEncodedStringData,
        # TargetInvalidSchemaTest,
        # TargetMultipleStateMessages,
        TargetNoPrimaryKeys,
        TargetOptionalAttributes,
        # TargetRecordBeforeSchemaTest,
        # TargetRecordMissingKeyProperty,
        TargetSchemaNoProperties,
        # TargetSchemaUpdates,
        # TargetSpecialCharsInAttributes,
    ],
)
