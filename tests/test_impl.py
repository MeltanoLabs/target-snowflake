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
        """Test validation, called after `.test()`.

        This method is particularly useful in Target tests, to validate that records
        were correctly written to external systems.

        Raises:
            NotImplementedError: if not implemented.
        """
        msg = "Method not implemented."
        raise NotImplementedError(msg)


target_tests = TestSuite(
    kind="target",
    tests=[
        SnowflakeTargetArrayData,
        TargetCamelcaseComplexSchema,
        TargetCamelcaseTest,
        TargetCliPrintsTest,
        TargetDuplicateRecords,
        TargetEncodedStringData,
        TargetInvalidSchemaTest,
        # TargetMultipleStateMessages,
        TargetNoPrimaryKeys,
        TargetOptionalAttributes,
        TargetRecordBeforeSchemaTest,
        TargetRecordMissingKeyProperty,
        TargetSchemaNoProperties,
        TargetSchemaUpdates,
        TargetSpecialCharsInAttributes,
    ],
)
