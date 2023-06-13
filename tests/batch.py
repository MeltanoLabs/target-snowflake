"""BATCH Tests for Target Snowflake."""

from pathlib import Path

from singer_sdk.testing.suites import TestSuite
from singer_sdk.testing.target_tests import (
    TargetNoPrimaryKeys,
    TargetOptionalAttributes,
    TargetSpecialCharsInAttributes,
)
from singer_sdk.testing.templates import TargetFileTestTemplate

from .core import (
    SnowflakeTargetArrayData,
    SnowflakeTargetCamelcaseTest,
    SnowflakeTargetDuplicateRecords,
    SnowflakeTargetEncodedStringData,
    SnowflakeTargetRecordBeforeSchemaTest,
    SnowflakeTargetRecordMissingKeyProperty,
    SnowflakeTargetRecordMissingRequiredProperty,
    SnowflakeTargetSchemaNoProperties,
    SnowflakeTargetSchemaUpdates,
)


class SnowflakeTargetCustomTestTemplate(TargetFileTestTemplate):
    @property
    def singer_filepath(self) -> Path:
        """Get path to singer JSONL formatted messages file.

        Files will be sourced from `./target_test_streams/<test name>.singer`.

        Returns:
            The expected Path to this tests singer file.
        """
        current_dir = Path(__file__).resolve().parent
        return current_dir / "target_test_streams" / f"{self.name}.singer"


class SnowflakeTargetBatchArrayData(
    SnowflakeTargetCustomTestTemplate, SnowflakeTargetArrayData
):
    """Test that the target can handle batch messages."""

    name = "batch_array_data"


class SnowflakeTargetBatchCamelcase(
    SnowflakeTargetCustomTestTemplate, SnowflakeTargetCamelcaseTest
):
    """Test that the target can handle batch messages."""

    name = "batch_camelcase"
    stream_name = "TestBatchCamelcase"


class SnowflakeTargetBatchDuplicateRecords(
    SnowflakeTargetCustomTestTemplate, SnowflakeTargetDuplicateRecords
):
    """Test that the target can handle batch messages."""

    name = "batch_duplicate_records"


class SnowflakeTargetBatchEncodedStringData(
    SnowflakeTargetCustomTestTemplate, SnowflakeTargetEncodedStringData
):
    """Test that the target can handle batch messages."""

    name = "batch_encoded_string_data"
    stream_names = [
        "test_batch_strings",
        "test_batch_strings_in_objects",
        "test_batch_strings_in_arrays",
    ]


# class SnowflakeTargetBatchMultipleStateMessages(
#     SnowflakeTargetCustomTestTemplate, TargetMultipleStateMessages
# ):
#     """Test that the target can handle batch messages."""

#     name = "batch_multiple_state_messages"


# class SnowflakeTargetBatchNoPrimaryKeysAppend(
#     SnowflakeTargetCustomTestTemplate, TargetNoPrimaryKeysAppend
# ):
#     """Test that the target can handle batch messages."""

#     name = "batch_no_primary_keys_append"


class SnowflakeTargetBatchNoPrimaryKeys(
    SnowflakeTargetCustomTestTemplate, TargetNoPrimaryKeys
):
    """Test that the target can handle batch messages."""

    name = "batch_no_primary_keys"


class SnowflakeTargetBatchOptionalAttributes(
    SnowflakeTargetCustomTestTemplate, TargetOptionalAttributes
):
    """Test that the target can handle batch messages."""

    name = "batch_optional_attributes"


class SnowflakeTargetBatchRecordBeforeSchemaTest(
    SnowflakeTargetCustomTestTemplate, SnowflakeTargetRecordBeforeSchemaTest
):
    """Test that the target can handle batch messages."""

    name = "batch_record_before_schema"


class SnowflakeTargetBatchRecordMissingKeyProperty(
    SnowflakeTargetCustomTestTemplate, SnowflakeTargetRecordMissingKeyProperty
):
    """Test that the target can handle batch messages."""

    name = "batch_record_missing_key_property"


class SnowflakeTargetBatchRecordMissingRequiredProperty(
    SnowflakeTargetCustomTestTemplate, SnowflakeTargetRecordMissingRequiredProperty
):
    """Test that the target can handle batch messages."""

    name = "batch_record_missing_required_property"


class SnowflakeTargetBatchSchemaNoProperties(
    SnowflakeTargetCustomTestTemplate, SnowflakeTargetSchemaNoProperties
):
    """Test that the target can handle batch messages."""

    name = "batch_schema_no_properties"
    stream_names = [
        "test_batch_object_schema_with_properties",
        "test_batch_object_schema_no_properties",
    ]


class SnowflakeTargetBatchSchemaUpdates(
    SnowflakeTargetCustomTestTemplate, SnowflakeTargetSchemaUpdates
):
    """Test that the target can handle batch messages."""

    name = "batch_schema_updates"


class SnowflakeTargetBatchSpecialCharsInAttributes(
    SnowflakeTargetCustomTestTemplate, TargetSpecialCharsInAttributes
):
    """Test that the target can handle batch messages."""

    name = "batch_special_chars_in_attributes"


batch_target_tests = TestSuite(
    kind="target",
    tests=[
        # BATCH
        # TODO: failing assertion
        # SnowflakeTargetBatchArrayData,
        SnowflakeTargetBatchCamelcase,
        # TODO: as above
        # SnowflakeTargetBatchDuplicateRecords,
        SnowflakeTargetBatchEncodedStringData,
        # TODO: as above
        # SnowflakeTargetBatchMultipleStateMessages,
        # TODO: in sdk but not enabled
        # SnowflakeTargetBatchNoPrimaryKeysAppend,
        SnowflakeTargetBatchNoPrimaryKeys,
        # TODO: failing on SQL Merge (NULLs in non-Nullable columns)
        # SnowflakeTargetBatchOptionalAttributes,
        SnowflakeTargetBatchRecordBeforeSchemaTest,
        SnowflakeTargetBatchRecordMissingKeyProperty,
        SnowflakeTargetBatchSchemaNoProperties,
        SnowflakeTargetBatchSchemaUpdates,
        SnowflakeTargetBatchSpecialCharsInAttributes,
    ],
)
