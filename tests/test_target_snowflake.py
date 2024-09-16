"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import copy
import os
import uuid
from typing import Any

import pytest
import sqlalchemy as sa
from singer_sdk.testing import TargetTestRunner, get_target_test_class

from target_snowflake.target import TargetSnowflake

from .batch import batch_target_tests
from .core import target_tests

SAMPLE_CONFIG: dict[str, Any] = {
    "user": os.environ["TARGET_SNOWFLAKE_USER"],
    "password": os.environ["TARGET_SNOWFLAKE_PASSWORD"],
    "account": os.environ["TARGET_SNOWFLAKE_ACCOUNT"],
    "database": os.environ["TARGET_SNOWFLAKE_DATABASE"],
    "warehouse": os.environ["TARGET_SNOWFLAKE_WAREHOUSE"],
    "role": os.environ["TARGET_SNOWFLAKE_ROLE"],
    "schema": "PUBLIC",
}


class BaseSnowflakeTargetTests:
    """Base class for Snowflake target tests."""

    @pytest.fixture
    def connection(self, runner):
        return runner.singer_class.default_sink_class.connector_class(
            runner.config,
        ).connection

    @pytest.fixture
    def resource(self, runner, connection):
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        connection.execute(
            sa.text(f"create schema {runner.config['database']}.{runner.config['default_target_schema']}"),
        )
        yield
        connection.execute(
            sa.text(f"drop schema if exists {runner.config['database']}.{runner.config['default_target_schema']}"),
        )


# Custom so I can implement all validate methods
STANDARD_TEST_CONFIG = copy.deepcopy(SAMPLE_CONFIG)
STANDARD_TEST_CONFIG["default_target_schema"] = f"TARGET_SNOWFLAKE_{uuid.uuid4().hex[0:6]!s}"
StandardTargetTests = get_target_test_class(
    target_class=TargetSnowflake,
    config=STANDARD_TEST_CONFIG,
    custom_suites=[target_tests],
    suite_config=None,
    include_target_tests=False,
)


class TestTargetSnowflake(BaseSnowflakeTargetTests, StandardTargetTests):  # type: ignore[misc, valid-type]
    """Standard Target Tests."""


# Custom so I can implement all validate methods
BATCH_TEST_CONFIG = copy.deepcopy(SAMPLE_CONFIG)
BATCH_TEST_CONFIG["default_target_schema"] = f"TARGET_SNOWFLAKE_{uuid.uuid4().hex[0:6]!s}"
BATCH_TEST_CONFIG["add_record_metadata"] = False
BatchTargetTests = get_target_test_class(
    target_class=TargetSnowflake,
    config=BATCH_TEST_CONFIG,
    custom_suites=[batch_target_tests],
    suite_config=None,
    include_target_tests=False,
)


class TestTargetSnowflakeBatch(BaseSnowflakeTargetTests, BatchTargetTests):  # type: ignore[misc, valid-type]
    """Batch Target Tests."""


def test_invalid_database():
    INVALID_TEST_CONFIG = copy.deepcopy(SAMPLE_CONFIG)  # noqa: N806
    INVALID_TEST_CONFIG["database"] = "FOO_BAR_DOESNT_EXIST"
    runner = TargetTestRunner(
        TargetSnowflake,
        config=INVALID_TEST_CONFIG,
        input_filepath="tests/target_test_streams/existing_table.singer",
    )
    with pytest.raises(Exception):  # noqa: B017, PT011
        runner.sync_all()
