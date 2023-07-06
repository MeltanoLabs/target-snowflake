"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import copy
import os
import uuid
from typing import Any

import pytest
from singer_sdk.testing import TargetTestRunner, get_test_class

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

    @pytest.fixture(scope="class")
    def connection(self, runner):
        return runner.singer_class.default_sink_class.connector_class(
            runner.config
        ).connection

    @pytest.fixture(scope="class")
    def resource(self, runner, connection):  # noqa: ANN201
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        connection.execute(
            f"create schema {runner.config['database']}.{runner.config['default_target_schema']}"
        )
        yield
        connection.execute(
            f"drop schema if exists {runner.config['database']}.{runner.config['default_target_schema']}"
        )


# Custom so I can implement all validate methods
STANDARD_TEST_CONFIG = copy.deepcopy(SAMPLE_CONFIG)
STANDARD_TEST_CONFIG[
    "default_target_schema"
] = f"TARGET_SNOWFLAKE_{uuid.uuid4().hex[0:6]!s}"
StandardTargetTests = get_test_class(
    test_runner=TargetTestRunner(
        target_class=TargetSnowflake,
        config=STANDARD_TEST_CONFIG,
    ),
    test_suites=[target_tests],
    suite_config=None,
)


class TestTargetSnowflake(BaseSnowflakeTargetTests, StandardTargetTests):  # type: ignore[misc, valid-type]  # noqa: E501
    """Standard Target Tests."""


# Custom so I can implement all validate methods
BATCH_TEST_CONFIG = copy.deepcopy(SAMPLE_CONFIG)
BATCH_TEST_CONFIG[
    "default_target_schema"
] = f"TARGET_SNOWFLAKE_{uuid.uuid4().hex[0:6]!s}"
BATCH_TEST_CONFIG["add_record_metadata"] = False
BatchTargetTests = get_test_class(
    test_runner=CustomRunner(
        target_class=TargetSnowflake,
        config=BATCH_TEST_CONFIG,
    ),
    test_suites=[batch_target_tests],
    suite_config=None,
)


class TestTargetSnowflakeBatch(BaseSnowflakeTargetTests, BatchTargetTests):  # type: ignore[misc, valid-type]  # noqa: E501
    """Batch Target Tests."""
