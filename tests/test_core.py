"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from singer_sdk.testing import TargetTestRunner, get_test_class

from target_snowflake.target import TargetSnowflake

from .test_impl import target_tests

SAMPLE_CONFIG: dict[str, Any] = {
    "user": os.environ["TARGET_SNOWFLAKE_USER"],
    "password": os.environ["TARGET_SNOWFLAKE_PASSWORD"],
    "account": os.environ["TARGET_SNOWFLAKE_ACCOUNT"],
    "database": os.environ["TARGET_SNOWFLAKE_DATABASE"],
    "warehouse": os.environ["TARGET_SNOWFLAKE_WAREHOUSE"],
    "role": os.environ["TARGET_SNOWFLAKE_ROLE"],
    "schema": "PUBLIC",
    "default_target_schema": f"TARGET_SNOWFLAKE_{uuid.uuid4().hex[0:6]!s}",
    "add_record_metadata": False,
}


# TODO: replace when upstream issue resolves
# https://github.com/meltano/sdk/pull/1752
class CustomRunner(TargetTestRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def sync_all(self, *args, **kwargs):
        try:
            super().sync_all(*args, **kwargs)
        finally:
            self.target_input = None


# Custom so I can implement all validate methods
StandardTargetTests = get_test_class(
    test_runner=CustomRunner(
        target_class=TargetSnowflake,
        config=SAMPLE_CONFIG,
    ),
    test_suites=[target_tests],
    suite_config=None,
)


class TestTargetSnowflake(StandardTargetTests):  # type: ignore[misc, valid-type]  # noqa: E501
    """Standard Target Tests."""

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
