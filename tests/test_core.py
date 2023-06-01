"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import os
import typing as t
from typing import Any, Dict

import pytest
from singer_sdk.testing import get_target_test_class

from target_snowflake.target import TargetSnowflake

SAMPLE_CONFIG: Dict[str, Any] = {
    "user": os.environ["TARGET_SNOWFLAKE_USER"],
    "password": os.environ["TARGET_SNOWFLAKE_PASSWORD"],
    "account": os.environ["TARGET_SNOWFLAKE_ACCOUNT"],
    "database": os.environ["TARGET_SNOWFLAKE_DATABASE"],
    "warehouse": os.environ["TARGET_SNOWFLAKE_WAREHOUSE"],
    "role": os.environ["TARGET_SNOWFLAKE_ROLE"],
    "schema": "PYTEST_SCHEMA",
    "default_target_schema": "PYTEST_SCHEMA",
}


# Run standard built-in target tests from the SDK:
StandardTargetTests = get_target_test_class(
    target_class=TargetSnowflake,
    config=SAMPLE_CONFIG,
)


class TestTargetSnowflake(StandardTargetTests):  # type: ignore[misc, valid-type]  # noqa: E501
    """Standard Target Tests."""

    @pytest.fixture(scope="class")
    def resource(self):  # noqa: ANN201
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        return "resource"
