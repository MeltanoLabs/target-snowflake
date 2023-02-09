"""Tests standard target features using the built-in SDK tests library."""

import os
from typing import Any, Dict

from singer_sdk.testing import get_standard_target_tests

from target_snowflake.target import TargetSnowflake

SAMPLE_CONFIG: Dict[str, Any] = {
    "user": os.environ["SF_USER"],
    "password": os.environ["SF_PASSWORD"],
    "account": os.environ["SF_ACCOUNT"],
    "database": os.getenv("SF_DATABASE"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "role": os.getenv("SF_ROLE"),
    "schema": "PYTEST_SCHEMA",
    "add_record_metadata": False,
}


StandardSnowflakeTests = get_standard_target_tests(
    target_class=TargetSnowflake, config=SAMPLE_CONFIG
)
