"""Tests standard target features using the built-in SDK tests library."""

import datetime
import os
from typing import Any, Dict

import pytest
from singer_sdk.testing import StandardSqlTargetTests

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


@pytest.fixture(scope="class")
def config():
    return SAMPLE_CONFIG


@pytest.fixture(scope="class")
def Target():
    return TargetSnowflake


@pytest.fixture(scope="class")
def sqlalchemy_connection(Target, config):
    return Target.default_sink_class.connector_class(config=config).connection


class TestTargetSnowflake(StandardSqlTargetTests):
    """Test Snowlflake using the standard SDK Target Tests."""
