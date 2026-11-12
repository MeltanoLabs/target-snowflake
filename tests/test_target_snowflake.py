# Copyright (C) 2026 Meltano.

"""Tests standard target features using the built-in SDK tests library."""

from __future__ import annotations

import copy
import io
import json
import os
import uuid
from typing import Any

import pyarrow as pa
import pytest
from dotenv import load_dotenv
from pyarrow import ipc
from singer_sdk.testing import TargetTestRunner, get_target_test_class
from sqlalchemy import text

from target_snowflake.connector import SnowflakeConnector
from target_snowflake.target import TargetSnowflake

from .batch import batch_target_tests
from .core import target_tests

load_dotenv()

SAMPLE_CONFIG: dict[str, Any] = {
    "user": os.environ["TARGET_SNOWFLAKE_USER"],
    "private_key": os.environ["TARGET_SNOWFLAKE_PRIVATE_KEY"],
    "account": os.environ["TARGET_SNOWFLAKE_ACCOUNT"],
    "database": os.environ["TARGET_SNOWFLAKE_DATABASE"],
    "warehouse": os.environ["TARGET_SNOWFLAKE_WAREHOUSE"],
    "role": os.environ["TARGET_SNOWFLAKE_ROLE"],
    "schema": os.environ["TARGET_SNOWFLAKE_SCHEMA"],
}


class BaseSnowflakeTargetTests:
    """Base class for Snowflake target tests."""

    @pytest.fixture
    def connection(self, runner):
        connector = runner.singer_class.default_sink_class.connector_class(runner.config)
        with connector.connect() as conn:
            yield conn

    @pytest.fixture
    def resource(self, runner, connection):
        """Generic external resource.

        This fixture is useful for setup and teardown of external resources,
        such output folders, tables, buckets etc. for use during testing.

        Example usage can be found in the SDK samples test suite:
        https://github.com/meltano/sdk/tree/main/tests/samples
        """
        connection.execute(
            text(f"create schema {runner.config['database']}.{runner.config['default_target_schema']}"),
        )
        yield
        connection.execute(
            text(f"drop schema if exists {runner.config['database']}.{runner.config['default_target_schema']}"),
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


class TestTargetSnowflake(BaseSnowflakeTargetTests, StandardTargetTests):  # type: ignore[misc, valid-type]  # ty:ignore[unsupported-base]
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


class TestTargetSnowflakeBatch(BaseSnowflakeTargetTests, BatchTargetTests):  # type: ignore[misc, valid-type]  # ty:ignore[unsupported-base]
    """Batch Target Tests."""


def test_invalid_database():
    INVALID_TEST_CONFIG = copy.deepcopy(SAMPLE_CONFIG)  # noqa: N806
    INVALID_TEST_CONFIG["database"] = "FOO_BAR_DOESNT_EXIST"
    runner = TargetTestRunner(
        TargetSnowflake,
        config=INVALID_TEST_CONFIG,
        input_filepath="tests/target_test_streams/existing_table.singer",  # ty:ignore[invalid-argument-type]
    )
    with pytest.raises(Exception):  # noqa: B017, PT011
        runner.sync_all()


def test_arrow_batch_loads_json_as_objects(tmp_path):
    """An `arrow.json` value loads as JSON, as from a RECORD message, also inside a struct."""
    config = {
        **SAMPLE_CONFIG,
        "default_target_schema": f"TARGET_SNOWFLAKE_{uuid.uuid4().hex[0:6]!s}",
        "add_record_metadata": False,
    }
    json_values = pa.array(['{"route": "Guardian"}', None], type=pa.json_(pa.string()))
    document = pa.StructArray.from_arrays(
        [pa.array(["a", "b"]), json_values, json_values],
        names=["name", "data", "Other"],
    )
    table = pa.table({"id": pa.array([1, 2]), "document": document, "payload": json_values})
    batch_path = tmp_path / "batch.arrow"
    with ipc.new_file(batch_path, table.schema) as writer:
        writer.write_table(table)
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "document": {"type": ["object", "null"]},
            "payload": {"type": ["object", "null"]},
        },
    }
    messages = [
        {"type": "SCHEMA", "stream": "test_arrow_json", "key_properties": ["id"], "schema": schema},
        {
            "type": "BATCH",
            "stream": "test_arrow_json",
            "encoding": {"format": "arrow"},
            "manifest": [batch_path.as_uri()],
        },
    ]
    runner = TargetTestRunner(
        TargetSnowflake,
        config=config,
        input_io=io.StringIO("".join(json.dumps(message) + "\n" for message in messages)),
    )
    schema_name = f"{config['database']}.{config['default_target_schema']}"

    with SnowflakeConnector(config).connect() as conn:
        conn.execute(text(f"create schema {schema_name}"))
        try:
            runner.sync_all()
            rows = conn.execute(text(f"select document, payload from {schema_name}.test_arrow_json order by id")).all()
        finally:
            conn.execute(text(f"drop schema if exists {schema_name}"))

    assert [(json.loads(document), payload and json.loads(payload)) for document, payload in rows] == [
        ({"name": "a", "data": {"route": "Guardian"}, "Other": {"route": "Guardian"}}, {"route": "Guardian"}),
        ({"name": "b"}, None),
    ]
