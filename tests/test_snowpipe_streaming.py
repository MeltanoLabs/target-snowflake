# Copyright (C) 2026 Meltano.

"""Integration tests for `ingestion_method: snowpipe_streaming`, against a live account.

Unlike `tests/test_target_snowflake.py`, this does not run the full SDK standard
test suite: most of those tests assume upsert/hard-delete/activate-version
behaviors that are explicitly out of scope for the (append-only-only) Snowpipe
Streaming MVP. Instead, this exercises the two things unique to the streaming
path: basic append-only ingestion, and schema evolution mid-sync.

Requires the `snowpipe` extra (`pip install '.[snowpipe]'`) and the same
`TARGET_SNOWFLAKE_*` environment variables used by `test_target_snowflake.py`
(same key-pair auth, no new credentials needed).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import pytest
from dotenv import load_dotenv
from singer_sdk.testing import TargetTestRunner
from sqlalchemy import text

from target_snowflake.connector import SnowflakeConnector
from target_snowflake.target import TargetSnowflake

pytest.importorskip(
    "snowflake.ingest.streaming",
    reason="requires the 'snowpipe' extra: pip install '.[snowpipe]'",
)

load_dotenv()

SAMPLE_CONFIG: dict[str, Any] = {
    "user": os.environ["TARGET_SNOWFLAKE_USER"],
    "private_key": os.environ["TARGET_SNOWFLAKE_PRIVATE_KEY"],
    "account": os.environ["TARGET_SNOWFLAKE_ACCOUNT"],
    "database": os.environ["TARGET_SNOWFLAKE_DATABASE"],
    "warehouse": os.environ["TARGET_SNOWFLAKE_WAREHOUSE"],
    "role": os.environ["TARGET_SNOWFLAKE_ROLE"],
    "schema": os.environ["TARGET_SNOWFLAKE_SCHEMA"],
    "ingestion_method": "snowpipe_streaming",
    "load_method": "append-only",
}


@pytest.fixture
def streaming_schema():
    """Create and drop an isolated schema for this test run."""
    schema_name = f"TARGET_SNOWFLAKE_SNOWPIPE_{uuid.uuid4().hex[0:6]!s}"
    connector = SnowflakeConnector(config=SAMPLE_CONFIG)
    with connector.connect() as conn:
        conn.execute(text(f"create schema {SAMPLE_CONFIG['database']}.{schema_name}"))
    yield schema_name
    with connector.connect() as conn:
        conn.execute(text(f"drop schema if exists {SAMPLE_CONFIG['database']}.{schema_name}"))


def test_snowpipe_streaming_append_and_schema_evolution(streaming_schema):
    """Append-only ingestion via Snowpipe Streaming, including a mid-sync column add.

    `tests/target_test_streams/snowpipe_streaming_basic.singer` sends two records,
    then a SCHEMA message adding `col_new`, then a third record populating it. This
    exercises the sink-swap lifecycle documented in the plan: the SDK instantiates
    a fresh `SnowpipeStreamingSink` (new channel) after `prepare_table` ALTERs the
    table for the new column.
    """
    config = {**SAMPLE_CONFIG, "default_target_schema": streaming_schema}
    runner = TargetTestRunner(
        TargetSnowflake,
        config=config,
        input_filepath=Path("tests/target_test_streams/snowpipe_streaming_basic.singer"),
    )
    runner.sync_all()

    # Snowpipe Streaming's channel `close(wait_for_flush=True)` (used in
    # SnowpipeStreamingSink.clean_up()) confirms the client-side buffer was sent to
    # Snowflake, but rows can take a few seconds longer to become visible to SELECT
    # -- so poll rather than asserting immediately.
    connector = SnowflakeConnector(config=config)
    query = text(
        f"select id, col_str, col_new from {SAMPLE_CONFIG['database']}.{streaming_schema}"
        ".snowpipe_streaming_basic order by id",
    )
    expected = [(1, "foo", None), (2, "bar", None), (3, "baz", "added")]
    deadline = time.monotonic() + 60
    rows: list[tuple] = []
    while time.monotonic() < deadline:
        with connector.connect() as conn:
            rows = [tuple(row) for row in conn.execute(query).fetchall()]
        if rows == expected:
            break
        time.sleep(2)

    assert rows == expected


def test_snowpipe_streaming_stdout_is_pure_singer_protocol(streaming_schema, tmp_path):
    """stdout must contain only Singer protocol messages (STATE, here), never the
    snowpipe-streaming SDK's own logging.

    That SDK's Rust core has its own logger -- independent of Python's `logging` --
    that defaults to writing to stdout, which would corrupt the channel Meltano
    reads STATE messages from. `SnowpipeStreamingSink.setup()` sets
    `SS_LOG_TARGET=stderr` before importing the SDK to prevent this.

    `TargetTestRunner` (used by the test above) captures Python-level `sys.stdout`,
    which would NOT catch a native extension writing directly to the OS file
    descriptor -- confirmed by the fact this bug was invisible to that runner and
    only showed up when manually shell-redirecting a real CLI invocation. So this
    test runs the target as an actual subprocess and inspects its real stdout/stderr.
    """
    config = {**SAMPLE_CONFIG, "default_target_schema": streaming_schema}
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config))

    input_path = Path("tests/target_test_streams/snowpipe_streaming_basic.singer")
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "target_snowflake.target", "--config", str(config_path)],
        input=input_path.read_text(),
        capture_output=True,
        text=True,
        timeout=90,
        check=False,
    )

    assert result.returncode == 0, result.stderr

    stdout_lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert stdout_lines, "expected at least one STATE message on stdout"
    for line in stdout_lines:
        # `Target._write_state_message()` writes the bare state-value dict (no
        # type/value envelope) -- just confirm every line is valid JSON, which a
        # leaked Rust log line (free text with spaces/pipes/colons) never would be.
        message = json.loads(line)
        assert isinstance(message, dict)

    # Not just silently swallowed -- confirm the logging actually landed on stderr.
    assert "core::" in result.stderr
