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

import io
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from dotenv import load_dotenv
from singer_sdk.exceptions import RecordsWithoutSchemaException
from singer_sdk.helpers._batch import BaseBatchFileEncoding
from singer_sdk.testing import TargetTestRunner
from sqlalchemy import text

from target_snowflake.connector import SnowflakeConnector
from target_snowflake.streaming_sink import SnowpipeStreamingSink, setup_streaming_sdk_logger
from target_snowflake.target import TargetSnowflake

if TYPE_CHECKING:
    from collections.abc import Callable

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

streaming = pytest.importorskip(
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


def test_snowpipe_streaming_append_and_schema_evolution(streaming_schema) -> None:
    """Append-only ingestion via Snowpipe Streaming, including a mid-sync column add.

    `tests/target_test_streams/snowpipe_streaming_basic.singer` sends two records,
    then a SCHEMA message adding `col_new`, then a third record populating it. This
    exercises the sink-swap lifecycle documented in the plan: the SDK instantiates
    a fresh `SnowpipeStreamingSink` (new channel) after `prepare_table` ALTERs the
    table for the new column.
    """
    config = {**SAMPLE_CONFIG, "default_target_schema": streaming_schema}
    runner = TargetTestRunner(TargetSnowflake, config=config)
    with Path("tests/target_test_streams/snowpipe_streaming_basic.singer").open(encoding="utf-8") as input_file:
        runner.target_input = input_file
        runner.sync_all()

    connector = SnowflakeConnector(config=config)
    query = text(
        f"select id, col_str, col_new from {SAMPLE_CONFIG['database']}.{streaming_schema}"
        ".snowpipe_streaming_basic order by id",
    )
    expected = [(1, "foo", None), (2, "bar", None), (3, "baz", "added")]
    # Snowflake makes acknowledged rows queryable some seconds after the target exits.
    deadline = time.monotonic() + 60
    while True:
        with connector.connect() as conn:
            rows = [tuple(row) for row in conn.execute(query).fetchall()]
        if rows == expected or time.monotonic() > deadline:
            break
        time.sleep(1)

    assert rows == expected


def test_setup_streaming_logging(caplog: pytest.LogCaptureFixture, subtests: pytest.Subtests) -> None:
    logger_name = "test.streaming"
    logger = logging.getLogger(logger_name)

    environ: dict[str, str] = {}
    with subtests.test("debug"), caplog.at_level(logging.DEBUG, logger=logger_name):
        setup_streaming_sdk_logger(environ=environ, logger=logger)

        assert environ == {
            "SS_LOG_TARGET": "stderr",
            "SS_LOG_LEVEL": "debug",
        }

    environ = {}
    with subtests.test("not-quite-info"), caplog.at_level(logging.INFO - 1, logger=logger_name):
        setup_streaming_sdk_logger(environ=environ, logger=logger)

        assert environ == {
            "SS_LOG_TARGET": "stderr",
            "SS_LOG_LEVEL": "info",
        }

    environ = {}
    with subtests.test("info-and-above"), caplog.at_level(logging.INFO, logger=logger_name):
        setup_streaming_sdk_logger(environ=environ, logger=logger)

        assert environ == {
            "SS_LOG_TARGET": "stderr",
            "SS_LOG_LEVEL": "warn",
        }


class AckOnFlushChannel:
    """An elastic channel that acknowledges the appended rows only when the target flushes it."""

    def __init__(self) -> None:
        self.appended: list[dict] = []
        self.acknowledged: list[dict] = []

    def set_success_handler(self, handler: Callable[[Any], None]) -> None:
        self._on_success = handler

    def set_error_handler(self, handler: Callable[[Any], None]) -> None:
        self._on_error = handler

    def append_row(self, row: dict, **_: object) -> None:
        self.appended.append(row)

    def initiate_flush(self) -> None:
        rows = self.appended[len(self.acknowledged) :]
        self.acknowledged += rows
        if rows:
            self._on_success(streaming.SuccessDetail(append_tokens=tuple(rows), request_id="", retry_count=0))


def open_sink(monkeypatch: pytest.MonkeyPatch, channel: AckOnFlushChannel) -> SnowpipeStreamingSink:
    client = MagicMock()
    client.get_elastic_channel.return_value = channel
    monkeypatch.setattr(streaming.StreamingIngestClient, "from_table", MagicMock(return_value=client))

    connector = MagicMock()
    connector.format_identifier.side_effect = lambda name, **_: name
    sink = SnowpipeStreamingSink(
        target=TargetSnowflake(config=SAMPLE_CONFIG),
        stream_name="some_stream",
        schema={"properties": {"id": {"type": "integer"}}},
        key_properties=[],
        connector=connector,
    )
    sink.setup()
    return sink


def test_batch_file_rows_reach_the_channel(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    channel = AckOnFlushChannel()
    sink = open_sink(monkeypatch, channel)
    batch_file = tmp_path / "batch.jsonl"
    batch_file.write_text('{"id": 1}\n{"id": 2}\n', encoding="utf-8")
    sink.process_batch_files(BaseBatchFileEncoding(format="jsonl"), [batch_file.as_uri()])

    assert channel.acknowledged == [{"id": 1}, {"id": 2}]


class FailOnFlushChannel(AckOnFlushChannel):
    """An elastic channel whose upload of the appended rows fails."""

    @override
    def initiate_flush(self) -> None:
        error = streaming.StreamingIngestError(
            streaming.StreamingIngestErrorCode.SF_API_USER_ERROR,
            "Rows rejected",
            400,
            "Bad Request",
        )
        self._on_error(streaming.ErrorDetail(append_tokens=(1,), error=error, request_id="", retry_count=0))


def test_drain_fails_when_an_append_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    sink = open_sink(monkeypatch, FailOnFlushChannel())
    sink.process_record({"id": 1}, {})

    with pytest.raises(streaming.StreamingIngestError, match="Rows rejected"):
        sink.process_batch({})


def run_until_failure(
    messages: list[dict],
    *,
    max_record_age_in_minutes: float = 5,
    **config: Any,
) -> AckOnFlushChannel:
    """Run the target on the messages, then fail it before clean-up can close the client."""
    channel = AckOnFlushChannel()
    client = MagicMock()
    client.get_elastic_channel.return_value = channel
    connector = MagicMock()
    connector.format_identifier.side_effect = lambda name, **_: name

    class StubClientSink(SnowpipeStreamingSink):
        streaming_client = client

    class StubConnectorTarget(TargetSnowflake):
        target_connector = connector
        _MAX_RECORD_AGE_IN_MINUTES = max_record_age_in_minutes

        @override
        def get_sink_class(self, stream_name: str) -> type[SnowpipeStreamingSink]:
            return StubClientSink

    unknown_stream_record = {"type": "RECORD", "stream": "unknown_stream", "record": {}}
    target = StubConnectorTarget(config={**SAMPLE_CONFIG, **config})
    with pytest.raises(RecordsWithoutSchemaException):
        target.listen(io.StringIO("\n".join(json.dumps(message) for message in [*messages, unknown_stream_record])))
    return channel


SCHEMA_MESSAGE = {"type": "SCHEMA", "stream": "some_stream", "schema": {"properties": {"id": {"type": "integer"}}}}


def test_state_waits_for_the_acknowledgement_of_streamed_rows(capsys: pytest.CaptureFixture[str]) -> None:
    messages = [
        SCHEMA_MESSAGE,
        {"type": "RECORD", "stream": "some_stream", "record": {"id": 1}},
        {"type": "STATE", "value": {"bookmark": 1}},
        {"type": "RECORD", "stream": "some_stream", "record": {"id": 2}},
    ]
    # Drain all sinks and emit `STATE` after each record, as the target does after 5 minutes.
    channel = run_until_failure(messages, max_record_age_in_minutes=-1)

    states = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert states == [{"bookmark": 1}]
    assert [row["id"] for row in channel.acknowledged] == [1, 2]


def test_intake_does_not_pause_for_the_acknowledgement_at_batch_size_rows() -> None:
    messages = [
        SCHEMA_MESSAGE,
        {"type": "RECORD", "stream": "some_stream", "record": {"id": 1}},
        {"type": "RECORD", "stream": "some_stream", "record": {"id": 2}},
    ]
    channel = run_until_failure(messages, batch_size_rows=1)

    assert channel.acknowledged == []


class SaturatedOnceChannel(AckOnFlushChannel):
    """An elastic channel whose inflight buffer is full for the first append."""

    def __init__(self) -> None:
        super().__init__()
        self._saturated = True

    @override
    def append_row(self, row: dict, **_: object) -> None:
        if self._saturated:
            self._saturated = False
            raise streaming.StreamingIngestError(
                streaming.StreamingIngestErrorCode.RECEIVER_SATURATED,
                "Inflight data limit reached for channel",
                429,
                "Too Many Requests",
            )
        super().append_row(row)


def test_append_waits_while_the_channel_is_saturated(monkeypatch: pytest.MonkeyPatch) -> None:
    channel = SaturatedOnceChannel()
    sink = open_sink(monkeypatch, channel)
    monkeypatch.setattr("time.sleep", lambda _: None)
    sink.process_record({"id": 1}, {})

    assert channel.appended == [{"id": 1}]
