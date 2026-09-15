# Copyright (C) 2026 Meltano.

"""Unit tests for `ingestion_method: snowpipe_streaming` config validation.

These tests exercise `TargetSnowflake._validate_config()` and `get_sink_class()`
directly, with no live Snowflake account required.
"""

from __future__ import annotations

import re
import sys
from unittest.mock import MagicMock

import pytest
from singer_sdk.exceptions import ConfigValidationError

from target_snowflake.sinks import SnowflakeSink
from target_snowflake.streaming_sink import MISSING_DEPENDENCY_MESSAGE, SnowpipeStreamingSink
from target_snowflake.target import TargetSnowflake

BASE_CONFIG = {
    "user": "test_user",
    "account": "test_account",
    "database": "test_db",
    "private_key": "dGVzdA==",  # base64 "test", never actually used to connect
}


def test_snowpipe_streaming_requires_append_only():
    config = {**BASE_CONFIG, "ingestion_method": "snowpipe_streaming", "load_method": "upsert"}
    with pytest.raises(ConfigValidationError) as exc_info:
        TargetSnowflake(config=config)
    assert any("append-only" in error for error in exc_info.value.errors)


def test_snowpipe_streaming_rejects_overwrite():
    config = {**BASE_CONFIG, "ingestion_method": "snowpipe_streaming", "load_method": "overwrite"}
    with pytest.raises(ConfigValidationError) as exc_info:
        TargetSnowflake(config=config)
    assert any("append-only" in error for error in exc_info.value.errors)


def test_snowpipe_streaming_rejects_hard_delete():
    config = {
        **BASE_CONFIG,
        "ingestion_method": "snowpipe_streaming",
        "load_method": "append-only",
        "hard_delete": True,
    }
    with pytest.raises(ConfigValidationError) as exc_info:
        TargetSnowflake(config=config)
    assert any("hard_delete" in error for error in exc_info.value.errors)


def test_snowpipe_streaming_requires_key_pair_auth():
    config = {
        "user": "test_user",
        "account": "test_account",
        "database": "test_db",
        "password": "test_password",
        "ingestion_method": "snowpipe_streaming",
        "load_method": "append-only",
    }
    with pytest.raises(ConfigValidationError) as exc_info:
        TargetSnowflake(config=config)
    assert any("key-pair authentication" in error for error in exc_info.value.errors)


def test_snowpipe_streaming_requires_some_auth_method():
    config = {
        "user": "test_user",
        "account": "test_account",
        "database": "test_db",
        "ingestion_method": "snowpipe_streaming",
        "load_method": "append-only",
    }
    with pytest.raises(ConfigValidationError) as exc_info:
        TargetSnowflake(config=config)
    assert any("key-pair authentication" in error for error in exc_info.value.errors)


def test_snowpipe_streaming_with_append_only_and_key_pair_is_valid():
    config = {**BASE_CONFIG, "ingestion_method": "snowpipe_streaming", "load_method": "append-only"}
    target = TargetSnowflake(config=config)
    assert target.config["ingestion_method"] == "snowpipe_streaming"


def test_file_staging_ignores_streaming_constraints():
    """The default `ingestion_method` must not be affected by these new checks."""
    config = {**BASE_CONFIG, "load_method": "upsert", "hard_delete": True}
    target = TargetSnowflake(config=config)
    assert target.config.get("ingestion_method", "file_staging") == "file_staging"


def test_get_sink_class_selects_streaming_sink():
    config = {**BASE_CONFIG, "ingestion_method": "snowpipe_streaming", "load_method": "append-only"}
    target = TargetSnowflake(config=config)
    assert target.get_sink_class("some_stream") is SnowpipeStreamingSink


def test_get_sink_class_defaults_to_file_staging_sink():
    target = TargetSnowflake(config=BASE_CONFIG)
    assert target.get_sink_class("some_stream") is SnowflakeSink


def test_missing_optional_dependency_raises_clear_error(monkeypatch):
    """`setup()` should fail with a clear, actionable message if `snowpipe-streaming`
    isn't installed, rather than a bare `ModuleNotFoundError`."""
    monkeypatch.setitem(sys.modules, "snowflake.ingest.streaming", None)

    target = TargetSnowflake(
        config={**BASE_CONFIG, "ingestion_method": "snowpipe_streaming", "load_method": "append-only"},
    )
    connector = MagicMock()
    connector.format_identifier.side_effect = lambda name, **_: name
    sink = SnowpipeStreamingSink(
        target=target,
        stream_name="some_stream",
        schema={"properties": {"id": {"type": "integer"}}},
        key_properties=[],
        connector=connector,
    )

    with pytest.raises(ImportError, match=re.escape(MISSING_DEPENDENCY_MESSAGE)):
        sink.setup()
