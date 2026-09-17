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


def test_snowpipe_streaming_allows_upsert_at_config_level():
    """`load_method: upsert` can't be rejected here -- whether it's actually safe
    depends on the stream's key properties, which aren't known until a SCHEMA
    message arrives. See `SnowpipeStreamingSink.setup()` for the per-stream check.
    """
    config = {**BASE_CONFIG, "ingestion_method": "snowpipe_streaming", "load_method": "upsert"}
    target = TargetSnowflake(config=config)
    assert target.config["load_method"] == "upsert"


def test_snowpipe_streaming_allows_overwrite():
    """`overwrite` truncates via a regular SQL connection before streaming begins --
    it doesn't need MERGE, so it's not subject to the same limitation as `upsert`.
    """
    config = {**BASE_CONFIG, "ingestion_method": "snowpipe_streaming", "load_method": "overwrite"}
    target = TargetSnowflake(config=config)
    assert target.config["load_method"] == "overwrite"


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


def _make_streaming_sink(*, load_method, key_properties):
    target = TargetSnowflake(
        config={**BASE_CONFIG, "ingestion_method": "snowpipe_streaming", "load_method": load_method},
    )
    connector = MagicMock()
    connector.format_identifier.side_effect = lambda name, **_: name
    return SnowpipeStreamingSink(
        target=target,
        stream_name="some_stream",
        schema={"properties": {"id": {"type": "integer"}}},
        key_properties=key_properties,
        connector=connector,
    ), connector


def test_streaming_sink_rejects_upsert_with_key_properties():
    """Snowpipe Streaming has no MERGE, so `upsert` is only safe for streams with
    no key properties -- this can only be checked once a stream's key properties
    are known, i.e. in the sink, not in `TargetSnowflake._validate_config()`."""
    sink, connector = _make_streaming_sink(load_method="upsert", key_properties=["id"])

    with pytest.raises(ConfigValidationError, match="key properties"):
        sink.setup()

    connector.prepare_schema.assert_not_called()
    connector.prepare_table.assert_not_called()


def test_streaming_sink_allows_upsert_without_key_properties(monkeypatch):
    """`upsert` with no key properties behaves the same as `append-only`, so it's
    allowed -- proven here by confirming setup() proceeds past the PK check (past
    DDL prep, up to the point of importing the optional streaming dependency)."""
    monkeypatch.setitem(sys.modules, "snowflake.ingest.streaming", None)
    sink, connector = _make_streaming_sink(load_method="upsert", key_properties=[])

    with pytest.raises(ImportError, match=re.escape(MISSING_DEPENDENCY_MESSAGE)):
        sink.setup()

    connector.prepare_table.assert_called_once()
    connector.truncate_table.assert_not_called()


def test_streaming_sink_truncates_for_overwrite(monkeypatch):
    """`load_method: overwrite` truncates via the regular SQL connection before
    streaming begins -- unlike `upsert`, it needs no MERGE, so it's not subject to
    the key-properties restriction."""
    monkeypatch.setitem(sys.modules, "snowflake.ingest.streaming", None)
    sink, connector = _make_streaming_sink(load_method="overwrite", key_properties=["id"])

    with pytest.raises(ImportError, match=re.escape(MISSING_DEPENDENCY_MESSAGE)):
        sink.setup()

    connector.truncate_table.assert_called_once_with(sink.full_table_name)
