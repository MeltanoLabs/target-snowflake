# Copyright (C) 2026 Meltano.

"""Unit tests for Arrow BATCH message helpers (no Snowflake connection needed)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow import ipc

from target_snowflake.arrow_batch import (
    coerce_arrow_table,
    convert_arrow_manifest_to_parquet,
    read_arrow_batch_file,
    resolve_manifest_path,
    write_parquet_file,
)

if TYPE_CHECKING:
    from pathlib import Path


def write_ipc_file(table: pa.Table, path: Path) -> None:
    with ipc.new_file(path, table.schema) as writer:
        writer.write_table(table)


@pytest.mark.parametrize(
    ("file_uri", "expected_path"),
    [
        ("file:///tmp/batch.arrow", "/tmp/batch.arrow"),  # noqa: S108
        ("file:///tmp/dir/batch.arrow", "/tmp/dir/batch.arrow"),  # noqa: S108
        ("/already/a/local/path.arrow", "/already/a/local/path.arrow"),
    ],
)
def test_resolve_manifest_path(file_uri: str, expected_path: str):
    assert resolve_manifest_path(file_uri) == expected_path


def test_coerce_arrow_table_widens_decimal32_and_decimal64():
    table = pa.table(
        {
            "d32": pa.array([1, 2], type=pa.decimal32(5, 2)),
            "d64": pa.array([3, 4], type=pa.decimal64(10, 2)),
            "s": pa.array(["a", "b"]),
        },
    )

    coerced = coerce_arrow_table(table)

    assert coerced.schema.field("d32").type == pa.decimal128(5, 2)
    assert coerced.schema.field("d64").type == pa.decimal128(10, 2)
    assert coerced.schema.field("s").type == pa.string()
    assert coerced.column("d32").to_pylist() == table.column("d32").cast(pa.decimal128(5, 2)).to_pylist()


def test_coerce_arrow_table_unwraps_json_extension_type():
    json_array = pa.array(['{"a": 1}', '{"b": 2}'], type=pa.json_(pa.string()))
    table = pa.table({"payload": json_array})

    coerced = coerce_arrow_table(table)

    assert coerced.schema.field("payload").type == pa.string()
    assert coerced.column("payload").to_pylist() == ['{"a": 1}', '{"b": 2}']


def test_coerce_arrow_table_passthrough_when_no_problematic_types():
    table = pa.table({"i": pa.array([1, 2]), "s": pa.array(["a", "b"])})

    coerced = coerce_arrow_table(table)

    assert coerced.schema == table.schema
    assert coerced.equals(table)


def test_read_arrow_batch_file_round_trip(tmp_path: Path):
    table = pa.table({"i": pa.array([1, 2, 3])})
    path = tmp_path / "batch.arrow"
    write_ipc_file(table, path)

    result = read_arrow_batch_file(str(path))

    assert result.equals(table)


def test_write_parquet_file_round_trip(tmp_path: Path):
    table = pa.table({"i": pa.array([1, 2, 3]), "s": pa.array(["x", "y", "z"])})
    path = tmp_path / "out.parquet"

    write_parquet_file(table, str(path))

    assert pq.read_table(str(path)).equals(table)


def test_convert_arrow_manifest_to_parquet(tmp_path: Path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    output_dir = tmp_path / "output"

    tables = [
        pa.table({"i": pa.array([1, 2])}),
        pa.table({"i": pa.array([3, 4])}),
    ]
    manifest = []
    for index, table in enumerate(tables):
        path = source_dir / f"file_{index}.arrow"
        write_ipc_file(table, path)
        manifest.append(path.as_uri())

    output_uris = convert_arrow_manifest_to_parquet(manifest, output_dir=str(output_dir))

    assert len(output_uris) == 2
    for uri, table in zip(output_uris, tables, strict=True):
        output_path = resolve_manifest_path(uri)
        assert output_path.endswith(".parquet")
        assert pq.read_table(output_path).equals(table)
        # source files are untouched by default
    assert all((source_dir / f"file_{i}.arrow").exists() for i in range(2))


def test_convert_arrow_manifest_to_parquet_cleans_up_source_files(tmp_path: Path):
    source_dir = tmp_path / "source"
    source_dir.mkdir()
    output_dir = tmp_path / "output"

    table = pa.table({"i": pa.array([1, 2])})
    source_path = source_dir / "file_0.arrow"
    write_ipc_file(table, source_path)

    convert_arrow_manifest_to_parquet(
        [source_path.as_uri()],
        output_dir=str(output_dir),
        clean_up_source_files=True,
    )

    assert not source_path.exists()
