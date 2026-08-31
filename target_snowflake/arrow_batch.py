# Copyright (C) 2026 Meltano.

"""Pure, DB-free helpers for consuming Arrow-encoded Singer BATCH messages.

Taps/mappers such as pipelinewise-tap-mysql and mapper-fivetran can emit BATCH
messages with ``encoding: {"format": "arrow"}``, whose manifest entries are
local ``file://`` URIs pointing to Arrow IPC file format files. None of the
functions here talk to Snowflake -- they only deal with pyarrow tables and the
local filesystem, so they can be unit tested without any DB connection.
"""

from __future__ import annotations

import os
import typing as t
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import ipc

if t.TYPE_CHECKING:
    from collections.abc import Sequence

#: The Singer BATCH ``encoding.format`` value for Arrow IPC file format files.
#: singer-sdk's ``BaseBatchFileEncoding`` has no subclass registry to hook into for
#: custom formats (unlike its built-in ``jsonl``/``parquet``), so this format is
#: recognised by a plain string comparison against ``encoding.format`` in the sink.
ARROW_ENCODING_FORMAT = "arrow"


def resolve_manifest_path(file_uri: str) -> str:
    """Resolve a manifest ``file://`` URI (or bare local path) to a filesystem path.

    Args:
        file_uri: A manifest entry, e.g. ``"file:///tmp/batch.arrow"``.

    Returns:
        The local filesystem path.
    """
    return urlparse(file_uri).path or file_uri


def coerce_arrow_table(table: pa.Table) -> pa.Table:
    """Coerce Arrow types that Snowflake's Parquet loader may not support.

    - decimal32/decimal64 columns are widened to decimal128.
    - Extension-typed columns (e.g. the canonical ``arrow.json`` extension) are
      cast down to their underlying storage type.

    Args:
        table: The table to coerce.

    Returns:
        A table with the same column order/names, with any unsupported column
        types replaced by a Snowflake/Parquet-safe equivalent.
    """
    for index, field in enumerate(table.schema):
        new_type: pa.DataType | None = None

        if pa.types.is_decimal32(field.type) or pa.types.is_decimal64(field.type):
            new_type = pa.decimal128(field.type.precision, field.type.scale)
        elif isinstance(field.type, pa.BaseExtensionType):
            new_type = field.type.storage_type

        if new_type is not None:
            table = table.set_column(index, field.name, table.column(index).cast(new_type))

    return table


def read_arrow_batch_file(path: str) -> pa.Table:
    """Read an Arrow IPC file format file into a table.

    Args:
        path: Local filesystem path to the Arrow IPC file.

    Returns:
        The file's contents as a single table.
    """
    with ipc.open_file(path) as reader:
        return reader.read_all()


def write_parquet_file(table: pa.Table, path: str) -> None:
    """Write a table to a local Parquet file.

    Args:
        table: The table to write.
        path: Local filesystem path to write to.
    """
    pq.write_table(table, path)


def convert_arrow_manifest_to_parquet(
    manifest: Sequence[str],
    output_dir: str,
    *,
    clean_up_source_files: bool = False,
) -> list[str]:
    """Convert each Arrow IPC manifest file into a local Parquet file.

    Args:
        manifest: A list of ``file://`` URIs pointing to Arrow IPC files.
        output_dir: Local directory to write the converted Parquet files to.
        clean_up_source_files: Whether to delete each source Arrow file after
            it has been read, per the ecosystem convention that BATCH manifest
            files are consume-once.

    Returns:
        A list of ``file://`` URIs pointing to the newly-written Parquet files.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    output_uris = []
    for file_uri in manifest:
        source_path = resolve_manifest_path(file_uri)
        table = coerce_arrow_table(read_arrow_batch_file(source_path))

        output_path = os.path.join(output_dir, f"{uuid4()}.parquet")  # noqa: PTH118
        write_parquet_file(table, output_path)
        output_uris.append(Path(output_path).as_uri())

        if clean_up_source_files and os.path.exists(source_path):  # noqa: PTH110
            os.remove(source_path)  # noqa: PTH107

    return output_uris
