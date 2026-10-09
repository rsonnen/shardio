"""Regenerate index.parquet from sealed shard-*.zip files in a directory.

Caller-supplied metadata columns are recovered from per-entry JSON comments
stamped by the writer. Entries with empty comments (``{}``) contribute only
library columns.
"""

from __future__ import annotations

import json
import os
import zipfile
from pathlib import Path
from typing import Any

import pyarrow as pa  # type: ignore[import-untyped]  # pyarrow ships no stubs
import pyarrow.parquet as pq  # type: ignore[import-untyped]  # pyarrow ships no stubs

from shardio._zip import compute_data_offset
from shardio.writer import _RESERVED_SCHEMA, _fsync_dir


def reindex(
    directory: str | os.PathLike[str],
    *,
    metadata_from_comments: bool = True,
) -> None:
    dir_path = Path(directory)
    shard_files = sorted(dir_path.glob("shard-*.zip"))

    rows: list[dict[str, Any]] = []
    all_metadata_keys: set[str] = set()
    for shard_path in shard_files:
        shard_idx = int(shard_path.stem.split("-")[1])
        with zipfile.ZipFile(shard_path, "r") as zf:
            for info in zf.infolist():
                offset, length = compute_data_offset(zf, info)
                row: dict[str, Any] = {
                    "logical_name": info.filename,
                    "shard": shard_idx,
                    "offset": offset,
                    "length": length,
                }
                if metadata_from_comments:
                    comment_bytes = info.comment or b""
                    try:
                        meta = json.loads(comment_bytes.decode("utf-8")) if comment_bytes else {}
                    except (UnicodeDecodeError, json.JSONDecodeError) as e:
                        raise ValueError(
                            f"invalid metadata comment on {info.filename!r} "
                            f"in {shard_path.name}: {e}"
                        ) from e
                    if not isinstance(meta, dict):
                        raise ValueError(
                            f"metadata comment on {info.filename!r} is not a JSON object"
                        )
                    row.update(meta)
                    all_metadata_keys.update(meta.keys())
                rows.append(row)

    if metadata_from_comments:
        for row in rows:
            for k in all_metadata_keys:
                row.setdefault(k, None)

    rows.sort(key=lambda r: r["logical_name"])
    # A directory with zero shard-*.zip files is a valid empty shard set —
    # consistent with the ShardWriter contract that "empty is a value, not
    # a missing state". Fall back to the reserved-column schema so the
    # emitted parquet still satisfies ShardReader's column lookups.
    if rows:
        table = pa.Table.from_pylist(rows)
    else:
        table = pa.Table.from_pylist([], schema=_RESERVED_SCHEMA)
    tmp_path = dir_path / "index.parquet.tmp"
    tmp_path.unlink(missing_ok=True)
    pq.write_table(table, tmp_path)
    fd = os.open(tmp_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    tmp_path.rename(dir_path / "index.parquet")
    _fsync_dir(dir_path)
