"""Append-and-rotate writer that emits sealed zip shards and a parquet index."""

from __future__ import annotations

import json
import os
import zipfile
from pathlib import Path
from types import TracebackType
from typing import Any, Self

import pyarrow as pa  # type: ignore[import-untyped]  # pyarrow ships no stubs
import pyarrow.parquet as pq  # type: ignore[import-untyped]  # pyarrow ships no stubs

from shardio._zip import compute_data_offset

_RESERVED_SCHEMA = pa.schema(
    [
        ("logical_name", pa.string()),
        ("shard", pa.int64()),
        ("offset", pa.int64()),
        ("length", pa.int64()),
    ]
)
_RESERVED_COLUMNS = frozenset(_RESERVED_SCHEMA.names)
# ZIP spec: per-entry comment length field is a uint16 (65535 bytes max).
_MAX_ZIP_COMMENT_BYTES = 65535


def _fsync_dir(dir_path: Path) -> None:
    dir_fd = os.open(dir_path, os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


class ShardWriter:
    def __init__(
        self,
        directory: str | os.PathLike[str],
        entries_per_shard: int = 10_000,
        schema: pa.Schema | None = None,
    ) -> None:
        if entries_per_shard <= 0:
            raise ValueError(f"entries_per_shard must be > 0, got {entries_per_shard}")
        self._dir = Path(directory)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock_path = self._dir / ".shardio.lock"
        if self._lock_path.exists():
            raise RuntimeError(
                f"shardio lock present at {self._lock_path}; "
                f"another writer is active or the directory was not cleanly closed"
            )
        self._lock_path.write_text(str(os.getpid()))
        self._entries_per_shard = entries_per_shard
        self._schema = schema

        self._shard_index = 0
        self._entries_in_shard = 0
        self._zf: zipfile.ZipFile | None = None
        self._current_shard_path: Path | None = None
        self._sealed_shards: list[Path] = []
        self._rows: list[dict[str, Any]] = []
        self._seen_names: set[str] = set()
        self._metadata_keys: set[str] | None = None
        self._closed = False

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        try:
            if exc_type is not None:
                if self._zf is not None:
                    self._zf.close()
                return
            self.close()
        finally:
            self._release_lock()

    def _release_lock(self) -> None:
        try:
            self._lock_path.unlink(missing_ok=True)
        except OSError:
            pass

    def _ensure_shard_open(self) -> zipfile.ZipFile:
        if self._zf is None:
            path = self._dir / f"shard-{self._shard_index:05d}.zip"
            self._zf = zipfile.ZipFile(path, "w", zipfile.ZIP_STORED, allowZip64=True)
            self._current_shard_path = path
        return self._zf

    def _rotate_if_full(self) -> None:
        if self._entries_in_shard < self._entries_per_shard:
            return
        assert self._zf is not None
        assert self._current_shard_path is not None
        self._zf.close()
        self._sealed_shards.append(self._current_shard_path)
        self._zf = None
        self._current_shard_path = None
        self._shard_index += 1
        self._entries_in_shard = 0

    def add(
        self,
        logical_name: str,
        data: bytes,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        if self._closed:
            raise RuntimeError("ShardWriter is closed")
        if logical_name in self._seen_names:
            raise ValueError(f"duplicate logical_name: {logical_name!r}")
        if metadata:
            bad = _RESERVED_COLUMNS & metadata.keys()
            if bad:
                raise ValueError(f"metadata keys collide with reserved columns: {sorted(bad)}")
        keys = set(metadata.keys()) if metadata else set()
        if self._metadata_keys is None:
            self._metadata_keys = keys
        else:
            # Only reject when the new key set is entirely disjoint from the
            # running union and both are non-empty. Subsets and supersets are
            # fine; missing values are null-filled at index write time.
            if keys and self._metadata_keys and keys.isdisjoint(self._metadata_keys):
                raise ValueError(
                    f"disjoint metadata keys {sorted(keys)!r}; "
                    f"prior keys: {sorted(self._metadata_keys)!r}"
                )
            self._metadata_keys |= keys
        comment_bytes = json.dumps(metadata or {}, sort_keys=True).encode("utf-8")
        if len(comment_bytes) > _MAX_ZIP_COMMENT_BYTES:
            raise ValueError(
                f"metadata JSON comment for {logical_name!r} is "
                f"{len(comment_bytes)} bytes, exceeds ZIP entry-comment limit "
                f"of {_MAX_ZIP_COMMENT_BYTES} bytes"
            )
        self._seen_names.add(logical_name)
        zf = self._ensure_shard_open()
        zinfo = zipfile.ZipInfo(filename=logical_name)
        zinfo.compress_type = zipfile.ZIP_STORED
        zinfo.comment = comment_bytes
        zf.writestr(zinfo, data)
        info = zf.getinfo(logical_name)
        offset, length = compute_data_offset(zf, info)
        row: dict[str, Any] = {
            "logical_name": logical_name,
            "shard": self._shard_index,
            "offset": offset,
            "length": length,
        }
        for k in self._metadata_keys or ():
            row[k] = metadata.get(k) if metadata else None
        self._rows.append(row)
        self._entries_in_shard += 1
        self._rotate_if_full()

    def close(self) -> None:
        if self._closed:
            return
        if self._zf is not None:
            self._zf.close()
            assert self._current_shard_path is not None
            self._sealed_shards.append(self._current_shard_path)
            self._zf = None
            self._current_shard_path = None
        for path in self._sealed_shards:
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        self._write_index()
        self._closed = True
        self._release_lock()

    def _write_index(self) -> None:
        table = self._build_index_table()
        tmp_path = self._dir / "index.parquet.tmp"
        tmp_path.unlink(missing_ok=True)
        pq.write_table(table, tmp_path)
        fd = os.open(tmp_path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        tmp_path.rename(self._dir / "index.parquet")
        _fsync_dir(self._dir)

    def _build_index_table(self) -> pa.Table:
        # A closed shard set must always have an index.parquet, even with
        # zero entries — "empty" is a value (e.g. a batch with no extractable
        # items), not a missing state. The reader uses index.parquet's
        # presence as the "closed cleanly" signal; making zero-row writes
        # produce no file would collapse "empty" and "never finalized" into
        # the same on-disk shape.
        if self._rows:
            all_keys: set[str] = set(_RESERVED_COLUMNS)
            if self._metadata_keys:
                all_keys |= self._metadata_keys
            for row in self._rows:
                for k in all_keys:
                    row.setdefault(k, None)
            self._rows.sort(key=lambda r: r["logical_name"])
            return pa.Table.from_pylist(self._rows, schema=self._schema)
        # Zero rows: caller's explicit schema is honored when given; without
        # one, fall back to the reserved columns so the reader's lookups
        # still resolve.
        if self._schema is not None:
            return pa.Table.from_pylist([], schema=self._schema)
        return pa.Table.from_pylist([], schema=_RESERVED_SCHEMA)
