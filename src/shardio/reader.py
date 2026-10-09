"""Reader for shard sets: name -> bytes via direct pread at parquet-recorded offsets."""

from __future__ import annotations

import os
import threading
from collections import OrderedDict
from collections.abc import Iterator
from pathlib import Path
from types import TracebackType
from typing import Self

import pyarrow as pa  # type: ignore[import-untyped]  # pyarrow ships no stubs
import pyarrow.parquet as pq  # type: ignore[import-untyped]  # pyarrow ships no stubs

_INTERNAL_COLUMNS = ("shard", "offset", "length")


class ShardReader:
    def __init__(
        self,
        directory: str | os.PathLike[str],
        max_open_shards: int = 64,
    ) -> None:
        if max_open_shards <= 0:
            raise ValueError(f"max_open_shards must be > 0, got {max_open_shards}")
        self._dir = Path(directory)
        index_path = self._dir / "index.parquet"
        if not index_path.is_file():
            raise FileNotFoundError(f"no index.parquet in {self._dir}")
        table = pq.read_table(index_path)

        names = table["logical_name"].to_pylist()
        shards = table["shard"].to_pylist()
        offsets = table["offset"].to_pylist()
        lengths = table["length"].to_pylist()
        self._index: dict[str, tuple[int, int, int]] = {
            n: (s, o, ln) for n, s, o, ln in zip(names, shards, offsets, lengths, strict=True)
        }

        keep = [c for c in table.column_names if c not in _INTERNAL_COLUMNS]
        self._metadata: pa.Table = table.select(keep)

        self._max_open_shards = max_open_shards
        # OrderedDict as LRU: most recently used at the end.
        self._fd_cache: OrderedDict[int, int] = OrderedDict()
        self._fd_lock = threading.Lock()

    @property
    def metadata(self) -> pa.Table:
        return self._metadata

    def _acquire_fd(self, shard: int) -> int:
        """Return a caller-owned dup of the cached template fd for ``shard``.

        The dup is independent of the cache: even if the cached template is later
        evicted and closed, the caller's dup remains valid until the caller
        closes it. This prevents EBADF races between concurrent preads and LRU
        eviction by other threads.
        """
        with self._fd_lock:
            cached = self._fd_cache.get(shard)
            if cached is not None:
                self._fd_cache.move_to_end(shard)
            else:
                path = self._dir / f"shard-{shard:05d}.zip"
                cached = os.open(path, os.O_RDONLY)
                self._fd_cache[shard] = cached
                while len(self._fd_cache) > self._max_open_shards:
                    _, evicted_fd = self._fd_cache.popitem(last=False)
                    try:
                        os.close(evicted_fd)
                    except OSError:
                        pass
            return os.dup(cached)

    def read(self, logical_name: str) -> bytes:
        try:
            shard, offset, length = self._index[logical_name]
        except KeyError as e:
            raise KeyError(logical_name) from e
        fd = self._acquire_fd(shard)
        try:
            return os.pread(fd, length, offset)
        finally:
            os.close(fd)

    def iter(self) -> Iterator[tuple[str, bytes]]:
        by_shard: dict[int, list[tuple[int, int, str]]] = {}
        for name, (shard, offset, length) in self._index.items():
            by_shard.setdefault(shard, []).append((offset, length, name))
        for shard in sorted(by_shard):
            fd = self._acquire_fd(shard)
            try:
                for offset, length, name in sorted(by_shard[shard]):
                    yield name, os.pread(fd, length, offset)
            finally:
                os.close(fd)

    def __iter__(self) -> Iterator[tuple[str, bytes]]:
        return self.iter()

    def close(self) -> None:
        with self._fd_lock:
            for fd in self._fd_cache.values():
                try:
                    os.close(fd)
                except OSError:
                    pass
            self._fd_cache.clear()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
