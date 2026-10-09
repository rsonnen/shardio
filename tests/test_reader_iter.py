import os
from collections import Counter
from pathlib import Path

import pytest

from shardio import ShardReader, ShardWriter


def test_iter_yields_all_entries(tmp_path: Path) -> None:
    entries = {f"e_{i:02d}.bin": f"p-{i}".encode() for i in range(15)}
    with ShardWriter(tmp_path, entries_per_shard=4) as w:
        for name, data in entries.items():
            w.add(name, data)

    with ShardReader(tmp_path) as r:
        seen = dict(r.iter())
    assert seen == entries


def test_iter_order_is_shard_then_offset(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=3) as w:
        for i in range(7):
            w.add(f"e_{i:02d}.bin", f"p-{i}".encode())

    with ShardReader(tmp_path) as r:
        names = [n for n, _ in r.iter()]
    assert names == [
        "e_00.bin",
        "e_01.bin",
        "e_02.bin",
        "e_03.bin",
        "e_04.bin",
        "e_05.bin",
        "e_06.bin",
    ]


def test_dunder_iter_alias(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=2) as w:
        w.add("a.bin", b"1")
        w.add("b.bin", b"2")
    with ShardReader(tmp_path) as r:
        assert dict(iter(r)) == {"a.bin": b"1", "b.bin": b"2"}


def test_reader_handles_empty_shard_set(tmp_path: Path) -> None:
    """A shard set closed with zero entries opens cleanly and iter() yields
    nothing. Empty is a value, not an error — readers must round-trip it."""
    with ShardWriter(tmp_path, entries_per_shard=10):
        pass

    with ShardReader(tmp_path) as r:
        assert list(r.iter()) == []
        assert r.metadata.num_rows == 0
        with pytest.raises(KeyError):
            r.read("anything.bin")


def test_iter_opens_each_shard_once(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with ShardWriter(tmp_path, entries_per_shard=2) as w:
        for i in range(6):
            w.add(f"e_{i}.bin", f"p{i}".encode())

    # 3 shards. Count os.open calls per path during iter().
    counts: Counter[str] = Counter()
    real_open = os.open

    def counting_open(
        path: str | bytes | os.PathLike[str] | os.PathLike[bytes],
        flags: int,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> int:
        counts[os.fsdecode(path)] += 1
        return real_open(path, flags, mode, dir_fd=dir_fd)

    with ShardReader(tmp_path, max_open_shards=64) as r:
        monkeypatch.setattr(os, "open", counting_open)
        list(r.iter())

    for i in range(3):
        shard_path = str(tmp_path / f"shard-{i:05d}.zip")
        assert counts[shard_path] <= 1
