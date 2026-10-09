from pathlib import Path

import pytest

from shardio import ShardReader, ShardWriter


def test_reader_round_trip(tmp_path: Path) -> None:
    entries = {f"entry_{i:03d}.bin": f"payload-{i}".encode() for i in range(25)}
    with ShardWriter(tmp_path, entries_per_shard=10) as w:
        for name, data in entries.items():
            w.add(name, data, metadata={"idx": int(name.split("_")[1].split(".")[0])})

    with ShardReader(tmp_path) as r:
        for name, data in entries.items():
            assert r.read(name) == data


def test_reader_missing_name_raises(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=10) as w:
        w.add("a.bin", b"x")
    with ShardReader(tmp_path) as r:
        with pytest.raises(KeyError):
            r.read("nonexistent.bin")


def test_reader_metadata_table_excludes_internal_columns(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=10) as w:
        w.add("a.bin", b"x", metadata={"score": 0.9})
        w.add("b.bin", b"y", metadata={"score": 0.5})
    with ShardReader(tmp_path) as r:
        cols = set(r.metadata.column_names)
    assert "logical_name" in cols
    assert "score" in cols
    assert "offset" not in cols
    assert "length" not in cols
    assert "shard" not in cols
