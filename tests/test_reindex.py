from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]  # pyarrow ships no stubs
import pyarrow.parquet as pq  # type: ignore[import-untyped]  # pyarrow ships no stubs

from shardio import ShardReader, ShardWriter, reindex


def _by_name(t: pa.Table) -> dict[str, dict[str, object]]:
    names = t["logical_name"].to_pylist()
    return {n: {c: t[c][i].as_py() for c in t.column_names} for i, n in enumerate(names)}


def test_reindex_recovers_library_and_metadata_columns(tmp_path: Path) -> None:
    entries = {f"e_{i:02d}.bin": f"payload-{i}".encode() for i in range(12)}
    with ShardWriter(tmp_path, entries_per_shard=5) as w:
        for name, data in entries.items():
            idx = int(name.split("_")[1].split(".")[0])
            w.add(name, data, metadata={"idx": idx, "label": f"L{idx}"})

    original = pq.read_table(tmp_path / "index.parquet")
    (tmp_path / "index.parquet").unlink()

    reindex(tmp_path)

    assert (tmp_path / "index.parquet").is_file()
    table = pq.read_table(tmp_path / "index.parquet")
    assert set(table.column_names) >= {
        "logical_name",
        "shard",
        "offset",
        "length",
        "idx",
        "label",
    }
    assert _by_name(table) == _by_name(original)

    with ShardReader(tmp_path) as r:
        for name, data in entries.items():
            assert r.read(name) == data


def test_reindex_handles_entries_with_empty_metadata(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=5) as w:
        w.add("a.bin", b"x")
        w.add("b.bin", b"y")
    (tmp_path / "index.parquet").unlink()
    reindex(tmp_path)
    table = pq.read_table(tmp_path / "index.parquet")
    assert set(table.column_names) == {"logical_name", "shard", "offset", "length"}


def test_reindex_metadata_from_comments_true_recovers_metadata(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=5) as w:
        w.add("a.bin", b"x", metadata={"idx": 1})
        w.add("b.bin", b"y", metadata={"idx": 2})
    (tmp_path / "index.parquet").unlink()
    reindex(tmp_path, metadata_from_comments=True)
    table = pq.read_table(tmp_path / "index.parquet")
    assert "idx" in table.column_names
    assert set(table["idx"].to_pylist()) == {1, 2}


def test_reindex_metadata_from_comments_false_skips_metadata(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=5) as w:
        w.add("a.bin", b"x", metadata={"idx": 1})
        w.add("b.bin", b"y", metadata={"idx": 2})
    (tmp_path / "index.parquet").unlink()
    reindex(tmp_path, metadata_from_comments=False)
    table = pq.read_table(tmp_path / "index.parquet")
    assert set(table.column_names) == {"logical_name", "shard", "offset", "length"}


def test_reindex_handles_empty_shard_set(tmp_path: Path) -> None:
    """A directory with zero shard-*.zip files is a valid empty shard set
    after the writer's "empty is a value" contract. reindex must emit an
    index.parquet with the reserved columns and zero rows — not raise."""
    with ShardWriter(tmp_path, entries_per_shard=5):
        pass
    (tmp_path / "index.parquet").unlink()

    reindex(tmp_path)

    assert (tmp_path / "index.parquet").is_file()
    table = pq.read_table(tmp_path / "index.parquet")
    assert table.num_rows == 0
    assert set(table.column_names) == {"logical_name", "shard", "offset", "length"}
    with ShardReader(tmp_path) as r:
        assert list(r.iter()) == []
