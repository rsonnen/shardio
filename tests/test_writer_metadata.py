from pathlib import Path

import pyarrow.parquet as pq  # type: ignore[import-untyped]  # pyarrow ships no stubs
import pytest

from shardio import ShardWriter


def test_metadata_columns_in_index(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=10) as w:
        w.add("a.bin", b"x", metadata={"seq_no": 1, "score": 0.9})
        w.add("b.bin", b"y", metadata={"seq_no": 2, "score": 0.5})

    table = pq.read_table(tmp_path / "index.parquet")
    assert set(table.column_names) >= {
        "logical_name",
        "shard",
        "offset",
        "length",
        "seq_no",
        "score",
    }
    assert table["seq_no"].to_pylist() == [1, 2]
    assert table["score"].to_pylist() == [0.9, 0.5]


def test_disjoint_metadata_keys_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="disjoint metadata"):
        with ShardWriter(tmp_path, entries_per_shard=10) as w:
            w.add("a.bin", b"x", metadata={"alpha": 1})
            w.add("b.bin", b"y", metadata={"beta": 2})


def test_subset_metadata_keys_allowed_with_nulls(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=10) as w:
        w.add("a.bin", b"x", metadata={"seq_no": 1, "score": 0.9})
        w.add("b.bin", b"y", metadata={"seq_no": 2})  # missing score

    table = pq.read_table(tmp_path / "index.parquet")
    assert table["score"].to_pylist() == [0.9, None]


def test_superset_metadata_keys_allowed(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=10) as w:
        w.add("a.bin", b"x", metadata={"a": 1})
        w.add("b.bin", b"y", metadata={"a": 2, "b": 20})

    table = pq.read_table(tmp_path / "index.parquet")
    by_name = dict(zip(table["logical_name"].to_pylist(), range(table.num_rows), strict=True))
    a_vals = table["a"].to_pylist()
    b_vals = table["b"].to_pylist()
    # Prior row has 'b' backfilled as null; new row has both.
    assert a_vals[by_name["a.bin"]] == 1
    assert b_vals[by_name["a.bin"]] is None
    assert a_vals[by_name["b.bin"]] == 2
    assert b_vals[by_name["b.bin"]] == 20


def test_partially_overlapping_metadata_keys_allowed(tmp_path: Path) -> None:
    # {a,b} then {b,c} — share 'b', so not disjoint; should work.
    with ShardWriter(tmp_path, entries_per_shard=10) as w:
        w.add("a.bin", b"x", metadata={"a": 1, "b": 2})
        w.add("b.bin", b"y", metadata={"b": 3, "c": 4})
    table = pq.read_table(tmp_path / "index.parquet")
    assert set(table.column_names) >= {"a", "b", "c"}
