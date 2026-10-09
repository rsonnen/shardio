from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]  # pyarrow ships no stubs
import pyarrow.parquet as pq  # type: ignore[import-untyped]  # pyarrow ships no stubs
import pytest

from shardio import ShardWriter


def test_single_entry_round_trip(tmp_path: Path) -> None:
    payload = b"hello shardio"
    with ShardWriter(tmp_path, entries_per_shard=10) as w:
        w.add("a.bin", payload)

    assert (tmp_path / "shard-00000.zip").is_file()
    assert (tmp_path / "index.parquet").is_file()

    table = pq.read_table(tmp_path / "index.parquet")
    assert table.num_rows == 1
    row = {c: table[c][0].as_py() for c in table.column_names}
    assert row["logical_name"] == "a.bin"
    assert row["shard"] == 0
    assert row["length"] == len(payload)

    with open(tmp_path / "shard-00000.zip", "rb") as f:
        f.seek(row["offset"])
        assert f.read(row["length"]) == payload


def test_multi_shard_rotation(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=3) as w:
        for i in range(7):
            w.add(f"entry_{i:02d}.bin", f"payload-{i}".encode())

    # 3 shards: 3+3+1
    assert (tmp_path / "shard-00000.zip").is_file()
    assert (tmp_path / "shard-00001.zip").is_file()
    assert (tmp_path / "shard-00002.zip").is_file()
    assert not (tmp_path / "shard-00003.zip").exists()

    table = pq.read_table(tmp_path / "index.parquet")
    shard_values = table["shard"].to_pylist()
    name_values = table["logical_name"].to_pylist()
    name_to_shard = dict(zip(name_values, shard_values, strict=True))
    assert name_to_shard["entry_00.bin"] == 0
    assert name_to_shard["entry_02.bin"] == 0
    assert name_to_shard["entry_03.bin"] == 1
    assert name_to_shard["entry_05.bin"] == 1
    assert name_to_shard["entry_06.bin"] == 2


def test_duplicate_logical_name_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="duplicate logical_name"):
        with ShardWriter(tmp_path, entries_per_shard=10) as w:
            w.add("a.bin", b"first")
            w.add("a.bin", b"second")


def test_invalid_entries_per_shard_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="entries_per_shard must be > 0"):
        ShardWriter(tmp_path, entries_per_shard=0)
    with pytest.raises(ValueError, match="entries_per_shard must be > 0"):
        ShardWriter(tmp_path, entries_per_shard=-5)


def test_add_after_close_raises(tmp_path: Path) -> None:
    w = ShardWriter(tmp_path, entries_per_shard=10)
    w.add("a.bin", b"x")
    w.close()
    with pytest.raises(RuntimeError, match="closed"):
        w.add("b.bin", b"y")


def test_oversized_metadata_comment_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="exceeds ZIP entry-comment limit"):
        with ShardWriter(tmp_path, entries_per_shard=10) as w:
            w.add("a.bin", b"x", metadata={"big": "X" * 70_000})


def test_empty_shard_set_writes_index(tmp_path: Path) -> None:
    """Closing a writer with zero entries still produces a valid index.parquet.

    Empty is a value, not a missing state: downstream readers use the
    presence of ``index.parquet`` as the "closed cleanly" signal, so a
    zero-row writer must emit one or "empty" collapses into "never
    finalized". The reserved columns must always be present so reader
    column lookups still resolve.
    """
    with ShardWriter(tmp_path, entries_per_shard=10):
        pass

    assert (tmp_path / "index.parquet").is_file()
    assert not list(tmp_path.glob("shard-*.zip"))

    table = pq.read_table(tmp_path / "index.parquet")
    assert table.num_rows == 0
    cols = set(table.column_names)
    for required in ("logical_name", "shard", "offset", "length"):
        assert required in cols, f"missing required column {required!r}"


def test_empty_shard_set_honors_explicit_schema(tmp_path: Path) -> None:
    """When the caller passes a schema and writes zero entries, the empty
    ``index.parquet`` must carry that exact schema — not a stripped-down
    fallback — so downstream consumers can rely on column shape regardless
    of row count."""
    schema = pa.schema(
        [
            ("logical_name", pa.string()),
            ("shard", pa.int64()),
            ("offset", pa.int64()),
            ("length", pa.int64()),
            ("seq_no", pa.uint32()),
            ("quality_score", pa.float32()),
        ]
    )
    with ShardWriter(tmp_path, entries_per_shard=10, schema=schema):
        pass

    table = pq.read_table(tmp_path / "index.parquet")
    assert table.num_rows == 0
    assert table.schema.equals(schema), (
        f"empty parquet did not preserve caller schema; got {table.schema}"
    )
