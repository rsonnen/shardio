from pathlib import Path

import pyarrow as pa  # type: ignore[import-untyped]  # pyarrow ships no stubs
import pyarrow.parquet as pq  # type: ignore[import-untyped]  # pyarrow ships no stubs
import pytest

from shardio import ShardReader, ShardWriter


def _index_schema(score_type: pa.DataType) -> pa.Schema:
    return pa.schema(
        [
            ("logical_name", pa.string()),
            ("shard", pa.uint16()),
            ("offset", pa.uint64()),
            ("length", pa.uint64()),
            ("score", score_type),
        ]
    )


def test_explicit_schema_round_trip(tmp_path: Path) -> None:
    schema = _index_schema(pa.float32())
    with ShardWriter(tmp_path, entries_per_shard=10, schema=schema) as w:
        w.add("a.bin", b"hello", metadata={"score": 0.25})
        w.add("b.bin", b"world", metadata={"score": 0.75})

    table = pq.read_table(tmp_path / "index.parquet")
    got = {name: table.schema.field(name).type for name in table.schema.names}
    assert got["logical_name"] == pa.string()
    assert got["shard"] == pa.uint16()
    assert got["offset"] == pa.uint64()
    assert got["length"] == pa.uint64()
    assert got["score"] == pa.float32()

    with ShardReader(tmp_path) as r:
        assert r.read("a.bin") == b"hello"
        assert r.read("b.bin") == b"world"


def test_explicit_schema_type_mismatch_raises(tmp_path: Path) -> None:
    # Schema says score is int64, but metadata is a string — pyarrow cannot coerce.
    schema = _index_schema(pa.int64())
    with pytest.raises((pa.lib.ArrowInvalid, pa.lib.ArrowTypeError, ValueError, TypeError)):
        with ShardWriter(tmp_path, entries_per_shard=10, schema=schema) as w:
            w.add("a.bin", b"x", metadata={"score": "not-an-int"})
            w.add("b.bin", b"y", metadata={"score": "also-not-an-int"})
