"""End-to-end integration: 10K entries, 10 shards, writer + reader + iter + reindex."""

from pathlib import Path

from shardio import ShardReader, ShardWriter, reindex


def test_end_to_end_10k_entries(tmp_path: Path) -> None:
    n = 10_000
    with ShardWriter(tmp_path, entries_per_shard=1000) as w:
        for i in range(n):
            name = f"e_{i:06d}.bin"
            data = (f"payload-{i}" * (i % 50 + 1)).encode()
            w.add(name, data, metadata={"idx": i, "mod7": i % 7})

    assert len(list(tmp_path.glob("shard-*.zip"))) == 10

    with ShardReader(tmp_path) as r:
        for i in [0, 1, 999, 1000, 5432, 9999]:
            name = f"e_{i:06d}.bin"
            expected = (f"payload-{i}" * (i % 50 + 1)).encode()
            assert r.read(name) == expected

        seen = sum(1 for _ in r.iter())
        assert seen == n

        meta = r.metadata
        assert meta.num_rows == n
        assert set(meta.column_names) >= {"logical_name", "idx", "mod7"}

    (tmp_path / "index.parquet").unlink()
    reindex(tmp_path)
    with ShardReader(tmp_path) as r2:
        for i in [0, 4321, 9999]:
            name = f"e_{i:06d}.bin"
            expected = (f"payload-{i}" * (i % 50 + 1)).encode()
            assert r2.read(name) == expected
