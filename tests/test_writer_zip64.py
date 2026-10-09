"""ZIP64 boundary test. Slow. Writes ~4.5 GiB to tmp_path.

Requires ~5 GiB of free space on the filesystem where pytest tmp is rooted.
"""

from pathlib import Path

import pyarrow.parquet as pq  # type: ignore[import-untyped]  # pyarrow ships no stubs
import pytest

from shardio import ShardWriter


@pytest.mark.slow
def test_shard_larger_than_4gib(tmp_path: Path) -> None:
    chunk = b"A" * (64 * 1024 * 1024)  # 64 MiB
    with ShardWriter(tmp_path, entries_per_shard=100) as w:
        for i in range(72):  # 72 * 64 MiB = 4.5 GiB
            w.add(f"chunk_{i:03d}.bin", chunk)

    shard = tmp_path / "shard-00000.zip"
    assert shard.stat().st_size > 4 * 1024**3

    table = pq.read_table(tmp_path / "index.parquet")
    assert table.num_rows == 72

    name = "chunk_071.bin"
    names = table["logical_name"].to_pylist()
    row_idx = names.index(name)
    offset = table["offset"][row_idx].as_py()
    length = table["length"][row_idx].as_py()
    assert length == len(chunk)
    with open(shard, "rb") as f:
        f.seek(offset)
        assert f.read(1024) == b"A" * 1024
