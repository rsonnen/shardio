"""Performance regression checks for ShardReader. Run with `-m slow`."""

from __future__ import annotations

import random
import time
from pathlib import Path

import pytest

from shardio import ShardReader, ShardWriter


@pytest.mark.slow
def test_reader_open_and_random_access_perf(tmp_path: Path) -> None:
    n_entries = 100_000
    payload = b"X" * 16

    with ShardWriter(tmp_path, entries_per_shard=10_000) as w:
        for i in range(n_entries):
            w.add(f"e_{i:06d}.bin", payload)

    t0 = time.perf_counter()
    reader = ShardReader(tmp_path)
    open_elapsed = time.perf_counter() - t0
    print(f"\n[perf] ShardReader construction over {n_entries} entries: {open_elapsed:.3f}s")
    assert open_elapsed < 2.0, f"reader construction too slow: {open_elapsed:.3f}s"

    rng = random.Random(0xC0FFEE)
    names = [f"e_{rng.randrange(n_entries):06d}.bin" for _ in range(10_000)]

    try:
        t0 = time.perf_counter()
        for name in names:
            reader.read(name)
        read_elapsed = time.perf_counter() - t0
    finally:
        reader.close()
    mean_us = (read_elapsed / len(names)) * 1_000_000
    print(f"[perf] 10k random reads: {read_elapsed:.3f}s total, mean {mean_us:.1f} us/read")
    # Generous 1 ms per read to avoid CI flakiness while still catching regressions.
    assert read_elapsed / len(names) < 1e-3, (
        f"mean read too slow: {read_elapsed / len(names) * 1000:.3f} ms"
    )
