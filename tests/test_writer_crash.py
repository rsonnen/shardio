"""Crash simulation: SIGKILL during writer leaves lock; subsequent writer fails until wiped."""

import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from shardio import ShardWriter

CRASH_SCRIPT = """
import sys
import time
from shardio import ShardWriter

target = sys.argv[1]
w = ShardWriter(target, entries_per_shard=10_000)
for i in range(100):
    w.add(f"e_{i:04d}.bin", b"X" * 1024)
# Sleep long enough for the parent to send SIGKILL; never reach close().
time.sleep(60)
"""


def test_lock_survives_sigkill_and_blocks_next_writer(tmp_path: Path) -> None:
    proc = subprocess.Popen(
        [sys.executable, "-c", CRASH_SCRIPT, str(tmp_path)],
    )
    time.sleep(1.5)
    proc.send_signal(signal.SIGKILL)
    proc.wait(timeout=5)

    assert (tmp_path / ".shardio.lock").is_file()

    with pytest.raises(RuntimeError, match="lock"):
        ShardWriter(tmp_path, entries_per_shard=10_000)

    for p in tmp_path.iterdir():
        p.unlink()
    with ShardWriter(tmp_path, entries_per_shard=10) as w:
        w.add("a.bin", b"ok")
    assert (tmp_path / "index.parquet").is_file()
