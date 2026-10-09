from pathlib import Path

import pytest

from shardio import ShardWriter


def test_lock_file_created_and_released(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=10) as w:
        w.add("a.bin", b"x")
        assert (tmp_path / ".shardio.lock").is_file()
    assert not (tmp_path / ".shardio.lock").exists()


def test_second_writer_refuses_with_existing_lock(tmp_path: Path) -> None:
    (tmp_path / ".shardio.lock").write_text("owned")
    with pytest.raises(RuntimeError, match="lock"):
        ShardWriter(tmp_path, entries_per_shard=10)


def test_lock_released_on_exception(tmp_path: Path) -> None:
    try:
        with ShardWriter(tmp_path, entries_per_shard=10) as w:
            w.add("a.bin", b"x")
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert not (tmp_path / ".shardio.lock").exists()
