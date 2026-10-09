import threading
from pathlib import Path

from shardio import ShardReader, ShardWriter


def test_fd_cache_honors_max_open_shards(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=2) as w:
        for i in range(10):
            w.add(f"e_{i:02d}.bin", f"p-{i}".encode())

    # 5 shards total; cap at 2 open fds.
    with ShardReader(tmp_path, max_open_shards=2) as r:
        for i in range(10):
            r.read(f"e_{i:02d}.bin")
            assert len(r._fd_cache) <= 2


def test_fd_cache_lru_eviction_order(tmp_path: Path) -> None:
    with ShardWriter(tmp_path, entries_per_shard=1) as w:
        for i in range(4):
            w.add(f"e_{i}.bin", f"p{i}".encode())

    with ShardReader(tmp_path, max_open_shards=2) as r:
        r.read("e_0.bin")  # opens shard 0: cache {0}
        r.read("e_1.bin")  # opens shard 1: cache {0,1}
        r.read("e_0.bin")  # touches 0: cache {1,0}
        r.read("e_2.bin")  # evicts 1: cache {0,2}
        assert set(r._fd_cache.keys()) == {0, 2}


def test_concurrent_reads_under_lru_pressure(tmp_path: Path) -> None:
    """Many threads reading from many shards with a tight max_open_shards.

    Without the dup fix, evictions race with concurrent preads and produce EBADF.
    """
    n_shards = 20
    entries_per_shard = 5
    with ShardWriter(tmp_path, entries_per_shard=entries_per_shard) as w:
        for i in range(n_shards * entries_per_shard):
            w.add(f"e_{i:04d}.bin", f"payload-{i}".encode())

    r = ShardReader(tmp_path, max_open_shards=2)
    errors: list[BaseException] = []
    barrier = threading.Barrier(8)

    def worker() -> None:
        try:
            barrier.wait()
            for _ in range(200):
                for i in range(0, n_shards * entries_per_shard, 7):
                    name = f"e_{i:04d}.bin"
                    expected = f"payload-{i}".encode()
                    assert r.read(name) == expected
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    r.close()
    assert not errors, errors
