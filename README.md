# shardio

Sharded ZIP archive storage with a Parquet index. Built for many-small-blob workloads on network filesystems, where per-file metadata round trips dominate latency.

## Why

A few hundred thousand small files on SMB/NFS makes the kernel cry: every `open` / `stat` / `close` is a network round trip, and bulk operations like `cp` of an output directory degrade to serial per-file latency. shardio collapses that into a handful of sealed ZIP archives plus a Parquet sidecar — the round-trip count drops by three to four orders of magnitude while random-access reads stay O(1) via direct `pread()`.

## Install

```bash
pip install shardio
# or
uv add shardio
```

## Usage

### Write

```python
from shardio import ShardWriter

with ShardWriter("/path/to/set", entries_per_shard=10_000) as w:
    for name, data, meta in my_entries():
        w.add(name, data, metadata=meta)
```

- `entries_per_shard` is the only sealing trigger (no byte cap). Default 10,000.
- `metadata` is optional. Keys must be consistent across calls (subset OK; missing keys → null in Parquet). Disjoint key sets raise.
- `logical_name` must be unique within the shard set; duplicates raise.
- Reserved column names (`logical_name`, `shard`, `offset`, `length`) cannot appear in metadata.

### Read

```python
from shardio import ShardReader

with ShardReader("/path/to/set") as r:
    blob: bytes = r.read("item_000142.bin")
```

- Construction loads the Parquet index fully into memory (~64 bytes/entry).
- `read()` is O(1) lookup + a single `pread()`. File handles are LRU-cached per shard.
- Thread-safe for reads.

### Iterate in shard order (efficient bulk passes)

```python
with ShardReader("/path/to/set") as r:
    for name, blob in r.iter():
        process(name, blob)
```

`iter()` yields entries in shard+offset order, opening each shard exactly once.

### Inspect metadata without reading blobs

```python
with ShardReader("/path/to/set") as r:
    table = r.metadata  # pyarrow.Table — caller columns only
    filtered = table.filter(table["score"] >= 0.8)
    for name in filtered["logical_name"].to_pylist():
        blob = r.read(name)
        ...
```

Internal columns (`shard`, `offset`, `length`) are hidden from `r.metadata`; `logical_name` is included as a join key.

## Recovery

If `index.parquet` is lost or corrupted but the `shard-*.zip` files are intact:

```python
from shardio import reindex

reindex("/path/to/set")
```

Both the library columns (`logical_name`, `shard`, `offset`, `length`) AND the caller-supplied metadata columns are recovered: each ZIP entry carries a JSON-encoded copy of its metadata in the entry comment, and `reindex()` parses those comments to rebuild the Parquet index. No external source-of-truth required.

## Crash contract

shardio does not implement resume. If a writer crashes mid-write:

1. The lock file `.shardio.lock` is left behind. The next writer construction raises.
2. The caller must wipe the directory and restart.

This matches the assumption that consumers are idempotent and that wipe-and-retry is cheap.

## Constraints (v1)

- **One writer per shard-set directory.** Enforced by lock file.
- **Many concurrent readers.** Thread-safe.
- **Writes are not resumable.** Crashes require wiping the directory.
- **No compression.** Blobs are stored `ZIP_STORED`. Callers supply already-compressed content (JPEG, PNG, mp4 fragments, etc.).
- **No deletion of individual entries.** Rewrite the shard set if entries must be removed.
- **POSIX paths only on the writer side.** Object-storage support is a future concern.

## Dependencies

- Python ≥ 3.14
- `pyarrow ≥ 25.0.1` (Parquet I/O)
- Standard library `zipfile` (ZIP_STORED, ZIP64)

## Layout on disk

```
<shard-set-dir>/
├── shard-00000.zip
├── shard-00001.zip
├── ...
├── shard-NNNNN.zip
└── index.parquet
```

ZIP files use `ZIP_STORED` with `allowZip64=True`. The Parquet index records `(logical_name, shard, offset, length)` plus all caller-supplied columns, sorted by `logical_name` for binary-search-friendly on-disk lookups. A duplicate of the caller metadata is stamped into each ZIP entry's comment as JSON, enabling full reindex recovery.

## Testing

```bash
uv sync --group dev
uv run pytest                 # default suite
uv run pytest -m slow         # ZIP64 boundary test (~5 GiB on tmpfs/disk)
uv run mypy                   # strict type checking
uv run ruff check . && uv run ruff format --check .
```
