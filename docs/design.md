# Design

shardio stores many small byte blobs as a handful of sealed ZIP archives ("shards") plus a Parquet index, so random-access reads stay cheap and bulk operations (copy, backup, listing) stop paying per-file round trips. It is domain-agnostic: blobs are opaque bytes, metadata is caller-supplied columns.

## Motivation

On network filesystems (SMB, NFS) every `open`, `stat` and `close` is a round trip. Hundreds of thousands of small files make bulk operations degrade to serial per-file latency. Consolidating into a few large archives cuts the round-trip count by three to four orders of magnitude while keeping O(1) random access.

## Scope

In scope: an append-and-rotate writer, a reader serving blobs by name via `pread()`, a reindexer that rebuilds the index from the shards, and arbitrary caller metadata stored alongside the library columns.

Out of scope (v1): compression, concurrent writers, resuming a crashed write, modifying sealed shards, deleting individual entries, writing to object storage.

## On-disk layout

```
<shard-set-dir>/
  shard-00000.zip ... shard-NNNNN.zip
  index.parquet
  .shardio.lock        (only while a writer is open or after a crash)
```

- Shards use `ZIP_STORED` with ZIP64 always enabled, so a shard can exceed 4 GiB. The caller supplies already-compressed content.
- The entry name is the caller's `logical_name`, unique across the shard set.
- Each entry's ZIP comment holds a JSON copy of its metadata. It duplicates the Parquet columns and lets `reindex()` recover them if the index is lost.
- `index.parquet` has one row per entry, sorted by `logical_name`. Library columns: `logical_name`, `shard`, `offset`, `length` (offset points at the payload, past the local file header). Caller columns follow. Callers may pass an explicit `pyarrow.Schema`; otherwise types are inferred.

## Writer

- `entries_per_shard` (default 10,000) is the only sealing trigger; there is no byte cap.
- Metadata keys may vary between calls as long as consecutive key sets are not entirely disjoint; missing values become nulls. Reserved column names are rejected, as are duplicate names and metadata comments over the 65,535-byte ZIP comment limit.
- `close()` seals the last shard, fsyncs shards, writes `index.parquet.tmp`, fsyncs it, renames it atomically over `index.parquet`, fsyncs the directory and releases the lock.
- A zero-entry writer still produces an `index.parquet` (with the caller's schema, or the reserved columns). "Empty" is a value; the index's presence means "closed cleanly".

## Crash contract

One writer per directory, enforced by `.shardio.lock`. A crashed writer leaves the lock behind and the next writer refuses to start. There is no resume and no stale-lock recovery: wipe the directory and rerun. This suits idempotent producers where retry is cheap. An exception inside the `with` block releases the lock but leaves the partial output as is.

## Reader

- Construction loads the whole index into memory (a name to shard/offset/length dict, roughly 64 bytes per entry).
- `read(name)` is a dict lookup and one `os.pread()`. It never goes through `zipfile`; the recorded offset is authoritative.
- Shard file descriptors are cached in an LRU. Callers receive a `dup()` of the cached descriptor, so eviction by another thread cannot close a descriptor mid-read. Reads are thread-safe.
- `iter()` yields entries in shard-then-offset order, opening each shard once.
- `metadata` is a `pyarrow.Table` of the caller columns plus `logical_name` as a join key.

## Reindexer

`reindex(dir)` walks `shard-*.zip` in name order, reads each central directory, computes payload offsets from the local header (whose name and extra-field lengths can differ from the central directory's), parses the JSON comments to recover metadata, and writes the index atomically. `metadata_from_comments=False` rebuilds the library columns only. Invalid comments fail fast.
