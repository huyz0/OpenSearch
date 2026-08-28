# Lazy block-range reads

- Code: `serverless/shell/src/main/java/org/opensearch/serverless/store/` — `BlockCache`,
  `ObjectStoreIndexInput`, `BlockCacheDirectory`
- Tests: `ServerlessBlockReadTests` (2)
- Result: **94 tests, 0 failures**; `check` green on both projects.

## What changed

A reader no longer downloads a shard to serve it. Its Lucene `Directory` reads published segments out of
the object store a block at a time, caching what it touches:

| | Before | After |
|---|---|---|
| Serving a shard | download every segment file whole | fetch the blocks a query reads |
| Cost of a cache miss | the whole shard | one 64 KiB range request |

Measured on a 20,000-document shard publishing **422 KB** of segments:

```
open  : 57,833 bytes
query :  12,288 bytes      (a selective term match)
total : 70,121 bytes  =  16% of the shard
```

A repeated query fetches **0 bytes** and is served from cache.

## Why this matters more than it looks

It is what stops cache placement from being load-bearing. With whole-segment downloads a misrouted query
costs seconds, so placement has to be right; with block ranges it costs tens of milliseconds, so
placement is an optimisation and the system stays correct when it is wrong. That is the distinction
M11's `ReaderPlacement` documentation depends on — and until now it was a claim the code did not support.

## Open costs a fixed amount; queries cost what they read

An earlier run of this test, on a 53 KB index, reported **84%** and looked like a failure. It was not:
open reads segment metadata and OpenSearch's `Store` hashes small files whole for recovery diffing, and
on a tiny index that fixed cost *is* the index. At 422 KB it is 14%; at realistic shard sizes it is a
rounding error, because it does not grow with the shard while the shard does.

The test asserts both halves separately — total under half the shard, and the **query increment** under a
tenth — because the second is the sharper claim and the one that keeps holding as shards grow.

## The directory is hybrid, and had to be

Purely read-only does not work: opening a restored shard runs `Store#bootstrapNewHistory`, which writes
a fresh `segments_N`. So local disk holds what this node writes and the object store holds what was
published, with lookups preferring local. That is also the honest description of the architecture rather
than a workaround for it.

**Deletes of published files are ignored.** Lucene removes superseded segments as it commits, but a
published blob is not a reader's to delete — other readers may be serving the same commit, and
reclaiming it belongs to `GarbageCollector`, which knows what the manifest still references.

## Canaries

| Canary | Failure produced |
|---|---|
| Read from offset 0 to end of file instead of the block | `codec footer mismatch (file truncated?)` |
| Prefetch every block on first touch — correct, but eager | `fetched 417,423 of 417,423 published bytes -- that is not lazy` |

The second is the one that matters. The first proves the range arithmetic is load-bearing but fails on
correctness; only the second shows the measurement would catch a silent regression to eager reading,
which is the failure mode that would otherwise look like everything working.

## A bug this surfaced

The directory factory reaches the metadata plane through the node, but several call sites pass the plane
as a method argument and never set it on the node. Those nodes built **local-only** directories and
served an empty shard with no error — `IndexNotFoundException ... files: [extra0]`, which reads like a
missing index rather than a misconfigured one. `syncFrom` and `serveAsReader` now adopt the plane they
are given.

Also worth recording: the store's directory arrives through `newFSDirectory(location, lockFactory,
settings)`, not `newDirectory(indexSettings, shardPath)`. There is no shard identity in those arguments,
so it is recovered from the index name in the settings and the shard number in the path.

## What this does NOT establish

- **Writers still download.** A writer needs a local writable copy to merge into, so only readers are
  lazy. Making writers lazy is a different and larger problem.
- **Block size is a guess.** 64 KiB by default, tunable via `serverless.block_cache.block_size`; nothing
  has measured the right value against a real object store's per-request cost.
- **The cache is per-node, in-heap, and bounded by block count**, not by bytes or by pressure. There is
  no disk tier, no eviction policy beyond LRU, and no sharing between nodes.
- **No readahead.** Sequential scans fetch one block at a time and will be slower than they need to be.
- **`FsBlobContainer` only** (D5/R11). Range requests against a local file are not range requests against
  S3, and this is precisely the kind of claim R11 exists to check.
