---
title: Configuration
description: Node- and index-level settings registered by the serverless storage plugin.
---

All node settings are defined in `ServerlessStoragePlugin.java`. Most background machinery ships **disabled by default** — see the [Core Changes](/changes/) page for which defaults were deliberately kept off even after their underlying fix landed (WAL batching).

## Enabling the engine

| Setting | Level | Purpose |
|---|---|---|
| `index.serverless_storage.enabled` | index | Opts an index into this engine instead of the classic one. |
| `index.serverless_storage.lazy_directory.enabled` | index | Reader engine fetches only the files a query touches, instead of downloading full bundles eagerly. |
| `index.serverless_storage.wal.dedicated_stream` | index | Gives the shard its own WAL stream instead of sharing a node-level one — higher cost, used when a shard needs isolation from noisy neighbors. |

## Object store target

| Setting | Purpose |
|---|---|
| `serverless_storage.base_path` | Base path prefix within the repository for this plugin's blobs. |
| `serverless_storage.repository` | Name of the registered blob-store repository to use. |
| `serverless_storage.encryption_key` | Key material for AES-GCM encryption of blobs (see `security/`). |

## Caching

| Setting | Purpose |
|---|---|
| `serverless_storage.bundle_cache_size` | Size of the local-disk ciphertext cache in front of the object store (`LocalDiskCachingBundleStore`). |
| `serverless_storage.local_cache_max_bytes` | Cap on local cache usage in bytes. |
| `serverless_storage.max_file_cache_usage_ratio` | Fraction of available disk the file cache may consume. |
| `serverless_storage.lazy_directory.cache_size` | Cache size for the lazy-directory (per-file) fetch path. |
| `serverless_storage.reader_cache_affinity` | TTL for routing repeat reads of a shard back to the same node's warm cache. |

## WAL

| Setting | Purpose |
|---|---|
| `serverless_storage.wal_mirroring.enabled` | Mirrors ordinary translog writes into the object-store WAL (`WalMirroringTranslog`). |
| `serverless_storage.wal_gc_interval` | How often the WAL chunk GC sweep runs. |
| `serverless_storage.wal_per_shard_budget` | Per-shard WAL size budget. |
| `serverless_storage.wal_flush.batching.enabled` | Enables group-commit batching (`AsyncIOProcessor`-based). **Off by default** — see Core Changes. |
| `serverless_storage.wal_flush.interval` | Batching flush interval. |
| `serverless_storage.wal_flush.queue_capacity` | Max queued records before backpressure. |
| `serverless_storage.wal_flush.byte_threshold` | Byte-count that triggers an early flush regardless of interval. |
| `serverless_storage.wal_flush.backlog` | Backlog threshold used for request-level 429 backpressure. |
| `serverless_storage.wal_dedicated_stream` | Node-level default for the index-level dedicated-stream setting above. |

## GC and retention

| Setting | Purpose |
|---|---|
| `serverless_storage.gc_interval` | How often `GcSchedulerTask` sweeps for superseded/orphaned manifests and bundles. |
| `serverless_storage.gc_retention_window` | How long a superseded manifest generation is kept before it becomes GC-eligible. |
| `serverless_storage.pitr_window` | Point-in-time-recovery retention window; pins manifest generations within this window. |

## Compaction

| Setting | Purpose |
|---|---|
| `serverless_storage.compaction_interval` | How often the Lucene-merge-based rebase/compaction scheduler runs. |
| `serverless_storage.partition_rewrite_interval` | How often the background physical rewrite (post-split file-range cleanup) runs. |
| `serverless_storage.publication_rate_limit` | Rate limit on manifest publication. |

## Scale-to-zero

| Setting | Purpose |
|---|---|
| `serverless_storage.scale_to_zero.idle_threshold` | How long a shard must be idle before it becomes a suspension candidate. |
| `serverless_storage.scale_to_zero.lag_threshold` | Max acceptable replication/WAL lag for a candidate. |
| `serverless_storage.scale_to_zero.eval_interval` | How often `ScaleToZeroCandidatesSchedulerTask` evaluates candidates. |
| `serverless_storage.scale_to_zero.suspend_enabled` | Master switch — evaluation runs either way, but actual suspension only happens when this is true. |
| `serverless_storage.scale_to_zero.search_reactivation_wait` | How long a reactivating shard waits before serving search after a request triggers it. |
| `serverless_storage.scale_to_zero.cooldown` | Minimum time between suspend/reactivate cycles for the same shard. |

## Scale-up

| Setting | Purpose |
|---|---|
| `serverless_storage.scale_up.enabled` | Master switch for reader-replica expansion. |
| `serverless_storage.scale_up.qpm_threshold` | Queries-per-minute threshold that triggers expansion. |
| `serverless_storage.scale_up.max_search_replicas` | Cap on reader replicas per shard. |
| `serverless_storage.scale_up.eval_interval` | How often `ScaleUpCandidatesSchedulerTask` evaluates. |
| `serverless_storage.scale_up.required_consecutive_ticks` | Consecutive over-threshold ticks required before acting, to avoid flapping. |
| `serverless_storage.scale_up.max_expansions_per_tick` | Cap on how many expansions happen in a single evaluation tick. |
| `serverless_storage.shard_count_advisor_eval_interval` | Eval interval for the data-stream shard-count advisor. |

## Resharding (split / merge)

| Setting | Purpose |
|---|---|
| `serverless_storage.resharding.split_candidate.wpm_threshold` | Writes-per-minute threshold for split candidacy. |
| `serverless_storage.resharding.split_candidate.size_threshold_bytes` | Size threshold for split candidacy. |
| `serverless_storage.resharding.auto_split.enabled` | Master switch for automatic in-place splitting. |
| `serverless_storage.resharding.auto_split.eval_interval` | How often split candidates are evaluated. |
| `serverless_storage.resharding.auto_split.required_consecutive_ticks` | Consecutive over-threshold ticks required before splitting. |
| `serverless_storage.resharding.auto_split.max_splits_per_tick` | Cap on splits triggered per evaluation tick. |
| `serverless_storage.resharding.merge_candidate.combined_wpm_threshold` | Combined writes-per-minute threshold, below which a split's two children become merge candidates. |
| `serverless_storage.resharding.merge_candidate.combined_size_threshold_bytes` | Combined size threshold for merge candidacy. |
| `serverless_storage.resharding.auto_merge.enabled` | Master switch for automatic in-place merging. |
| `serverless_storage.resharding.auto_merge.eval_interval` | How often merge candidates are evaluated. |
| `serverless_storage.resharding.auto_merge.required_consecutive_ticks` | Consecutive under-threshold ticks required before merging. |
| `serverless_storage.resharding.auto_merge.max_merges_per_tick` | Cap on merges triggered per evaluation tick. |
| `serverless_storage.resharding.auto_merge.min_cooldown` | Minimum time before a freshly-split pair becomes merge-eligible again. |

:::note
Exact setting names above are transcribed from `Setting.*` declarations in `ServerlessStoragePlugin.java`; confirm exact key strings and defaults against that file before relying on them in an automated config, since this page summarizes purpose rather than reproducing every declaration verbatim.
:::
