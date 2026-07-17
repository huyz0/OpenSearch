# serverless-storage

Object-store-native storage engine for OpenSearch's serverless data plane: segment
bundles, commit manifests, a shared write-ahead log, background GC, and separate
writer/reader engines. See `rfc-serverless-opensearch.md` at the repo root for the
full architecture and design rationale; this file only covers how to build, enable,
and run it locally.

An index only gets the object-store engine if it opts in explicitly — everything
else keeps the classic engine untouched.

## Build and test

```
./gradlew :plugins:serverless-storage:build
./gradlew :plugins:serverless-storage:internalClusterTest
```

## Enabling it on a cluster

Remote cluster state is a hard prerequisite — it's a final (non-dynamic) node
setting, so it must be set at node startup:

```yaml
cluster.remote_store.state.enabled: true
```

Point the plugin at a blob store. For local testing, a plain filesystem directory
is enough — no fake object store needed:

```yaml
serverless_storage.base_path: /path/to/local/blob/root
```

For a real object store, register a snapshot repository (S3/GCS/Azure) first, then
reference it by name instead:

```yaml
serverless_storage.repository: my-s3-repo
```

`ServerlessStorageS3FixtureIT` is a working template for running the S3-backed path
against the in-process `s3-fixture` test fixture, no real cloud credentials
required.

Then create an index with the object-store engine turned on:

```
PUT /my-index
{
  "settings": {
    "index.serverless_storage.enabled": true
  }
}
```

Other index-level opt-ins:

- `index.serverless_storage.lazy_directory.enabled` — searchable-snapshot-style
  lazy directory for reader copies, instead of eagerly downloading full bundles.
- `index.serverless_storage.wal.dedicated_stream` — gives the index its own WAL
  chunk stream instead of sharing one across indices.

## What's off by default

Most background machinery (compaction, GC, WAL GC, scale-to-zero, scale-up,
auto-split, auto-merge, WAL mirroring, WAL flush batching) ships disabled —
either via an `enabled` flag or an `interval` defaulting to `-1`. Nothing runs
until you turn it on. See `ServerlessStoragePlugin.java` for the full settings
list and defaults, or `rfc-serverless-opensearch.md` for the reasoning behind
each one.

Two worth knowing about up front:

- `serverless_storage.wal_mirroring.enabled` (default `false`) — mirrors writes
  to the WAL in the object store for durability. `serverless_storage.wal_flush.batching.enabled`
  (default `false`) group-commits those WAL uploads instead of one PUT per
  document; still off by default pending load testing against the RFC's cost
  target (see `dynamic-partitioning-progress.md`'s WAL batching section).
- `serverless_storage.encryption_key` — a base64 AES key stored in the node
  keystore. Unset means bundles are written unencrypted.

## Operator REST API

All under `/_plugins/_serverless/storage`.

**Split / merge / resharding**
- `POST .../_split`, `POST .../_shrink`, `POST .../_shrink/{source_index}/_retire`
- `GET  .../_resharding/split_candidates`
- `POST .../_resharding/_orchestrate_split/{source}`
- `POST .../_resharding/_provision_split_targets/{source}`
- `POST .../_resharding/_cutover/{alias}`
- `POST .../_resharding/_enable_write_routing/{alias}`, `POST .../_resharding/_disable_write_routing`
- `POST .../_partition_rewrite`
- `GET  .../_scale_up/candidates`, `GET .../_scale_to_zero/candidates`, `POST .../_reactivate`

**Clone / migrate / compact**
- `POST .../_clone`
- `POST .../{index_uuid}/{shard_id}/_migrate`
- `POST .../_compact`

**Retention / snapshot**
- `POST .../_snapshot_pin`, `POST .../index/{index}/_snapshot_pin`
- `POST .../_snapshot_release`, `POST .../index/{index}/_snapshot_release`
- `POST .../_snapshot_restore`, `POST .../index/{index}/_snapshot_restore`
- `GET  .../{index_uuid}/{shard_id}/_retention_stats`

**Stats / diagnostics** (all `GET` unless noted)
- `.../_cache_stats`, `.../_object_store_request_stats`, `.../_wal_backlog`,
  `.../_idle_shards`, `.../_manifest_lag`
- `.../{index_uuid}/{shard_id}/_idle_time`
- `.../{index_uuid}/{shard_id}/_realtime_get/{id}`
- `.../{index_uuid}/{shard_id}/_wait_for_generation`
- `POST .../{index_uuid}/{shard_id}/_poll_now`

## Further reading

- `rfc-serverless-opensearch.md` (repo root) — the plugin's RFC and full design.
- `dynamic-partitioning-plan.md` / `dynamic-partitioning-progress.md` (repo root) —
  design and session-by-session implementation log for shard split/merge and WAL
  batching.
- `plugins/serverless-storage/formal/` — TLA+ specs for GC safety, shard head
  invariants, and WAL replay fencing.
- `ServerlessStorageIntegTestCase.java` (`src/internalClusterTest`) — base class
  for real-cluster integration tests; individual subclasses show working
  configuration examples.
