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

## Upgrading a blob store written by an older build

Descriptors are stored with a format marker and a version. A blob written before
that marker existed is refused on read, with a message saying so, rather than
parsed as though its layout matched the current one.

This matters because it used to fail the other way. The descriptor record gains
fields over time and the stored bytes carry no transport version to gate them, so
a reader always assumed the newest layout was present. A blob written before a
field was added parsed without complaint and returned values read from the wrong
offsets: a shard count taken from the middle of a uuid, which routes.

There is no migration. If you have a blob store written by a build from before
this change, clear its `descriptors-root` prefix and recreate the indices. The
alternative was to keep guessing, and a wrong answer about where a shard lives is
worse than a refusal to answer.

Live descriptors would heal on their own, since they are rewritten whenever their
index changes. Tombstones would not: they are written once and then left for a
retention window, so an unreadable one stays unreadable, and the scrubber will
decline to reclaim it rather than delete something whose age it cannot establish.

## Reclaiming deleted index records

Deleting a gated index does not remove its record. It writes a tombstone at
`<base>/descriptors-root/tombstones/<name>`, so that a node partitioned through
the delete consults the descriptor on rejoin, finds the deletion recorded, and
drops its local shard data instead of keeping it. Absence alone would not tell it
that: a name that resolves to nothing is indistinguishable from one that never
existed.

Tombstones are small, a few hundred bytes each, and nothing removes them unless
you arrange it. Left alone they accumulate with every index ever deleted, not
with the number that currently exist, so the count tracks the cluster's lifetime
rather than its size.

**How long they need to live.** A tombstone has to outlast the longest a node can
be partitioned and still rejoin carrying shard data from before the delete. That
is an operational judgement about your own failure modes rather than something
the plugin can derive, so pick it deliberately. Seven days is a reasonable
starting point and is what `serverless_storage.descriptor.tombstone_retention`
defaults to.

Reclaiming late costs storage. Reclaiming early costs a node the record it needed
to drop stale data, so err long.

### Preferred: let the object store expire them

An object-store lifecycle rule over the tombstone prefix costs nothing to run.
There is no listing, no read, and no request of any kind charged to the cluster,
and the store enforces it whether or not any node is healthy. Where your store
supports it, this is the right mechanism.

On S3, scoped to the tombstone prefix and nothing else:

```json
{
  "Rules": [
    {
      "ID": "serverless-descriptor-tombstones",
      "Status": "Enabled",
      "Filter": { "Prefix": "descriptors-root/tombstones/" },
      "Expiration": { "Days": 7 }
    }
  ]
}
```

Prefix it with your repository's own base path if the repository is not rooted at
the bucket. GCS calls the same thing an Object Lifecycle rule with an `age`
condition; Azure calls it a blob lifecycle management policy.

Expiring by object age is exactly right here, because a tombstone is written once
and never rewritten. Its age and its recorded deletion time are the same thing.

**Scope the rule.** It must match only `descriptors-root/tombstones/`. A rule over
`descriptors-root/` would expire live descriptors, which are not rewritten unless
the index changes, so a quiet index would lose its record and its shards would
stop resolving.

### Fallback: the built-in scrubber

For stores with no lifecycle facility, and for deployments where nobody
configured a rule, the plugin can reclaim tombstones itself. It is off unless you
set an interval:

```yaml
serverless_storage.descriptor.tombstone_scrub_interval: 1h
serverless_storage.descriptor.tombstone_retention: 7d
```

A pass costs one listing of the tombstone space plus one read per tombstone it
examines, and runs on the elected cluster manager only. That is affordable at a
modest population and is not how anyone should reclaim a hundred million records:
prefer the lifecycle rule where you can have it.

The scrubber decides from each tombstone's own recorded deletion time rather than
from any index of what to delete, so there is no second structure to fall out of
step with the first. It refuses to reclaim anything whose age it cannot establish,
including tombstones written before the record carried a timestamp.

### If you configure neither

Nothing breaks and nothing is lost. Tombstones accumulate, and the storage they
occupy grows with the number of indices ever deleted. At a few hundred bytes each
that is cheap for a long time, and it is still unbounded, so it is worth deciding
rather than defaulting into.

## Bounding the descriptor change log

Nodes learn about descriptor writes on other nodes by tailing a change log under
`<base>/descriptors-root/changelog/`, bucketed by minute. Buckets older than
`serverless_storage.descriptor.change_log_retention` (default one hour) are
deleted by the elected cluster manager.

Nothing reads history: a starting node begins at the bucket it started in,
because its descriptor cache is empty and it has no shards open, so there is
nothing for older entries to act on. The retention window is slack for a tailer
that has been stalled, not a replay buffer, and there is no reason to raise it far.

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
