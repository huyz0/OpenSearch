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

### The `serverless_` namespace

An index whose name begins with `serverless_` is a **gated** index: it gets no cluster
state entry at all. Its record is a descriptor in the object store, its placement is
computed rather than published, and creating one costs the cluster nothing that grows
with how many indices already exist — which is what makes an index-per-tenant fleet of
that size possible.

```
PUT /serverless_tenant-4711
```

No setting is needed; the name is the declaration, and the engine settings above are
derived from it. The namespace is also a limit, and the creation is refused rather than
quietly downgraded if you cross it: an index in it may not carry an alias, an index
context, a data stream name, or be the target of a shrink/split/clone. Those all need
cluster state entries the index does not have.

An index *outside* the namespace can still use the object-store engine with the setting
above. It keeps its cluster state entry — which is exactly what a data stream backing
index or an alias-bearing index needs.

Other index-level opt-ins:

- `index.serverless_storage.lazy_directory.enabled` — searchable-snapshot-style
  lazy directory for reader copies, instead of eagerly downloading full bundles.
- `index.serverless_storage.wal.dedicated_stream` — gives the index its own WAL
  chunk stream instead of sharing one across indices.

## WAL mirroring is now on by default

`serverless_storage.wal_mirroring.enabled` and
`serverless_storage.wal_flush.batching.enabled` both now default to **`true`**.
They changed together on purpose, and if you turn one off you should think about
the other.

**Why mirroring flipped.** With it off, a serverless index kept its ordinary
local translog as the only durability for anything not yet flushed. Kill the node
and every write acknowledged since the last published manifest is gone —
silently, with no error anywhere, on a shard that by design has zero writer
replicas to recover from. That made the RFC's first goal ("the object store is
the sole durable home of write-ahead data; local disk is strictly a cache") false
in the shipped default, and broke its stated contract that writes degrade to
rejection and never to silent un-durability.

**Why batching had to flip with it.** Mirroring on with batching off flushes one
WAL chunk per operation — roughly one object-store PUT per document — which is
orders of magnitude over the RFC's own request budget and inverts the cost
argument for having a node-level WAL at all. The durability contract is
unchanged: `index.translog.durability=REQUEST` still waits for the upload.

**WAL GC flipped with them.** `serverless_storage.wal_gc.interval` now defaults
to **1 minute** instead of `-1` (off). WAL chunks are reclaimed by nothing else,
so turning mirroring on while leaving its collector off would have traded a
data-loss bug for an unbounded-storage bug. Its cadence is a cost choice, not a
safety one: what is deletable is fixed by published state — a shard replays
strictly forward from the position its own last published manifest covers — so
sweeping more or less often changes only how much already-dead garbage is lying
around, never what may be deleted. That is why this sweep needs no retention
window, unlike segment GC.

`serverless_storage.wal_mirroring.required` (default `false`) is the stricter
option: a serverless writer shard that opens with no WAL service — because
mirroring was turned off, or because the shared WAL container failed to resolve
on that node — refuses to open rather than degrading. Either way it now logs a
warning naming the index and shard.

`serverless_storage.wal_flush.backlog_reject_threshold` also now defaults to
512 MB rather than "unbounded", so the RFC's "bounded window then rejects" is
true rather than aspirational.

## What's off by default

Most background machinery (compaction, GC, scale-to-zero, scale-up,
auto-split, auto-merge) ships disabled — either via an `enabled` flag or an
`interval` defaulting to `-1`. Nothing runs until you turn it on. See
`ServerlessStoragePlugin.java` for the full settings list and defaults, or
`rfc-serverless-opensearch.md` for the reasoning behind each one.

Two worth knowing about up front:

- `serverless_storage.encryption_key` — a base64 AES key stored in the node
  keystore. Unset means bundles are written unencrypted. It must decode to
  exactly 16, 24 or 32 raw bytes; anything else now fails node start with a
  message naming the setting, rather than starting cleanly and failing every
  write afterwards.
- `serverless_storage.reclaim_on_index_delete` (default `false`) — **deleting a
  serverless index currently frees no object-store bytes at all.** GC is per
  shard and runs from a scheduler attached to a live shard, so once the index is
  gone nothing is left that could sweep its prefix, and everything it ever wrote
  stays in the bucket permanently. Turning this on reclaims a deleted shard's
  prefix, refusing whenever a live pin (a clone hop, a snapshot, a PITR window)
  still names it. It ships off for one release because it is the first code path
  here that deletes a whole shard prefix in one call; leaving it off forever is
  not the safe choice, it is the one where storage grows without bound.

### GC and compaction now run without search replicas

`serverless_storage.gc.interval` and `serverless_storage.compaction.interval`
used to be settings you could turn on and have nothing happen. Both schedulers
hung off the *reader* engine, and a reader shard requires
`index.number_of_search_replicas`, which requires `remote_store.enabled`. The
common serverless index — one writer shard, no search replicas — therefore had
neither scheduler anywhere in the cluster, so object-store storage grew forever
and quiescent shards never compacted, silently, no matter what the intervals
said.

Both are now started per shard by the plugin itself, on the node holding that
shard's writer primary, and stopped when that shard closes or relocates. Nothing
about the defaults changed: both intervals still default to `-1`, and setting
them is still how you turn the work on. What changed is that setting them now
does something.

## Security boundaries and known gaps

RFC §12 describes the encryption and credential-scoping model this plugin aims
at. Several parts of it are not implemented, and the code's shape implies
otherwise in a few places, which is more dangerous than a plain omission. What
follows is what is actually true of a deployed node today. §3 already states that
multi-tenant isolation is a non-goal for this repository; this section is the
concrete form of that statement.

### There is one encryption key per node, not one per index

§12 describes a per-index data key, and the code looks like it has one:
`EncryptionKeyProvider#currentKey(String indexUuid)` exists,
`PerIndexEncryptionKeyProvider` exists and is tested, and both the WAL record
path and the blob path call the per-index overload. **No node configuration can
build a per-index provider.** `serverless_storage.encryption_key` is a single
node-level secret and `StaticEncryptionKeyProvider` — which returns that one key
for every index — is the only provider anything ever constructs.

So a reader can follow every call site, find them all correct, and conclude that
per-index key isolation works. It does not. The call sites are correct so that
wiring a real provider becomes a configuration change rather than a code change;
that is worth having, and it is not the same as having isolation. §12's "a
compromised chunk yields nothing without per-index keys" is false as shipped.

`PerIndexEncryptionKeyProvider`'s javadoc records what wiring it for real would
take, and why the blocker is the key-provisioning lifecycle rather than the
setting.

### Blocks are bound to their position; blobs written before this build are not

Each 64 KiB block is an independent AES-GCM envelope. Its tag now also covers
associated data naming the index, shard, container path, blob name, format
version, declared sizes and block index, so a block cannot be moved between
blobs, between indices, or to a different offset, and a blob's cleartext header
cannot be rewritten to truncate it. This holds under the single node-wide key,
because it is an integrity property, not a key-separation one.

Blobs written by earlier builds carry format version 1, which had no associated
data at all. They are still readable, and they are still substitutable,
reorderable and truncatable for as long as they exist. To migrate:

1. Upgrade every node. New writes are version 2 from then on.
2. Rewrite existing bundles — run compaction (`POST
   .../{index_uuid}/{shard_id}/_compact`, or leave
   `serverless_storage.compaction.interval` running), or reindex.
3. Let `serverless_storage.gc.interval` reclaim the superseded manifests and
   bundles. Until then the old objects are still in the bucket.
4. Set `serverless_storage.encryption.require_authenticated_blocks: true` on
   every node and restart. A version 1 blob is then refused rather than read.

Step 4 last: setting it before step 3 finishes makes any surviving version 1 blob
an unreadable shard. The default is `false` so that an upgrade does not turn
existing data into an outage — the plugin has no way to know whether your bucket
still holds version 1 objects, so it is your assertion to make.

### Registers are neither encrypted nor authenticated

Shard-head and pin registers pass through unencrypted, deliberately: they hold
node ids, terms and generation numbers, not document data. The part that was not
written down anywhere is that they are also **unauthenticated**. A caller with
bucket write access can point a shard's head register at any manifest generation
that exists, including a stale one, and every subsequent read is of a
correctly-decrypting, correctly-authenticated, wrong commit. Term-fencing defends
against concurrent writers inside the protocol; it does not defend against a
writer outside it. Closing this needs a MAC over the register value, which is a
wire-format change and is not done.

### The shared write-ahead-log container is not credential-scoped at all

`RestrictingBlobContainer` implements §12's tier model in-process: readers get
GET-only, the writer/compaction tier gets GET+PUT, and GC alone gets DELETE. The
**node-shared WAL container is not wrapped by it**. Every writer shard on the
node therefore holds full read, write *and delete* on the node-shared WAL prefix
— a prefix that by construction carries every other index's WAL records. §12 says
deletion belongs to GC alone; for the WAL, it belongs to every writer. The
per-record encryption on WAL records does not help, because deleting a chunk
needs no key.

The fix is the split `getEngineFactory` already performs for GC: wrap the shared
WAL container delete-denied for writer shards, and give the WAL GC task its own
unrestricted instance.

Note also that everything `RestrictingBlobContainer` provides is an in-process
assertion, not a boundary: the node's actual bucket credentials are unchanged, so
it turns a bug into an exception on the code path that has the wrapper and does
nothing about any other route. Real per-tier scoping is IAM, which §12 marks as
out of scope here.

### The plugin's own API has no index-level authorization

None of this plugin's transport requests implement `IndicesRequest`, so every one
of its actions is authorized as a cluster action. An operator cannot grant
`_clone`, `_shrink`, `_split` or `_snapshot_release` on a subset of indices — the
grant is cluster-wide — DLS/FLS never applies to anything this plugin exposes,
and audit logs record no index for these operations. `_realtime_get` in
particular is classified `cluster:monitor`, so a principal holding a monitoring
role can probe document existence in any index. Treat every one of these APIs as
cluster-admin-equivalent when granting privileges.

### Key rotation is not supported

Neither wire format records a key identifier, so there is no way at read time to
know which key encrypted what. Changing the key an
`EncryptionKeyProvider` returns makes all prior data permanently unreadable — a
loud AEAD failure on every affected read, not silent corruption, but with no
recovery. See `EncryptionKeyProvider`'s javadoc.

### What is encrypted where, when a key is configured

- **Object store:** ciphertext for bundles, manifests and segment data. Registers
  are plaintext, by design (above).
- **Local disk cache:** ciphertext, including the temporary files written during
  a cache fill.
- **Page cache:** ciphertext only, since the disk tier is encrypted.
- **Heap:** plaintext, by design, in the shared in-memory bundle cache. Its keys
  embed the index UUID and shard, so cached bytes cannot be served across
  indices; cache-timing side channels between tenants remain, and §12 accepts
  them as out of scope pending §3.

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
