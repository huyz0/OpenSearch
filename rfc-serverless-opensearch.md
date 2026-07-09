# RFC: A Serverless Architecture for OpenSearch

- Status: DRAFT
- Related branch: `feature/pluggable-engine-per-shard-role` (core seams: per-shard-role engine dispatch, `InternalEngine` extensibility)
- Related work in-repo: remote-backed storage, searchable snapshots, search-only replicas, writable warm / composite directory, remote cluster state

---

## 1. Motivation

OpenSearch today couples compute and storage at the shard level: a shard is owned by a node, its
segments and translog live on that node's disk, and durability/scale both come from adding more
full copies on more nodes. Remote-backed storage, searchable snapshots, and the warm tier have
loosened the *durability* half of that coupling — the object store can already hold segments,
translog, and cluster state — but the *compute* half is intact: every shard copy still runs the
same writable engine, holds local state that must be recovered, and is pinned to a node by the
allocator. The consequences:

- **No scale-to-zero.** An idle index still consumes heap, disk, and allocation slots on every
  node that holds a copy.
- **Symmetric scaling.** Ingest-heavy and search-heavy workloads must scale together, because
  every replica is a full writable copy that both indexes and serves queries.
- **Expensive elasticity.** Adding or draining a node means shard relocation — copying data that
  already exists in the object store onto yet another local disk.
- **Recovery is data movement.** Node loss triggers peer recovery or remote re-download of full
  shard contents before the copy is usable.

The goal of this RFC is an architecture where **the object store is the only source of truth**,
and every node is a **disposable cache plus compute**: ingest capacity, search capacity, and
stored data each scale independently, and any node can be killed at any time with no data loss
and no rebalancing debt.

## 2. Goals

1. Object store (S3/GCS/Azure/FS) is the sole durable home of segments, write-ahead data, and
   cluster metadata. Local disk is strictly a cache.
2. Two asymmetric shard roles with different engines:
   - **writer shards** — accept indexing, publish committed data to the object store;
   - **reader shards** — serve search from object-store-backed segments, never promotable,
     never hold a full local copy.
3. Ingest tier and search tier scale independently; search tier can scale to zero per index.
4. Node loss recovers by *re-opening* from the object store, not by copying data between peers.
5. Delivered predominantly as a **plugin/module**, using existing extension points plus the
   small set of core seams listed in §15; core changes stay minimal and generally useful.
6. Backward compatible: classic (local, symmetric) mode remains the default and untouched.

## 3. Non-Goals (for this RFC)

- Multi-tenant isolation between different customers inside one cluster (separate effort;
  the design should not preclude it).
- Query-level billing/metering (hooks are identified, implementation is out of scope).
- Replacing Lucene or the document/mapping model.
- Cross-region replication of the object store.

## 4. What Exists Today, and the Precise Gaps

| Building block | State today | Gap for serverless |
|---|---|---|
| Remote segment store (`RemoteSegmentStoreDirectory`) | Segments uploaded post-refresh; replicas can sync from remote | Upload is per-file and refresh-driven; local disk is still the primary copy; readers still materialize full local segments |
| Remote translog | Translog uploaded per shard | Per-shard blob traffic; fsync semantics tied to local translog lifecycle |
| Searchable snapshots (`RemoteSnapshotDirectory`) | Read-only mount of snapshot data with `FileCache` block caching | Immutable only; tied to snapshot/restore machinery; separate directory implementation from remote store |
| Writable warm (`TieredDirectory`/`CompositeDirectory`) | Local + remote composite with file cache, experimental | Still a full writable engine per copy; warm is an index property, not a shard-role property |
| Search-only replicas (`isSearchOnly()`) | Allocation-level role; skips translog recovery | **Still instantiates the same writable engine with a full local index** — role exists in routing but not in the engine layer |
| Remote cluster state (`gateway/remote`) | Cluster metadata persisted to object store | Not the bottleneck; reusable as-is |
| Segment replication checkpoints (`SegmentReplicationCheckpointPublisher`) | Primary→replica checkpoint push over transport | Reusable as the *notification* transport for "new data committed" events |
| `EnginePlugin` | Per-index engine factory | **Fixed at index creation; cannot vary by shard role** — addressed on this branch |
| `InternalEngine` internals | Merge scheduler / deletion policy / reader managers were `private final` | Subclasses could not hook merge or commit lifecycle — addressed on this branch |

The one-sentence summary of the gap: *OpenSearch has remote storage; it does not have remote-native
engines.* Everything below is about closing that.

## 5. Design Principles

1. **One storage abstraction, not three.** Writer, reader, warm, and snapshot access all go
   through a single object-store layout and a single cache. No parallel directory hierarchies.
2. **Batch writes, stream reads.** Object stores charge and throttle per request. The write path
   coalesces aggressively (group commit, bundled uploads); the read path fetches lazily in
   fixed-size blocks and caches.
3. **Metadata travels, bytes don't.** Nodes exchange small "what changed" notifications;
   the bytes move only object-store↔cache, never node↔node.
4. **Fencing by term, not by lock.** Correctness under split-brain comes from primary-term
   fencing embedded in object keys and manifests, not from distributed locks.
5. **Mechanics in open code, policy at the edges.** Engines, storage format, cache, and recovery
   are the platform; scaling policy, API gating, and metering are pluggable at the boundary.

## 6. Storage Layer

### 6.1 Object store layout

```
<base>/
  cluster-state/...                          # existing remote cluster state
  indices/<index-uuid>/<shard>/
    manifests/
      manifest-<term>-<gen>                  # commit manifest (small, JSON/SMILE)
    bundles/
      bundle-<term>-<gen>                    # segment bundle (large, immutable)
    wal/
      <writer-epoch>/log-<seq>               # write-ahead log chunks
  leases/
    node-<node-ephemeral-id>                 # unified per-node lease: reader pins + ownership (see 6.5)
```

### 6.2 Segment bundles

A flush produces new Lucene segment files. Uploading each file as its own object is wasteful
(many small `.si`/`.fnm`/metadata files) and makes atomic visibility awkward. Instead the writer
serializes the *new files of one or more commits* into a single immutable **segment bundle**:

- Header: list of `(logical file name, offset, length, checksum)` entries.
- Body: raw file bytes, concatenated.
- Bundles are append-batched: several quick successive commits may share one bundle. Upload
  triggers are size (`serverless.bundle.max_size`), commit count, and age — tunable so heavy
  ingest amortizes request cost while light ingest still publishes promptly.
- Multipart upload for large bundles where the repository supports it.

A logical Lucene file is therefore addressed as `(bundle object key, offset, length)`. Files are
never rewritten; merges produce new files in new bundles, and obsolete bundles are garbage
collected (§6.5).

### 6.3 Commit manifests

The unit of *visibility* is the **commit manifest**: a small object naming one Lucene commit —
its `segments_N` metadata, the full file→(bundle, offset, length) map, sequence-number
watermarks, the WAL position covered by this commit, plus two fields that other parts of the
design depend on:

- **pruning statistics**: per-shard doc count, time range of `@timestamp`-like fields, and
  min/max for a configurable set of fields — small enough to stay in the manifest, rich enough
  for a coordinator to decide *this shard cannot match* without opening it (consumed by the
  prune-before-activate protocol, metadata-plane RFC §5.1);
- **mapping version**: the index-metadata version this commit was written under, so a reader
  never applies a manifest before it has loaded mappings at least that fresh (metadata-plane
  RFC §4.1).

Manifests are written *after* their bundle(s) and named `manifest-<primary-term>-<generation>` so:

- readers can list/fetch the latest manifest and reconstruct the full file map with one small read;
- a stale writer (older term) can keep uploading bundles harmlessly — its manifests lose the
  term comparison and are ignored and GC'd;
- point-in-time reads pin a manifest, not a node.

*Discovery vs. authority:* in early phases "latest" is determined by listing manifest names
(highest term, then generation). Once the metadata plane's shard-head object lands, the head's
CAS'd latest-manifest pointer becomes the authority — every publisher (writer *and* compactor,
§7.4) commits a publication by CAS-ing the head — and listing remains the bootstrap/fallback
discovery path. Anything that only ever *reads* manifests works identically in both regimes.

### 6.4 Write-ahead log

Per-operation durability cannot wait for a flush+bundle upload. The WAL replaces the local
translog as the durability mechanism between commits:

- A **node-level WAL service** (not per-shard) buffers operations from all writer shards on the
  node and group-commits them to the object store as one WAL chunk per interval (target:
  100–250 ms) or size threshold (target: 8–16 MB). One upload fsyncs many shards' operations —
  this is what keeps per-document object-store request cost sane.
- The indexing request acknowledgment policy becomes explicit:
  - `durable` (default): ack after the covering WAL chunk upload succeeds — honest durability,
    adds up to one flush interval of latency;
  - `buffered`: ack after in-memory buffering (bounded loss window, for logs/metrics tiers).
- WAL chunks are keyed under a **writer epoch** so a superseded writer's tail chunks can be
  fenced and discarded on failover.
- Once a commit manifest covers a WAL position, chunks up to that position are eligible for
  deletion (delayed, batched).
- **Backpressure is explicit, not emergent**: the WAL buffer is bounded; when upload backlog
  crosses a threshold the node rejects new indexing with 429 (per-shard budgets first, node-wide
  second), and the same signal feeds autoscaling (§10). An object-store slowdown therefore
  surfaces as clean pushback at the API, not as unbounded memory growth.
- Cost sanity check: at 16 MB chunks, 1 GB/s of sustained node ingest ≈ 64 PUTs/s/node — cents
  per hour; the design goal is that WAL request cost stays two orders of magnitude below the
  compute cost of the node producing it.

**Integration point, corrected**: this section originally assumed `TranslogFactory`'s per-shard
`BiFunction<IndexSettings, ShardRouting, TranslogFactory>` resolution (`IndicesService`) was a
plugin-extensible seam the WAL-backed translog adapter could slot into without core changes. It
isn't — that `BiFunction` is computed inside `IndicesService` itself, choosing between
`InternalTranslogFactory`/`RemoteBlobStoreInternalTranslogFactory` based on settings, with no
plugin hook to override it. The seam that actually works is `InternalEngine#createTranslogManager`
(already `protected`/overridable, the same pattern `getTranslogDeletionPolicy` and &sect;7.1.1's
`globalCheckpointSupplierForCombinedDeletionPolicy` use) — `ObjectStoreWriterEngine` overrides it
and constructs an `InternalTranslogManager` directly with a `WalMirroringTranslogFactory` in place
of `engineConfig.getTranslogFactory()`. This needed one further small core change:
`InternalEngine#getLocalCheckpointTracker()`, which that constructor needs, was package-private
(inconsistent with its sibling accessors `readLock`/`ensureOpen` used in the same method, both
already `protected`/`public`) — widened to `protected`. **Not yet true**: "local translog fsync is
disabled for writer shards" — `WalMirroringTranslog extends LocalTranslog` and mirrors into the WAL
*in addition to* normal local fsync (deliberately, per its own javadoc: "Local translog files
remain the source of truth for recovery on this node exactly as they are today"), so this is
belt-and-suspenders today, not yet the disk-bandwidth-saving swap this line originally described --
matching &sect;7.1.1's own local-translog-retention story, which already trims aggressively once a
commit is durable, independent of whether WAL mirroring is active.

**Status**: implemented and wired into the real writer engine, gated behind
`serverless_storage.wal_mirroring.enabled` (default off — new wiring without production
experience yet). One `WalChunkService` instance is built per node incarnation in
`ServerlessStoragePlugin#createComponents` (a dedicated top-level `wal/` blob container, separate
from any index/shard's own path, with a fresh UUID epoch each node start) and shared by every
writer shard on the node, matching the cross-shard batching this section's cost-sanity argument
depends on. `WalRecord` now carries `primaryTerm` (wire format bumped to version 2) — see the
fencing note below. `WalMirroringTranslog#lastFlushedWalChunkSequence()` is what
`ObjectStoreCommitHeadPublisher`'s manifest now records as the real `WalPosition`, replacing a
permanent `(String.valueOf(primaryTerm), 0)` placeholder that had never actually been wired to
anything. Verified end-to-end (a real engine, WAL mirroring enabled, asserts the published
manifest's `WalPosition` reflects a real chunk sequence, and that the constructor-ordering bridge
this needed -- a `ThreadLocal` set immediately before `super(engineConfig)` and cleared immediately
after, since `createTranslogManager` needs the constructor argument before any field or even
another constructor's local variable is reachable from that call -- actually works, not just
compiles).

**Writer-epoch sharing model, resolved; fencing itself, only partially.** `WalChunkService`'s own
javadoc previously contradicted itself on what a "writer epoch" is -- described as both node-level
(spanning every writer shard, matching this section's cross-shard design) and "unique to one
actual writer lifetime... derived from the shard's primary term" (shard-term-scoped). Resolved as
node-level, matching the actual reason a node-level WAL service exists over a per-shard one -- that
part is settled. It rules out fencing a superseded writer by discarding its epoch directory (a
shared epoch can't be discarded without fencing every other shard using it), so
`WalChunkReader#filterByShardAndMinimumTerm` filters per-record instead, accepting only a record
whose `primaryTerm` is at least the term being replayed under.

**That filter is a necessary but not sufficient fencing mechanism, found by hand-tracing before
writing any replay code** (the same discipline that caught real bugs in `ShardHead.tla` earlier
this effort): it correctly excludes a term that never validly held the lease at all, but cannot
exclude a record legitimately tagged with a once-valid term that was actually appended *after*
that term was superseded. Concretely: writer N1 holds term T1; the lease is reassigned to N2/T2;
N1, unaware (paused, slow GC, or simply hasn't yet attempted a publish that would reveal the
fencing), keeps appending WAL records correctly tagged `term=T1` for a real window after T1 stopped
being current -- `WalChunkService#append` performs no fencing check of its own, it is
unconditional. Term-tagging says "written by whoever believed T1 was current," not "written while
T1 actually was current," and no term-only filter can distinguish those. Closing this needs either
a real append-time fencing check (in tension with this service's reason for existing: cheap,
unsynchronized buffering) or a cutoff tied to the actual moment of lease transfer rather than to
term identity.

**Now formally verified**, before any Java implementation, in
`plugins/serverless-storage/formal/WalReplayFencing.tla` (a small, deliberately abstracted model of
just this race -- it does not model shards, manifests, or the `ShardHead` CAS itself, since those
are already covered by `ShardHead.tla`; it models the one thing that isn't: `WalChunkService#append`
being unconditional). Two replay strategies checked side by side, same pattern as `ShardHead.tla`'s
`Spec`/`SpecBuggy`:

- `NaiveReplay` (the filter actually shipped, `filterByShardAndMinimumTerm`): **violated**, with a
  minimal 4-state counterexample TLC finds immediately -- a term change, a stale writer appending
  one record still tagged with its old belief, and a replay that wrongly includes it. This is a
  machine-verified confirmation of the bug found by hand above, not merely a hypothesis.
- `FixedReplay` (the proposed fix -- bound replay by *both* the term filter *and*
  `leaseTransferWalPos`, the WAL length snapshotted at the exact moment the current term was
  granted; anything appended after that snapshot is excluded regardless of tag): **holds**,
  exhaustively, across the complete reachable state space for the model's bound (603,722 distinct
  states, search depth 16, 0 states left on the queue).

**The fencing snapshot itself is now implemented** -- but not as originally sketched. `ShardHead`
was not the right place for it: `ShardHead#withNewLease` has no caller anywhere in the codebase
today (lease acquisition is not yet wired into any real activation/failover path; term authority is
still borrowed from core cluster coordination, per &sect;7.1's own "term authority bridge" note).
The real "this node just became the writer under a new term" event today is
`ObjectStoreWriterEngine`'s own construction, not a not-yet-existing metadata-plane lease grant.
`WalChunkService#currentChunkSequenceUpperBound()` exposes the real-world analogue of the model's
`Len(wal)`, and `ObjectStoreWriterEngine#activationWalPosition` snapshots it as early as possible --
before `super(engineConfig)` even runs, ahead of any of this engine's own construction work
including local translog recovery -- via the same `ThreadLocal`-bridging pattern
`createTranslogManager` already needed to cross the constructor-ordering boundary. Verified with
tests proving the actual boundary, not just that a value gets set: chunks written before activation
are captured, chunks written after are not, and the snapshot stays fixed regardless of subsequent
WAL activity.

**Honest limitation, stated in the field's own javadoc, not glossed over**: this narrows the race
the TLA+ model's `AcquireLease` action captures atomically with the term change itself, but isn't
perfectly equivalent to it -- by the time `ObjectStoreWriterEngine`'s constructor runs, core cluster
coordination has *already* decided this node holds the new term, so there's a small residual gap
between the true term change and this snapshot that a fully atomic metadata-plane lease-grant CAS
(still future work, per &sect;7.1) would close but this cannot.

**The read/filter/decode side of replay is now implemented and tested end-to-end against a real
two-writer failover**, in `wal/WalReplayRecovery.java`: given the last durably-published manifest's
`WalPosition` (or none, for a brand new shard) and a writer's own `activationWalPosition`, it lists
the WAL chunks in that range, reads and decodes each one, filters by `(indexUuid, shardId,
minPrimaryTerm)` exactly as `WalReplayFencing.tla`'s verified `FixedReplay` requires -- both the term
floor *and* the position cutoff, never either alone -- and returns an ordered
`List<Translog.Operation>` (payloads are exactly what `WalMirroringTranslog#add` serialized via
`Translog.Operation.writeOperation`, decoded back via `Translog.Operation.readOperation`).
`ObjectStoreWriterEngine#replayWalOperations()` wires this to a real activating engine: it reads the
shard's latest manifest via a new `ObjectStoreCommitHeadPublisher#readLatestManifest` (added
alongside a small `ObjectStoreCommitPublisher#readManifest` passthrough and a
`WalChunkService#blobContainer()` getter, both trivial plumbing), computes the term floor as
`ReplayFloor = currentTerm - 1` per the TLA+ model, and calls `WalReplayRecovery` with those and
`activationWalPosition`. Tested with a real two-`ObjectStoreWriterEngine` scenario -- one writer
indexes and publishes a manifest, more operations land durably in the WAL afterward but before any
further manifest, a second writer activates under a higher term against the same shared
`WalChunkService`/manifest store -- and asserts replay returns exactly the unmanifested operations,
no more and no less.

**Applying the replayed operations is now implemented too, via a new, small, generic core seam --
weighed against an Engine-only approach (accept a documented mapping-update limitation) first, and
rejected because it couldn't reuse core's own mapping-aware apply logic without either duplicating it
or silently degrading correctness.** `IndexShard`'s own `applyIndexOperation`/
`applyDeleteOperation`/`markSeqNoAsNoop` (the methods behind `applyTranslogOperation`) are private to
`IndexShard` because they need `MapperService`-based document parsing and mapping-update detection
that a bare `Engine` subclass cannot do itself -- confirmed by reading `IndexShard.java` directly, not
assumed. Reimplementing that logic inside `ObjectStoreWriterEngine` was rejected: it would mean
either duplicating real chunks of `IndexShard` or silently skipping mapping-update handling, a
correctness risk. Instead, `Engine#engineRecoveryOperations()` (server module,
`Engine.java`, default `List.of()`) is a new overridable hook, and
`IndexShard#openEngineAndRecoverFromTranslog()` calls it immediately after local translog recovery
completes, unwrapping the shard's `Indexer` back to a concrete `Engine` via the existing
`EngineBackedIndexer#getEngine()` accessor (the same unwrap-and-call idiom `IndexShard` already uses
for `IngestionEngine#awaitWarmupComplete()`) and feeding whatever it returns through the *exact same*
`runTranslogRecovery`/`applyTranslogOperation` path local translog recovery just used, via a small
new list-backed `Translog.Snapshot` adapter (`IndexShard.ListBackedTranslogSnapshot`) -- so mapping
updates and version/seqno bookkeeping are handled identically, not reimplemented. Every other
`InternalEngine` in the codebase is unaffected (default empty list, zero behavior change).
`ObjectStoreWriterEngine#engineRecoveryOperations()` overrides this and delegates straight to
`replayWalOperations()`, surfacing a WAL fetch/decode failure as an `EngineException` rather than
silently degrading to a recovery gap.

Verified with a new, engine-agnostic core test (`server/src/test/java/.../EngineRecoveryOperationsTests.java`,
deliberately not testing anything WAL-specific -- that stays this plugin's own concern) proving a
real `IndexShard` actually applies an engine-supplied extra operation on shard start, plus the full
existing `IndexShardTests`/`InternalEngineTests` suites (264 tests) unchanged and passing, confirming
this additive seam doesn't disturb any existing engine's recovery path.

**A real gap found and scoped while attempting the genuine crash-recovery test this section's own
work enables, not yet closed: bundle materialization on writer activation.** WAL replay (everything
above) only recovers operations *after* the last durable manifest -- it assumes local Lucene already
reflects that manifest's own content, true on a same-node restart but not a genuine cross-node
failover, where `ObjectStoreWriterEngine` activates against a completely empty local store. Nothing
today materializes a manifest's bundle into local Lucene on writer activation (`ObjectStoreCommitMaterializer`
does this today only for the *reader* engine, via `ObjectStoreReaderEngine#open`'s materialize-then-construct
technique). A first attempt to close this by adding the same technique to `WriterEngineFactory#newReadWriteEngine`
(materialize before constructing the engine) **failed a real test and was reverted**: for
`RecoverySource.Type.EMPTY_STORE` (the actual recovery type used here), core's own
`StoreRecovery#recoverEmptyStore` (`server/src/main/java/org/opensearch/index/shard/StoreRecovery.java:855`)
already creates and associates a fresh local translog with the (trivial, empty) Lucene commit
*before* any `EngineFactory` is ever invoked -- materializing a manifest's segment files afterward
overwrites that commit with one carrying the *previous* writer's stale translog-UUID reference,
which `InternalEngine`'s constructor correctly rejects as `TranslogCorruptedException`. The earlier-considered
alternative, `IndexStorePlugin.DirectoryFactory` (which runs early enough, before `StoreRecovery`
altogether), doesn't fix this either: `recoverEmptyStore` unconditionally calls `store.createEmpty()`
regardless of what a `DirectoryFactory` already put there, wiping any pre-population.

**Design for closing this properly is now written up in &sect;7.1.2**, after further reading of core
allocation/recovery code turned up that the fix is smaller than first feared on both counts: (1) a
genuine cross-node failover of an already-written primary actually carries `RecoverySource.Type.EXISTING_STORE`,
not `EMPTY_STORE` (the type the reverted attempt above tested was the wrong one to begin with) --
`StoreRecovery#internalRecoverFromStore`'s `EXISTING_STORE` branch throws before any translog
handling when local segments are missing, which is a much narrower, more surgical hook point than a
whole new `RecoverySource.Type`; and (2) getting such a shard allocated at all (this plugin's "no
peer recovery" shards look to core's default allocator like they can never have a valid copy
anywhere, which -- confirmed by reading `PrimaryShardAllocator` directly -- leaves them unassigned
indefinitely with no automatic timeout/fallback) has an existing, zero-core-change answer: the
already-real `ExistingShardsAllocator` SPI, swappable per index via
`ClusterPlugin#getExistingShardsAllocators()`, which this plugin already implements `ClusterPlugin`
for (&sect;9). See &sect;7.1.2 for the full design and what's proven vs. still to be implemented.

**Why per-record fencing instead of per-shard path fencing (`RemoteFsTranslog`-style), considered
and rejected on cost grounds.** Core OpenSearch's own remote-store translog fences the identical
race for free, structurally: each shard uploads its own generations under a path keyed by primary
term, and recovery only ever lists the current term's path -- a stale writer's post-fencing uploads
simply land somewhere recovery never reads, no per-record filter or position snapshot required. The
reason this plugin can't just copy that is the same reason it went node-level in the first place:
**PUT request cost dominates object-store spend for a WAL, by orders of magnitude, at the shard
density this architecture targets.** A back-of-envelope check: per-shard independent uploads on even
a modest 1 s flush interval, at 1,000 shards on one node (a realistic multi-tenant serverless
density), is 1,000 PUT/s/node -- roughly 86M PUT/day/node, on the order of $400+/day/node at typical
S3 PUT pricing. The node-level shared buffer this section already commits to collapses that to one
PUT per flush interval regardless of shard count -- roughly three orders of magnitude cheaper, and
the entire basis for this section's own cost-sanity check above.

A natural-seeming compromise -- keep the shared buffer but namespace chunks by `(writerEpoch, term)`
instead of bare `writerEpoch`, getting path-level fencing back "for free" -- does not survive
contact with how terms actually vary: primary term is a *per-shard*, independently-incrementing
counter, not a node-wide clock, so shards sharing one flush buffer can each be at a different term
at the same instant. Grouping a shared flush by absolute term value does not collapse to one bucket
per flush; it fragments into as many buckets as distinct terms currently held across the buffered
shards, which degrades toward one chunk per shard in the worst case -- silently reintroducing the
per-shard PUT cost this design exists to avoid. `RemoteFsTranslog` doesn't pay this cost because each
shard already owns an independent upload stream; a shared buffer has no analogous place to absorb it
for free. The per-record term filter plus `activationWalPosition` cutoff, despite the residual
atomicity gap documented above, adds zero extra PUT-time cost (the cutoff is a comparison made at
replay time, not a write-time concern) -- which is why it's the design being carried forward here
rather than path-scoped fencing, and why closing the residual gap fully is a metadata-plane
term-authority fix (&sect;7.1's "term authority bridge"), not a storage-layout one.

### 6.5 Garbage collection and leases

Deletion is the hardest correctness problem in shared-storage designs. Rules:

- Deletion *decisions* are made only under the shard's current term (a stale-term actor must
  never mark anything deletable); deletion *execution* is confined to the dedicated GC
  principal — the only credential in the system with DELETE rights (§12). Writers and
  compactors publish; they never delete.
- A manifest is deletable when (a) a newer manifest exists, (b) its retention window (PIT/scroll
  horizon) has passed, (c) no **lease** pins it, and (d) no **durable pin** (snapshot or PITR
  policy, §14) names it. Leases cover *live* actors and expire with them; durable pins are
  explicit retention records that survive any node and are removed only by policy. There is
  exactly one lease mechanism in the whole system: a per-node lease object (TTL'd,
  heartbeat-renewed, batched — one object per node, see metadata-plane RFC §6) that carries
  both *ownership claims* (writer/compactor roles) and *manifest pins* (open searchers,
  PIT/scroll contexts). One liveness mechanism means GC and activation can never disagree
  about which nodes are alive.
- A bundle is deletable when no live manifest **of any index** references any range inside it —
  the qualifier matters because cheap clones (§14) create cross-index bundle references, so
  bundle liveness is a reference count over manifests, not a per-shard property.
- All deletion is asynchronous, batched, and delayed by a safety window (default: minutes) —
  the object store is cheap to keep slightly-too-much data in and catastrophic to delete from
  prematurely.

## 7. Engine Layer

Both engines are selected by the per-shard-role `EnginePlugin.getEngineFactory(IndexSettings,
ShardRouting)` seam added on this branch: promotable routings get the writer engine, search-only
routings get the reader engine.

### 7.1 Writer engine

`ObjectStoreWriterEngine extends InternalEngine`. It deliberately reuses everything mechanical —
document indexing, version map, soft deletes, merge scheduling (via the now-overridable
`newMergeScheduler()` hook), deletion policy bookkeeping — and swaps only the durability layer:

- operations are teed to the node WAL service instead of fsynced to local translog;
- flush is extended: after the local Lucene commit, package new files into a bundle, upload,
  write the manifest, then release covered WAL chunks;
- refresh is coordinated with publication: an external refresh that would make data visible
  locally forces the corresponding commit/upload first, so a writer node never serves (or
  advertises) data that could be lost with the node. Writer-side search stays possible (for
  `_get` by id, update-by-query) but the *search tier* is authoritative for queries;
- the merge scheduler override hooks `beforeMerge`/`afterMerge` to (a) prefetch merge inputs
  into the local cache if evicted and (b) prioritize publishing post-merge commits, since merges
  produce the largest bundles and the biggest reader-cache invalidations. An *active* writer
  owns merging for its shard (lowest latency — inputs are local); everything else is the
  compaction service's job (§7.4), and a size threshold lets the writer offload its largest
  merges there too, so heavy compaction never starves indexing CPU;
- deletion-policy hook retains local commits until their manifests are safely uploaded.

Failover: the new writer bumps the primary term, reads the latest manifest of the previous
term, replays WAL chunks past the manifest's covered position, verifies epoch fencing, and
resumes. No peer recovery, no segment copy — recovery time is dominated by WAL replay length,
which is bounded by publication frequency. *Term authority bridge:* in early phases the term
bump uses the existing cluster-coordination mechanism; once the metadata plane lands, term
authority migrates to CAS on the shard-head object (metadata-plane RFC §4/§12) — the engine
code is agnostic, it consumes a term from a `ShardStateStore` interface either way.

#### 7.1.1 Local retention once object storage is durable (design, not yet implemented)

**Status**: design only, written up here before any code so the safety argument gets scrutiny
first — see §16 Phase 2's own status note ("local files are kept exactly as a normal
`InternalEngine` would keep them, so this is belt-and-suspenders durability today rather than the
disk-bandwidth savings target"). This section is that redesign, worked through far enough to be
implementable, plus the one open call that's a genuine product/risk-tolerance decision, not an
engineering one.

**Why today's retention is more conservative than this architecture needs.** Core OpenSearch
already ties local commit and translog retention tightly to the global checkpoint, not to
wall-clock time, even with zero replicas: `CombinedDeletionPolicy` deletes every local Lucene
commit older than the "safe commit" (the newest commit whose `max_seq_no` is ≤ the persisted
global checkpoint), and `SoftDeletesPolicy`/`TranslogDeletionPolicy` retain ops and translog
generations back to the same boundary, driven by retention leases. Critically, a sole primary with
no replicas still holds a **self-issued peer-recovery retention lease**
(`ReplicationTracker#addPeerRecoveryRetentionLeaseForSolePrimary`), pinned at
`globalCheckpoint + 1` and exempt from the normal 12h lease-expiry floor as long as the shard
stays assigned — so this isn't a large excess in steady state, but it exists purely to protect a
peer-recovery path that, by this plugin's design (§7.1: "No peer recovery, no segment copy"), can
never happen. That lease, and the retention it drives, is dead weight for a serverless writer
shard specifically.

**The actual opportunity.** This plugin's failover path already recovers by manifest + WAL replay,
never by replaying local translog or copying local segments (§7.1 above). Once that's true, local
translog and local Lucene commits stop being *the* recovery mechanism and become a same-node
fast-path optimization only: reusing local files on a routine restart is faster than a full
manifest+WAL replay, but is never required for correctness, since the manifest+WAL path already
has to be correct and tested for cross-node failover anyway. That reframing is what licenses much
more aggressive local retention than core's global-checkpoint-driven default: a local commit or
translog generation can be deleted as soon as **the data it holds is durably reconstructable from
object storage**, not just when a global checkpoint or a lease says so.

**Precise safety condition.** Define generation *g*'s content as durable once
`ObjectStoreCommitHeadPublisher#publishCommitAsHead` has returned `true` for it (i.e. it's
reflected in the shard head, not merely uploaded — an uploaded-but-unpublished manifest is not
yet the shard's authoritative state, see `ObjectStoreCommitPublisher`'s own "never the reverse"
invariant). Given that:

1. **Local Lucene commits**: once generation *g*'s manifest is durable, every local commit older
   than *g* may be deleted immediately — a strictly more aggressive version of "safe commit" than
   `CombinedDeletionPolicy`'s, since it doesn't wait for the global checkpoint to catch up, only
   for object-store durability. The current (newest) local commit is always kept regardless
   (`IndexWriter` needs an open commit point to keep writing) — this is unconditional, not a
   policy choice.
2. **Local translog**: once every operation in translog generation *t* is covered by a durably
   published manifest (i.e. *t*'s highest seq-no ≤ the durable generation's `max_seq_no`) *and*
   every operation in *t* has itself been durably group-committed to a WAL chunk (already true by
   construction — `WalMirroringTranslog` "retries a transient mirror failure before failing the
   write", so WAL durability and local-translog-write are effectively synchronous with the
   operation's ack), generation *t* may be deleted. In practice this means local translog beyond
   the currently-open generation only needs to survive until the *next* flush/publish cycle
   completes, not until a global checkpoint converges across replicas that don't exist.
3. **What must still be respected regardless of durability**: any local commit currently
   snapshotted (`CombinedDeletionPolicy#acquireIndexCommit`, e.g. a core-snapshot-repository
   operation still pointed at this shard) is never deleted out from under it — this plugin's own
   PITR/clone durability (§6.5, §14) is served from object storage, not local disk, so it never
   needs a local-commit snapshot lock, but a operator-triggered legacy snapshot to a non-serverless
   repository (if ever allowed on a serverless index, which is a separate open question) would.

**Two-part mechanism, gated by what's actually pluggable today.**

- **Translog retention — implemented.** `InternalEngine.getTranslogDeletionPolicy(EngineConfig)`
  was already `protected` and overridable (`InternalEngine.java:989`) — the same seam
  `ObjectStoreWriterEngine` already uses for `newMergeScheduler()`, no core changes needed.
  `ObjectStoreDurabilityTranslogDeletionPolicy` implements condition 2 above directly: a sticky
  `AtomicLong` watermark, advanced only by `ObjectStoreWriterEngine#commitIndexWriter` after
  `publishCommitAsHead` actually returns `true` (never on upload alone), floors
  `minTranslogGenRequired` there instead of deferring to the safe-commit/retention-lease chain --
  every reader generation whose ops are all covered by that watermark is deletable, subject only to
  open retention locks (`getMinTranslogGenRequiredByLocks`), which are always still respected.
  Wiring the object it returns back into a field required care: `getTranslogDeletionPolicy` is
  called by `InternalEngine`'s own constructor from inside `super(engineConfig)`, i.e. before
  `ObjectStoreWriterEngine`'s own field initializers would normally run — the field that caches it
  is declared with no initializer expression specifically so the value assigned during `super()`
  survives (an explicit `= null` would execute afterward and clobber it), verified directly by a
  test that asserts the field is non-null immediately after construction, not just that the code
  compiles. **Known current limitation, not yet closed**: `Translog#trimUnreferencedReaders`
  combines this policy's floor with a *second*, still-untouched floor derived from
  `CombinedDeletionPolicy`'s safe-commit tracking (core's own global-checkpoint-driven retention) --
  so until local commit retention (below) is also implemented, the *combined* effective retention
  is still bounded by whichever of the two is more conservative.
- **Local commit retention — implemented.** The actual core seam needed turned out smaller than
  the `newCombinedDeletionPolicy()` factory hook originally proposed here: `CombinedDeletionPolicy`
  itself doesn't need to be swappable, because its constructor already accepted a `LongSupplier`
  for the global checkpoint as a plain argument (`InternalEngine.java`, the call site building
  it) — that supplier is a pure "safe to delete at or below this seq-no" threshold, never a floor
  that suppresses deletion, so widening it can only permit deleting *more*, never less, than core's
  default. The seam added is `Engine#globalCheckpointSupplierForCombinedDeletionPolicy(TranslogManager)`,
  `protected`, defaulting to exactly what `InternalEngine` always passed before this method existed
  (`translogManagerRef::getLastSyncedGlobalCheckpoint`) for every engine that doesn't override it.
  `ObjectStoreWriterEngine` overrides it to return `max(real global checkpoint, the same durability
  watermark the translog policy above already tracks)`, reusing that single watermark as the one
  source of truth for both halves rather than tracking a second one. §15's core-seams list
  correction (below) reflects the seam that actually landed, not the originally-sketched one.

  Verified end-to-end against a real `EngineTestCase`-provisioned engine, and against a real,
  structural finding worth recording: retention cannot ever fully catch up to the very newest
  local commit within the same flush that created it, because `CombinedDeletionPolicy#onCommit`
  fires synchronously as part of the Lucene commit itself (via Lucene's own `IndexDeletionPolicy`
  callback), which happens *before* `ObjectStoreWriterEngine`'s own durability watermark for that
  same flush can be updated (durability can only be confirmed once `publishCommitAsHead` returns,
  which requires the commit to already exist locally) — so retention is always exactly one flush
  cycle behind, both for local commits (settles on "the previous flush's commit, plus the current
  one, always" — 2 commits in steady state, not 1) and for translog generations (current
  generation minus one, not the current generation itself). This is not a shortcoming to fix; it's
  the same "proportional to one publish cycle" bound this section's own milestone already states,
  now confirmed by a real, deterministic test rather than assumed. `InternalEngineTests` and
  `CombinedDeletionPolicyTests` (core's own regression suites for exactly this machinery) pass
  unchanged, confirming the default (non-overriding) behavior is untouched for every other engine.

**Open decision (product/risk-tolerance, not engineering) — how aggressive:**

- **Option A — trim to the bare minimum immediately on durability confirmation**: one local
  commit, translog back only to the last durable generation. Maximizes disk savings; fully
  consistent with "No peer recovery, no segment copy" already being the stated design. A same-node
  restart between publish cycles pays a manifest+WAL replay instead of a free local reopen for
  whatever wasn't yet durable at crash time — bounded by publication frequency, same bound §7.1
  already accepts for cross-node failover.
- **Option B — retain a short bounded safety margin** (e.g. the last 2-3 commits / a few minutes
  of translog) even past durability confirmation, decaying that margin over time rather than
  collapsing it instantly. Costs a small, fixed amount of the disk savings back; buys a local
  fast-path fallback if a subtle bug in `ObjectStoreCommitMaterializer` or manifest/WAL replay
  ever made remote reconstruction wrong or unavailable at the exact moment a same-node restart
  needed it — worth deciding with unusually low modeling confidence and no way to formally verify
  a code path's absence of bugs, only its presence.

This RFC's default recommendation is **Option A**, on the grounds that Option B's "safety margin"
only helps if the manifest+WAL replay path has a bug *and* that bug specifically only manifests
on a fresh reconstruction rather than during the original publish's own read-back — an unlikely
combination — while Option A directly delivers the section's whole stated goal. But this is the
one call in this section that's a judgment about acceptable risk, not a derived fact, and should
be made explicitly before implementation starts, not defaulted into by whichever margin is easiest
to code.

**Milestone — met** (Option A, as recommended): both translog and local Lucene commit retention are
now durability-driven, implemented, and verified end-to-end against a real engine. A serverless
writer shard's local disk footprint is proportional to one publish cycle's worth of data (in
steady state: the previous flush's safe commit plus the current one, and translog back only to the
previous flush's generation) — not to index age or total data volume, and not gated on a real
global checkpoint that, for a shard with no replicas, would otherwise never advance on its own.
`InternalEngineTests`/`CombinedDeletionPolicyTests` (core's own regression suites) and this
plugin's full test suite pass unchanged, confirming every other engine's default behavior is
untouched.

### 7.1.2 Cross-node writer failover: allocation and activation materialization

**Status**: both halves implemented and tested. Design was written up before any code, matching
&sect;7.1.1's own "safety argument gets scrutiny first" discipline, since this touches core
allocation and recovery, not just an `Engine`/`IndexShard` seam.

**The gap this closes.** &sect;6.4's WAL replay (fencing formally verified, fetch/filter/decode
implemented, apply-to-shard wired through a real core seam) only recovers operations *after* the
last durable manifest — it assumes local Lucene already reflects that manifest's own content, true
on a same-node restart but not a genuine cross-node failover, where the new node's local store has
never held this shard's data at all. Nothing today materializes a manifest's bundle into local
Lucene on writer activation. A first attempt to close this (materializing inside
`WriterEngineFactory#newReadWriteEngine`, mirroring how `ObjectStoreReaderEngine#open` already
solves the identical problem for reader shards) **failed a real test and was reverted**: for
`RecoverySource.Type.EMPTY_STORE`, core's `StoreRecovery#recoverEmptyStore` already creates and
associates a fresh local translog with the (trivial, empty) Lucene commit *before* any plugin
`EngineFactory` is ever invoked, so materializing a manifest's segment files afterward leaves the
commit referencing the *previous* writer's stale translog UUID — `TranslogCorruptedException`.

**Two separate problems, not one, discovered by reading core allocation code directly rather than
assumed:**

1. **Store population** — even with the right hook, how does a fresh local `Store` end up with the
   right Lucene commit *and* a translog whose UUID actually matches it, before `InternalEngine`'s
   constructor (inside `openEngineAndRecoverFromTranslog`) ever opens the directory?
2. **Allocation** — this plugin's shards are "no peer recovery" by design (&sect;7.1): no node ever
   holds a locally-persisted authoritative copy, the object store is the only durable copy. Core's
   default primary allocator has no concept of this. Confirmed by reading
   `PrimaryShardAllocator.getAllocationDecision`/`buildNodeShardsResult`
   (`server/src/main/java/org/opensearch/gateway/PrimaryShardAllocator.java`) directly: when every
   candidate node's `NodeGatewayStartedShards` response has a null allocation id (the "no local
   data" signal), `orderedAllocationCandidates` is empty, and the shard gets
   `AllocateUnassignedDecision.no(NO_VALID_SHARD_COPY, ...)` — **left unassigned indefinitely**, not
   a one-time failure. Every reroute cycle repeats the same empty fetch and the same `NO` decision;
   there is no timeout, delay setting, or automatic fallback anywhere in gateway/allocation code
   that converts this into an automatic empty allocation — only an explicit operator command
   (`AllocateEmptyPrimaryAllocationCommand`, requiring `acceptDataLoss=true`) escapes it today. A
   plugin whose every shard activation looks exactly like this to core's allocator would deadlock on
   every single failover without operator intervention, which is obviously unacceptable for a
   system whose whole premise is unattended recovery.

**Problem 2's resolution needs zero core changes** — confirmed by reading the actual SPI, not
assumed from a passing familiarity with it. `ExistingShardsAllocator`
(`server/src/main/java/org/opensearch/cluster/routing/allocation/ExistingShardsAllocator.java`) is
already a swappable-per-index allocator: `index.allocation.existing_shards_allocator` (an
`IndexScope` setting) names which registered allocator handles that index's unassigned shards, and
`ClusterPlugin#getExistingShardsAllocators()` is exactly the seam a plugin uses to register its own
— `ClusterModule.setExistingShardsAllocators` merges plugin-supplied allocators with the two
built-ins by name, no core modification required. Critically, `ExistingShardsAllocator`'s contract
(`allocateUnassigned(ShardRouting, RoutingAllocation, UnassignedAllocationHandler)`) never has to
call `TransportNodesListGatewayStartedShards`/reason about local allocation ids at all — a plugin
implementation can pick any `AllocationDeciders`-approved node and call
`unassignedAllocationHandler.initialize(nodeId, allocationId, expectedSize, allocation)`
immediately, because *this plugin's* recovery correctness never depended on which node has local
data (there never is any) — it depends on manifest+WAL replay, which works identically regardless
of which node gets picked. `ServerlessStoragePlugin` already `implements ClusterPlugin` (for
`ReaderShardPlacementAllocationDecider`, &sect;9), so adding `getExistingShardsAllocators()` is
additive to a seam already in use, not a new one. Estimated size: ~150-300 lines — `beforeAllocation`/
`afterPrimariesBeforeReplicas`/`cleanCaches` are no-ops; `allocateUnassigned` runs the standard
`AllocationDeciders.canAllocate` check across candidate nodes and initializes the first `YES`; the
existing `ObjectStoreCommitHeadPublisher#publishCommitAsHead` term-fencing CAS (already implemented
and formally verified as part of &sect;6.4/&sect;6.5's work) is what actually protects correctness
if this allocator ever picked a node that shouldn't really hold the primary — allocation here is
only "willingness to try," never final authority; a node that loses the fencing race simply fails
its first publish attempt and is fenced out, exactly as already tested in
`ObjectStoreCommitHeadPublisherTests`/`ObjectStoreWriterEngineTests`.

**Problem 1 needs one small, narrow core change** — smaller than the `RecoverySource.Type` addition
originally considered and rejected as too large. Reading `StoreRecovery#internalRecoverFromStore`
directly (not `recoverEmptyStore`, which is `EMPTY_STORE`-only and irrelevant here): for
`EXISTING_STORE` (the type a genuine failover of an already-written primary actually carries — the
earlier, reverted attempt tested the wrong recovery type), when local segments are missing
(`store.readLastCommittedSegmentsInfo()` throws, `si == null`), it throws
`IndexShardRecoveryException("shard allocated for local recovery, should exist, but doesn't")`
immediately — before any translog handling. That is precisely where a plugin-facing hook needs to
be: given the chance to materialize *before* that throw (both the manifest's segment files *and* a
freshly-created, correctly-associated local translog, in the right order — mirroring exactly what
`recoverEmptyStore` already does for its own case, just plugin-driven instead of unconditional), core
re-reads `si` (now non-null) and proceeds down the ordinary `indexShouldExists` branch unmodified.
No new `RecoverySource.Type`, no allocation-dispatch changes, no BWC/serialization surface — one
conditional branch in one method, gated behind a new small SPI method (shape still to be finalized:
most likely an `IndexStorePlugin`-style hook, keyed the same way `IndexStorePlugin.DirectoryFactory`
already is, called with the `IndexShard`/`Store` at exactly this point) that every existing engine's
recovery path is entirely unaffected by when unimplemented (default: hook absent, current
`IndexShardRecoveryException` behavior unchanged) — the same "additive, default no-op" shape every
other core seam this effort has added (`Engine#engineRecoveryOperations()`,
`Engine#globalCheckpointSupplierForCombinedDeletionPolicy`, `InternalEngine#getLocalCheckpointTracker`)
already follows.

**Allocation half — implemented, no core change needed, exactly as designed above.**
`ServerlessStorageExistingShardsAllocator` (`allocation/ServerlessStorageExistingShardsAllocator.java`)
implements `ExistingShardsAllocator`: `allocateUnassigned` picks the first
`AllocationDeciders`-approved node for a shard and initializes it immediately, never consulting
local shard-data presence at all (no `TransportNodesListGatewayStartedShards`, no allocation-id
bookkeeping, no caches — there is nothing to cache when there is no fetch);
`explainUnassignedShardAllocation` mirrors the same logic for the `_cluster/allocation/explain` API.
`ServerlessStorageIndexSettingProvider` (registered via `Plugin#getAdditionalIndexSettingProviders()`)
is what actually selects it per-index: `index.allocation.existing_shards_allocator` carries
`PrivateIndex` and can't be set directly at index creation, so this provider injects it
automatically for every index with `index.serverless_storage.enabled` set, the same way core itself
uses `IndexSettingProvider` for other managed defaults. `ServerlessStoragePlugin` registers the
allocator under its own name via `ClusterPlugin#getExistingShardsAllocators()` — additive to a seam
the plugin already implements `ClusterPlugin` for. Verified with real `RoutingAllocation`/
`AllocationDeciders` fixtures (`OpenSearchAllocationTestCase`): allocates immediately when every
decider says yes (the scenario that would deadlock `PrimaryShardAllocator` forever), correctly
removes-and-ignores with `DECIDERS_NO` when every decider says no, and the explain-API path returns
the matching `YES`/`NO` `AllocationDecision` in each case.

**Store-population half — implemented.** `Engine#engineRecoveryOperations`-shaped precedent applied
again: `EngineFactory#recoverMissingLocalStore(IndexShard, Store)` (server module,
`EngineFactory.java`, default `false`, zero behavior change for every existing `EngineFactory`) is
the new SPI method. `StoreRecovery#internalRecoverFromStore`'s `EXISTING_STORE` branch calls it
exactly where it used to unconditionally throw "shard allocated for local recovery, should exist,
but doesn't" -- `store.readLastCommittedSegmentsInfo()` throwing (no local commit at all) now tries
`recoverMissingLocalStoreFromEngine` first (resolving the shard's `EngineFactory` through the same
`EngineBackedIndexerFactory` wrapping every plugin engine is already reachable through) and only
falls through to the original failure if that returns `false`. On `true`, the segments info is
re-read and recovery proceeds completely normally from there — no other change to
`internalRecoverFromStore`'s logic.

`WriterEngineFactory#recoverMissingLocalStore` implements it: materializes the shard's latest
manifest into the fresh `Store`'s directory via `ObjectStoreCommitMaterializer` (the same technique
`ObjectStoreReaderEngine#open` already uses, now applied at the point that actually works instead of
the `EngineFactory#newReadWriteEngine`-level attempt that failed and was reverted earlier), then
bootstraps and associates a fresh local translog against that *now-populated* commit -- the ordering
that avoids the translog-UUID mismatch which sank the earlier attempt. Deliberately does not call
`store.bootstrapNewHistory()`: the manifest carries this shard's real prior history
(`maxSeqNo`/`localCheckpoint`), which must be preserved, not reset. Returns `false` (unchanged core
behavior) when there is no prior manifest at all -- a shard `EXISTING_STORE`-routed with nothing
anywhere to recover from is still a genuine failure, not a first activation.

Verified end-to-end through the real `IndexShard`/`StoreRecovery` path (not a hand-built `Engine`
construction) in `WriterEngineFactoryCrossNodeFailoverTests`: a first writer indexes and publishes,
a second writer -- same shard identity, completely fresh local `Store` (`newShard` always allocates
a new path; nothing simulated) -- recovers under `RecoverySource.Type.EXISTING_STORE` and the
document is visible, where before this change the exact same setup threw
`IndexShardRecoveryException` immediately. A second test confirms the fallback: with no manifest
ever published, `EXISTING_STORE` recovery still fails exactly as it always has. Full existing
`StoreRecoveryTests`/`IndexShardTests`/`InternalEngineTests`/`EngineRecoveryOperationsTests` suites
(269 tests) pass unchanged, confirming every other engine's recovery path is unaffected.

Both halves of &sect;7.1.2 are now implemented and tested: allocation
(`ServerlessStorageExistingShardsAllocator`, zero core changes) and store population
(`EngineFactory#recoverMissingLocalStore`, one new default-`false` SPI method plus one conditional
branch in `StoreRecovery`).

**The real multi-node integration test this whole effort was building toward is now written and
passing too**: `ServerlessStorageWriterFailoverIT` (new `internalClusterTest` source set, added via
`apply plugin: 'opensearch.internal-cluster-test'` in this plugin's `build.gradle` -- this plugin had
none before). A real three-node cluster (one cluster-manager-only, two data), all sharing one
`serverless_storage.base_path` directory standing in for the object store a real deployment's nodes
would all share; a document indexed and explicitly flushed (durably published); the node actually
holding the primary killed via `internalCluster().stopRandomNode(...)`; `ensureGreen` succeeding at
all on the survivor (previously this would either never leave `UNASSIGNED` or throw
`IndexShardRecoveryException` outright); and the document still searchable afterward.

Building this surfaced one more real, previously-latent bug, caught only because this was the first
test to exercise a genuine `Node`/`ClusterService` lifecycle rather than a hand-built `EngineConfig`:
`ServerlessStoragePlugin#createComponents` resolved `localNodeId` via `clusterService.localNode()`,
which reads `ClusterService#state()` -- unavailable this early in node startup (`"initial cluster
state not set yet"`), and, once deferred lazily to `getEngineFactory` time, *still* unsafe: shard/engine
creation runs from inside `IndicesClusterStateService`'s own cluster-state-applier callback, and
`ClusterApplierService` asserts against exactly this kind of reentrant `state()` call (`"should not
be called by a cluster state applier"`). Fixed by resolving the local node id from
`NodeEnvironment#nodeId()` instead -- this node's own persisted identity (the same ID that becomes
its `DiscoveryNode#getId()` once cluster state exists), available immediately at `createComponents`
time with no cluster-state dependency at all.

What remains, tracked in &sect;16 Phase 2, is no longer a design gap and no longer missing
end-to-end proof: it's the residual, already-documented limitation on `activationWalPosition`'s
atomicity (&sect;6.4) -- a metadata-plane term-authority migration, explicitly out of scope for this
effort -- and broadening `ServerlessStorageWriterFailoverIT` itself (WAL-only, unflushed data;
encryption enabled; more than one shard) rather than any further unproven piece.

### 7.2 Reader engine

`ObjectStoreReaderEngine extends Engine` directly — a sibling of `ReadOnlyEngine`/`NoOpEngine`,
not a restricted `InternalEngine`, because a reader shard has no writer state at all: no
IndexWriter, no version map, no merge scheduler, no translog. It consists of:

- a **manifest listener**: subscribes to publication notifications (§8), maintains a queue,
  and applies them in order;
- a **remote directory view**: a read-only Lucene `Directory` whose "files" are
  `(bundle, offset, length)` ranges resolved through the block cache — `createOutput`/
  `deleteFile` throw; the directory holds a file map, not file bytes;
- a reader manager that refreshes `DirectoryReader`s when a new manifest is applied, with
  **admission control**: if applying a refresh would exceed the heap/cache budget (segment
  metadata, terms index, points index are the expensive parts), the refresh is deferred and
  retried on a backoff or when an old reader closes. A reader that falls behind serves slightly
  stale data rather than OOMing — staleness is observable (manifest generation lag) and drives
  autoscaling;
- generation pinning: open searchers pin their manifest generation (feeding the lease
  mechanism in §6.5) so GC never yanks a bundle out from under an in-flight query.

Reader shards are **never promotable**. Failover of a reader is trivial: start another one
anywhere and let it hydrate its cache; the allocator needs no special ceremony.

### 7.3 Suspended writers and scale-to-zero

An index with no recent writes can drop its writer entirely: the last act of a writer being
suspended is a final flush+publication, a manifest marked *quiescent*, and a fold of the
shard's final pruning statistics into the index's pruning digest — the update that makes
prune-before-activate safe for this shard from now on (metadata-plane RFC §5.1). The index
then costs zero compute. The first write after suspension triggers writer re-activation (allocate a writer
shard, read latest manifest, open, index) — a cold-start measured in seconds, hidden behind the
indexing queue. Reader shards scale to zero the same way with cold-start on first query
(manifest fetch + lazy block loads; a small "boot set" of blocks — segment metadata, terms
index roots — can be listed in the manifest to prefetch in one round trip).

### 7.4 Compaction service (merging without a writer)

Merging cannot be exclusively a writer concern, for two reasons the scale-to-zero story would
otherwise break on:

1. a shard that goes quiescent with forty small segments stays fragmented *forever* — its cold
   queries get permanently slower and its bundle count never shrinks;
2. on hot shards, large merges compete with indexing for the writer's CPU, which is precisely
   the coupling this architecture exists to remove.

So merging becomes a **compaction service**: a pool of stateless maintenance workers (runnable
on the cheapest compute, spot-friendly, no cache warmth required) that:

- select candidates from manifest metadata alone (segment count, size skew, delete ratio —
  all readable without opening the shard);
- acquire the shard's *compactor role* via the unified lease mechanism (§6.5) — an active
  writer implicitly holds merge rights, so the service only touches shards without one, or
  accepts explicit offload handoffs from busy writers;
- stream input bundles through the block cache, run a standard Lucene merge, write merged
  bundles, and publish a new manifest.

**The rebase protocol** — the subtle part. A compactor's output must not clobber a concurrent
writer re-activation (first write arriving mid-compaction). Publication — for writers and
compactors alike — is a CAS on the shard-head's latest-manifest pointer (§6.3): the compactor's
new manifest is computed as *(latest manifest − input segments + merged segment)*. If the CAS
fails because a writer published generation N+1 meanwhile, the compactor **rebases**: recompute
against the new latest (its merge output is still valid — merged segments are immutable), retry
the CAS, and give up after a bounded number of attempts on a genuinely hot shard (the active
writer merges there anyway). Compaction is thus always safe, at worst wasted work.

**Hard dependency**: this protocol requires the shard-head CAS primitive from the metadata
plane (metadata-plane RFC §4, Phase 2.5). List-based manifest discovery cannot serialize two
independent publishers — so the compaction service is sequenced *after* shard-heads exist, and
until then merging remains writer-only (the today-equivalent behavior, minus the quiescent-
shard benefit).

`_forcemerge` is redefined as an explicit request to this service (enqueue + optional wait),
which finally gives it honest semantics: it no longer monopolizes a data node's threads, and it
works on indices that have no writer at all.

**Status: verified correct while a writer is active, via a narrower path than this section
originally proposed.** `_forcemerge`'s REST/transport chain only ever reaches an engine while a
writer is actively open on some node (`IndexShard.forceMerge` requires a live shard); a design
that unconditionally redirects it to this section's *object-store-materialized* compaction
service -- independent of whatever local Lucene state that active writer holds -- creates two
divergent generation-numbering paths for the same shard: the compactor's publish advances
`latestManifestGeneration` based on the object store's history, while the writer's own next
ordinary flush independently computes its next generation from its *local* `IndexWriter`'s
segment-generation counter (see `ObjectStoreWriterEngine#commitIndexWriter`). If the compactor's
publish lands a generation number at or ahead of what the writer's own counter would produce
next, `ObjectStoreCommitHeadPublisher#publishCommitAsHead` treats the writer's subsequent real
commit as "already published" and silently drops it -- never surfaced as an error, just a lost
write. This was caught during design, before being built, not found by a test after the fact.
`ObjectStoreWriterEngine` therefore does *not* override `forceMerge`: it inherits
`InternalEngine`'s real local Lucene merge unchanged, which is safe precisely because the already
-verified `commitIndexWriter` override publishes the merge's result through the same
generation-numbering path every ordinary flush uses -- there is only ever one path when a writer
is open, not two. Verified directly: indexing across three separate flushes (three real Lucene
segments), then `forceMerge(maxNumSegments=1)`, publishes a strictly newer generation whose
materialized manifest is genuinely one segment with all three documents intact. What §7 actually
proposed -- routing `_forcemerge` through this service so it "works on indices that have no
writer at all" -- remains unbuilt and is still the right target for *that* case specifically; it
needs either a real check that no writer is currently active before redirecting, or a fencing
protocol between the compactor and a writer that might activate mid-compaction, neither of which
exists yet. That case is exactly the "no writer ever activating" milestone below, still open.

## 8. Publication and Consistency

- After each manifest upload, the writer sends a small **publication notification**
  (index, shard, term, generation, new-file summary) to the shard's current readers. Transport:
  generalize the existing segment-replication checkpoint publisher — same shape of message,
  different payload. *Where does the writer learn reader locations?* In early phases, from the
  routing table (classic control plane); once the metadata plane lands, from the directory tier
  (metadata-plane RFC §5), whose entries it already touches on every publication to bump the
  generation hint. Notifications are an optimization in either case; the object store is the
  truth. A reader that missed notifications (restart, partition, stale directory entry) lists
  manifests and catches up.
- **Consistency model (default): monotonic bounded staleness.** A reader never goes backward
  (manifest generations are totally ordered per shard) and lag is bounded by publication
  frequency plus notification delivery. This matches the existing segment-replication model.
- **Read-after-write where required:** a search request may carry a minimum visible generation
  (returned to the client in the index response). The coordinator routes to a reader at or above
  that generation, or the reader waits for it (with timeout). This gives per-request RYW without
  making the whole system synchronous.
- `_get` by document id can optionally route to the writer shard for true realtime gets (the
  writer has the live version map), controlled per request.
- **No cross-shard snapshot isolation** — a multi-shard search may observe different shards at
  different generations. This is already true of OpenSearch today; it is stated here so nobody
  assumes the manifest mechanism accidentally added it.
- **`_refresh` changes meaning honestly**: it becomes "flush, publish, notify" — a durable,
  observable operation with real cost, rather than a local reader reopen. Per-index refresh
  intervals translate to publication cadence; the API contract ("changes visible to search after
  refresh returns") is preserved, but callers hammering `_refresh` per document will feel it in
  latency and request cost — documentation and a per-index publication rate limit protect
  against that footgun.

## 9. Cache Layer

One **node block cache** shared by all shards on a node, replacing per-feature caches:

- fixed-size blocks (default 1 MB region granularity, small-object short-circuit for tiny
  files like `segments_N`), addressed by `(bundle key, block index)`;
- two-tier: off-heap/mmap'd disk cache (capacity: most of the node's disk) with an in-memory
  index; the disk *is* the cache — nodes should be provisioned as cache size, not data size;
- eviction: LRU with pinning for (a) blocks referenced by open searchers and (b) writer-shard
  merge inputs; optional timestamp-aware policy for time-series (older blocks decay faster);
- **warming inputs**: manifest application (prefetch new segment metadata blocks), merge
  scheduling (prefetch inputs, retain outputs), and query-driven demand;
- cache hit-rate and cold-read latency are first-class metrics — they are the search-tier
  autoscaling signal.

Reuse note: the existing `FileCache` used by searchable snapshots/warm is the starting point;
the work is unifying its keying to the bundle layout and making it the single cache for all
remote reads, rather than adding a fourth cache.

**Status: a first, minimal slice done; not the design above.** `LocalDiskCachingBundleStore` is a
read-through, whole-file, unbounded local-disk cache in front of any `BundleFileReader`, wired in
for reader shards in `ServerlessStoragePlugin`. It gets the *correctness* half right (bundle files
are immutable, so no invalidation is needed; a corrupted local copy is detected via checksum and
re-fetched rather than trusted) and the basic hit-path right (16 concurrent readers of the same
file share exactly one real fetch; a fresh instance over the same directory survives a process
restart) but is not the design above: no block granularity (whole files, not 1 MB regions), no
node-shared single cache (one instance per shard, not one per node), no eviction/pinning policy at
all (unbounded -- a real deployment would fill its disk), and no warming/prefetch. This is the
foundation such a cache would sit on top of, not a replacement for building it.

**Encryption interaction, resolved.** When `EncryptingBlobContainer` is enabled, bytes reaching
`LocalDiskCachingBundleStore` from its delegate are already plaintext (decrypted at the
object-store seam) -- naively caching them meant every reader node's local disk, and the OS page
cache backing it, silently held decrypted segment data at rest despite encryption being "on," with
no equivalent protection to the object store's. `LocalDiskCachingBundleStore` now optionally takes
the same `EncryptionKeyProvider`, encrypting each file before it's written to its cache directory
and decrypting on read back (a corrupted or undecryptable cache entry -- including a leftover
plaintext file from before encryption was enabled -- falls back to a real re-fetch, same as a
checksum mismatch). That closes the gap but reintroduces a decrypt on every disk-cache hit, which
is exactly the cost enabling the disk cache was meant to avoid paying repeatedly.
`InMemoryPlaintextBundleCache` is the fix for that: a bounded, size-limited, in-process LRU cache
of decrypted bytes sitting in front of the (now ciphertext-on-disk) disk cache, wired in for reader
shards alongside it. The actually-hot working set stays decrypted in heap, so a repeated read of
the same file -- the common case for a reader shard serving many queries against the same recent
segments -- skips both the disk I/O and the decrypt; only a disk-cache hit that missed this
in-memory layer pays either cost. Verified: the on-disk cache file genuinely does not contain the
plaintext when encryption is enabled; a second read decrypts back to the exact original bytes; a
cache directory encrypted with one key correctly falls back to a real fetch (not an error, not
garbage) when read by an instance holding a different key; the in-memory LRU correctly evicts the
least-recently-used entry once its byte budget is exceeded, and an entry larger than the entire
budget is still served correctly, just never cached.

**One node-shared instance, not one per shard -- and why it's on-heap rather than off-heap.**
The first cut of this cache was a bug: each reader shard's engine factory built its own instance
with a fixed cap, so a node hosting a thousand reader shards had no relationship between the
cache's total footprint and what the node could actually afford -- worst case, shard-count times
the per-shard cap. `InMemoryPlaintextBundleCache` no longer holds a fixed delegate or is
constructed per shard: `ServerlessStoragePlugin` builds exactly one instance in
`createComponents`, sized from `serverless_storage.bundle_cache.size` (a percentage-of-heap or
absolute-byte-value setting, the same idiom `indices.fielddata.cache.size` uses -- default `5%`,
deliberately conservative since this cache has no production tuning behind it yet, unlike
fielddata's long-validated `35%`). `CachingBundleFileReader` adapts that one shared instance back
into a plain `BundleFileReader` per shard, so each shard's engine factory still gets something
that looks like its own reader, while every shard's reads actually compete for the same node-wide
budget. Bundle names already embed index UUID and shard id (`ObjectStoreCommitPublisher`), so
cache keys can never collide across shards despite sharing one map.

Off-heap storage (native memory, immune to GC scan cost, sizeable independently of `-Xmx`) is the
right target given the RFC's actual scale goal -- at hundreds of millions of shards with
thousands of hot reader shards per node, a useful cache is realistically GB-scale, which competes
hard with heap sized for indexing/search if left on-heap. It is deliberately not done here: the
JDK Foreign Memory API (`java.lang.foreign.Arena`/`MemorySegment`), the safe modern way to do
this, is still a preview feature on this project's JDK 21 toolchain (confirmed by actually
compiling against it -- `error: Arena is a preview API and is disabled by default`), not
finalized until JDK 22+; and the legacy alternative (`ByteBuffer.allocateDirect` plus manually
forcing its `Cleaner` to run early on eviction) needs reflective access blocked by the module
system without JVM-wide flags, and is genuinely unsafe under concurrent access -- an evicted
buffer forcibly freed while another reader still holds a reference is a use-after-free, not just
a bug. Revisit once the build's JDK floor moves to 22+, or a proper reference-counted off-heap
allocator (e.g. a Netty-`ByteBuf`-style pool) is worth taking on as a dependency.

## 10. Allocation, Topology, and Autoscaling

- **Node roles**: `ingest-compute` (hosts writer shards), `search-compute` (hosts reader
  shards). Existing search-only allocation deciders extend naturally; a new decider forbids
  writer shards on search-compute nodes and vice versa.
- **Placement cost model changes**: reader shards are cheap to move (no data), so the balancer
  should optimize for cache locality second and load first — a moved reader loses its cache,
  so add a hysteresis penalty rather than treating moves as free.
- **Autoscaling hooks** (policy pluggable, mechanism in the platform):
  - ingest tier: WAL upload backlog, indexing queue depth, writer CPU;
  - search tier: manifest-generation lag, cache hit rate, query queue depth;
  - scale-to-zero: no writer activity → suspend writer; no queries in window → drop readers.
- **Cluster state**: remote cluster state (already in-repo) becomes mandatory in serverless
  mode. *Superseded in the long term:* this RFC assumes the classic coordination layer as the
  control plane; the companion metadata-plane RFC replaces it (for serverless indices) with
  shard-head CAS + a directory tier + a minimal control cell — see that document for the target
  state and the bridge between the two.

**Status: node-role placement done as a real `AllocationDecider`, now symmetric in both
directions; cost model, autoscaling hooks, and mandatory remote cluster state not started.**
`ReaderShardPlacementAllocationDecider` is the "new decider" the first bullet calls for. A node
opts in to hosting reader shards via `node.attr.serverless_storage_reader: "true"` (the same
node-attribute mechanism `cluster.routing.allocation.require.*` filters already use), and reader
shards of a serverless-storage index can only be allocated to such a node -- and, the "vice versa"
half that was previously missing, a *writer* shard (or any other non-reader copy) of a
serverless-storage index may no longer land on a reader-designated node either, so that reserved
reader capacity is never silently consumed by an ordinary writer/primary shard. Writer shards have
no opt-in attribute of their own and stay unrestricted everywhere else, matching how writer
placement works outside serverless storage today. Every shard of a non-serverless index gets a
`YES` (no opinion) from this decider regardless of node, exactly the behavior every other
`AllocationDecider` in the allocator is expected to have. Verified directly against `canAllocate`:
a reader shard is allowed only on a reader-designated node; a writer shard is allowed everywhere
*except* a reader-designated node; a non-serverless index's shards are unaffected even on a
reader-designated node. Not implemented: the placement cost model / cache-locality hysteresis
(second bullet), every autoscaling hook (third bullet -- no signal collection or scale-to-zero
mechanism exists), and mandatory remote cluster state enforcement (fourth bullet).

## 11. API Surface in Serverless Mode

Many APIs are meaningless or dangerous when storage is disaggregated (`_forcemerge` semantics
change, shard-store APIs lie, snapshot/restore is partially redundant with the native format).
Introduce a **handler capability annotation** on REST handlers declaring availability in
serverless mode (`available`, `internal-only`, `unavailable`); the REST controller enforces it
when the node runs in serverless mode. Unannotated handlers default to unavailable — new APIs
must opt in consciously. This is a small core change with value beyond serverless (it doubles
as an operator/managed-service API gating mechanism).

## 12. Security and Encryption Boundaries

Disaggregation moves the trust boundary: today a shard's bytes live on nodes an operator
controls; here they live in a shared object store and flow through shared node-level services.
Three problems must be designed in, not bolted on:

- **The WAL/encryption conflict.** The node-level WAL (§6.4) deliberately mixes many indices'
  operations in one object — but per-index encryption keys (the model used by index-level
  crypto directories today) require that one index's ciphertext not share a key domain with
  another's. Resolution: **per-record envelope encryption inside shared chunks** — each WAL
  record is encrypted with its index's data key before entering the shared buffer; the chunk
  itself carries only framing in the clear. Group-commit efficiency is preserved; a compromised
  chunk yields nothing without per-index keys. Indices with hard co-residency prohibitions
  (regulatory) can opt into dedicated WAL streams at higher request cost — an explicit,
  per-index trade.
- **Bundles and manifests** are single-index by construction (§6.1 layout), so index-level
  encryption applies unchanged: encrypt bundle payloads with the index data key; manifests
  contain no document data (stats can optionally be suppressed for sensitive fields at the cost
  of pruning effectiveness).
- **Credential scoping per tier.** Search-compute needs GET-only on data prefixes; the
  compaction service needs GET+PUT but no DELETE (deletion stays with GC); the GC/reconciler
  role is the only DELETE-capable principal. Scoped credentials turn several classes of bug
  (a reader "fixing" state, a compactor deleting inputs early) into authorization errors —
  cheap defense for the invariants §6.5 depends on.

The security plugin's document/field-level security model evaluates at query time on readers
and is unaffected structurally — but the *cache* is node-shared, so cache keys must never leak
across indices (they don't: keyed by bundle object key), and cache-timing side channels between
tenants are accepted as out of scope pending the multi-tenancy effort (§3).

**Status: bundles/manifests encryption done; WAL per-record envelope encryption and credential
scoping not started.** `EncryptingBlobContainer` implements the second bullet directly: it wraps
any real `BlobContainer` transparently (AES-256-GCM, random IV per blob, authenticated -- a
tampered or corrupted blob fails to decrypt loudly rather than silently), and because it's wired
in at the per-shard container construction seam in `ServerlessStoragePlugin`, every bundle,
manifest, and shard-state register that shard writes is encrypted with the same index-scoped key,
matching "bundles and manifests are single-index by construction... encrypt bundle payloads with
the index data key" exactly (registers are the one deliberate exception -- passed through
unencrypted, since they carry no document data, only node ids/terms/generations). Optional and
backward compatible: unset by default, every existing deployment of this plugin is untouched.
**Now implemented**: true partial-range decryption, closing what was previously a known, explicit
tradeoff (encryption applied to the whole blob, so a ranged read had to fetch and decrypt the
entire blob before slicing in memory). `EncryptingBlobContainer`'s wire format changed from one
whole-blob AES/GCM envelope to a fixed header (`BlockLayout`) followed by a sequence of
independently-encrypted, independently-authenticated fixed-size blocks (default 64KiB); a ranged
read fetches a small header, computes exactly which blocks overlap the requested range via pure
offset arithmetic (`BlockLayout`, no I/O, unit-tested on its own), and issues exactly one
additional ranged fetch to the delegate spanning only those blocks -- never the whole blob.
Verified directly, not just inferred: a recording `BlobContainer` wrapper confirms a 3-byte read
out of a 1000-byte, 100-block blob triggers exactly two delegate calls (the header, then one
sub-100-byte block fetch), a read spanning multiple block boundaries returns the exact expected
byte range, and -- a genuinely new guarantee the old whole-blob-envelope scheme didn't have -- a
ranged read entirely within an uncorrupted block still succeeds even when a different block in
the same blob has been corrupted, where the old scheme would have failed to decrypt *any* range
once *any* byte anywhere in the blob was corrupted. The cache layer (&sect;9) still matters for
repeated reads of the same file (a cache hit skips the fetch and decrypt entirely, which even a
single-block partial fetch does not), see &sect;9's "Encryption interaction, resolved". **Also
implemented**: the WAL-specific per-record envelope encryption design (bullet 1).
`WalChunkService` is a node-level shared component that group-commits records from many shards
(potentially many indices) into one chunk blob, so it can't just be routed through a per-shard
`EncryptingBlobContainer` the way bundle/manifest/register containers are -- a single whole-blob
key wouldn't respect per-index key boundaries. Instead, `WalRecordCrypto` encrypts each `WalRecord`'s
`payload` individually (leaving `indexUuid`/`shardId`/`seqNo` plaintext, since a chunk reader needs
them to route/filter records without decrypting anything), and `EncryptingWalChunkService` wraps
`WalChunkService` so every `append` is encrypted before it ever reaches the underlying service,
which stays completely unaware encryption is happening -- it already treated `payload` as opaque
bytes by design. The AES/GCM envelope logic itself was extracted out of `EncryptingBlobContainer`
into a shared `AesGcmCipher` utility rather than duplicated. Verified: a chunk written through
`EncryptingWalChunkService` contains ciphertext, not the plaintext payload, when read back raw;
decrypting via `WalRecordCrypto#decryptAll` with the right key recovers the exact original
payloads and preserves indexUuid/shardId/seqNo through the real wire format; a wrong key fails
loudly rather than returning garbage. **Known, explicit limitation**: today's
`EncryptionKeyProvider` only ever supplies one key regardless of index, so this doesn't yet buy
real per-index key isolation for WAL data the way whole-blob encryption does for bundles --
the record-level design means it will, the moment a per-index-aware key provider exists, without
any WAL wire-format or wiring change; and `WalChunkService`/`EncryptingWalChunkService` are not
yet wired into the plugin at all (no `TranslogFactory` hook constructs one), matching this phase's
pre-existing "notification wiring still open" status -- this is a real, tested component waiting
for that integration, not integrated into a running engine yet. **Not implemented**: credential
scoping per tier (bullet 3), which needs IAM/role-assumption wiring per cloud backend, not just a
core primitive like the ones built so far.

## 13. Degraded Modes: Object-Store Brownouts

The object store is now the single dependency of everything, so its failure modes must map to
*designed* behaviors — static stability, not surprise:

| Object store state | Readers | Writers | GC / compaction / reconcilers |
|---|---|---|---|
| Healthy | normal | normal | normal |
| Slow (elevated latency/throttling) | serve from cache; freshness lag grows and is visible in metrics | acks slow; backpressure engages (§6.4) before memory does | back off (they are never urgent) |
| Down | **keep serving** — cached blocks + last-applied manifest remain valid indefinitely; staleness is monotonic and observable | stop acking `durable` immediately (honest failure); `buffered` mode keeps a bounded window then rejects | **halt** — never delete or publish on partial information |

Two rules generalize the table: *reads degrade to staleness, never to unavailability, for any
data already cached*; *writes degrade to rejection, never to silent un-durability*. The
boundary case is honest too: **cold activation is unavailable during a full outage** (a
quiescent shard's manifest cannot be fetched) — already-active shards keep serving, cold ones
fail fast with a distinct error rather than queueing indefinitely. The chaos suite (§17) must
include a full object-store outage with assertions on all three behaviors, plus the recovery
stampede after restoration (WAL backlogs, deferred publications, and queued activations must
drain with jittered backoff, not synchronized thundering herd).

## 14. Snapshots, Clones, and Point-in-Time Recovery

The manifest mechanism makes several traditionally expensive operations nearly free, and this
RFC claims them deliberately rather than leaving them implicit:

- **Snapshot = pinned manifest set.** A snapshot of an index is a retention-pinned manifest per
  shard plus a copy of the index metadata object — metadata-only, O(shards) small writes, no
  data movement. Restore-in-place is "point the shard-heads at the pinned manifests."
- **Clone = new index referencing existing bundles.** A zero-copy clone writes new manifests
  (under a new index UUID) that reference the source's bundles. This is the feature that makes
  dev/test-on-production-data and A/B reindexing cheap — and it is exactly why bundle GC must
  be a cross-index reference count (§6.5). The GC model-check (§18.5) explicitly includes
  clone-then-delete-source interleavings.
- **PITR** falls out of retention policy: within the PITR window, keep all manifests, the
  bundles they reference, and *all WAL chunks* (a recovery to time T replays WAL from the
  newest manifest ≤ T forward to T, so chunks must survive as long as any manifest that might
  serve as a replay base). Cost is object retention, not infrastructure.
- **Cross-cluster / cross-account restore** = grant read on the prefix + import manifests.
  Export to a foreign format (or deep-copy for isolation) remains available via the existing
  snapshot API surface, which stays supported as the *interchange* path (§11 gating decides
  which variants are exposed in serverless mode).
- **PIT/scroll resumability** (small but real win): a PIT context is a pinned manifest, not
  node-local reader state — so a reader crash no longer invalidates long-running scrolls;
  another reader can resume the same pinned generation.

## 15. Core Changes vs Plugin Code

**Core seams (small, generally useful, some already done on this branch):**

1. ✅ Per-shard-role engine dispatch: `EnginePlugin.getEngineFactory(IndexSettings, ShardRouting)`.
2. ✅ `InternalEngine` extensibility: protected merge scheduler and reader managers, overridable
   `newMergeScheduler()`; `TranslogDeletionPolicy` is also already overridable
   (`getTranslogDeletionPolicy(EngineConfig)`). ✅ Commit-level retention is now also pluggable,
   landed on this branch: `Engine#globalCheckpointSupplierForCombinedDeletionPolicy(TranslogManager)`,
   a small `protected` hook wrapping the `LongSupplier` argument `InternalEngine` already passed
   to `CombinedDeletionPolicy`'s constructor, defaulting to today's unchanged behavior for every
   non-overriding engine. Smaller than the `newCombinedDeletionPolicy()` factory hook originally
   sketched here — `CombinedDeletionPolicy` itself never needed to be swappable, only the threshold
   value fed into it, since that value is a pure "safe to delete at or below" floor, never a
   suppressor of deletion. See §7.1.1 for the full design and `ObjectStoreWriterEngine`'s use of it.
3. Flush/commit lifecycle hook: a post-commit callback carrying the commit + new-file set
   (needed by the writer engine's publication step; today reachable only via deletion-policy
   gymnastics).
4. Search-only routing → engine selection: `IndexShard` passes its routing into engine-factory
   resolution (done via `IndexService.createShard` on this branch); additionally, reader shards
   must skip `ReplicationTracker` paths that assume a local checkpointable engine.
5. Generalized checkpoint/notification publisher (widen segment-replication checkpoint
   publishing to carry opaque payloads).
6. REST handler capability annotation (§11).
7. Node WAL service registration point (a `Plugin`-provided node-level component — likely
   already expressible via `createComponents`; verify lifecycle ordering vs `IndicesService`).

**Plugin/module code (the bulk):** writer/reader engines, bundle+manifest format, WAL service
(with per-record envelope encryption, §12), compaction service (§7.4), block cache unification,
GC/lease service, snapshot/clone/PITR manifest operations (§14), allocation deciders,
autoscaling signal emitters, serverless-mode settings and API annotations application.

## 16. Phased Roadmap

**Phase 0 — Seams (done).** Branch `feature/pluggable-engine-per-shard-role`:
role-aware engine dispatch + engine extensibility, landed in core (`EnginePlugin`,
`IndicesService`/`IndexModule`/`IndexService` per-shard resolution, `InternalEngine`
extensibility). Core seams 3–7 remain open as individual small PRs.

**Phase 1 — Storage format (done, standalone; not yet wired to an engine).** Implemented in
`plugins/serverless-storage` on the same branch: `BundleWriter`/`BundleReader` (segment bundles,
&sect;6.2), `CommitManifest`/`FileReference`/`PruningStats`/`WalPosition` (manifest schema,
&sect;6.3), `ManifestRetentionPolicy`/`BundleReferenceCounter` (GC rules, &sect;6.5, including
the cross-index-clone reference-counting case), and `BlobContainerBundleStore`/
`BlobContainerManifestStore` (real I/O against `BlobContainer`, FS-tested). An end-to-end test
(`ServerlessStorageEndToEndTests`) exercises the full write&rarr;publish&rarr;read&rarr;GC cycle
against a real filesystem blob store with no mocks. JMH microbenchmarks establish baseline
throughput for both formats. Remaining for this phase: one real cloud-object-store integration
test (currently FS/mock only, per the original scope note above).

**Phase 2 — Writer engine (WAL format, translog adapter, commit publishing, and head CAS wiring
done; local-disk retention policy once object storage is authoritative still open).** WAL chunk
format (`WalChunkWriter`/`WalChunkReader`/`WalRecord`, &sect;6.4) is implemented and tested,
including a multi-shard group-commit/per-shard-replay-filter test. The WAL-backed translog adapter
(`WalMirroringTranslogFactory`/`WalMirroringTranslog`) plugs into the existing
`translogFactorySupplier` seam, keeps `LocalTranslog`'s on-disk recovery untouched, mirrors every
appended operation into a node-level `WalChunkService` that group-commits buffered records into WAL
chunk blobs, and retries a transient mirror failure before failing the write. `ObjectStoreCommitPublisher`
packs one local Lucene commit's `SegmentInfos` into a `SegmentBundle` and writes the corresponding
`CommitManifest`, idempotently under retry. `ObjectStoreCommitHeadPublisher` makes the term-fencing
decision `ObjectStoreCommitPublisher` deliberately doesn't: it CASes the packaged manifest onto the
shard's head via `ShardStateStore`, refusing to publish (and reporting so) if a different primary
term already holds the head -- verified with real `IndexWriter` commits and a real
`BlobContainerShardStateStore`, including the fenced-out and idempotent-retry cases.
`ObjectStoreWriterEngine extends InternalEngine` wires this in: it overrides `commitIndexWriter` to
call `super` (local commit unchanged) and then `ObjectStoreCommitHeadPublisher`, failing the engine
if this writer has been fenced out -- verified end-to-end via a real `EngineTestCase`-provisioned
engine (`test:framework` already carries `EngineTestCase`, so this needed no new build plumbing):
one test confirms a flush publishes a manifest onto the shard head, another confirms a writer
already superseded by a higher term has its engine failed by the very next flush, including that
subsequent writes are then also rejected. **Local commit/translog retention is now durability-driven
end to end** (&sect;7.1.1, both parts done): `ObjectStoreDurabilityTranslogDeletionPolicy` floors
translog retention on a watermark advanced only after `publishCommitAsHead` actually returns
`true`; `Engine#globalCheckpointSupplierForCombinedDeletionPolicy` (a small additive core seam,
smaller than the `newCombinedDeletionPolicy()` originally sketched -- see &sect;15) lets
`ObjectStoreWriterEngine` widen the same watermark into `CombinedDeletionPolicy`'s own safe-commit
threshold, so local Lucene commits no longer wait on a real global checkpoint that a shard with no
replicas would otherwise never advance. Verified end-to-end against a real engine, including the
structural finding that retention is always exactly one flush cycle behind (not the literal newest
commit) since `CombinedDeletionPolicy#onCommit` fires synchronously as part of the very commit it's
evaluating -- matching this section's own "proportional to one publish cycle" milestone language,
not a shortcoming. Core's own `InternalEngineTests`/`CombinedDeletionPolicyTests` pass unchanged. The disk-usage-bound
half of &sect;7.1.1's own milestone is now met as a result. Separately, still not yet met: an index
whose durability is object-store-only survives `kill -9` of its node with zero data loss (durable
ack mode), recovering by manifest+WAL replay -- no crash-survival integration test exercising this
exists yet; the retention work above makes local disk usage bounded, it doesn't by itself prove
crash recovery. **WAL replay fencing is now formally verified** (`formal/WalReplayFencing.tla`,
`FixedReplay` holds exhaustively; the naively-shipped term-only filter does not), **the
read/filter/decode side is implemented and tested against a real two-writer failover**
(`wal/WalReplayRecovery.java`, `ObjectStoreWriterEngine#replayWalOperations()`, &sect;6.4), **and the
apply step is now wired end to end** through a new generic core seam (`Engine#engineRecoveryOperations()`,
`IndexShard#openEngineAndRecoverFromTranslog()`, &sect;7.1) proven with a real `IndexShard` in
`EngineRecoveryOperationsTests`. **What the "no crash-survival integration test exists yet" note
above was waiting on turned out to be two real, deeper gaps -- both now found, scoped, designed,
implemented, and tested, and both smaller than the single `RecoverySource.Type` originally feared**
(&sect;7.1.2): allocation (`ServerlessStorageExistingShardsAllocator` sidesteps
`PrimaryShardAllocator`'s indefinite-unassignment deadlock entirely, needing *zero* core changes;
`ServerlessStorageIndexSettingProvider` selects it automatically for every serverless-storage index)
and store population (`EngineFactory#recoverMissingLocalStore`, one new default-`false` SPI method
plus one conditional branch in `StoreRecovery` -- `WriterEngineFactory`'s implementation
materializes the last manifest and bootstraps a matching local translog, in the ordering that avoids
the translog-UUID mismatch that sank the first, `EngineFactory#newReadWriteEngine`-level attempt).
Verified end-to-end through the real `IndexShard`/`StoreRecovery` path in
`WriterEngineFactoryCrossNodeFailoverTests`: a second writer, same shard identity, completely fresh
local `Store`, recovers under `RecoverySource.Type.EXISTING_STORE` where before this work it threw
immediately. Every piece this crash-recovery story depends on -- fencing math, fetch/filter/decode,
apply-to-shard, allocation, and now store population -- is implemented and tested. **The multi-node
integration test itself is done too**: `ServerlessStorageWriterFailoverIT` (new `internalClusterTest`
source set) starts a real cluster, indexes and flushes a document, kills the node actually holding
the primary, and confirms the document survives on the node it fails over to -- catching, in the
process, one more real latent bug (`ServerlessStoragePlugin`'s local-node-id resolution crashing
against `ClusterApplierService`'s reentrancy assertion, fixed via `NodeEnvironment#nodeId()`) that
no earlier unit-level test could have found, since none of them exercised a real `Node`/`ClusterService`
lifecycle.

**The WAL-only half is done too, closing this section's own previously-flagged gap.** The same test
now also indexes a second document *without* flushing it (`WalMirroringTranslog#add` flushes each
operation's WAL chunk synchronously, so it's already WAL-durable by the time the client call
returns, but never becomes part of any manifest) before killing the primary's node. Both documents
survive: the first via manifest materialization, the second via `WalReplayRecovery`/
`ObjectStoreWriterEngine#engineRecoveryOperations()` replaying past `activationWalPosition` -- the
first time the *entire* chain (fencing, fetch/filter/decode, apply-to-shard, allocation, store
population) has been exercised together under a real cluster rather than piece by piece.
Required `serverless_storage.wal_mirroring.enabled=true` on the test's nodes and
`addMockInternalEngine() = false` (`OpenSearchIntegTestCase`'s own randomized mock-engine injection
otherwise collides with `WriterEngineFactory` -- "multiple engine factories provided" -- an
IT-only-visible gotcha, unrelated to any bug in this plugin itself).

**A second, unrelated latent bug found and fixed while getting this stable**: a pre-existing test,
`LocalDiskCachingBundleStoreTests#testWithAnEncryptionKeyTheDiskFileDoesNotContainThePlaintext`,
was intermittently flaky (`java.io.IOException: Is a directory`) under specific random seeds. Root
cause: the test's own directory-listing filter checked `endsWith(".tmp")`, but
`LocalDiskCachingBundleStore`'s actual temp-file naming is `<key>.tmp-<threadId>` (see
`#writeAtomically`) -- never a bare `.tmp` suffix, so the filter never excluded anything; combined
with Lucene's randomized `createTempDir()` occasionally injecting extra junk subdirectories into the
same temp directory, `Files.readAllBytes` would occasionally pick up a directory instead of the
cache file. Fixed by filtering on `Files::isRegularFile` and a `contains(".tmp")` check instead --
verified stable across 5 runs with fresh random seeds after the fix, including the exact
previously-failing seed.

What remains is the residual `activationWalPosition` atomicity limitation already documented in
&sect;6.4 (out of scope -- needs the metadata-plane term-authority migration) and further broadening
the IT (encryption enabled, multiple shards), not any further unproven piece of the crash-recovery
story.

**Phase 3 — Reader engine (materializer and open-from-manifest done; refresh-to-newer-generation
and notification wiring still open).** `ObjectStoreCommitMaterializer` fetches every file a
`CommitManifest` references (checksum-verified) and writes it into a target Lucene `Directory`,
producing an ordinary valid commit -- proven by opening a plain `DirectoryReader` against a
materialized manifest and running a real query, and separately by opening a full
`ReadOnlyEngine` against one via `ObjectStoreReaderEngine.open`. Deliberately reuses
`ReadOnlyEngine` rather than writing a new `Engine` subclass: once materialization has populated
the store's directory, `ReadOnlyEngine` already implements the entire read-only surface (search,
get, completion stats, refusing writes) against exactly that, so the object-store-specific work
stays confined to materialization itself. Two scoped tradeoffs, both explicit rather than
accidental: (1) full-materialization, not the lazy/block-cached object-store-native `Directory`
this phase's milestone targets -- correct and useful for small segment sets today, not the
eventual read path; (2) one fixed generation per `open` call, no in-place refresh to a newer
manifest without a full engine reopen. Still open: manifest-change notifications, block cache
unification, admission control. Milestone: search-only shards serve queries with no local index,
freshness lag p99 < 15 s under sustained ingest.

**Refresh-to-newer-generation: design, not yet implemented, and it is a bigger change than
tradeoff (2) above first suggested.** Investigated by reading `ReadOnlyEngine` directly rather than
assumed: it is not a small patch. `ReadOnlyEngine`'s reader and segment-infos fields
(`server/src/main/java/org/opensearch/index/engine/ReadOnlyEngine.java`) are `private final`, set
once from a single fixed `IndexCommit` inside its own constructor; `refresh(String)`/
`maybeRefresh(String)` are explicit no-ops and `refreshNeeded()` always returns `false` -- by
design ("we could allow refreshes if we want down the road," per that class's own comment).
`ObjectStoreReaderEngine` cannot gain in-place refresh while it `extends ReadOnlyEngine`; there is
no reopen hook to hang one on.

Core has already solved the identical shape of problem, just not via `ReadOnlyEngine`:
`NRTReplicationEngine extends Engine` directly (not `ReadOnlyEngine`), paired with its own
`NRTReplicationReaderManager`. Its `updateSegments(SegmentInfos)` is called externally once new
segment files have already landed in the shard's `Store` directory (from segment-replication's
target service), and does an in-place `ReferenceManager.maybeRefresh()`-driven `DirectoryReader`
reopen against the same `Directory` -- reusing unchanged leaf readers, opening only the new
segments, no engine close/reopen, no `IndexShard`-level engine swap. `IndexShard`'s one sanctioned
"replace this shard's Engine instance" path, `resetEngineToGlobalCheckpoint()`, is a heavyweight
mechanism (requires all operations blocked, replays translog) built for primary
relocation/promotion, not a cheap per-refresh operation -- confirmed by reading it, not the right
tool here regardless.

`ObjectStoreCommitMaterializer.materialize` is already additive-safe for this: it writes each of a
manifest's referenced files by name via `Directory#createOutput`, with no assumption the directory
starts empty, so materializing a *second*, newer manifest into the same already-open `Directory`
lands new segment files alongside the old ones without conflict (distinct segment generations mean
distinct file names) -- exactly the precondition `NRTReplicationReaderManager`'s reopen already
relies on for segment replication's own directory.

**The design, concretely**: `ObjectStoreReaderEngine` stops extending `ReadOnlyEngine` and becomes
its own direct `Engine` subclass with its own reader manager (mirroring
`NRTReplicationReaderManager`'s `refreshIfNeeded` -> `StandardDirectoryReader.open(directory,
newInfos, ...)` shape), reserving none of `ReadOnlyEngine`'s write-refusal/translog-absence
machinery that reader shards still need (a `ReadOnlyEngine`-style stub is still needed for those
concerns; only the fixed-reader part changes). A periodic task -- reusing the scheduling
infrastructure `ObjectStoreReaderEngine`'s own `directoryRefreshTask` already establishes --
polls `ShardStateStore` for a newer manifest generation than the one currently open; when found,
materializes the delta into the existing `Directory` and triggers the reopen. This closes the
"still open: manifest-change notifications" item too, in its simplest form (polling, not true
pub/sub) -- true event-driven notification via the directory/gossip tier (&sect;9, already built)
is a possible later refinement, not a prerequisite.

This also resolves an inconsistency worth naming plainly: &sect;7.2's own target-design text above
already says `ObjectStoreReaderEngine extends Engine` directly with "a reader manager that
refreshes `DirectoryReader`s when a new manifest is applied" -- the actual Phase 3 implementation
took a deliberate, explicitly-documented shortcut (reuse `ReadOnlyEngine`) that this design would
now walk back, not a new design decision being introduced for the first time. Sized as a moderate,
well-precedented change (~150-200 lines mirroring `NRTReplicationReaderManager`'s already-proven
shape), not a `ReadOnlyEngine` patch and not an `IndexShard` change -- not attempted in this pass.

**Plugin wiring (done).** `ServerlessStoragePlugin implements EnginePlugin` assembles everything
above into a working `getEngineFactory(IndexSettings, ShardRouting)`: an index only gets an
object-store engine if it opts in via `index.serverless_storage.enabled` (every other index is
untouched, matching Goal 6), each shard gets its own blob container scoped to
`indices/<index-uuid>/<shard>/` under a node-configured base path (resolved through
`Environment#resolveRepoFile`, the same sanctioned path-resolution seam `repository-fs` uses,
rather than trusting an arbitrary settings string directly), and `ShardRouting.isSearchOnly()`
picks `ReaderEngineFactory` vs. `WriterEngineFactory`. Verified end-to-end at the settings/routing
level (opted-out index gets no factory, missing base path fails loudly, primary/search-only/absent
routing each resolve to the right factory type). The base path is local-filesystem-only for now
(swapping in a real repository-backed container only touches one method, `blobContainerFor`;
nothing else in the plugin or the classes it wires together is FS-specific) pending the
`repository-s3` register support noted below extending to this plugin's own wiring.

**`compareAndSwapRegister` backends: FS done; S3 done; GCS done; Azure done -- all four planned
backends now implemented.**
`S3BlobContainer.readRegister`/`compareAndSwapRegister` are implemented using S3's real
conditional-write primitives (`If-Match`/`If-None-Match` on `PutObject`) as the actual concurrency
guard -- a freshly-read generation is checked client-side purely as a fast-fail, but the
authoritative check is S3 itself evaluating the conditional `PutObject` atomically against the
object's live ETag, so a racing writer between our read and our write is caught there (a 412
response mapped to `VERSION_CONFLICT`), not missed. This could not have been verified as anything
more than "compiles against the SDK" without a fixture that actually enforces those headers, so
`test/fixtures/s3-fixture`'s `S3HttpHandler` (shared by every `repository-s3` test) was extended to
honor `If-Match`/`If-None-Match` on `PutObject` with real 412 semantics -- a small, generally
useful addition to that fixture, not a special-cased test double. Verified end-to-end against that
now-honest fixture: put-if-absent, stale-generation conflict, and (the property that actually
matters) 8 concurrent CAS-retry-loop incrementers against one register losing zero updates.

`GoogleCloudStorageBlobStore.readRegister`/`compareAndSwapRegister` follow the same shape but map
onto our `BlobRegister.generation()` abstraction even more directly than S3: GCS objects already
carry a real, monotonically increasing generation number as first-class metadata (unlike S3's
opaque ETag), so no self-managed counter needs to be embedded in the register's own bytes at
all -- the value is stored verbatim, and `BlobTargetOption.doesNotExist()` /
`generationMatch(long)` are GCS's own atomic conditional-write primitives, evaluated server-side
exactly like S3's `If-Match`. `test/fixtures/gcs-fixture`'s `GoogleCloudStorageHttpHandler` (shared
by every `repository-gcs` test) didn't track per-object generations or enforce `ifGenerationMatch`
at all before this, so both were added -- again a generally useful fixture extension, not a
special-cased double. Verified with the same test shape as S3: put-if-absent, stale-generation
conflict, and 8 concurrent CAS-retry-loop incrementers losing zero updates, plus the full existing
`GoogleCloudStorageBlobStoreRepositoryTests` suite (16 tests) still passing against the extended
fixture.

`AzureBlobStore.readRegister`/`compareAndSwapRegister` follow the same shape as S3 (Azure ETags are
opaque like S3's, so a self-managed generation counter is embedded in the register's own bytes,
unlike GCS's native generation field): `BlobRequestConditions.setIfNoneMatch(ETAG_WILDCARD)` /
`setIfMatch(etag)` are Azure's own atomic conditional-write primitives on blob upload, evaluated
server-side the same way. `test/fixtures/azure-fixture`'s `AzureHttpHandler` already had partial
`If-None-Match: *` support (a real, if narrow, existing feature -- used by the plugin's own
`failIfAlreadyExists` writes) but no `If-Match` handling and no ETag exposure on GET/HEAD/PUT
responses at all, so those were added. One genuine bug surfaced and fixed during verification, not
a design choice: the Azure SDK's `BlobDownloadHeaders#getETag()` strips the HTTP quoting before
handing the ETag back to callers, and `setIfMatch()` expects that same unquoted form -- the fixture
initially generated and stored quoted ETags, so every real `If-Match` comparison failed against a
value the SDK itself had produced from the fixture's own prior response. Verified with the same
test shape as S3/GCS: put-if-absent, stale-generation conflict, and 8 concurrent CAS-retry-loop
incrementers losing zero updates, plus the full existing `AzureBlobStoreRepositoryTests` suite (13
tests, including its own randomized transient-fault injection) still passing against the extended
fixture.

All four backends `compareAndSwapRegister` was planned for (FS, S3, GCS, Azure) are now real,
tested implementations -- this seam is no longer blocked on cloud credentials for verification.
Each of the three cloud fixtures (S3, GCS, Azure) needed real conditional-write enforcement added
where it didn't already exist, and each of those additions is a generally useful improvement to
shared test infrastructure other plugins' tests benefit from too, not a special-cased double built
only for this RFC's purposes.

**Phase 4 — Topology (4–6 weeks).** Role-separated allocation, suspended writers,
scale-to-zero/cold-start, balancer hysteresis. Milestone: idle index consumes zero compute;
first query after idle returns < 5 s p95 for a cached-manifest index.

**Phase 4.5 — Compaction service (candidate selection, rebase protocol, real Lucene merge,
size-tiered shaping, background scheduling, and a real concurrent-writer data-loss bug all
done/fixed; lease-offload negotiation's remaining design work still open).** Its hard dependency,
shard-head CAS (metadata-plane RFC Phase 2.5), is done and generically `BlobContainer`-backed
(`BlobContainerShardStateStore`). `CompactionPolicy` (candidate selection from
segment-count/size/delete-ratio metrics, matching &sect;7.4's "readable without opening the
shard") and `CompactionRebaseExecutor` (the rebase-on-CAS-conflict publication loop) are
implemented and tested, including the rebase protocol's two defining properties under real
concurrency. `LuceneMergeCompactionPublisher` is the real `CompactionPublisher` that was missing:
materializes the shard's current commit, folds its segments together via an actual `IndexWriter`
merge (`addIndexes` + `forceMerge(n)` -- no document is re-parsed, only segment files combined),
and republishes the result as a new manifest, carrying over the seq-no/checkpoint/mapping-version
metadata a pure segment merge doesn't change. The merge target is now size-tiered rather than
always one segment: `CompactionPolicy#targetSegmentCount` turns the source manifest's total byte
size into a target segment count so each resulting segment stays under `maxTargetBundleSizeBytes`,
and that target -- not a hardcoded `1` -- is what `forceMerge` is asked to reach. Verified against
a real multi-segment index: all documents survive with none lost or duplicated; under the default
policy (5GB target) a small test shard still collapses to one segment as before; under a
deliberately tight target relative to the source's real size, compaction stops at multiple
segments and never exceeds the computed target count. A compaction whose starting point is raced
by a concurrent writer publication still rebases onto the writer's newer generation rather than
overwriting it (using the already-tested rebase executor, now exercised with a real merge instead
of a synthetic head transition). Known, explicitly accepted inefficiency: every rebase retry
redoes the full materialize-merge-publish sequence rather than caching the
(generation-independent) merged bundle across retries -- correct, not maximally cheap under
contention. `_forcemerge` while a writer is active is now verified, deliberately *not* via the
redirect-to-compaction-service design &sect;7 originally called for -- see that section's status
note for why.

**A real, previously-latent data-loss bug was found and fixed while investigating lease-offload
negotiation, before it was ever built.** `ObjectStoreCommitHeadPublisher#publishCommitAsHead`'s
"already published, treat as success" shortcut used `manifest.generation() <= currentHead.latestManifestGeneration()`.
`LuceneMergeCompactionPublisher` computes its own next generation as
`currentHead.latestManifestGeneration() + 1`, entirely independent of a writer's local Lucene
generation counter -- so a compactor running several cycles while a writer was idle can push
`latestManifestGeneration` ahead of whatever that writer's own next local commit computes. When
such a writer resumes, its commit's generation can land *at or below* the compactor-advanced
value, and the old `<=` check silently treated that as "my write succeeded" -- the caller (`commitIndexWriter`)
never saw a failure, but the actually-visible head was the compactor's merged content, not the
writer's newly committed documents. A genuinely reachable silent-data-loss bug, not a hypothetical
edge case, and exactly what the "surprise writer re-activation" milestone below is meant to guard
against. Fixed: the check is now strict (`<` fails/fences the writer, matching how a term mismatch
already does; `==` is preserved as the narrow, still-safe idempotent-retry case). Verified with a
new regression test that reproduces the exact scenario -- a compactor advancing the head, then a
writer's own next commit landing below that -- and asserts the writer is correctly told it failed,
with the head left exactly as the compactor published it. **Known, narrower residual risk, left
open and documented in code**: the preserved `==` branch cannot distinguish "this writer's own
retried publish" from "a different actor's different content that happens to compute the same
generation number," since `ShardHead` carries no manifest-identity field to check against; today's
call pattern (one publish attempt per local flush, never retried with the same manifest) can't
trigger this, but a truly airtight fix means decoupling a writer's generation numbering from local
Lucene state entirely -- always computing its target as `currentHead.latestManifestGeneration() + 1`
read fresh under the retry loop, exactly like the compactor already does. **Implemented**: this
redesign was formally verified sound first (see §18 risk #5, `ShardHeadDecoupled.cfg`/
`SpecDecoupled`), then landed in `ObjectStoreCommitHeadPublisher#publishCommitAsHead`, which no
longer takes a caller-supplied `generation` at all -- it now computes the target live inside its own
retry loop and repackages the commit at the freshly computed generation on every CAS-loss retry.
`ObjectStoreWriterEngine` no longer threads `segmentInfos.getGeneration()` through to publication.
One consequence: what was previously the fenced-out case "a compactor advanced the head past this
writer's own next local generation" (`testPublicationSupersededByAConcurrentCompactionUnderTheSameTermFailsRatherThanFalselySucceeding`)
is no longer a failure at all -- the writer just publishes at the live head's generation + 1, same
as the compactor -- so that test was replaced with
`testPublicationAfterAConcurrentCompactionUnderTheSameTermSucceedsAtTheNextLiveGeneration`, asserting
the new (correct) behavior. Term-fencing (a different primary term already holding the head) is
unchanged and still the only way `publishCommitAsHead` returns `false`. This closes both the
silent-data-loss risk this session originally found and the narrower manifest-identity-collision
risk noted above, since there is no longer an externally supplied generation number to collide with
anything.

**Background scheduling done**: `CompactionSchedulerTask` runs the `CompactionPolicy` decision on a
fixed schedule per shard, so a quiescent shard with no active writer now gets compacted without a
human or a writer ever triggering it -- the "no writer ever activating" case this service exists
for. Each tick reads the live `ShardHead`; if the lease is currently held (an active writer is
present, whose own local Lucene merges already handle this shard) or the shard was never activated
or has never published anything, the tick is a no-op. Otherwise it reads the manifest at the current
generation and evaluates `CompactionPolicy#shouldCompact` against `ManifestSegmentMetrics` --
segment count and size derived straight from the manifest's file map via
`IndexFileNames#parseSegmentName` (no bundle opened), matching &sect;7.4's "readable without opening
the shard" requirement exactly. Delete ratio is not derivable this way (it lives inside a segment's
doc-values data, not its file name or size) and is always reported as `0.0`; per
`CompactionRebaseExecutor`'s own safety argument this only means the delete-reclaim trigger never
fires from this estimator, a missed optimization rather than a correctness issue -- the
segment-count/size triggers, which don't depend on it, are unaffected.

Still open: the compactor role/lease-offload negotiation with an active writer (now precisely
scoped above, not vague). Milestone (met): a quiescent 40-segment shard is compacted to size-tiered
shape with no writer ever activating, concurrently with a surprise writer re-activation -- the
rebase protocol, a real merge, size-tiered shaping, and now background scheduling are all
implemented and tested together.

**Phase 4.6 — Snapshots/clones/PITR (manifest pinning and PITR retention wiring done; clone not
started).** Durable pins (§6.5) are implemented as `DurablePinRegistry`/`BlobContainerDurablePinRegistry`
— same generic `BlobContainer.compareAndSwapRegister`-backed pattern as the shard-head store, so a
snapshot pin survives independently of any node's lease. `PinRecord` names *why* a generation is
pinned (snapshot id, `"pitr"`, ...) so independent retention reasons on one shard never clobber
each other; add/remove are idempotent, CAS-retry-based, and tested under concurrent pinners on
the same shard. An integration test confirms the full lifecycle end to end against
`ManifestRetentionPolicy`: pin a generation via the real registry, verify it survives a GC sweep
that would otherwise delete it, remove the pin, verify the generation becomes deletable again.

PITR retention wiring -- previously the one piece missing ("the registry is the building block;
nothing yet decides *when* to add a `"pitr"` pin") -- is now real: `PitrRetentionPolicy` (pure,
no I/O, mirroring `ManifestRetentionPolicy`'s shape) decides which manifest generations must
currently carry a `"pitr"` pin so any timestamp within the retention window can be restored to --
every manifest created inside the window, plus the single most-recent manifest immediately before
it (so a restore request right at the window's edge still resolves to something).
`PitrRetentionReconciler` is the impure counterpart: reads a shard's current `"pitr"` pins,
diffs against what the policy requires right now, and applies exactly that diff to a real
`DurablePinRegistry`, touching no other pinning reason on the shard. Idempotent by construction --
calling it more often than the window actually moves is wasted work, never incorrect.

Building this surfaced a real, pre-existing gap in `DurablePinRegistry` itself, not just a missing
caller: `removePin(String pinId)` removes *every* pin sharing that reason, which is correct for a
single-generation reason like a snapshot but wrong for PITR, where many generations share the
*same* `"pitr"` pinId simultaneously -- calling it to drop one aged-out generation would have
silently deleted every other in-window PITR pin too. Added `removePin(String, int, PinRecord)`,
matching on the full pin (reason + term + generation) so one generation's pin can be removed
without touching any other generation's pin under the same reason; `removePin(String, int,
String)`'s original all-matching-reason behavior is unchanged and still used for snapshots. Not
yet done: zero-copy clone with cross-index bundle refcounts, and the extended GC model check this
phase is gated on.

**PITR reconciliation is now actually invoked, not just correct in isolation.**
`PitrRetentionSchedulerTask` runs `PitrRetentionReconciler` for one shard on a fixed schedule (5
minutes -- deliberately much longer than the directory-tier refresh, since reconciliation
enumerates and reads every manifest the shard has ever written via the new
`BlobContainerManifestStore#listManifests`, real I/O the lightweight directory refresh doesn't
do). `ObjectStoreWriterEngine` owns one for as long as it stays open, the same lifecycle shape as
its directory-refresh task, canceled cleanly on `close()`. Configured via a new
`serverless_storage.pitr_window` node setting; a non-positive value (the default) disables PITR
retention entirely -- no `"pitr"` pins are ever added -- matching how `encryptionKeyProvider`
being absent means "encryption is off" elsewhere in this plugin. A failed reconciliation attempt
is swallowed and retried on the next tick rather than failing the shard's engine: it can only ever
leave pins stale, never delete anything, so a transient failure is never worse than the status quo.
Verified: constructing/closing the engine with PITR configured doesn't throw (the scheduling
internals themselves -- real background ticks, real pin add/remove, cancellation on close -- are
exhaustively covered directly in `PitrRetentionSchedulerTaskTests` against a short, configurable
interval, since the engine's real 5-minute interval is far too long to observe in a test).

**Phase 5 — Hardening (ongoing).** Chaos suite (§17) including full object-store outage modes
(§13), performance tuning of bundle/WAL batch parameters, API gating audit, autoscaling signal
calibration, resharding-by-copy (split/shrink via manifest rewrite + bundle copy — no reindex;
note: until bundles are physically rewritten, readers of a split target apply a doc-routing
partition filter, so the transition is logical-first, physical-later).

**Phase 6 — Migration tooling.** Conversion of existing remote-store indices to the bundle
format (their segments already sit in the object store — conversion is manifest synthesis plus
optional re-bundling, not data re-upload); snapshot-mount import for classic indices; both
directions documented so serverless adoption is not a one-way door.

## 17. Testing Strategy

- **Format-level**: property-based tests on bundle/manifest round-trips; corruption injection
  (truncated bundle, missing manifest, checksum mismatch) must fail closed.
- **Fencing**: dual-writer tests — old-term writer keeps publishing during/after failover;
  assert its manifests are never visible and are GC'd.
- **Chaos**: kill writer mid-bundle-upload, mid-manifest-write, mid-WAL-chunk; kill readers
  mid-refresh; object store fault injection (throttling, 5xx storms, elevated latency) — the
  mock repository infrastructure in-repo already supports much of this.
- **Staleness/consistency**: linearizability-style checker for the RYW path (indexed doc with
  generation token must be visible to a routed search); monotonicity checker for readers.
- **GC safety**: long-running PIT queries concurrent with aggressive ingest+merge; assert no
  read ever touches a deleted object.
- **Cost accounting**: per-workload object-store request counts as a regression metric —
  a change that doubles PUT count is a failed build, same as a latency regression.

## 18. Risks and Open Questions

1. **Object-store request economics.** Group-commit intervals and bundle batching directly trade
   durability latency vs request cost. Mitigation: make ack policy explicit (§6.4), publish
   request-count metrics from day one, and treat them as SLOs.
2. **Cold-query latency.** A cache-empty reader answering an aggregation over 100 GB will be
   slow no matter what. Mitigation: boot-set prefetch, honest documentation, and autoscaling on
   cache hit rate; consider tiered "pinned working set" for latency-critical indices.
3. **Reader heap under many shards.** Segment metadata heap cost per open reader bounds shard
   density. Admission control (§7.2) prevents OOM but caps density; needs measurement early
   (Phase 3 gate). **Status: a coarser stand-in implemented, not §7.2's actual target design.**
   §7.2 describes per-*refresh* admission control weighed against a real heap/cache byte budget,
   deferring a refresh that would exceed it while the shard keeps serving slightly stale data --
   that needs the lazy, block-cache-backed remote `Directory` view §7.2 also describes, which
   doesn't exist yet (today's `ObjectStoreReaderEngine` fully materializes a manifest up front, a
   separately documented tradeoff). `ReaderShardAdmissionController` is a simpler mechanism
   reachable without that prerequisite: a fixed cap on the *count* of concurrently open reader
   engines on one node (`serverless_storage.max_concurrent_reader_shards`, disabled by default),
   checked once at `ObjectStoreReaderEngine.open` time and released on `close()`. It bounds the
   same underlying risk, just less precisely (an over-capacity open fails outright rather than
   degrading to stale-but-serving) and without any actual memory measurement. Verified: acquiring
   up to the configured limit succeeds, the next acquire beyond it throws without leaking a permit
   from the failed attempt, and closing a previously-opened engine frees a permit for the next
   open to succeed.
4. **WAL multiplexing fairness.** One node-level WAL means one noisy shard can delay acks for
   others. Mitigation: per-shard budget within a chunk, overflow to dedicated chunks.
   **Status: implemented and tested.** `WalChunkService` now takes an optional
   `perShardBudgetBytes`; a shard whose own cumulative buffered payload bytes since its last
   flush crosses that budget is immediately siphoned out of the shared buffer into its own
   dedicated chunk -- written right away, independent of the caller's own flush timer/threshold --
   leaving every other shard's buffered records and byte counters untouched. Disabled by default
   (`<= 0`, matching every other optional-feature-off default in this plugin) so existing callers
   are unaffected. Verified: a noisy shard crossing budget overflows immediately without a
   `flush()` call; a quiet shard sharing the buffer is undisturbed by another shard's overflow; the
   overflowed shard's byte counter resets so it can accumulate again rather than overflowing on
   every subsequent append; an ordinary `flush()` clears every shard's counter too, not just the
   buffer. `WalMirroringTranslog`'s one current caller flushes after every single append already
   (per-operation durability), so it never actually accumulates a multi-shard buffer today and
   doesn't yet pass a budget -- this mitigation is real infrastructure for the node-level,
   genuinely-batched caller `WalChunkService`'s own class javadoc already describes as the
   component's actual target use, not yet exercised by a real multi-shard-batching caller.
5. **Coordination-free GC** is the subtlest correctness surface. The lease/TTL design must be
   model-checked (TLA+ or equivalent) before Phase 1 completes — this is the one component
   where a design bug destroys data. The model must cover cross-index bundle references from
   clones (§14) and compactor/writer publication races (§7.4), not just the single-writer case.
   **Status**: a TLA+ model of the core primitive underneath all of this — `ShardHead`'s
   term/lease/generation state machine and its version-CAS guard, now extended with the
   compaction service as a distinct actor — is written at
   `plugins/serverless-storage/formal/ShardHead.tla` (with companion `ShardHead.cfg` for the
   real, fixed production code and `ShardHeadBuggy.cfg` for the pre-fix code, see below).
   **Machine-checked with TLC** (TLC2 2.19, via `tla2tools.jar`) under two configurations:

   - `ShardHead.cfg` (`Spec`, matching production as it is today): six properties -- at most one
     valid lease holder at a time, term never decreases, generation never regresses within a
     term, a new term always resets generation to 0, only the real current holder can ever
     publish, and (`AuthorshipHonest`) a writer's publish attempt is never accepted unless the
     head genuinely ends up authored by that writer -- all hold across the complete reachable
     state space for the model's bound (3 nodes, term/generation up to 3): 3,029,216 distinct
     states, search depth 25, 0 states left on the queue, an exhaustive, not sampled, search.
   - `ShardHeadBuggy.cfg` (`SpecBuggy`, modeling the *pre-fix* production code via an added
     `PublishBuggy` action): `AuthorshipHonest` is **violated**, with TLC reporting a concrete,
     minimal counterexample -- a writer acquires the lease, a compactor independently advances
     the live generation while the writer's cached view is stale, and the writer's publish
     attempt at its own next generation is accepted as success even though the head is actually
     authored by the compactor. This is a machine-verified demonstration that the bug found and
     fixed this session in `ObjectStoreCommitHeadPublisher#publishCommitAsHead` (the
     `manifest.generation() <= currentHead.latestManifestGeneration() => return true` shortcut)
     was a genuine protocol-level violation, not merely an implementation nitpick -- and that the
     fix closes it: every other property still holds under `SpecBuggy` too, consistent with the
     bug never mutating `head` incorrectly, only lying to its caller about what happened.

   Getting to a trustworthy result took three real mistakes, each caught only by running TLC and
   noticing a result that didn't match hand-tracing a reachable counterexample, not by
   inspection: (1) the original `NoNode == CHOOSE v : v \notin Nodes` is an unbounded CHOOSE,
   which TLC cannot evaluate at all -- fixed by declaring `NoNode`/`Compactor` as their own
   `CONSTANT` model values; (2) `AuthorshipHonest` was first written wrapped in `[...]_Vars`,
   whose standard "or stutter" semantics made it vacuously true on exactly the transitions
   (`PublishBuggy`, whose entire postcondition is `UNCHANGED Vars`) it existed to catch -- TLC
   reported no violation where one was clearly reachable by hand; (3) the next attempt --
   referencing the bare `PublishBuggy(n,g)` action formula directly inside a property -- made the
   property fail even under the *correct* `Spec` (which never takes that action at all), because
   TLA+ action formulas are checked structurally against any `(state, state')` pair regardless of
   which actual `Next`-disjunct produced it; an idempotent `Read` that happened to leave every
   variable's value unchanged satisfied `PublishBuggy`'s shape by coincidence. The robust fix,
   used in the final spec: a dedicated ghost variable (`authorshipViolated`) that only
   `PublishBuggy` itself ever sets, turning "did this specific action really fire" into an
   observable fact checked as an ordinary state invariant, rather than reconstructed after the
   fact from a value-comparison other actions can coincidentally also satisfy.

   A third configuration, `ShardHeadDecoupled.cfg` (`SpecDecoupled`), formally verifies the
   *proposed* fix for the residual risk noted above (decoupling a writer's generation numbering
   from local Lucene state entirely, always computing its target live as `head.generation + 1`
   and fencing against the live `head.holder`, mirroring how the compactor's publish already
   works) **before** implementing it in the hot-path Java code. All properties -- including a
   dedicated `OnlyTheCurrentHolderCanPublishDecoupled` fencing property -- hold across the
   complete reachable state space (2,742,008 distinct states, search depth 22, 0 states left on
   the queue, exhaustive). One design flaw was caught and fixed by hand, before ever running TLC
   on this variant: an earlier draft fenced on the writer's own stale cached belief
   (`localHead[n].holder = n`) instead of the live `head.holder = n`, which would have let a
   writer already fenced out by a newer term's lease acquisition still slip a generation bump
   through under the new holder's identity. With the live-fencing version, the redesign was
   formally verified sound and has since been **implemented** in `ObjectStoreCommitHeadPublisher` /
   `ObjectStoreWriterEngine` (see §16 Phase 4.5's risk note for the Java-side detail) without any
   further protocol-level surprises during implementation.

   Still does not cover clones/cross-index references (§14).
6. **Interplay with existing warm/composite work.** Writable warm solves an overlapping problem
   (disk smaller than data) with a different mechanism (composite local+remote directory under a
   writable engine). Decision needed: converge warm onto the reader-engine + bundle layout in
   the long term, or keep both. Recommendation: converge — two remote formats is one too many
   (§5, principle 1) — but only after Phase 3 proves the reader path.
7. **Segment replication compatibility.** Serverless mode supersedes segrep (publication *is*
   segment replication via storage). Indices can't mix modes; enforce at index-settings
   validation.
8. **Vector/kNN workloads.** HNSW graph traversal is random-access over large structures —
   nearly the worst case for a 1 MB-block LRU cache. Vector-heavy indices likely need a
   distinct cache class (pin whole graphs while a shard is query-active) and possibly
   graph-aware boot sets; sizing rules differ enough from text search that kNN gets its own
   measurement gate in Phase 3 rather than an assumption of "it's just another file."
9. **Aggregation-heavy heap costs on readers** (global ordinals, fielddata) behave differently
   when segment data is cold — building global ordinals on a cache-miss storm is a latency
   cliff. Mitigation candidates: ordinal structures included in boot sets, or eager ordinal
   builds pinned behind the admission controller (§7.2). Needs Phase 3 measurement.

## 19. Summary

The strategy is: keep Lucene and the mechanical engine internals; replace *where bytes live and
how visibility propagates*. Writers become "Lucene + group-committed WAL + bundle publication";
readers become "manifest subscriber + block cache + read-only directory view"; the object store
plus a small manifest protocol replaces peer replication, peer recovery, and shard relocation.
The cluster stops being a set of data owners and becomes a pool of interchangeable caches with
roles — which is the property that makes true elasticity, role-based scaling, and scale-to-zero
possible.
