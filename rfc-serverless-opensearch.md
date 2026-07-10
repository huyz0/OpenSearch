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

**Status: WAL chunk deletion (the fourth bullet above) is now implemented, closing a real,
previously-unflagged gap found while auditing this section rather than while building something
else.** `WalChunkService` used to write chunks and never delete any -- unlike the manifest/bundle
GC sweep (§6.5, `GcSchedulerTask`), there was no WAL analogue at all. The design question that
first looked like it needed formal model-checking (`CloneGc.tla`-style) turned out to dissolve once
chunk sequence numbers became a single, globally-continuous space shared by every node (the
collision fix below): "which epoch a shard's latest manifest references" is not actually a physical
retention boundary in this design -- there is only ever one shared WAL container, so the real
question reduces to "has every shard that could still need a chunk sequence already published past
it," independent of epoch entirely.

`WalShardRegistry` (new) is a durable, CAS-backed, deliberately grow-only registry of every
(indexUuid, shardId) known to have mirrored into the shared container -- `ObjectStoreWriterEngine`
registers its own shard once per activation. Growing-only is safe by construction, the same shape
`CloneGc.tla`'s `Fixed` variant already proves sound for a different mechanism (pin before read,
never remove early); the one case actually safe to *remove* an entry for is a real index deletion
(independent proof that shard will never publish again), wired as a sibling to the existing
clone-pin deletion listener in `ServerlessStoragePlugin#onIndexModule`. A shard merely relocating to
a different node is not a removal case at all: it keeps publishing (now via a different writer, a
different epoch, the *same* shared container) and its own `WalPosition` keeps legitimately
advancing, so the registry's min-covered-position computation is never stuck on it -- only a
genuinely dead (deleted) shard, or one that stops publishing forever without its index ever being
deleted, would freeze the bound; the latter is an accepted, documented limitation, not a
data-loss risk (the failure mode is "deletes less than it could," never "deletes something still
needed").

`WalGcSchedulerTask` (new, node-level, gated behind `serverless_storage.wal_gc.interval`, off by
default) sweeps on a fixed schedule: reads every registered shard's latest published manifest,
takes the minimum `WalPosition#offset()` across all of them, and deletes every chunk sequence at or
below that bound. Deliberately conservative when information is incomplete -- a registered shard
with no published manifest yet, or a latest manifest carrying no real `WalPosition`, blocks the
*entire* sweep for that tick rather than computing a bound that could be unsafe. No separate
time-based retention window is needed the way `GcSchedulerTask`'s own sweep uses one: once every
known shard's latest manifest covers a chunk sequence, nothing will ever legitimately replay from
at or below it again (replay only ever reads forward from a shard's own last-covered position).
Verified with real multi-shard scenarios: a sweep respects the *slowest* of several registered
shards' coverage, not just one; a shard that's registered but has never published blocks the whole
sweep rather than being silently skipped; and end-to-end in a real cluster, a writer shard registers
on activation and is deregistered when its index is genuinely deleted.

**A separate, more severe sibling bug was also found while investigating the gap above -- and,
unlike the deletion gap, verified and fixed in the same session: two nodes with WAL mirroring
enabled against the same shared `serverless_storage.base_path` could silently overwrite each
other's WAL chunks -- not a rare race, an unrecoverable data-loss bug real production use of this
feature (more than one writer-hosting node) would have hit continuously.** Root cause: `WalChunkService`'s chunk sequence numbers used to
be assigned by a purely local `AtomicLong`, seeded once at construction from
`firstUnusedChunkSequence` (a listing of whatever the shared container already contains) and
incremented independently thereafter, with zero cross-instance coordination -- yet every node's
`WalChunkService` writes into the exact same physical `<base_path>/wal/` container
(`ServerlessStoragePlugin#createComponents` derives it from the one shared `basePath` every node in
a cluster uses, per `ServerlessStorageWriterFailoverIT`'s own setup), and `writeChunk` calls
`BlobContainer#writeBlob` with `failIfAlreadyExists=false` -- a silent overwrite, not a thrown
error, whenever two nodes' independently-numbered sequences collided. **Verified before fixing, not
just reasoned about**: `WalChunkServiceTests` first added a regression test proving the collision
(two `WalChunkService` instances, distinct epochs as two real node incarnations would have, against
one shared container -- the second's chunk 0 write genuinely destroyed the first's).

**Fixed**: chunk sequence numbers are now claimed via `blobContainer`'s own
`BlobContainer#compareAndSwapRegister` -- the same primitive `ShardStateStore`/`DurablePinRegistry`
already rely on -- using a dedicated register blob's own generation as the counter (a register's
`generation()` is already "monotonically increasing with each successful write," exactly a
distributed atomic counter, for free). `WalChunkService#currentChunkSequenceUpperBound()` is now a
live, non-cacheable read of that same register rather than a local field, so
`ObjectStoreWriterEngine#activationWalPosition`'s fencing snapshot can never be stale in the unsafe
direction (a cached-too-low value silently under-covering replay, the exact failure mode
`WalReplayFencing.tla` exists to rule out); making that read live required a new `IOException` at
the one call site inside `ObjectStoreWriterEngine#beginConstruction` -- the `ThreadLocal`-bridged,
`super()`-ordering-constrained static helper that snapshots `activationWalPosition` before this
engine's own fields are reachable -- caught and rewrapped as `UncheckedIOException` there rather
than threading a new checked-exception signature through that already-delicate constructor chain.

**Known, accepted cost of this fix**: `claimNextChunkSequence` now does a real register
read-then-CAS round trip (on `FsBlobContainer`, backed by `FileChannel#lock()`) on every chunk
write, where the old (unsafe) design was a free in-memory `AtomicLong` increment.
`WalMirroringTranslog#add` calls `flushWithRetry()` -- and therefore this -- once per indexed
operation under the default per-operation-durability configuration, so this is a real per-write
latency/throughput cost, not a one-time or background cost. Correctness took priority over
preserving that free-increment performance, consistent with this section's own "durable ack costs
up to one flush interval of latency" framing already accepting real durability-driven cost --
worth measuring under real load if WAL mirroring throughput becomes a concern, but not treated as
blocking this fix.

The regression test above now proves the fix instead of the bug: two concurrently-writing instances
against one shared container get genuinely distinct chunk sequences, and a second instance's
`currentChunkSequenceUpperBound()` reflects a first instance's write live, not from a stale cache.
A follow-up stress test raises this from two sequential instances to 20 genuinely concurrent ones
(real thread contention on the CAS retry loop itself, not just sequential calls), asserting every
claimed sequence is globally unique and every chunk independently recoverable. Full plugin unit
suite and multi-node internal cluster tests (including the real WAL-mirroring crash-failover IT)
green, stable across repeated runs with fresh seeds. A follow-up audit of every other
`BlobContainer#writeBlob`/`writeBlobAtomic` call site in this plugin found no sibling instance of
this bug pattern: bundle writes already use `writeBlobAtomic(..., failIfAlreadyExists=true)` keyed
by a CAS-protected `(primaryTerm, generation)` pair, so a colliding write fails loudly instead of
silently overwriting -- this WAL chunk service was the only place in the plugin using
`failIfAlreadyExists=false` against a name that wasn't already guaranteed unique some other way.

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
work enables -- since closed, see &sect;7.1.2 and this same section's own later "store population"
paragraph below for the actual fix and its end-to-end proof: bundle materialization on writer
activation.** WAL replay (everything
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

#### 7.1.1 Local retention once object storage is durable (implemented)

**Status**: originally written up here as design-only, before any code, so the safety argument
would get scrutiny first — since implemented and verified end to end (see §16 Phase 2's own status
note, "Local commit/translog retention is now durability-driven end to end (§7.1.1, both parts
done)"). This section is that design, worked through far enough to have been implementable, plus
the one open call that was a genuine product/risk-tolerance decision, not an engineering one --
left in place below as the record of the reasoning the implementation follows, not as a
still-pending proposal.

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
  compiles. **A structural note, not an open gap**: `Translog#trimUnreferencedReaders` combines
  this policy's floor with a *second* floor derived from `CombinedDeletionPolicy`'s safe-commit
  tracking (core's own global-checkpoint-driven retention) -- so the *combined* effective retention
  is always bounded by whichever of the two is more conservative. This was the reason local commit
  retention (below) needed its own fix too, not just translog retention alone; both are now
  implemented, so this combination no longer leaves excess retention on the table the way it would
  have with only one side fixed.
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
(269 tests) pass unchanged, confirming every other engine's recovery path is unaffected. **The seam
itself also now has its own engine-agnostic core-level test**, `RecoverMissingLocalStoreTests`
(server module, sibling to `EngineRecoveryOperationsTests`, same generic-not-plugin-specific
shape): a bare `EngineFactory` override that materializes an empty commit + translog via
`Store#createEmpty`/`Translog#createEmptyTranslog` (mirroring what `StoreRecovery#recoverEmptyStore`
already does for its own unconditional case) proves the conditional branch genuinely re-reads the
store and lets recovery proceed on `true`, and a companion test with the plain default
`InternalEngineFactory` proves the original "shard allocated for local recovery, should exist, but
doesn't" failure is completely unchanged when the hook isn't overridden at all -- generic
regression coverage for the seam itself, independent of `WriterEngineFactoryCrossNodeFailoverTests`
already covering this plugin's own specific manifest-materializing implementation.

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
writer at all" -- remains unbuilt as an *API*, though the underlying capability it was chasing is
now delivered a different way: `CompactionSchedulerTask`'s background scheduling (Phase 4.5 below)
already compacts a writer-less shard on its own fixed schedule, with no `_forcemerge` call needed
at all -- the "no writer ever activating" milestone this section originally pointed at is done, see
below. What's still genuinely missing is narrower than originally scoped: wiring the `_forcemerge`
*API call itself* to redirect to this service when no writer is active, so a caller doesn't have to
wait out the background schedule. That redirect needs real core transport-layer work beyond this
plugin -- `_forcemerge`'s REST/transport chain, as noted above, only ever reaches an engine while a
writer is live, so there is currently no route for it to reach a writer-less shard at all; the
compaction-safety half of the original concern (a fencing protocol against a writer racing
mid-compaction) is no longer an open question, since the rebase protocol used by both the
already-shipped background scheduler and this hypothetical redirect already handles exactly that
race, and real writer lease acquisition/renewal (Phase 4.5 below) now gives an accurate,
non-dead-code "is a writer currently active" check to gate the redirect on.

**A caller no longer has to wait out the background schedule -- via this plugin's own namespace,
not a `_forcemerge` redirect.** `POST /_plugins/_serverless/storage/_compact` (body:
`{"index_uuid", "shard_id"}`) triggers one immediate compaction-candidacy check and, if the shard
is a candidate, one publish attempt, without touching core's `_forcemerge` machinery or needing
the core transport-layer work that redirect would require. `CompactionSchedulerTask`'s own
per-tick decision logic was factored into a `public static maybeCompact(...)` so the scheduled
task and this on-demand trigger share exactly one implementation, never two copies that could
drift; `TransportCompactionTriggerAction` builds the same shape of stores the scheduled task
already needs from `ServerlessStoragePlugin#blobContainerForDirectoryFactory` (the same resolution
seam the clone action already established) and requires no routing to a specific data node, since
the attempt operates purely against the shared object store and the shard's CAS-guarded head, not
any node-local state -- not even a live writer's lease, matching `CompactionSchedulerTask`'s own
deliberate "does not skip just because a writer's lease is held" behavior. Verified end to end
over the real transport layer in a running cluster: a real shard with ten genuinely separate
flushed segments (`NoMergePolicy` on the writer, so Lucene's own auto-merge never interferes) is
recognized as a compaction candidate and merged down to one segment via
`client().execute(CompactionTriggerAction.INSTANCE, ...)`, and a shard that was never activated
returns a safe `attempted: false` rather than an error.

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

**A real lazy, block-cached remote `Directory` now exists, standalone -- built first, then wired
into `ObjectStoreReaderEngine`'s `open()` path once the blocker below was resolved (see "Resolved
with a small, targeted core change" further down this section).** This is the piece
&sect;7.2 describes ("a read-only Lucene `Directory` whose 'files' are `(bundle, offset, length)`
ranges resolved through the block cache -- `createOutput`/`deleteFile` throw") and &sect;18 risk #3
names as the missing prerequisite for real heap-budget admission control. Built by reusing, not
reimplementing, core's own searchable-snapshots machinery -- confirmed to be exactly the same
problem (lazy, block-granular, cached remote file reads) before writing anything: `LazyBundleIndexInput`
extends core's `org.opensearch.index.store.remote.file.AbstractBlockIndexInput` directly (the same
base class `RemoteSnapshotDirectory`'s own `OnDemandBlockSnapshotIndexInput` uses), and
`LazyBundleDirectory`'s `openInput` wires it to a `TransferManager`/`FileCache` pair -- both
`@PublicApi`/stable-enough core classes, not reimplemented. Simpler than the snapshot version in one
respect: this plugin's bundle format never splits one logical file across more than one blob, so
`fetchBlock` issues a single-part `BlobFetchRequest` per block, not the snapshot version's
multi-part chunking logic. `BlobContainerBundleStore` gained one new method, `openRange`, whose
signature was deliberately made to match `TransferManager.StreamReader` exactly so a method
reference passes directly as one -- no adapter class needed.

**Known, documented limitation of this first slice**: no per-block checksum verification.
`BlobContainerBundleStore#readFile` verifies a whole logical file's checksum in one shot, but this
bundle format has no sub-file checksum an arbitrary block range can be verified against -- a real
gap this slice does not close, distinct from (and smaller than) the block-granularity/node-sharing/
eviction-policy gaps `LocalDiskCachingBundleStore` above still has.

Verified with three tests (`LazyBundleDirectoryTests`) against a real object store (no mocks): a
real Lucene `DirectoryReader`/`IndexSearcher` opened directly against a `LazyBundleDirectory`
returns correct search results with zero upfront materialization; `listAll()`/`fileLength()` are
answerable purely from the manifest's own file map with no fetch at all; every write operation
(`createOutput`/`deleteFile`/`rename`) is rejected, matching &sect;7.2's read-only contract exactly.

**Wiring this into `ObjectStoreReaderEngine.open()` was attempted next and found to be genuinely
blocked, not merely unstarted -- worth recording precisely, not just "still open."** The
`ReadOnlyEngine`/`Store` FSDirectory-assumption question above was checked directly and came back
clean: `ReadOnlyEngine`'s constructor, `Store`'s constructor (`Directory directory` typed
parameter, no `FSDirectory` cast anywhere on this path), and `SegmentInfos.readCommit` are all
generic-`Directory`-based; nothing here would reject `LazyBundleDirectory`. The actual blocker was
narrower and more structural: swapping the `Directory` a reader shard's `EngineConfig`/`Store`
already wrap requires either (a) rebuilding `EngineConfig` with just its `Store` field replaced, or
(b) an earlier, core-registered seam that constructs the `Store`/`Directory` from the start. (a)
stayed blocked (`EngineConfig` genuinely has no copy-with/rebuild mechanism); (b) existed as
`IndexStorePlugin.DirectoryFactory` but was selected per-*index*, not per-shard-*copy*, so using it
directly would have forced writer shards onto the same read-only directory as reader shards on the
same index.

**Resolved with a small, targeted core change -- (b) made `ShardRouting`-aware, mirroring a pattern
core already has for exactly this problem.** `EnginePlugin.getEngineFactory(IndexSettings,
ShardRouting)` already lets a plugin pick a different *engine* per shard copy; `DirectoryFactory`
had no equivalent for the *directory*. Before writing this, every existing implementor of
`DirectoryFactory` across the whole tree was enumerated (four direct implementors, two subclasses,
four plugin registration points) and the actual call site was traced (`IndexService#createShard`,
which already has the shard's `ShardRouting` in scope as its own first parameter -- no plumbing
needed) to confirm this was genuinely a small, safe, backward-compatible addition before touching
core at all:

- `IndexStorePlugin.DirectoryFactory` gained one new *default* method,
  `newDirectory(IndexSettings, ShardPath, @Nullable ShardRouting)`, delegating to the existing
  two-argument overload -- every existing implementor's behavior is completely unchanged unless it
  explicitly opts in by overriding the new overload, the same non-breaking shape
  `EnginePlugin.getEngineFactory`'s own two-argument-to-three-argument addition already used in
  this codebase.
- `IndexService#createShard`'s one call site now passes the already-in-scope `routing` through.

**`ServerlessStorageLazyDirectoryFactory`, this plugin's own implementation, closes the loop.**
Registered under a new store type, `serverless_storage_lazy` (opted into per-index via a new
`index.serverless_storage.lazy_directory.enabled` setting, translated to `index.store.type` by
`ServerlessStorageIndexSettingProvider` the same way that class already hides
`index.allocation.existing_shards_allocator` behind a plugin-level setting). Its `newDirectory`
override branches on the passed `ShardRouting`: a search-only copy gets a real `LazyBundleDirectory`
(resolving the shard's current manifest the same way `ReaderEngineFactory` does, since this runs
*before* engine construction); anything else (a writer/primary, or the fallback when the lazy
directory feature is off) gets a completely normal `FsDirectoryFactory`-built directory via
delegation -- a writer shard on an index with the lazy directory enabled is entirely unaffected.
Reads `ServerlessStoragePlugin`'s own fields (`threadPool`, the shared `FileCache`) lazily at
`newDirectory`-call time rather than at construction time, since `getDirectoryFactories()` is
invoked by core during node startup *before* `createComponents` runs (confirmed by tracing `Node`'s
own constructor) -- capturing those fields eagerly would have captured them uninitialized.

`ObjectStoreReaderEngine` itself needed one small adaptation: `applyManifestToDirectory` now
branches on whether `FilterDirectory.unwrap(config.getStore().directory())` is a
`LazyBundleDirectory` (the same unwrap idiom `ReadOnlyEngine`'s own constructor already uses to
detect a `RemoteSnapshotDirectory`) -- a lazy directory gets `LazyBundleDirectory#advanceToManifest`
(a plain in-memory map merge, no I/O), everything else still gets
`ObjectStoreCommitMaterializer#materialize` exactly as before. `LazyBundleDirectory` itself gained
`advanceToManifest` (additive, same semantics as the eager materializer's own additive writes) and
now owns and closes its own per-shard on-disk block-cache directory (constructed fresh by the
factory specifically for it, unlike the shared node-wide `FileCache`/`TransferManager`, which
outlive any one shard and are not this class's to close).

Verified with a new end-to-end test, `LazyDirectoryReaderEngineTests`, that goes one level deeper
than the standalone `LazyBundleDirectoryTests`: a real `Store` built around a real
`LazyBundleDirectory` (exactly what the factory would hand core), a real `EngineConfig` built
around that `Store`, and a real `ObjectStoreReaderEngine.open()`/search against it -- with the
materializer passed a `BundleFileReader` that throws if ever invoked, proving the lazy branch is
what actually ran, not merely that both code paths compile. Full unit suite, the new lazy-directory
tests (stable across 3 repeated runs), multi-node internal cluster tests, and core's own
`IndexService`/`IndexStorePlugin`/`IndicesService` test suites all green after the core change.

**Block granularity tuned to &sect;9's real target.** `LazyBundleIndexInput` inherited
`AbstractBlockIndexInput.Builder`'s own default (8 MiB, tuned for whole snapshot files) rather than
&sect;9's stated "default 1 MB region granularity" -- a one-line fix
(`LazyBundleIndexInput.BLOCK_SIZE_SHIFT = 20`, threaded through the root `IndexInput`'s builder;
slices/clones already inherited `blockSizeShift` correctly). Verified with a real multi-block fetch,
not just a parameter-value assertion: a 2.5 MiB file read end to end through the lazy path produces
exactly 3 distinct block-cache entries (`AbstractBlockIndexInput#isBlockFilename` over the on-disk
cache directory's listing) and the bytes read back match exactly what was written -- this test also
caught a real test-authoring bug on its first run (a hand-built `FileReference` with offset 0
instead of the bundle's real post-header offset, causing a genuine content mismatch), a reminder
that even a test fixture needs the same "verify by testing" discipline as production code.

**Admission control now weighs a real byte budget, not just shard count.**
`ReaderShardAdmissionController` gained an optional `FileCache`-backed check: given the node's
shared lazy-directory block cache and a configured usage-ratio threshold
(`serverless_storage.reader_admission.max_file_cache_usage_ratio`, default 0.9), `acquire()` now
refuses to open another reader shard once that cache's `usage()` crosses the configured fraction of
its `capacity()` -- even if the fixed shard-count cap
(`serverless_storage.max_concurrent_reader_shards`) still has headroom. `ServerlessStoragePlugin`
wires its own `lazyDirectoryFileCache` straight into the controller it already owned; when no lazy
directory cache is configured (or the count cap itself is off), this degrades back to the original
count-only check, unchanged. This closes the gap this section's own status note previously called
out: the check is now against the node's actual measured cache usage, not merely a guessed-at
shard count. **The per-refresh half of §7's target design -- deferring a refresh that would exceed
budget while the shard keeps serving slightly stale data, rather than only failing outright at
open -- is closed too** (see §18 risk #3): `ObjectStoreReaderEngine`'s background manifest poll
now checks `ReaderShardAdmissionController#isOverBudgetForRefresh` before materializing a newer
generation, skipping an over-budget tick entirely rather than pulling more segment bytes into an
already-over-budget cache; the next poll tick retries automatically once pressure eases.

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
(second bullet), the actual scale-to-zero mechanism (third bullet), and mandatory remote cluster
state enforcement (fourth bullet). **The third bullet's signal-collection half is no longer
entirely missing, though**: both named per-tier signals -- ingest tier's writer idle activity and
search tier's manifest-generation lag -- now have a real, tested, per-node discovery surface (see
&sect;16 Phase 4 below for both). What's still genuinely absent is everything downstream of
collecting those signals: no policy consumes them, and no suspend/scale-to-zero mechanism exists
to act on a policy's decision even if one did.

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
any WAL wire-format or wiring change.

**`EncryptingWalChunkService` is now actually wired in, closing what had been a real, silent gap**:
this component was fully built and tested but never once invoked by a running engine --
`ObjectStoreWriterEngine`'s own `WalChunkService` field flowed straight into `WalMirroringTranslogFactory`
unwrapped regardless of whether encryption was configured, meaning a deployment with an encryption
key set got encrypted bundles and manifests but *plaintext* WAL chunks, silently. `WalAppendTarget`
(new interface: `append`/`flush`/`bufferedRecordCount`) is what closes it without a larger rewrite:
`WalChunkService` and `EncryptingWalChunkService` both implement it, and `WalMirroringTranslog`/
`WalMirroringTranslogFactory` depend on the interface instead of `WalChunkService` concretely, so
`ObjectStoreWriterEngine#createTranslogManager` can wrap the configured `WalChunkService` in an
`EncryptingWalChunkService` (threaded through via the same `ThreadLocal`-across-`super()` bridge
`walChunkService`/`activationWalPosition` already use, since this decision has to be made before
this class's own instance fields are reachable) whenever an `EncryptionKeyProvider` is configured,
completely transparent to `WalMirroringTranslog` itself. Verified end to end, not just at the
component level (`EncryptingWalChunkServiceTests`/`WalRecordCryptoTests` already covered that): a
real indexed document, mirrored through a real `ObjectStoreWriterEngine` configured with a key
provider, produces a WAL chunk blob whose raw payload does not deserialize as a valid
`Translog.Operation` (genuine ciphertext on disk, not unasserted plaintext) and which decrypts back
to the exact original operation with the right key. **Not implemented**: credential scoping per
tier (bullet 3), which needs IAM/role-assumption wiring per cloud backend, not just a core
primitive like the ones built so far.

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
  **Status: pin/release/restore are now all implemented and tested per-shard; only index-wide
  orchestration is still open.** New `SnapshotPinAction`/`SnapshotReleaseAction`
  (`retention/action` package, REST at `POST /_plugins/_serverless/storage/_snapshot_pin` and
  `/_snapshot_release`) durably pin/release a single shard's *current* published manifest
  generation under a snapshot name, via the same `DurablePinRegistry` PITR retention and clone
  already depend on -- until this pass only that underlying generic mechanism existed, with
  nothing letting an operator actually create one. Create-or-replace, not additive: re-pinning
  under an already-used `snapshotId` at a newer generation adds the new pin *before* removing the
  old one (never the other way -- a crash between the two only ever leaves both generations
  pinned, never a window where the name resolves to nothing), verified end to end in
  `ServerlessStorageSnapshotPinActionIT` including this exact replace-not-accumulate behavior
  against a real advancing shard.

  **`SnapshotRestoreAction` closes the read side** -- "point the shard-heads at the pinned
  manifests," literally: it CASes the shard's head to the (primaryTerm, generation) a snapshot
  pins, bypassing `ShardHead#withPublishedGeneration`'s forward-only validation directly (a
  restore is deliberately a rollback, not a publication). Deliberately refuses to proceed while
  the shard's writer/compactor lease is currently held -- a live writer would either immediately
  overwrite the restore on its next flush or leave the head in a confusing state between the two.
  This surfaced a real, easy-to-miss timing gotcha the IT itself caught: closing an index stops
  its engine from *renewing* the lease, but does not retroactively clear the lease already
  recorded in the shard head -- a deliberate fencing-safety property (an already-published lease
  must stay formally valid until its own TTL naturally elapses, since this plugin has no way to
  tell a graceful close from an ungraceful one from the object store's side, the same guarantee
  that makes WAL replay fencing sound). `ServerlessStorageSnapshotRestoreActionIT` originally
  assumed closing the index released the lease instantly and failed immediately against that wrong
  assumption; fixed by polling (bounded to the engine's own 30s lease TTL) rather than asserting
  once -- catching, along the way, that `assertBusy`'s retry loop only catches `AssertionError`,
  not arbitrary exceptions, so the polling helper has to explicitly re-wrap failures.

  Deliberately scoped to one shard per call throughout, matching every other action this plugin
  exposes (compaction trigger, clone) -- callers needing every shard of an index covered by one
  name used the per-shard actions in a loop of their own until this pass.

  **Status: the index-wide orchestration layer is now built too.** `IndexSnapshotPinAction`/
  `IndexSnapshotReleaseAction`/`IndexSnapshotRestoreAction` (same `retention/action` package, REST
  at `POST /_plugins/_serverless/storage/index/{index}/_snapshot_pin`, `_snapshot_release`,
  `_snapshot_restore`) resolve an index name to its UUID and shard count via `ClusterService`, then
  fan out to the per-shard actions above, one shard at a time. Pin is **genuinely all-or-nothing**:
  `TransportIndexSnapshotPinAction` releases `snapshotId` from every shard it already pinned in the
  same call if any later shard's pin attempt fails (release is idempotent, so a failed compensating
  release is itself safe to retry) -- verified in `ServerlessStorageIndexSnapshotActionIT` by
  corrupting one shard's register with garbage bytes *after* two other shards already have a real
  published head, forcing a genuine late-in-the-fan-out failure and confirming every shard
  (including the two that had already succeeded) ends up unpinned. Restore, by contrast, is
  **honestly documented as not fully atomic** -- `IndexSnapshotRestoreAction`'s own javadoc explains
  exactly why (a shard's writer lease can be reacquired between its validation check and its own
  restore call, a race no compensating rollback closes without a second rollback log this plugin
  judged not worth the complexity for so rare an operation) rather than silently overpromising a
  guarantee the implementation doesn't provide.

  Landing this surfaced one real bug: the REST path `/_plugins/_serverless/storage/{index}/_snapshot_pin`
  collided with `ShardIdleTimeAction`'s existing `/_plugins/_serverless/storage/{index_uuid}/{shard_id}/_idle_time`
  route (same path position, two different wildcard names, which OpenSearch's router rejects at
  startup) -- caught immediately by the new IT failing at cluster boot, fixed by nesting the
  index-wide routes under `/_plugins/_serverless/storage/index/{index}/...` instead. It also
  surfaced a wrong assumption in the first version of the rollback IT itself: a brand-new shard
  already has a published manifest as an ordinary side effect of shard creation (an initial empty
  commit), so "never flush, expect a `never published` pin failure" -- which works fine for a
  *single* never-created shard (see `SnapshotPinAction`'s own never-published test) -- can't be used
  to force a failure partway through an already-flushed multi-shard fan-out; fixed by corrupting one
  shard's register directly instead of relying on an unreachable never-published state.
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
3. ✅ Flush/commit lifecycle hook: turned out not to need a new core seam at all --
   `InternalEngine#commitIndexWriter(DocumentIndexWriter, String)` was already `protected` and
   overridable before this branch touched anything. `ObjectStoreWriterEngine#commitIndexWriter`
   overrides it directly, calls `super.commitIndexWriter(...)` first, then reads back the just-
   committed `SegmentInfos` from `store.readLastCommittedSegmentsInfo()` and publishes it as the
   new head via `headPublisher.publishCommitAsHead(...)` -- no deletion-policy indirection needed;
   this list item was stale, carried over from before that override was written.
4. ✅ Search-only routing → engine selection: `IndexShard` passes its routing into engine-factory
   resolution (done via `IndexService.createShard` on this branch). The second half of this item
   ("reader shards must skip `ReplicationTracker` paths that assume a local checkpointable engine")
   turned out not to need any change either -- `ObjectStoreReaderEngine extends ReadOnlyEngine`,
   and core's own `ReadOnlyEngine` semantics already keep it out of those paths; nothing in the
   plugin ever references `ReplicationTracker` at all. Confirmed empirically, not just by absence
   of a reference: `ServerlessStorageSearchOnlyReplicaIT` runs a real search-only shard copy
   end-to-end (allocate, start, materialize from a manifest, serve a search) with no
   `ReplicationTracker`-related failures anywhere in the run.
5. Generalized checkpoint/notification publisher (widen segment-replication checkpoint
   publishing to carry opaque payloads).
6. ⚠️ REST handler capability annotation (§11) -- **the annotation itself now exists; enforcement
   does not yet.** `RestHandler#serverlessScope()` (`server/src/main/java/org/opensearch/rest/RestHandler.java`),
   a default method returning a new `RestHandler.ServerlessScope` enum (`AVAILABLE`/`INTERNAL_ONLY`/
   `UNAVAILABLE`), defaults to `UNAVAILABLE` -- matching §11's own "unannotated handlers default to
   unavailable" design exactly, so new handlers must opt in consciously once enforcement lands.
   `RestHandler.Wrapper` delegates it like every other method on that class, so a wrapped handler
   (deprecation wrapper, etc.) doesn't silently fall back to the default and misreport its
   delegate's real availability. Every one of this plugin's own 9 REST handlers now overrides it to
   `AVAILABLE` -- verified by a new plugin test (`testEveryRestHandlerDeclaresItselfAvailableUnderServerlessMode`)
   that iterates `getRestHandlers()` and fails if any handler reports anything else, catching the
   exact mistake of adding a tenth handler without the override. **Deliberately not wired into
   `RestController` yet**: enforcement needs a node-level "is this node in serverless mode" flag
   that doesn't exist as a wired setting today, and threading `Settings` through `RestController`'s
   construction (today's constructor takes none) to add that flag is real, separate, cross-cutting
   surgery on a class core code depends on -- landing the annotation now lets every handler (this
   plugin's and, eventually, core's own) record its intended availability incrementally rather than
   needing one atomic change spanning every REST handler in the codebase once enforcement is built,
   the same incremental-seam-before-consumer shape §15 item 8's `ownsRemoteSegmentDurability()`
   already used. `server`'s own `RestHandlerTests` (new) covers the default and the `Wrapper`
   delegation directly; `server:missingJavadoc`/`forbiddenApisMain` and this plugin's full `check`
   both pass unchanged.
7. ✅ Node WAL service registration point: no core change needed here either -- confirmed
   `createComponents` is exactly the right lifecycle point, already in production use.
   `ServerlessStoragePlugin#createComponents` builds one `WalChunkService` per node incarnation
   (stored in the `sharedWalChunkService` field) and every writer shard's `EngineFactory` receives
   the same instance, giving the one-WAL-per-node sharing rfc-serverless-opensearch.md &sect;6.4
   describes without any new core-level registration seam.
8. ✅ Remote-store-upload opt-out: `EngineFactory#ownsRemoteSegmentDurability()`, a default-`false`
   method landed on this branch (`server/src/main/java/org/opensearch/index/engine/EngineFactory.java`)
   letting an `EngineFactory` declare it already keeps every segment durably reachable remotely by
   its own mechanism, checked as one more condition guarding `RemoteStoreRefreshListener`'s
   registration in `IndexShard#createEngineConfig`. See §18 risk #10 for the concrete waste this
   closes (confirmed via `IndicesStatsResponse` upload-byte counts, not inferred) and why a broader
   heuristic (e.g. inferring "owns durability" from a non-default `index.store.type`) wouldn't have
   worked here.

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
the cross-index-clone reference-counting case -- now actually invoked by `GcSchedulerTask`, Phase
4.5 below; this phase's own "not yet wired" framing predates that), and `BlobContainerBundleStore`/
`BlobContainerManifestStore` (real I/O against `BlobContainer`, FS-tested). An end-to-end test
(`ServerlessStorageEndToEndTests`) exercises the full write&rarr;publish&rarr;read&rarr;GC cycle
against a real filesystem blob store with no mocks. JMH microbenchmarks establish baseline
throughput for both formats. Remaining for this phase: one real cloud-object-store integration
test (currently FS/mock only, per the original scope note above).

**Phase 2 — Writer engine (WAL format, translog adapter, commit publishing, head CAS wiring,
durability-driven local-disk retention, and crash recovery via WAL replay all done).** WAL chunk
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
half of &sect;7.1.1's own milestone is now met as a result. **The crash-survival claim itself is
now proven too, closing what this paragraph originally flagged as still missing**:
`ServerlessStorageWriterFailoverIT#testWriterShardSurvivesItsNodeBeingKilled` is a real multi-node
integration test that indexes one flushed (manifest-durable) and one unflushed (WAL-only-durable)
document, kills the node actually holding the primary (found via the real routing table, not
assumed to be a fixed node), and asserts both documents are still searchable once the surviving
node picks the shard back up with a completely empty local `Store` -- proving the full chain
(allocation onto a data-less node, manifest materialization, and WAL replay past
`activationWalPosition`) under a real `kill -9`-equivalent node death, not just each piece in
isolation. **WAL replay fencing is now formally verified** (`formal/WalReplayFencing.tla`,
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
&sect;6.4 (out of scope -- needs the metadata-plane term-authority migration), not any further
unproven piece of the crash-recovery story. **The encryption-enabled half of "further broadening
the IT" is now done too**: `ServerlessStorageWriterFailoverIT#testWriterShardSurvivesItsNodeBeingKilledWithEncryptionEnabled`
runs the identical crash-recovery scenario with every node configured with the same shared AES
key, so the survivor node has to genuinely decrypt the dying node's bundles, manifest, and WAL
chunks to recover -- not merely re-read plaintext that happened to already be readable. (Attaching
per-node secure settings inside an `InternalTestCluster` needed one small test-infrastructure
fix along the way: secure settings can only ever be attached once per `Settings` object, so the
key has to be threaded in via an overridden `nodeSettings(int)` -- the one place that builds each
node's settings from scratch -- rather than pre-built and passed to `startClusterManagerOnlyNode`/
`startDataOnlyNode` directly, which throws on the second, already-secure-settings-bearing merge.)

**The multiple-shards half is done too, closing "further broadening the IT" completely.**
`ServerlessStorageWriterFailoverIT#testMultipleShardsEachIndependentlySurviveTheirOwnPrimarysNodeBeingKilled`
runs a 3-shard index across a 4-node cluster (one cluster-manager, three data nodes), kills
specifically the node hosting shard 0's primary (found via routing table lookup, not assumed),
and asserts shard 0's own two documents (one manifest-durable, one WAL-only-durable) both survive
-- checked via a routing-pinned search against shard 0 specifically, not just the index-wide
total, since a bug that silently dropped a healthy shard's data while also losing the same count
elsewhere could otherwise coincidentally pass an index-wide-only check. Shard 1's document (whose
primary was never touched) is checked the same way, proving the recovery is scoped to exactly the
shard whose primary actually died, not the whole index indiscriminately. Routing values are found
by brute-force search against the real `OperationRouting#indexShards` hashing rather than assumed,
so the test is self-verifying against whatever shard-count/hash-function combination is actually
in effect.

**Phase 3 — Reader engine (materializer, open-from-manifest, and refresh-to-newer-generation all
done; notification wiring still open in its event-driven form).** `ObjectStoreCommitMaterializer`
fetches every file a `CommitManifest` references (checksum-verified) and writes it into a target
Lucene `Directory`, producing an ordinary valid commit -- proven by opening a plain
`DirectoryReader` against a materialized manifest and running a real query, and separately by
opening a full `ReadOnlyEngine` against one via `ObjectStoreReaderEngine.open`. Deliberately reuses
`ReadOnlyEngine` rather than writing a new `Engine` subclass: once materialization has populated
the store's directory, `ReadOnlyEngine` already implements the entire read-only surface (search,
get, completion stats, refusing writes) against exactly that, so the object-store-specific work
stays confined to materialization itself. Still open: block cache unification (the lazy directory's
`FileCache` and the eager materializer's own cache are two separate caches on the same node, not
one unified view). Admission control is no longer purely coarse -- `ReaderShardAdmissionController`
now weighs the lazy directory's real `FileCache` usage against a configured byte-budget ratio, not
just a fixed shard count, and (see &sect;9/&sect;18 risk #3 above) also applies that check
per-*refresh*, deferring rather than only gating at open.
Milestone: search-only shards serve queries with no local index, freshness lag p99 < 15 s under
sustained ingest.

**Refresh-to-newer-generation: implemented, and turned out to need much less than the design first
written up here concluded.** That first pass (based on reading `ReadOnlyEngine`'s *fields*: `private
final`, set once at construction) concluded `ObjectStoreReaderEngine` would have to stop extending
`ReadOnlyEngine` entirely and become its own `Engine` subclass mirroring `NRTReplicationEngine`/
`NRTReplicationReaderManager` -- a ~150-200 line rewrite. Reading `ReadOnlyEngine`'s *methods*
(not just its fields) more carefully found a much smaller path: `getReferenceManager(SearcherScope)`
-- the accessor for the exact reader manager those fields back -- is `protected`, not `private`, and
is already exactly the same `OpenSearchReaderManager` whose own `refreshIfNeeded` already calls
`DirectoryReader#openIfChanged` (Lucene's standard incremental-reopen mechanism -- the identical
primitive `NRTReplicationReaderManager` itself is built on). `ReadOnlyEngine` only ever *disables
triggering* that reopen: `refresh(String)`/`maybeRefresh(String)` are explicit no-ops there, by that
class's own design ("we could allow refreshes if we want down the road"). So the actual fix is three
method overrides in `ObjectStoreReaderEngine` (`refresh`, `maybeRefresh`, `refreshNeeded`) that call
through to the reader manager `ReadOnlyEngine`'s constructor already built, plus a new
`pollForNewerManifest()` (scheduled alongside the pre-existing `directoryRefreshTask`, every 5 s,
well under the 15 s p99 target) that checks `ShardStateStore` for a newer manifest generation and,
when found, materializes the delta into the same `Directory` and triggers the reopen. No new
`Engine` subclass, `ReadOnlyEngine` kept as-is otherwise, no `IndexShard`-level engine swap.

**A real bug found and fixed along the way, by the same "verify by testing, not by reasoning"
discipline this whole effort has used throughout**: the first design pass claimed
`ObjectStoreCommitMaterializer.materialize` was already "additive-safe" for landing a second,
newer manifest into an already-materialized directory. A real test proved that claim wrong --
`FileAlreadyExistsException` on `_0.si`. Two successive Lucene commits from the same shard routinely
reference the *same* unchanged segment file (Lucene only ever writes a new segment for new data; it
never rewrites an untouched one just because a later commit still references it), and
`Directory#createOutput` throws if the file already exists. Fixed by having `materialize` list the
target directory once up front and skip (not re-fetch, not re-verify) any file already present --
now genuinely safe to call more than once against the same directory, which the original claim had
only asserted, not tested.

Verified end-to-end in `ObjectStoreReaderEngineTests#testEngineAdvancesToANewerManifestGenerationOncePublished`:
a real `ObjectStoreReaderEngine` opens against a first manifest (one document), a second manifest
(two documents) is published from the *same continuing* `IndexWriter` (matching how a real shard's
commit history is one continuous sequence -- two independent fresh writers was an earlier, self-caught
test-construction bug: it collided from name-reuse Lucene wouldn't actually do), the shard head is
advanced, `pollForNewerManifest` is invoked directly (no need to wait out the real poll interval in a
test), and the previously-invisible second document becomes searchable -- proving the in-place
reopen actually works, not just that it compiles. Full existing reader-engine test suite (4 tests)
and the multi-node `ServerlessStorageWriterFailoverIT` both pass unchanged, confirming this doesn't
disturb the writer-side engine or the existing manifest-materialization path.

Notification wiring remains open in its originally-intended event-driven form (true pub/sub via the
directory/gossip tier, &sect;9) -- polling, not push, is what's implemented. That is a possible
later refinement for latency/efficiency, not a correctness gap: the milestone's own freshness-lag
target is already comfortably met by a 5 s poll interval against a 15 s p99 budget.

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

**Phase 4 — Topology (4–6 weeks).** Role-separated allocation (done, see &sect;10 and
`ServerlessStorageExistingShardsAllocator`/`ReaderShardPlacementAllocationDecider`), suspended
writers, scale-to-zero/cold-start, balancer hysteresis. Milestone: idle index consumes zero
compute; first query after idle returns < 5 s p95 for a cached-manifest index.

**First increment of "suspended writers" landed: the raw idle-activity signal any suspension or
scale-to-zero decision needs.** `ObjectStoreWriterEngine` now tracks the wall-clock time of its
own last real client write (`index`/`delete` overrides record it unconditionally on entry, not
gated on success), exposed via a new `millisSinceLastActivity()` accessor. Deliberately tracks
real write calls, not this plugin's own flush/refresh/compaction/GC background scheduling (all of
which run on fixed timers independent of client activity) -- using those as the signal would make
a genuinely idle index look perpetually active. Also excludes `index.origin().isRecovery()`
operations (`LOCAL_TRANSLOG_RECOVERY`/`PEER_RECOVERY`) -- caught during review, not the first
version written: without this exclusion, a shard's own normal translog replay on open (e.g. after
a crash/restart, already exercised by every existing test via `openWriterEngine`'s
`recoverFromTranslog` call) would re-apply already-happened operations through the same `index`/
`delete` entry points and reset the idle clock, making a genuinely idle-but-just-recovered shard
misreport as freshly active to any future consumer of this signal. Unit-tested two ways against a
real `EngineTestCase`-provisioned engine:
`testMillisSinceLastActivityUpdatesOnIndexAndDecreasesUntilTheNextOne` (asserting absolute time
bounds around a real sleep rather than a relative before/after comparison -- `index()` itself is
real, variable-duration work, so comparing its own duration against a short sleep window would have
been flaky by construction) and
`testMillisSinceLastActivityIgnoresTranslogRecoveryReplayOperations` (a directly constructed
`LOCAL_TRANSLOG_RECOVERY`-origin operation must not reset the clock).

**Second increment: the signal is now actually reachable outside the engine.** New
`ShardActivityRegistry` (`writerengine` package) -- one node-shared, in-memory index of every
`ObjectStoreWriterEngine` currently open on that node, by (indexUuid, shardId), the same
"one node-shared instance" shape `sharedBundleCache`/`sharedWalChunkService` already use. Holds a
`WeakReference` to each registered engine, not a strong one: this plugin has no hook into an
engine's own `close()` to explicitly deregister, so a strong reference would leak every writer
engine a node has ever hosted, across every shard relocation, for the node's entire lifetime; a
`WeakReference` instead lets a closed engine's entry become naturally uncollectable garbage once
core drops its own last strong reference, with `millisSinceLastActivity` simply reporting "not
tracked" for an already-collected entry -- the correct answer, since a shard with no live writer
engine on this node has no idle time to report anyway.

`WriterEngineFactory` gained an eighth constructor overload accepting a `ShardActivityRegistry`
(smaller/existing constructors delegate with `null`, same telescoping-constructor shape this class
already used seven times over for every other optional feature) and registers each engine it
produces immediately after construction, inside `newReadWriteEngine`. `ServerlessStoragePlugin`
builds one `ShardActivityRegistry` eagerly (no I/O needed, same reasoning as `shardDirectory`) and
passes it into the writer branch of `getEngineFactory`.

New `ShardIdleTimeAction`/`ShardIdleTimeRequest`/`ShardIdleTimeResponse`/`TransportShardIdleTimeAction`/
`RestShardIdleTimeAction` (`writerengine/action` package, same shape as `compaction/action`) expose
it: `GET /_plugins/_serverless/storage/{index_uuid}/{shard_id}/_idle_time`. Unlike
`TransportCompactionTriggerAction` (which operates purely against the shared object store and so
can run on whichever node receives the request), this action can only ever answer from its own
receiving node's local registry -- a request that lands on a node not hosting the shard (e.g. this
plugin's own IT initially got this wrong, routing through `client()`, which can pick the
cluster-manager-only node that hosts no shards at all -- fixed by using `dataNodeClient()` instead)
gets `ShardIdleTimeResponse#notTracked()`, not an error; the caller is expected to already know
which node to ask, the same way any other single-node transport action works. Verified end-to-end
in a real cluster (`ServerlessStorageShardIdleTimeActionIT`, stable across repeated runs): a real
writer engine is tracked as soon as it's constructed (not only after a first write), idle time
resets to near-zero right after a real write, and a shard number the index doesn't have correctly
reports not tracked rather than throwing or returning a stale value.

**Third increment: the signal is now discoverable per-node, not just askable one shard at a
time.** New `NodeIdleShardsAction`/`NodeIdleShardsRequest`/`NodeIdleShardsResponse`/
`TransportNodeIdleShardsAction`/`RestNodeIdleShardsAction` (`writerengine/action` package, REST at
`GET /_plugins/_serverless/storage/_idle_shards`) answer "what's idle on this node, and by how
much" in one call, rather than requiring a caller to already know every (indexUuid, shardId) pair
to ask `ShardIdleTimeAction` about individually -- the "signal collection" half of &sect;7.3/
&sect;10's still-open autoscaling story an external controller actually needs before it can do
anything. `ShardActivityRegistry` gained `snapshotAll()`, a point-in-time snapshot of every still-
live registered engine (silently skipping any `WeakReference` already collected, same "harmless
self-correcting garbage" reasoning as `millisSinceLastActivity` -- see that class's own javadoc);
the new transport action parses each `"indexUuid/shardId"` map key back into a small `IdleShardEntry`
value type. Same node-routing contract as `ShardIdleTimeAction` (only ever answers from the
receiving node's own local registry; a caller wanting cluster-wide data calls this once per data
node) -- deliberately not a new pattern, since inventing one would create two different discovery
shapes for the same underlying signal. Verified end-to-end in a real two-index, one-data-node
cluster (`ServerlessStorageNodeIdleShardsActionIT`): both writer shards' engines are listed with
their own correct `(indexUuid, shardId)` pair (not swapped or merged), registered at construction
time rather than lazily on first write, matching `ShardIdleTimeAction`'s own already-proven
before-any-write behavior.

**Fourth increment: the search tier's own named signal -- "manifest-generation lag" -- now exists
too, closing the ingest/search pair &sect;10 explicitly names.** `ObjectStoreReaderEngine` gained
`manifestGenerationLag()`: how many manifest generations behind the latest one this engine has
observed published for its shard, backed by a new `lastObservedLatestGeneration` field updated
every poll tick. New `ReaderShardActivityRegistry` (`readerengine` package) is the reader-tier
mirror of `ShardActivityRegistry` -- same node-shared, `WeakReference`-keyed shape, for the
identical no-close-hook reason -- and `ReaderEngineFactory` gained a ninth constructor overload
registering each engine it produces, same telescoping-constructor pattern `WriterEngineFactory`
already used for its own registry. New `NodeManifestLagAction`/`TransportNodeManifestLagAction`/
`RestNodeManifestLagAction` (`readerengine/action` package, REST at `GET
/_plugins/_serverless/storage/_manifest_lag`) expose it, mirroring `NodeIdleShardsAction`'s own
per-node discovery shape exactly rather than inventing a second one.

Landing this caught a real bug in the very feature being built, via this method's own test:
`pollForNewerManifest`'s first version checked the admission-budget skip *before* reading the
shard head, so the head read and the lag observation only ever happened in the same step as
actually applying a newer generation -- meaning `manifestGenerationLag()` could only ever read
zero immediately after any completed poll tick (skipped or applied), never a real nonzero backlog,
silently defeating the entire point of exposing it as a signal. Fixed by reordering: the (cheap,
already-documented-as-cheap) shard-head read and `lastObservedLatestGeneration` update now happen
unconditionally, before the admission-budget check, so an over-budget-skipped tick still correctly
reports how far behind it now is -- the expensive part (`manifestStore.readManifest` and the
actual materialization) stays gated behind the budget check exactly as before, so this costs
nothing extra under budget pressure. `ObjectStoreReaderEngineTests`'s existing over-budget-skip
test (&sect;18 risk #3's own proof) now also asserts the lag reads `1`, not `0`, during the skip --
the assertion that would have caught this bug had it shipped instead of being caught first.
Verified end-to-end in a real three-node `RemoteStoreBaseIntegTestCase` cluster with a real
search-only reader shard (`ServerlessStorageNodeManifestLagActionIT`, same cluster shape
`ServerlessStorageSearchOnlyReplicaIT` already proved a reader engine serves real reads under):
the shard is listed with its own correct `(indexUuid, shardId)` and a lag of `0` once caught up to
a just-flushed, just-served document.

Still open: routing either signal into an actual suspension decision, a scale-to-zero controller,
or cold-start reactivation is separate future work -- both increments close the "is the signal
reachable at all" question for their own tier, not "what does the cluster do with it."

**Phase 4.5 — Compaction service, fully done.** Candidate selection, rebase protocol, real Lucene
merge, size-tiered shaping, background scheduling, a real concurrent-writer data-loss bug, real
lease acquisition/renewal, and busy-writer offload are all implemented and tested. Its hard dependency,
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

**Real lease acquisition/renewal, closing a dormant fencing bug found while investigating this**:
`ShardHead.leaseHolderNodeId`/`leaseExpiryMillis` existed from the start, but nothing ever wrote
them -- `ShardHead#withNewLease` (the only method that touched them) had zero callers anywhere in
this plugin's main code, confirmed by grep before writing a single line here. Two consequences,
one cosmetic and one a real correctness gap:

1. `CompactionSchedulerTask`'s `isLeaseHeldAt` guard was dead code in practice -- the lease was
   never held, so the "skip if an active writer holds the lease" branch could never actually fire,
   silently defeating the whole point of that check.
2. The more serious one: `ObjectStoreCommitHeadPublisher#publishCommitAsHead`'s original fencing
   check was `currentHead.primaryTerm() != primaryTerm`, fencing out *any* mismatch, higher or
   lower. Since nothing ever advanced `ShardHead.primaryTerm()` ahead of an actual publish, a
   writer activating under a legitimately bumped primary term (a real failover) would find the head
   still at the old term and be fenced out on its own very first commit -- a dormant failover bug
   that only tests happening to use term 1 throughout (matching `ShardHead.initial()`'s coincidental
   term 1) had ever avoided exercising.

Fixed with two separable changes, kept deliberately independent so acquiring/renewing a lease can
never corrupt manifest addressing:

- `ObjectStoreWriterEngine` now acquires the lease synchronously during construction (so a
  superseded activation attempt fails fast rather than accepting writes it can never publish) and
  renews it on its own schedule (`LEASE_TTL_MILLIS` = 30s, renewed every 10s -- the same
  well-inside-the-TTL rationale as the existing directory-entry refresh) via
  `ObjectStoreCommitHeadPublisher#acquireOrRenewLease`, a CAS-retry loop mirroring
  `publishCommitAsHead`'s own shape.
- `acquireOrRenewLease` deliberately never writes `primaryTerm`: only `publishCommitAsHead` may
  legitimately advance it (via the new `ShardHead#withPublishedGeneration(primaryTerm, generation)`
  overload), fixing the fencing check itself to `currentHead.primaryTerm() > primaryTerm` (fence
  only a writer genuinely superseded by evidence of a newer publish, not merely an older-but-not-yet-
  superseded term). This was the real design trap in an earlier draft of this fix: writing
  `primaryTerm` eagerly during lease acquisition, ahead of any publish, looked appealing but broke
  `readLatestManifest`'s "the head's `(primaryTerm, latestManifestGeneration)` pair always points at
  the last real manifest" invariant, since a lease-only head could point at a `(term, 0)` manifest
  that was never written -- caught by `ObjectStoreWriterEngineTests`' own WAL-replay-across-failover
  test failing with a real `NoSuchFileException`, not reasoned out in advance.
- A second-order consequence of the same root cause, also caught by tests rather than assumed: a
  brand-new shard's very first lease acquisition now put-if-absents a `ShardHead` with generation 0
  *before* anything is published, which broke every caller that had assumed "a head exists" implied
  "something was published" (`readLatestManifest`, `ReaderEngineFactory`). Both now additionally
  check `latestManifestGeneration() == 0` as the "nothing published yet" sentinel,
  matching the convention `CompactionSchedulerTask` already used for the same reason. Caught first by
  `ServerlessStorageWriterFailoverIT` failing a brand-new shard's very first allocation (not a
  failover at all) with a manifest-not-found error -- a reminder that a multi-node IT can catch a
  regression a unit-level CAS test's narrower fixture won't.

Verified: full reader/writer/compaction/shardstate unit suites (stable across 3 repeated runs with
fresh seeds), and `ServerlessStorageWriterFailoverIT` (real multi-node kill-and-recover, not a
simulation) green again after the fix above.

**Background scheduling was real in tests only, never in a running node, until now.** Investigating
the `_forcemerge`-redirect idea below turned up that `CompactionSchedulerTask` -- fully implemented
and tested since the "background scheduling done" note above was written -- was never actually
instantiated anywhere in `ServerlessStoragePlugin`'s runtime wiring (confirmed by grep: only its own
class file and test file referenced it). "A quiescent shard now gets compacted without a human or a
writer ever triggering it" was true of the class in isolation, not of the plugin a node actually
runs. Fixed: a reader shard's own engine now owns a `CompactionSchedulerTask` for its shard
(`CompactionSchedulerConfig`, the same nullable-bundle-of-config-object shape as
`PitrRetentionConfig`, threaded through `ReaderEngineFactory`/`ObjectStoreReaderEngine`, gated on a
new node setting `serverless_storage.compaction.interval` -- non-positive, the default, disables it
entirely). Deliberately *not* attached to the writer engine as well: a writer's own lease is always
held while that writer is open, so a scheduler instance living there would always see its own lease
as held and never fire -- a reader shard is the only sensible home, since it exists independently of
whether any writer is currently active, including the "writer scaled to zero" case a writer-attached
instance would structurally miss. Running redundantly across multiple reader copies of the same
shard is safe, at worst wasted work, per `CompactionRebaseExecutor`'s own already-documented
rebase-on-conflict safety argument. Verified with a new end-to-end test,
`testReaderEnginesOwnBackgroundSchedulerCompactsAQuiescentShardWithNoWriterEverActivating`: three
real, unmerged Lucene commits published with no lease ever held, a reader engine opened with a short
scheduler interval, and `assertBusy` confirming the live head advances to a new, genuinely
fewer-segment manifest with no test-only direct method call forcing the tick (unlike the
generation-advance test above, which does call its poll method directly) -- this is the first test
in this area that exercises the *scheduled* path end to end rather than a hand-invoked one.

**GC (`ManifestRetentionPolicy`/`BundleReferenceCounter`, §6.5) had the exact same gap, found while
auditing for others after fixing `CompactionSchedulerTask`'s.** Both were implemented and tested
since Phase 1 (see that phase's own status note), but nothing in `ServerlessStoragePlugin` ever
constructed anything that called them -- no manifest or bundle this plugin ever wrote was reachable
by anything that deletes it, so storage grows without bound today. Fixed the same way: a new
`GcSchedulerTask`, attached to a reader shard's engine (`GcSchedulerConfig`, gated on a new
`serverless_storage.gc.interval` node setting), same reader-not-writer-shard home as
`CompactionSchedulerTask` and for the identical reason.

**Deletion is destructive, so its safety design got more scrutiny than compaction's did.**
`ManifestRetentionPolicy#computeDeletableManifests` takes both a lease-pin set and a durable-pin
set; this task always passes an *empty* lease-pin set, deliberately never derived from
`ShardDirectory` -- that tier is explicitly documented as node-local, in-memory "hints" (one
instance per node, not a real cluster-wide gossip store yet, per `ShardDirectoryEntry`'s own
javadoc and `ServerlessStoragePlugin`'s field comment), so deriving a delete-safety signal from it
would silently miss every reader open on any *other* node -- worse than not checking at all, since
it looks like a check. The two real safety mechanisms relied on instead: a generous, purely
time-based retention window (`serverless_storage.gc.retention_window`, default 30 minutes,
comfortably longer than any legitimate reader's own manifest-generation lag, bounded by
`ObjectStoreReaderEngine`'s 5 s poll interval) and durable pins (`DurablePinRegistry`, real and
CAS-backed, correct cluster-wide regardless of which node evaluates it). Bundles are always deleted
before the manifests that stopped referencing them, never the reverse, so a crash mid-sweep leaves
at worst a still-listed but already-deletable manifest pointing at already-gone bundles (retried
correctly next sweep), never an orphaned bundle no future sweep would revisit.

**A real, previously-unexercised race was found by a genuinely concurrent test, not reasoned out in
advance**: `BlobContainerManifestStore#listManifests` lists blob names, then reads each one --
harmless when nothing ever deleted concurrently, but once a real background sweep exists, a manifest
present in the initial listing can vanish before its own read completes, throwing
`NoSuchFileException`. Fixed by treating that specific exception as "concurrently swept, correctly
omit it" inside `listManifests` itself, rather than failing the whole listing over a manifest GC had
already legitimately deleted.

Verified: two dedicated `GcSchedulerTaskTests` (a full sweep proving exactly the superseded/unpinned
manifest and its now-orphaned bundle are deleted while a durably-pinned one and the current latest
both survive; a second proving nothing is deleted while still within the retention window even
though already superseded and unpinned), plus a new end-to-end reader-engine test exercising the
real scheduled path (not a direct method call) the same way the compaction wiring test does. Full
unit suite and multi-node internal cluster tests green, stable across repeated runs with fresh seeds.

**Writer-side compaction offload, closing Phase 4.5's last open item -- smaller than it looked once
verified, not a new mechanism.** The remaining "or accepts explicit offload handoffs from busy
writers" half of §7.4's original design was framed as still-open, correctness-sensitive negotiation
work. Before designing anything, the actual remaining risk was checked directly against the code
rather than assumed: `ObjectStoreCommitHeadPublisher#publishCommitAsHead` and
`LuceneMergeCompactionPublisher#computeNewHead` both already always recompute their target
generation live from the current head inside a CAS-retry loop, never from a caller-supplied or
locally-cached value -- the exact fix that closed the data-loss bug this whole "still open" note was
originally worried about (see this phase's own history above). That protocol is already proven safe
under real concurrent, continuous contention
(`CompactionRebaseExecutorTests#testConcurrentRebaseExecutorsNeverLoseAnUpdate`, 8 threads hammering
the same shard-head CAS with a strict no-lost-updates assertion) and formally verified
(`SpecDecoupled`/`ShardHeadDecoupled.cfg`). So a writer and this task racing continuously was never
actually unsafe -- `CompactionSchedulerTask`'s blanket "skip if lease held" gate was a *conservative
efficiency choice* ("don't bother, the writer's own local merges already handle this"), not a
correctness requirement, and its own code comment read more cautious than the already-proven safety
of the protocol underneath it actually supported.

Fixed by simply removing that gate: `maybeCompact` no longer checks `isLeaseHeldAt` at all --
`CompactionPolicy#shouldCompact`'s own segment-count/size/delete-ratio thresholds are what decide
whether a shard is worth touching, lease or no lease. A healthy, actively-merging writer naturally
stays under those thresholds (its own local merges keep segment count low) and is left alone exactly
as before; a *busy* writer -- accumulating small segments faster than its own merges clear them -- is
exactly the case those thresholds are tuned to catch, which is what "accepts offload handoffs from
busy writers" meant all along. No new field on `ShardHead`, no handshake protocol, no writer-side
code change at all was needed. Verified by rewriting the test that previously asserted the old
behavior (`testDoesNotCompactWhileAnActiveWriterHoldsTheLease`) into
`testCompactsAFragmentedShardEvenWhileAnActiveWriterHoldsTheLease`, proving both that compaction now
proceeds and succeeds with an active lease held, and that the lease itself (who holds it, until when)
is left completely untouched by the publish. Full unit suite and multi-node internal cluster tests
green, stable across repeated runs with fresh seeds.

Phase 4.5 is now fully closed. Milestone (met): a quiescent 40-segment shard is compacted to
size-tiered shape with no writer ever activating, concurrently with a surprise writer re-activation,
*and* a fragmented shard is compacted while its writer stays continuously active throughout -- the
rebase protocol, a real merge, size-tiered shaping, real lease acquisition/renewal, actually-running
background scheduling, and now busy-writer offload are all implemented, wired into the plugin, and
tested together. Separately (not a Phase 4.5 correctness gap): triggering an immediate compaction
pass on demand, rather than waiting out the scheduler's interval, was considered via redirecting
core's `_forcemerge` action and rejected this session -- see §7's status note for why (it would
require building against the experimental, actively-changing `Indexer`/`IndexerFactory` SPI, and
still needs a real local `Store` underneath regardless, since `StoreRecovery` requires one ahead of
any `Indexer` construction). A plugin-owned on-demand-compaction action, reusing this scheduler's
exact tick logic directly (no core changes, no experimental SPI), remains a smaller, safer
alternative if the on-demand case turns out to matter in practice.

**Phase 4.6 — Snapshots/clones/PITR (manifest pinning and PITR retention wiring done; a first,
scoped slice of clone landed too -- see below).** Durable pins (§6.5) are implemented as `DurablePinRegistry`/`BlobContainerDurablePinRegistry`
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
String)`'s original all-matching-reason behavior is unchanged and still used for snapshots.

**Zero-copy clone: a first, scoped slice landed.** `ShardCloner.clone` (new
`clone` package) is the control-plane half: given a source shard with a published manifest and a
target index/shard with no head yet, it durably pins the exact source generation being cloned from
in the source's own `DurablePinRegistry` (pinId `"clone:<targetIndexUuid>:<targetShardId>"`,
written before anything else so a crash mid-clone only ever leaves a harmless unused pin, never a
published reference to an unprotected generation), then writes a brand-new manifest under the
target's own identity referencing the source's existing `FileReference`s verbatim -- no bundle
bytes copied, no `BundleWriter` invoked -- and CASes it onto the target's head with put-if-absent
semantics (refusing to clone onto an already-active shard). `FallbackBundleFileReader` closes the
read-path half: a cloned shard's own `BlobContainer` is scoped to its own sub-path, so an inherited
`FileReference` naming a bundle that physically lives under the source's sub-path would otherwise
be unreadable; this reader tries the shard's own bundle store first and falls back to the source's
only on `NoSuchFileException`, which also correctly handles a clone's future once it starts writing
its own commits (a mix of inherited and own-written bundles in the same manifest). Verified
end-to-end: a real `IndexWriter`-produced commit is published, cloned, and then genuinely searched
through `ObjectStoreCommitMaterializer` wired with the fallback reader -- against a target bundle
container proven empty (nothing copied) -- while a separate test confirms the exact pinned
generation matches what was cloned from.

This closes the GC-safety question `BundleReferenceCounter`'s own javadoc had flagged (see that
class's updated javadoc) without needing the cross-index manifest scan it originally called for:
pinning the exact cloned generation on the source is precise and needs zero change to
`GcSchedulerTask`.

**The pin no longer has to be permanent, either.** `CloneLineage`/`BlobContainerCloneLineageStore`
record, once, in the target's own container at clone time, exactly which source shard it was
cloned from (written after the pin but before the head CAS that makes the clone visible, so a
visible clone is always guaranteed to already have both). `ShardCloner.deleteClone` uses that
lineage to find its way back to the source's registry and release exactly the pin this clone placed
-- idempotent (a no-op if the lineage is already gone or was never there), and it touches nothing
about the target's own manifest/head, only the source-side pin and the lineage record itself.
Verified: the pin and lineage are both released together, a never-cloned shard's `deleteClone` is a
harmless no-op, and calling it twice in a row is safe.

**The lazy-directory (reader shard) read path is wired too, not just the eager one.**
`FallbackStreamReader` is `FallbackBundleFileReader`'s counterpart for
`TransferManager`/`LazyBundleIndexInput`, which resolve reads through a single {@code StreamReader}
function rather than a `BundleFileReader` object.
`ServerlessStorageLazyDirectoryFactory#resolveStreamReader` checks the shard's own
`BlobContainerCloneLineageStore` (a cheap single-blob read/miss, harmless for the overwhelming
majority of shards that were never cloned) and, only when it really is a clone, wraps the shard's
normal reader with a fallback to the clone source's own container -- so a cloned shard's
search-only reader copy, not just its eager materializer path, can resolve inherited bundles.
Verified end-to-end: a real `LazyBundleDirectory`, opened over a cloned shard's manifest and backed
by a `FallbackStreamReader`-wrapped `TransferManager`, genuinely fetches and searches source bundle
bytes it never copied.

**`deleteClone` now fires automatically on real index deletion, closing the last lifecycle gap.**
`ServerlessStoragePlugin#onIndexModule` registers an `IndexEventListener` that calls it from
`afterIndexRemoved(..., IndexRemovalReason.DELETED)` -- deliberately not the shard-level
`afterIndexShardDeleted`, which fires whenever a shard's local copy is physically wiped from a
node's disk (including plain relocation or a node simply losing a copy), not only on real index
deletion; using it would have released clone pins spuriously on every relocation.
`afterIndexRemoved` still fires once per node that had the index open, so in a multi-node cluster
more than one node can independently call `deleteClone` for the same clone -- safe only because
`deleteClone` was already idempotent under redundant/concurrent calls, an assumption that was now
actually load-bearing rather than just a nicety. Best-effort like every other background cleanup in
this plugin: a shard that was never a clone costs one cheap single-blob miss and is otherwise
untouched, and a genuine failure (the object store briefly unreachable) is swallowed rather than
blocking index deletion itself. Verified end to end in a real cluster: a synthetic source shard is
cloned into a real index's real UUID, the clone's pin and lineage are confirmed present, the real
index is deleted through the normal delete API, and both the pin and lineage are confirmed gone
without any explicit `deleteClone` call.

**Clone is now user-facing.** `POST /_plugins/_serverless/storage/_clone` (body:
`{"source": {"index_uuid", "shard_id"}, "target": {"index_uuid", "shard_id"}}`) is the first
REST/transport action this plugin has ever exposed -- `ServerlessStoragePlugin` now also implements
`ActionPlugin`. `ShardCloneAction`/`Request`/`Response` are a standard request/response triple;
`TransportShardCloneAction` needs no routing to a specific data node (`ShardCloner.clone` operates
purely against the shared object store, never node-local shard state, so whichever node receives
the request executes it directly via `client.executeLocally`) and dispatches onto
`ThreadPool.Names#GENERIC` rather than running its blob-store I/O on the transport thread. The
plugin instance itself is now also returned from `createComponents` so Guice can inject it into the
transport action, the same `blobContainerForDirectoryFactory` resolution
`ServerlessStorageLazyDirectoryFactory` and the deletion listener above already depend on. Verified
end to end over the real transport layer in a running cluster: a real published source is cloned
into a fresh target identity via `client().execute(ShardCloneAction.INSTANCE, ...)`, the pin and
lineage are confirmed present afterward, and a clone attempt against a source with no published
manifest fails loudly rather than silently acknowledging.

**No longer out of scope**: the extended GC model check this section originally anticipated as
still open is done -- see §18 risk #5's `formal/CloneGc.tla`, which models exactly the
clone-then-delete-source interleaving flagged above (§14's own "GC must be a cross-index reference
count" bullet), found a genuine TOCTOU race in `ShardCloner.clone`'s original pin-after-read
ordering, and verified the shipped pin-before-read fix sound across the complete reachable state
space.

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
   (Phase 3 gate). **Status: both halves of §7.2's target design are now implemented.** §7.2
   describes two things: (a) a byte-budget check, not just a raw shard-count cap, and (b) applying
   that check per-*refresh*, not just at open, deferring a refresh that would exceed budget while
   the shard keeps serving slightly stale data rather than failing outright. With the lazy,
   block-cache-backed remote `Directory` (§9) wired in, `ReaderShardAdmissionController` checks the
   node's shared `FileCache`'s actual `usage()` against a configured fraction of its `capacity()`
   (`serverless_storage.reader_admission.max_file_cache_usage_ratio`, default 0.9) on every
   `acquire()`, in addition to the original fixed cap on the *count* of concurrently open reader
   engines (`serverless_storage.max_concurrent_reader_shards`, disabled by default) -- either check
   can refuse an open. When no lazy directory cache is configured, this degrades back to the
   original count-only check. Verified: acquiring up to the configured count limit succeeds, the
   next acquire beyond it throws without leaking a permit from the failed attempt, closing a
   previously-opened engine frees a permit for the next open to succeed, and separately -- with a
   real `FileCache` supplied -- an acquire is refused once cache usage crosses the configured ratio
   even while the count cap still has headroom, without consuming a count permit on the rejected
   attempt.

   The per-refresh half is now closed too: `ObjectStoreReaderEngine#pollForNewerManifest` (the
   background poll that notices a writer/compactor published a newer generation and materializes
   it into the shard's directory) now checks
   `ReaderShardAdmissionController#isOverBudgetForRefresh` -- a non-throwing, count-permit-free
   variant of the open-time check, since an already-open shard's refresh doesn't need a new count
   permit, only the byte-budget question -- before pulling any new segment bytes in. An over-budget
   tick is skipped entirely (the shard keeps serving its current generation), and the very next
   poll tick retries automatically, so the shard catches up on its own once cache pressure eases
   (another shard closes, or its own cache entries get evicted), with no operator action or retry
   logic needed beyond the existing poll loop. Verified: a poll tick found while the cache is over
   budget does not materialize the newer generation (the engine's current generation is unchanged
   and the newer generation's documents remain invisible to search), and once the cache eases back
   under budget the very next tick catches up to the newer generation normally, with no other
   engine state disturbed by the skipped tick.
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
   (per-operation durability), so it never actually accumulates a multi-shard buffer today --
   this mitigation is real infrastructure for the node-level, genuinely-batched caller
   `WalChunkService`'s own class javadoc already describes as the component's actual target use.
   **The budget is now a real, configurable node setting, closing what was previously a
   construction-time-only capability with no way to actually set it**: `sharedWalChunkService` is
   now built from `serverless_storage.wal_per_shard_budget` (a `ByteSizeValue`, zero/disabled by
   default, matching every other optional-feature-off default in this plugin) instead of always
   passing the 2-argument, budget-disabled constructor overload. Verified: the default setting
   still constructs a disabled (`<= 0`) budget, and an explicit `512kb` value is threaded through
   to the constructed `WalChunkService` unchanged.
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

   **Clone's pin/GC interaction is now covered too, in a separate small model** (`formal/CloneGc.tla`,
   deliberately not folded into `ShardHead.tla` -- it checks a different mechanism, the durable
   pin/sweep interaction from §14, not the term/lease/generation state machine): given the same
   Spec/SpecBuggy-style comparison via two ghost variables checked in one TLC run, it demonstrates
   that `ShardCloner.clone`'s originally-shipped ordering -- read the source manifest (succeeds
   only while the generation is still live), *then* add the durable pin -- has a genuine TOCTOU
   race with `GcSchedulerTask`'s independent sweep: `NoCloneEverReferencesADeletedGenerationBuggy`
   is VIOLATED with a concrete 5-state counterexample (a commit supersedes the just-read
   generation, a sweep deletes it before the pin lands, and the pin that arrives moments later
   protects bundles that are already gone). The fix -- add the pin first, using the generation
   number already known from the `ShardHead` read itself, and only then read the manifest --
   is verified sound: `NoCloneEverReferencesADeletedGenerationFixed` HOLDS across the complete
   reachable state space (1,556 distinct states, search depth 17, 0 states left on the queue,
   exhaustive). **Fixed in `ShardCloner.clone` in the same session this model was written**, closing
   a real bug the pin-based clone design (see that class's own javadoc) had shipped with, not a
   hypothetical one -- caught by modeling the mechanism, not by code review or testing (the
   existing unit/integration test suite passed unchanged both before and after the fix, since
   nothing in it exercised the narrow interleaving window the model surfaces).
6. **Interplay with existing warm/composite work.** Writable warm solves an overlapping problem
   (disk smaller than data) with a different mechanism (composite local+remote directory under a
   writable engine). Decision needed: converge warm onto the reader-engine + bundle layout in
   the long term, or keep both. Recommendation: converge — two remote formats is one too many
   (§5, principle 1) — but only after Phase 3 proves the reader path.
7. **Segment replication compatibility.** Serverless mode supersedes segrep (publication *is*
   segment replication via storage). Indices can't mix modes; enforce at index-settings
   validation. **Status: implemented and tested.** `ServerlessStorageIndexSettingProvider` now
   rejects index creation outright when both `index.serverless_storage.enabled: true` and an
   explicit `index.replication.type: SEGMENT` are requested together, with a message explaining
   that manifest publication already is this shard's segment replication mechanism. The default
   (no explicit `index.replication.type`) and an explicit `DOCUMENT` are both still accepted --
   only an explicit request for core's own `SEGMENT` replication conflicts, since that is the one
   value that would configure a second, competing segment-distribution mechanism for the same
   shard. An ordinary (non-serverless-storage) index requesting `SEGMENT` replication is
   completely unaffected, as this validation only ever fires when serverless storage is enabled.
8. **Vector/kNN workloads.** HNSW graph traversal is random-access over large structures —
   nearly the worst case for a 1 MB-block LRU cache. Vector-heavy indices likely need a
   distinct cache class (pin whole graphs while a shard is query-active) and possibly
   graph-aware boot sets; sizing rules differ enough from text search that kNN gets its own
   measurement gate in Phase 3 rather than an assumption of "it's just another file."
9. **Aggregation-heavy heap costs on readers** (global ordinals, fielddata) behave differently
   when segment data is cold — building global ordinals on a cache-miss storm is a latency
   cliff. Mitigation candidates: ordinal structures included in boot sets, or eager ordinal
   builds pinned behind the admission controller (§7.2). Needs Phase 3 measurement.
10. **Reader-shard creation itself has never been exercised end to end, and depends on a core
    gate this plugin has not yet verified compatibility with.** Every piece of this plugin's
    reader-engine path (`ObjectStoreReaderEngine`, the lazy directory, admission control) is
    unit- and component-tested against a hand-built `ShardRouting` with `isSearchOnly()` forced
    `true` -- but nothing in this plugin has ever created a *real* search-only shard copy in a
    running cluster and observed which engine it actually gets. That matters because
    `isSearchOnly()` is core's existing search-replica flag (§4's own "what exists today" table),
    set via `index.number_of_search_replicas`, and `MetadataCreateIndexService#validateSearchOnlyReplicasSettings`
    currently requires `index.remote_store.enabled: true` before that setting is even accepted --
    a completely different remote-storage abstraction, with its own directory/repository model,
    that this plugin has never tested alongside its own `IndexStorePlugin.DirectoryFactory`-based
    storage. Whether the two coexist without conflict (e.g. both trying to own the shard's
    directory/repository configuration) or need an explicit compatibility decision is genuinely
    unknown -- found while investigating why no internal-cluster test exercises the lazy directory
    or reader engine against a real search-only shard, not by attempting the integration and
    hitting a failure.

    Tracing `IndexService#createShard` (not yet verified by running it) suggests the two are
    *structurally* independent, not directly conflicting: when `targetNode.isRemoteSegmentStoreNode()`,
    core builds a separate `remoteStore`/`remoteDirectory` pair (for its own remote segment-store
    upload machinery) entirely apart from this plugin's own local `directory =
    directoryFactory.newDirectory(this.indexSettings, path, routing)` call, which runs unconditionally
    either way and is what `ObjectStoreWriterEngine`/`ObjectStoreReaderEngine` actually use --
    neither engine ever references `remoteStore`. So a serverless-storage index with
    `remote_store.enabled: true` would likely *not* crash outright, but would likely run core's own
    remote segment-store upload machinery pointlessly in the background alongside this plugin's own
    manifest/bundle publication -- the same "two competing durability mechanisms for one shard"
    concern already identified and rejected for explicit `SEGMENT` replication type (§18 risk #7),
    just via a different setting this plugin's validation doesn't currently know to reject. If that
    reasoning holds, the real design question sharpens from "does it conflict" to "does
    `ServerlessStorageIndexSettingProvider` need to reject `remote_store.enabled` too, and if so,
    how does this plugin ever create a reader shard at all, since `index.number_of_search_replicas`
    currently requires it" -- a genuine unresolved tension between §18 risk #7's own reasoning and
    the reader-shard story, not just an unverified compatibility question.

    **This tension is now resolved in the negative, without writing the rejection**: confirmed by
    reading `MetadataCreateIndexService#validateSearchOnlyReplicasSettings` that core hard-requires
    `index.remote_store.enabled: true` whenever `index.number_of_search_replicas > 0` --
    unconditionally, with no serverless-storage-aware carve-out available anywhere in that check.
    A rejection symmetric with risk #7's `SEGMENT`-replication check (tried and reverted during
    this pass, confirmed via a full `ServerlessStorageIndexSettingProviderTests` run) would not
    merely be defensive-but-unnecessary here: it would make it *impossible* to ever create a
    reader shard through `index.number_of_search_replicas` on a serverless-storage index at all,
    which is this plugin's only entry point into that path today. So unlike explicit `SEGMENT`
    replication (risk #7, a real conflicting mechanism with no legitimate use this plugin needs)
    and unlike a hypothetical unconditional `remote_store.enabled` rejection, `remote_store.enabled`
    must stay accepted -- the "two competing durability mechanisms" concern is real (core's remote
    segment-store upload machinery would likely still run pointlessly in the background per the
    `IndexService#createShard` tracing above) but is the lesser cost against a plugin that cannot
    create reader shards at all. Still needs the real experiment described below to confirm the
    background-upload-is-merely-wasteful hypothesis and to observe what engine/directory the
    resulting search-only shard copy actually receives -- but the "should this plugin add a
    rejection" question itself is closed: it must not.

    **The real experiment was attempted, and it surfaced a second, more fundamental gap than the
    one it set out to check.** Ran a real three-node cluster (`RemoteStoreBaseIntegTestCase` +
    `ServerlessStoragePlugin`, one writer node, one dedicated search-only node, an index with
    `serverless_storage.enabled: true`, `number_of_replicas: 0`, `number_of_search_replicas: 1`)
    and hit a real bug on the very first attempt to create the index: the pre-existing risk #7
    `SEGMENT`-replication rejection fired unconditionally, blocking index creation outright, since
    core's own prerequisite chain (`number_of_search_replicas` requires `remote_store.enabled`,
    which requires explicit `replication.type: SEGMENT`) has no way to ask for a search-only
    replica without also requesting `SEGMENT`. **Fixed**: `ServerlessStorageIndexSettingProvider`'s
    `SEGMENT`-replication rejection is now scoped to `index.number_of_replicas > 0` only -- a
    search-only shard copy never goes through core's peer-to-peer segment-copy protocol regardless
    of this setting (routed via `RecoverySource.EmptyStoreRecoverySource` purely off
    `ShardRouting#isSearchOnly()`), so `SEGMENT` with zero writer replicas has no real conflicting
    mechanism to reject. Covered by a new unit test
    (`testAllowsExplicitSegmentReplicationWithZeroWriterReplicas`).

    With that fixed, index creation succeeded, but the search-only shard copy itself then sat
    `UNASSIGNED` against a plain `startSearchOnlyNode()` node. Instrumented
    `ServerlessStorageExistingShardsAllocator#allocateUnassigned` with temporary debug logging
    (removed before landing) to check whether it was even being invoked -- it was, on every reroute,
    but `firstDeciderApprovedNode` returned `null` every time (cluster health's own
    `allocation_status[no_attempt]` field turned out to be a stale/unchanged display value, not a
    reliable signal that the allocator was never called). Root cause: `startSearchOnlyNode()` gives
    a node the `search` role but not `node.attr.serverless_storage_reader: "true"`, and
    `ReaderShardPlacementAllocationDecider` was doing exactly what &sect;10 designed it to do --
    refusing to place a reader shard copy on any node lacking that explicit attribute, decider
    rejections included. **Not a bug** -- an easy-to-miss operational prerequisite (a reader node
    needs both the `search` role *and* the attribute) that had never been exercised end-to-end
    before this experiment. Setting the attribute on the search-only node during startup made the
    shard allocate and start immediately.

    One more real finding once the shard was placed: `client().admin().indices().prepareRefresh()`
    right after a writer-side flush was not enough to make the just-published manifest visible on
    the reader -- `ObjectStoreReaderEngine#refresh` only reopens against whatever this engine has
    already materialized locally; picking up a *newer* manifest is `pollForNewerManifest`'s job,
    which only runs on its own background schedule (`DIRECTORY_ENTRY_TTL_MILLIS / 3` = 20s), not
    synchronously off a client refresh call. The IT (below) polls with `assertBusy` over that window
    rather than asserting once.

    **`ServerlessStorageSearchOnlyReplicaIT` is now committed and green** (five consecutive local
    runs, no flakes) -- the end-to-end proof risk #10 asked for: a real serverless-storage index
    with a real search-only replica, on a real three-node `RemoteStoreBaseIntegTestCase` cluster,
    served by this plugin's own reader engine and lazy materialization path, not a hand-built
    `ShardRouting` in a unit test.

    **The last open half of risk #10 was confirmed, then fixed with a small, targeted core seam.**
    The IT initially read the primary shard's own `IndicesStatsResponse` segment stats
    (`SegmentsStats#getRemoteSegmentStats().getUploadBytesStarted()`) after a real flush, and it was
    reliably `> 0` (thousands of bytes, not a rounding artifact) across every run: core's remote
    segment-store upload machinery genuinely was uploading real segment bytes to the remote-store
    repository in the background on a serverless-storage index's writer shard -- confirmed wasted
    work, not a correctness bug (nothing in this plugin's reader/GC/retention paths ever read from
    that repository, so nothing broke), but a real, measured cost/efficiency gap for a plugin whose
    entire premise is object-store cost control.

    Root cause: core's remote segment-store upload path (`RemoteStoreRefreshListener`, wired into
    `IndexShard#createEngineConfig`) engages purely off `index.remote_store.enabled`, independent of
    `IndexModule.INDEX_STORE_TYPE_SETTING` -- this plugin's `DirectoryFactory` owns the shard's
    actual on-disk directory, but had no seam to also suppress core's separate remote-store upload
    listener. Rather than reach for a broad or heuristic fix (e.g. inferring "owns durability" from
    a non-default store type -- which wouldn't even have caught this case, since the *writer* shard
    never sets `index.store.type` at all, only the *reader* path's lazy directory does), added a new
    narrow, explicit, opt-in `EngineFactory` method: `EngineFactory#ownsRemoteSegmentDurability()`
    (`server/src/main/java/org/opensearch/index/engine/EngineFactory.java`), defaulting to `false`
    (zero behavior change for every existing `EngineFactory`, including every other plugin's). Wired
    into `IndexShard#createEngineConfig` as one more condition (`&& !engineFactoryOwnsRemoteSegmentDurability()`)
    guarding `RemoteStoreRefreshListener`'s registration -- surgically scoped to that one call site,
    not a redefinition of the shared private `isRemoteStoreEnabled()` helper, which has an unrelated
    second call site (`activateWithPrimaryContext`'s primary-relocation checkpoint logic) this change
    must not touch. `WriterEngineFactory` now overrides the new method to return `true`
    unconditionally -- safe, since `ServerlessStoragePlugin#getEngineFactory` only ever returns a
    `WriterEngineFactory` for an index that already has `serverless_storage.enabled: true`, and that
    engine's own manifest publication already is this shard's durable remote copy. Re-ran
    `ServerlessStorageSearchOnlyReplicaIT` after the fix and it now asserts the upload count is
    exactly `0`, not merely "no exception" -- proof the seam takes effect end-to-end, not just that
    it compiles. `server:test`'s `IndexShardTests` and `RemoteStoreRefreshListenerTests` (the two
    core test classes most directly exercising this code path) both still pass unmodified,
    confirming this is additive and doesn't change behavior for indices that don't opt in.

## 19. Summary

The strategy is: keep Lucene and the mechanical engine internals; replace *where bytes live and
how visibility propagates*. Writers become "Lucene + group-committed WAL + bundle publication";
readers become "manifest subscriber + block cache + read-only directory view"; the object store
plus a small manifest protocol replaces peer replication, peer recovery, and shard relocation.
The cluster stops being a set of data owners and becomes a pool of interchangeable caches with
roles — which is the property that makes true elasticity, role-based scaling, and scale-to-zero
possible.
