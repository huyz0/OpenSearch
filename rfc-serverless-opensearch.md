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

Integration point: `TranslogFactory` is already resolved per shard via
`BiFunction<IndexSettings, ShardRouting, TranslogFactory>` — the WAL-backed translog adapter
slots in there without core changes. Local translog fsync is disabled for writer shards
(durability is delegated), which also removes a disk-bandwidth consumer from the hot path.

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
2. ✅ `InternalEngine` extensibility: protected merge scheduler/deletion policy/reader managers,
   overridable `newMergeScheduler()`.
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
subsequent writes are then also rejected. Still open: local Lucene commit/translog retention policy
once object storage becomes the durability source of truth (right now local files are kept exactly
as a normal `InternalEngine` would keep them, so this is belt-and-suspenders durability today rather
than the disk-bandwidth savings &sect;5/&sect;6 target). Milestone (not yet met): an index whose
durability is object-store-only survives `kill -9` of its node with zero data loss (durable ack
mode), recovering by manifest+WAL replay.

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

**Phase 4.5 — Compaction service (candidate selection + rebase protocol done; not wired to a
real merge).** Its hard dependency, shard-head CAS (metadata-plane RFC Phase 2.5), is done and
now generically `BlobContainer`-backed (`BlobContainerShardStateStore`). On top of it,
`CompactionPolicy` (candidate selection from segment-count/size/delete-ratio metrics, matching
&sect;7.4's "readable without opening the shard") and `CompactionRebaseExecutor` (the
rebase-on-CAS-conflict publication loop itself) are implemented and tested, including the
rebase protocol's two defining properties under real concurrency: a compactor that gets raced by
a concurrent writer publication rebases and eventually succeeds rather than corrupting or losing
the writer's update, and many concurrent publishers hammering the same shard never lose an
update between them. What's still open: wiring an actual Lucene merge into
`CompactionPublisher` (today it's a caller-supplied function, exercised in tests with synthetic
head transitions, not a real segment merge), `_forcemerge` redefinition, and the compactor
role/lease-offload negotiation with an active writer. Milestone (not yet met): a quiescent
40-segment shard is compacted to size-tiered shape with no writer ever activating, concurrently
with a surprise writer re-activation (rebase test — the *protocol* for this is now tested; the
*real merge* is not).

**Phase 4.6 — Snapshots/clones/PITR (manifest pinning done; clone/PITR policy not started).**
Durable pins (§6.5) are implemented as `DurablePinRegistry`/`BlobContainerDurablePinRegistry` —
same generic `BlobContainer.compareAndSwapRegister`-backed pattern as the shard-head store, so a
snapshot pin survives independently of any node's lease. `PinRecord` names *why* a generation is
pinned (snapshot id, `"pitr"`, ...) so independent retention reasons on one shard never clobber
each other; add/remove are idempotent, CAS-retry-based, and tested under concurrent pinners on
the same shard. An integration test confirms the full lifecycle end to end against
`ManifestRetentionPolicy`: pin a generation via the real registry, verify it survives a GC sweep
that would otherwise delete it, remove the pin, verify the generation becomes deletable again.
Not yet done: zero-copy clone with cross-index bundle refcounts, PITR retention policy wiring
(the registry is the building block; nothing yet decides *when* to add a `"pitr"` pin), and the
extended GC model check this phase is gated on.

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
   (Phase 3 gate).
4. **WAL multiplexing fairness.** One node-level WAL means one noisy shard can delay acks for
   others. Mitigation: per-shard budget within a chunk, overflow to dedicated chunks.
5. **Coordination-free GC** is the subtlest correctness surface. The lease/TTL design must be
   model-checked (TLA+ or equivalent) before Phase 1 completes — this is the one component
   where a design bug destroys data. The model must cover cross-index bundle references from
   clones (§14) and compactor/writer publication races (§7.4), not just the single-writer case.
   **Status**: a first TLA+ model of the core primitive underneath all of this —
   `ShardHead`'s term/lease/generation state machine and its version-CAS guard — is written at
   `plugins/serverless-storage/formal/ShardHead.tla` (with a companion `ShardHead.cfg`), stating
   five properties: at most one valid lease holder at a time, term never decreases, generation
   never regresses within a term, a new term always resets generation to 0, and — the one that
   actually matters — a `Publish` action can only ever succeed when its actor is the *real*
   current holder, not merely believes itself to be. This is written but **not yet
   machine-checked with TLC** in this repo (tooling wasn't set up in this session); running
   `java -jar tla2tools.jar -config ShardHead.cfg ShardHead.tla` is the immediate next step
   before trusting it. It also does not yet cover clones/cross-index references or the
   compactor-vs-writer rebase race (§7.4) called out above — those need their own actions added
   to the model, not just this base activation/publish machine.
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
