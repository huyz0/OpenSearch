# RFC: A Metadata Plane for 100M+ Shards

- Status: DRAFT
- Companion to: `rfc-serverless-opensearch.md` (data plane: object-store-native engines)
- Question answered here: how does the *control/metadata* plane scale to hundreds of millions
  of shards, do we need an external state system, and do we still need a cluster-manager node?

---

## 1. Why Today's Design Caps Out Around 10⁵ Shards

Every scaling limit in the current control plane comes from one design decision: **cluster state
is a single logical document, fully materialized on every node, with all mutations serialized
through one elected cluster-manager.**

Concretely, per shard the cluster state carries routing entries, in-sync allocation IDs, and
recovery bookkeeping. Even at an optimistic ~200 bytes/shard amortized:

| Shards | State size (every node holds it) | Reality |
|---|---|---|
| 10⁵ | ~20 MB | today's practical ceiling; diffs and remote publication strain |
| 10⁷ | ~2 GB | heap-hostile on every node; publication and diff computation infeasible |
| 10⁸ | ~20 GB | not a document anymore; it's a database |

And size is the *lesser* problem. The serialization bottleneck is worse: every shard assignment,
every failover, every index creation is one task on one elected node's single-threaded state
update loop. At 10⁸ shards, even a 0.001%/minute churn rate is ~17 events/second *forever*,
each requiring a full cluster-state publish round. The elected-master-as-serializer model does
not bend at this scale; it breaks.

So the goal decomposes into two sub-problems:

1. stop materializing global state anywhere;
2. stop serializing all decisions through one process.

## 2. Study: How Systems at This Scale Actually Do It

We studied the two most instructive public designs: S3's index layer (hundreds of trillions of
objects) and DynamoDB's metadata/routing layer (from the USENIX ATC'22 paper). Patterns, not
mechanisms, are what transfer.

### 2.1 S3's index: the metadata *is* a partitioned database

- S3's keymap — the mapping from object key to physical location — is itself a
  **range-partitioned, replicated index**. No node holds the global map; lookups route through
  a partition tree.
- Partitions split **based on observed heat** (sustained request rates) and size, automatically
  and continuously. The keyspace layout is an operational artifact, not a configuration.
- The service is internally **cell-based**: independent instances of the whole stack with
  bounded blast radius; a cell failure strands a slice of the namespace, never the service.
- Critically for us: **an object that is never accessed costs the request-routing tier
  nothing.** Cost concentrates on the active set.

### 2.2 DynamoDB: authoritative store vs. cache tier, and constant-load caching

The DynamoDB paper describes the exact failure mode we must avoid and its fix:

- Originally request routers cached the full partition map per table. Cold routers caused
  metadata-service thundering herds (a cache-hit-rate cliff turned into a metastable outage
  risk).
- The fix, **MemDS**: a horizontally scaled, in-memory, replicated metadata fleet holding the
  full routing data behind a Perkle (Patricia+Merkle) index, sized to absorb *the entire
  request rate as if caches didn't exist*. Router caches remain, but a router **refreshes
  asynchronously even on cache hits**, so MemDS sees constant, predictable load regardless of
  cache hit rate. Caches accelerate; they never load-bear for availability. They call this
  *static stability*.
- Consensus is **per partition** (a Multi-Paxos leader per replication group), never global.
  The control plane (partition splits, moves, healing) is a set of asynchronous background
  services operating on small local views — there is no "master of DynamoDB."

### 2.3 The primitive that changed the answer: object-store CAS

All three major object stores now expose **conditional writes**:

- S3: `If-None-Match` (put-if-absent, Aug 2024) and `If-Match` ETag compare-and-swap on
  `PutObject`/`CompleteMultipartUpload` (Nov 2024);
- GCS: generation-number preconditions (long-standing);
- Azure Blob: ETag `If-Match` (long-standing).

Compare-and-swap is a universal consensus primitive. This means **per-shard linearizable state
transitions (term bumps, writer leases, manifest pointers) can be arbitrated by the object
store itself** — no external coordination system, no elected master, and it scales exactly as
far as the object store does, which is the same trust we already place in it for data.

### 2.4 Precedent for the external-store alternative

Snowflake runs its entire metadata plane (micro-partition maps, stats, catalogs, even lock
queues) on FoundationDB — a strictly-serializable distributed KV store — with stateless
services above it, since 2014. This is the proven "external state system" architecture and the
credible fallback if we need richer transactions than CAS-per-object provides.

## 3. The Domain Insight That Makes 10⁸ Feasible

The data-plane RFC gives us the property S3 exploits: **a quiescent shard costs zero
control-plane state.** A shard with no active writer and no active readers is *just objects in
the object store* — its manifests, bundles, and WAL tail fully describe it. Nothing about it
needs to exist in memory anywhere.

So "100M shards" is really "100M shards, of which the *active set* — those with a live writer
or serving readers — is 1–5%." The control plane is sized for the active set and the
activation rate, not for the total. This is the difference between an impossible problem
(materialize 10⁸ routing entries everywhere) and a conventional one (run a directory service
for ~10⁶ active entries with an on-demand activation path).

## 4. Decompose "Cluster State" Into Four Data Classes

The monolithic state document conflates four kinds of data with wildly different scale,
consistency, and churn profiles. Each gets its own home:

| Class | Cardinality | Consistency need | Home |
|---|---|---|---|
| A. Node membership & health | 10²–10⁴ nodes | eventually consistent, fast failure detection | gossip (SWIM-style) + small consensus group for disputes |
| B. Cluster-wide config & policies | ~KBs, rare writes | linearizable | small consensus group ("control cell") |
| C. Per-index metadata (mappings, settings) | 10⁶–10⁷ indices | read-heavy, versioned | object store (one metadata object per index), cached on demand by nodes that host that index |
| D. Shard state: term, writer lease, latest manifest, active locations | 10⁸ shards | **split** — see below | authoritative: object store CAS; discoverable: directory tier |
| | | | |

The critical move is splitting class D into **truth** and **hints**:

- **Truth** (correctness-bearing, linearizable, per-shard): current primary term, writer lease,
  latest manifest pointer. Lives in a small per-shard *shard-head object* in the object store,
  mutated only via CAS. A writer takes over a shard by CAS-ing the head (bump term, set lease);
  losers of the race see a precondition failure. The data plane's term fencing
  (companion RFC §6.3) already tolerates stale actors, so this is belt *and* suspenders.
  Heads are created **lazily** with put-if-absent on a shard's *first ever* activation —
  creating a 10K-shard index writes one metadata object, not 10K heads; a shard that is never
  written never has a head.
- **Hints** (performance-bearing, eventually consistent): "which nodes currently serve shard X"
  routing entries. Live in the **directory tier** (§5). If a hint is stale, a request lands on
  a node that no longer serves the shard, gets a redirect/miss, and the entry refreshes —
  degraded latency, never incorrectness.

### 4.1 Mapping updates without a serializer

Dynamic mapping updates are the one write path that today *requires* the elected
cluster-manager: a document introducing a new field blocks until the manager has serialized the
mapping change into cluster state. With class-C metadata in per-index objects, the flow becomes:

1. A writer shard hits an unmapped field. It reads the index metadata object, computes the
   merged mapping, and **CAS-writes it back** (version++). On CAS failure — another shard of
   the same index raced it — re-read, re-merge, retry. Mapping merges are deterministic and
   commutative for the additive case (two shards discovering different new fields converge in
   two rounds); genuine conflicts (same field, incompatible types) fail *both* writers'
   documents, which is today's behavior too.
2. The winning mapping version is **stamped into every subsequent commit manifest**
   (companion RFC §6.3). This is the propagation mechanism — there is no push.
3. A reader applying a manifest with mapping version *v* first ensures it has loaded index
   metadata ≥ *v* (one conditional GET on the metadata object, usually cache-hit). Readers
   therefore can never serve a segment whose fields they cannot interpret.

Two consequences worth stating: mapping-update throughput per index is bounded by CAS
contention (fine — mapping churn at high rate is pathological today too, and per-*index*
contention does not affect neighbors); and mapping *deletion*/breaking changes remain
disallowed, exactly as today, which is what makes the merge commutative.

## 5. The Directory Tier

A horizontally scalable, soft-state lookup service — our MemDS analog — mapping
`(index-uuid, shard) → {writer node, reader nodes, generation hint}` for **active shards only**:

- **Range-partitioned** by key with heat-based splitting (S3 keymap lesson): a tenant creating
  10M indices with a common prefix must not hotspot one directory partition.
- **Replicated in memory, rebuildable**: every entry is reconstructible from gossip
  (nodes advertise what they host) plus object-store listing (authoritative shard-heads).
  Directory loss is a latency event, not a data event.
- **Constant-load caching** (DynamoDB lesson): coordinators cache directory entries but refresh
  asynchronously even on hits — directory load is a function of request rate, not hit rate, so
  a mass cache flush cannot metastabilize the tier.
- **Write path**: nodes report placement changes to the directory (async, batched); the
  directory is *told about* decisions, it does not *make* them.

Sizing sanity check: 2M active shards × ~150 B/entry ≈ 300 MB — trivially held in memory,
replicated ×3, on a handful of directory nodes; scale linearly with the active set.

### 5.1 Prune-before-activate: making 10⁸ shards *searchable*, not just storable

Everything above scales the control plane; it does nothing for a query. A search against
`logs-*` matching 10K quiescent shards would otherwise trigger 10K activations — the control
plane survives, but the query latency and the bill do not. The missing piece is the ability to
say *"this shard cannot match"* without touching the shard:

- Commit manifests already carry **pruning statistics** (doc count, time ranges, configurable
  field min/max — companion RFC §6.3). But reading 10K manifests to plan one query is itself
  an activation storm, so statistics are aggregated one level up:
- Each index maintains a **pruning digest**: one bounded object (partitioned into
  `digest-<shard-range>` objects for very wide indices) summarizing per-shard stats
  (shard → time range, key field ranges, doc count).
- **Digest writes are rare by design, so it is not a hot key.** The digest only load-bears for
  *quiescent* shards — active shards are always consulted live, so their digest entries may be
  arbitrarily stale. Therefore the only *required* digest update is at **suspension** (the
  writer's final publication folds in final stats before the shard goes quiescent); intermediate
  updates are optional, batched per node, and best-effort. Update rate scales with the
  suspension rate, not the publication rate — no CAS contention under heavy ingest.
- **The invariant that makes pruning safe**: a shard only becomes quiescent *after* its final
  publication, and that final publication updates the digest (companion RFC §7.3). So a stale
  digest can only cause a false *activation* (wasted work), never a false *skip* (wrong
  results).
- The coordinator's plan for a wide query becomes: *(digest survivors among quiescent shards)*
  ∪ *(all directory-active shards of the index — an index-prefix range scan on the directory)*,
  with the cold half subject to an **activation budget** (per-query cap, default on the order
  of 10² concurrent cold activations; beyond it, the query either proceeds in waves or fails
  fast with an explicit "too broad" error the user can scope down — a *designed* failure
  instead of an accidental stampede).
- Time-partitioned patterns get this almost for free: `logs-*` over the last 15 minutes prunes
  to the handful of shards whose time ranges overlap, which is precisely the workload shape
  that motivates 10⁸ shards in the first place.

Digest sizing: 100M shards × ~100 B ≈ 10 GB *if one index had all of them* — but digests are
per-index, so a 10K-shard index digest is ~1 MB: one GET per query template, cached with
constant-load refresh like everything else in this tier.

## 6. Placement Without a Master

Who decides where a writer or reader runs? Nobody global:

- **Writer activation** (first write to a quiescent shard, or failover): the coordinator picks
  a candidate ingest node from its (gossip-derived) view of pool load; the candidate **acquires
  the shard via CAS on the shard-head** (term++, lease with TTL, its node id). CAS makes
  concurrent activation attempts safe — one winner; losers *read the winning head and route to
  the winner* (they must not retry activation elsewhere, which would just steal the shard back).
  Placement quality comes from load-aware candidate selection; placement *safety* comes from CAS.
- **Reader activation**: even simpler — readers are interchangeable caches; any search node can
  start serving any shard by reading its head + manifest. No coordination at all; the directory
  just learns about it.
- **Leases and failover**: one lease object per node (TTL'd, heartbeat-renewed), with per-shard
  heads referencing it. This is the **single lease mechanism for the whole system**: the same
  per-node object carries writer/compactor ownership claims *and* reader manifest pins for GC
  (companion RFC §6.5) — one liveness truth, so garbage collection and activation can never
  disagree about which nodes are alive. A dead writer's lease expires; the next write attempt
  re-activates through the same CAS path. Failure detection latency is tunable per index
  (lease TTL) rather than global.
- **Background reconcilers** (DynamoDB AutoAdmin lesson): fleets of stateless workers scan for
  imbalance, expired leases, orphaned activations, GC eligibility — each acting through the
  same CAS protocol as everyone else. Control logic becomes *many small idempotent loops*
  instead of one privileged serializer.

## 7. So: External State System, or Build It In?

Three candidate architectures for the class-D truth layer:

| | A. External strongly-consistent KV (FoundationDB-style) | B. Internal consensus cells (Raft groups over shard-space partitions) | C. Object-store CAS + soft directory (recommended) |
|---|---|---|---|
| New stateful infra | Yes — a distributed DB to operate | Yes — many Raft groups to manage/rebalance | **No** |
| Scale ceiling | High (proven at Snowflake scale) | High, but group management is its own metadata problem (recursive!) | Object-store scale |
| Latency of shard state change | ~ms | ~ms | ~10–100 ms (CAS PUT) |
| Rich transactions (multi-shard atomicity) | Yes | Within a group | No — single-object CAS only |
| Failure coupling | New dependency in every critical path | Self-hosted complexity | Already fate-shared with data (object store down = system down anyway) |
| Consistency with design thesis | Violates "object store is the only stateful system" | Partially | **Aligned** |

Recommendation: **C**, because the write rate to truth state is intrinsically low — term bumps
and lease acquisitions happen at activation/failover, not per request — so CAS latency is
irrelevant, and it adds zero operational surface. The two honest limitations, with mitigations:

1. *No multi-key transactions.* Needed rarely (e.g., atomic resharding cutover). Solve with the
   standard intent-object pattern: write an intent, CAS the affected heads to point at it,
   roll forward idempotently. Acceptable because rare.
2. *Lease heartbeat cost.* 10⁶ active writers heartbeating individually would be ~10⁶ PUTs/TTL.
   Batch to one lease object per *node* (10³–10⁴ PUTs/TTL) with shard-heads referencing the
   node lease. Fine.

Adopt **A (FoundationDB or equivalent) as the designed escape hatch**: the truth layer hides
behind a narrow `ShardStateStore` interface (`compareAndSet(head)`, `get(head)`,
`renewLease(batch)`). If CAS-on-object-store proves too slow for some future feature (e.g.,
sub-second failover SLAs demand ms-level lease churn), swap the implementation without touching
the architecture. Do not adopt B: running consensus groups *about* shard placement recreates
the problem it solves, one level down.

**Implementation update:** rather than a bespoke shard-head store, the CAS primitive was pushed
one layer down into the object-store abstraction itself. `BlobContainer` gained two default
methods, `readRegister`/`compareAndSwapRegister` (generation-versioned register semantics,
defaulting to `UnsupportedOperationException` so no existing implementer breaks), with a real
implementation for `FsBlobContainer`. `ShardStateStore` is now `BlobContainerShardStateStore`, a
thin translation layer over that primitive with **no storage-backend code of its own** — it
works against any `BlobContainer` that implements the primitive. This means S3/GCS/Azure support
is now "implement `compareAndSwapRegister` once per repository plugin using that provider's
native conditional write," not "build a whole new shard-head store per backend" — the reuse the
original design called for, achieved by generalizing the primitive rather than duplicating a
CAS implementation inside this module.

## 8. Do We Still Need a Cluster-Manager Node?

Break down what the elected cluster-manager does today, and where each duty goes:

| Today's cluster-manager duty | Where it goes |
|---|---|
| Hold & publish the routing table | Dissolved: directory tier (hints) + shard-heads (truth) |
| Serialize shard allocation decisions | Dissolved: CAS-arbitrated activation + background reconcilers |
| Detect node failure (pings) & react (reallocate) | Gossip detection; reaction becomes lazy (lease expiry + next-touch activation) and background (reconcilers) |
| Serialize index creation/deletion, mapping updates | Per-index metadata objects with CAS versioning; no global ordering needed across *different* indices |
| Cluster-wide settings, ILM-ish policies | **Control cell** — retained |
| Membership arbitration (who is in the cluster) | Gossip + **control cell** as tie-breaker |

So the honest answer: **no global master, but not "no consensus."** We retain a **control
cell** — a 3–5 node consensus group (the existing coordination machinery, drastically
shrunken) that owns classes A-disputes and B only: cluster config, membership arbitration,
directory-tier partition assignments. Its state is kilobytes-to-megabytes and its write rate is
human-scale, so it will never be the bottleneck — and, exactly as in DynamoDB and S3, it is
**not on the data path and not on the shard-activation path**. Every per-shard decision
(10⁸-scale) is arbitrated by object-store CAS; every routing lookup (request-rate-scale) is
served by the soft directory; the control cell could be down for minutes and ingest/search on
active shards would not notice.

This also simplifies deployment tiers: `control` (tiny), `directory` (small, memory-heavy),
`ingest-compute` and `search-compute` (elastic pools) — all stateless-restartable except the
control cell's small replicated log, which can itself checkpoint to the object store.

## 9. Activation Path (Cold Shard → Serving), End to End

1. Request for index I arrives at a coordinator. For a wide read, the pruning digest (§5.1)
   runs first — only surviving shards proceed, under the activation budget. Directory lookup
   for shard s misses (quiescent).
2. Coordinator reads shard-head object (one GET — or its negative cache). A 404 means the
   shard was *never* activated: reads answer empty immediately with no activation at all
   (a pleasant consequence of lazy heads — pre-provisioned but never-used shards cost nothing
   even to query); writes create the head via put-if-absent and proceed.
3. Write path: pick ingest node → CAS shard-head (term++, lease) → open writer from latest
   manifest + WAL replay (companion RFC §7.1) → report to directory. Budget: low seconds,
   hidden behind the indexing queue.
4. Read path: pick search node → read head + manifest → open reader, boot-set prefetch
   (companion RFC §7.3) → report to directory. Budget: sub-second to first (cold) results for
   metadata; data blocks stream on demand.
5. Idle timers reverse both: final publish, lease release, directory entry drop. The shard
   returns to costing nothing.

## 10. Compatibility Surface

- `_cluster/state`, `_cat/shards`, `_cluster/health` assume an enumerable global view. In
  serverless mode they become **paginated, directory-backed virtual views** scoped to active
  shards, with totals sourced from index metadata rather than enumeration. This lands on the
  API-gating mechanism from the companion RFC (§11).
- Index creation no longer round-trips a global state update; it writes the index metadata
  object and returns. "Green health" semantics change: an index is *ready* when its metadata
  exists (shards activate on demand), which is philosophically different and must be documented.
- Shard counts per index stop being precious. Today's guidance ("avoid oversharding") exists
  because shards cost cluster-state and heap everywhere; when a shard costs ~nothing at rest,
  per-tenant/per-day/per-stream sharding patterns become legitimate — which is precisely the
  workload shape that produces 10⁸ shards.

## 11. Napkin Math at Target Scale

100M shards, 2% active (2M), 10K nodes, 100K activations/minute peak:

- Shard-heads: at most 100M tiny objects (created lazily, so in practice only ever-activated
  shards have one) — noise for an object store; ~1.7K CAS/s at peak activation — noise for
  object-store request rates.
- Directory: 2M entries ≈ 300 MB ×3 replicas; lookup rate = coordinator request rate ×
  (1 − cache hit) + constant refresh — tens of directory nodes, DynamoDB-style.
- Gossip: 10K nodes is within SWIM's proven envelope.
- Listing 100M shard-heads for full reconciliation: ~100K LIST pages — a background sweep of
  minutes-to-hours, acceptable for an audit loop that isn't in any latency path.
- Control cell: untouched by all of the above.

## 12. Phasing (Amends Companion RFC §16)

- **Phase 2.5 — Shard-head & lease protocol (interface + FS reference implementation done).**
  `ShardStateStore`/`ShardHead`/`VersionedShardHead`/`CasResult` plus `FsShardStateStore` are
  implemented and tested in `modules/serverless-storage` (same branch as the companion RFC's
  Phase 1). Threading-level correctness is verified directly rather than only argued: a
  many-thread activation race resolves to exactly one winner, and concurrent publishers racing
  with retry (the compactor rebase protocol's core property) never lose an update — both run
  repeatedly in CI-style repetition to rule out flakiness, standing in for the
  TLA+/lightweight-formal model this phase still owes for the full term/lease/activation state
  machine (a from-scratch model, not yet done — the tests cover the two properties that matter
  most operationally, not the full state space). Still open: an object-store-backed
  implementation (S3 If-Match / GCS generation preconditions / Azure ETag If-Match) behind the
  same interface, and the dual-activation chaos-test gate. Bridge note: until an engine actually
  consumes it, primary-term authority in the data plane remains classic cluster coordination
  (companion RFC §7.1); wiring `ShardStateStore` in as that authority, and unblocking the
  compaction service (companion RFC Phase 4.5) that depends on its head pointer, are both still
  open.
- **Phase 4 (extended) — Directory tier + gossip membership.** Directory service, constant-load
  client caching, rebuild-from-listing; SWIM membership behind the existing node-join API.
- **Phase 5.5 — Control-cell diet.** Strip routing table and allocation from the coordination
  path for serverless indices; virtualized compat APIs; scale test: 10M shards (1 rack), then
  10⁸ (simulated heads + real active set).

## 13. Risks

1. **Metastability of the directory tier** — the exact failure DynamoDB's paper warns about.
   Mitigation is baked in (constant-load refresh), but load tests must include cache-wipe storms.
2. **Object-store CAS behavioral variance** across providers (ETag semantics vs generation
   preconditions; throttling of hot single keys). Mitigation: conformance test suite per
   repository implementation; shard-heads are per-shard so no single hot key exists.
3. **Lease-clock assumptions.** TTL leases assume bounded clock skew; use generous TTLs
   (seconds+) and require fencing (terms) to make skew a liveness issue only, never safety.
4. **Two-mode complexity.** Classic and serverless control planes coexist for years. The
   directory/head path must be a clean module; entangling it with the legacy allocation code
   would be worse than either alone.
5. **Deletion at namespace scale.** Deleting a tenant with 10⁶ shards means tens of millions
   of object deletes. Index deletion is therefore a *logical* operation (tombstone the index
   metadata object — instant, and the compat APIs honor it immediately) followed by background
   namespace GC that drains at a throttled rate, ideally delegated to object-store lifecycle
   policies on the index prefix where the provider supports prefix-scoped expiry. Napkin: 10⁷
   deletes at 3.5K DELETE/s/prefix drains in under an hour without touching any latency path.
6. **Organizational**: this RFC changes what "a cluster" means. Expect the compat surface
   (§10) to be the most contested part upstream — engage early with a paginated-views PR
   before the metadata plane itself.

## 14. Summary

Scale comes from three moves, all borrowed from systems that already operate at 10¹⁴-object
scale: (1) **make rest-state free** — a quiescent shard is just objects, so 10⁸ shards reduce
to a ~10⁶ active-set problem; (2) **split truth from hints** — linearizable per-shard state
lives in object-store CAS'd head objects, discovery lives in a rebuildable soft-state directory
with constant-load caching; (3) **replace the serializer with arbitration** — no global master
makes placement decisions; nodes race through CAS and background reconcilers converge the rest.
A small control cell survives for config and membership — consensus is retained where it is
cheap and removed from every path where it would have to scale.
