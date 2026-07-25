# Plan: Scalable Index & Shard Metadata Management

- Status: DRAFT proposal, no implementation attached
- Companion to: `rfc-serverless-metadata-plane.md` (design vision), `rfc-serverless-control-cell-diet.md`
  (prior Phase 5.5 attempt, whose core mechanism failed its own review), and
  `benchmarks/CLUSTERSTATE_METADATA_SPIKE_FINDINGS.md` (measured per-index heap cost)
- Basis: a five-track investigation of the current implementation, cited inline throughout

This supersedes `rfc-serverless-control-cell-diet.md`'s §2 mechanism, which proposed holding a
compact object *alongside* the full `IndexMetadata` and therefore saved nothing. The
investigation behind this document found a different seam that does work.

> **Measured-results update.** Seven spikes (S1-S7) were run after this plan was first written, to
> replace its estimated figures with measured ones. Results and harnesses are in
> `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md`. Four of this document's claims did not survive;
> each is corrected inline below and summarised in that document's "What the plan got wrong" table.
> The most important correction: **the allocator, not metadata residency, is the binding
> constraint**, and it binds far earlier than this plan assumed. Read §5 with that in mind.

---

## 1. What the investigation actually found

Five parallel traces through the current code. Every claim below is from reading the
implementation, not the design docs.

### 1.1 The cluster-manager rebuilds everything on every change

`Metadata.Builder.build()` has an incremental path (`buildMetadataWithPreviousIndicesLookups`,
`Metadata.java:1655`) and a full-recompute path (`buildMetadataWithRecomputedIndicesLookups`,
`:1678`). The recompute path allocates a fresh `TreeMap` holding an entry per index *and* per
alias, six length-N `String[]` arrays, and re-runs alias validation across every alias.

`MetadataDiff.apply` (`Metadata.java:1124`) calls the **no-arg** `builder()`, so
`previousMetadata` is null and the recompute path is taken unconditionally. **Every node in the
cluster does a full O(N log N) rebuild on every metadata-touching cluster state**, however small
the diff. A one-field settings change on one index costs every node a complete `indicesLookup`
reconstruction.

Two further unconditional O(N) costs on *both* paths: the `Metadata` constructor loops all
indices to sum shard counts and classify `RoutingPool` (`:319-331`), and `patchVersions`
(`ClusterManagerService.java:452`) triggers a second full build purely to bump the version.

The update thread is a single `newSinglePrioritizing` executor (`ClusterManagerService.java:181`)
that blocks on full-cluster publication ack (`:365`), with diff and serialization done under
`Coordinator.mutex`. Create-index and update-settings do **not** batch — each task is its own
executor (`:477-489`), so N index creations are N full cycles. Mapping updates do batch.

Publication itself is better than assumed: the diff is computed once and serialized once per
distinct wire `Version`, shared across all nodes (`PublicationTransportHandler.java:439-454`).

### 1.2 Every data node builds a full-cluster shard index, per state version

`ClusterState.getRoutingNodes()` (`ClusterState.java:326-331`) lazily constructs
`new RoutingNodes(this)`, which walks every shard of every index in the cluster
(`RoutingNodes.java:114-178`), and memoizes it on the `ClusterState` instance.

All eight of `IndicesClusterStateService.applyClusterState()`'s sub-methods call it, only to
then use `.node(localNodeId)`. The loop *bodies* are O(local shards); obtaining their input is
O(total cluster shards). A node hosting ten shards builds and retains a cluster-wide node→shard
inverse index on every applied state. (This corrects a claim in
`rfc-serverless-control-cell-diet.md` §3.)

On the allocation side, every state-changing reroute does two full O(total shards) rebuilds:
`new RoutingNodes(...)` (`AllocationService.java:713`, whose own comment says "costly... must
only be called once") and then a complete `RoutingTable.Builder` rebuild via `updateNodes`
(`RoutingTable.java:489-526`). `IndexRoutingTable.Builder.addShard` (`:809-818`) constructs a
fresh `IndexShardRoutingTable` per copy added, making the rebuild superlinear in replica count.

Per-shard object weight is heavier than the field list suggests: each `IndexShardRoutingTable`
allocates two shufflers, two mutexes, four volatile cache maps and six derived collections
(`IndexShardRoutingTable.java:84-178`); each `ShardRouting` eagerly builds a singleton list and,
when relocating, a second full `ShardRouting` (`ShardRouting.java:67-108`).

The balancer scans everything: `buildModelFromAssigned` per balancer instance, `moveShards` over
every started shard, `balanceByWeights` over every index × every node with a decider call each
(`BalancedShardsAllocator` / `LocalShardsBalancer`). The only existing brake is a wall-clock
timeout that abandons and reschedules — it defers work rather than reducing it. `RoutingPool`
gives exactly two partitions; nothing else bounds iteration scope.

One component is already incremental and is the model to copy: `IndexMetadataUpdater.applyChanges`
(`allocation/IndexMetadataUpdater.java:161-201`) touches only shards that actually changed.

### 1.3 Coordinating nodes need per-index metadata for indices they don't host

Everything below runs before any shard is contacted, on a node that may host nothing:

- `IndexNameExpressionResolver` consults `Metadata.getIndicesLookup()`, and
  `IndexAbstraction.Index` holds a hard reference to the full `IndexMetadata`
  (`IndexAbstraction.java:149-176`). `expand()` dereferences it per match just to read
  `getState()` and `isHidden()` (`INER:1289-1317`). **This is why wildcard resolution pins all
  index metadata resident.**
- Exact-name resolution is already a cheap map lookup. Suffix wildcards (`foo*`) use
  `TreeMap.subMap` and are sublinear (`INER:1242-1250`). Mid-pattern wildcards (`*foo`) regex-scan
  the entire lookup (`:1252-1260`).
- `TransportBulkAction:658-662` reads `metadata.index(...).mapping()` for **every index request**,
  to get `routingRequired` and for `IndexRequest.process`. Note that `routingRequired` is already
  a plain boolean field on `MappingMetadata` (`MappingMetadata.java:65-105`) — the full mapping is
  pulled to read one flag.
- Search additionally needs the full `IndexRoutingTable`, `isRemoteSnapshot()`, warm-index
  setting, `getNumberOfSearchOnlyReplicas()`, `isSystem()`, and every matched alias's filter,
  which is **parsed coordinator-side** (`IndicesService.java:2266-2282`).
- `_stats` with no index argument materializes the entire `RoutingTable`
  (`TransportBroadcastByNodeAction` subclasses).
- Core has no index-level authz. Security plugins typically expand `*` then filter by role, which
  triggers the full O(N) lookup walk. That risk is plugin-side but real.

The minimum a non-hosting coordinator genuinely needs per index: the routing math fields
(`routingNumShards`, `routingFactor`, `splitShardsMetadata`, `numberOfVirtualShards`,
`routingPartitionSize`), a handful of flags (`state`, `waitForActiveShards`, `creationVersion`,
`isAppendOnly`, `isSystem`, `isRemoteSnapshot`, warm, `numberOfSearchOnlyReplicas`),
`mapping().routingRequired()` as a boolean, alias routing values and filters, and the shard→node
assignment. **Not** the mapping body, and **not** the settings body.

### 1.4 Mappings: no dedup exists, and dedup is not enough

`MappingMetadata` holds `type` + `CompressedXContent` + `routingRequired`. Mappings are already
stored DEFLATE-compressed in cluster state: ~214 B for 20 fields, ~470 B for 100 fields, ~3.7 KB
at the 1000-field default limit.

There is **no deduplication anywhere**. `IndexMetadata.Builder#putMapping` (`:2193-2199`) is a
plain map put; deserialization allocates a fresh `byte[]` per index
(`CompressedXContent.java:163-166`) with no interning; `Metadata` has no `mappingsByHash`
structure (Elasticsearch 8.x added exactly that; OpenSearch never ported it). Two byte-identical
mappings are two distinct instances. The only sharing is temporal: an unchanged index's
`MappingMetadata` is carried by reference across versions via `CompleteDiff.apply`.

Templates **copy**, they do not reference. `MetadataCreateIndexService` collects template
mappings, merges them through a throwaway `MapperService`, and stores the merged result as the
index's own `MappingMetadata` (`:868-882`, `:1690-1702`, `:1622-1630`). No back-reference to the
template survives; deleting or editing the template does not affect existing indices.

On data nodes the parsed side is far larger: one `MapperService` per `IndexService`, whose
`MappingLookup`/`FieldTypeLookup` put each field in three to five hash maps with a `FieldMapper`
and `MappedFieldType` behind it — roughly **30-80 KB per resident index**, two orders of
magnitude above the compressed form, with no interning.

**Verdict on shared mappings:** content-addressed dedup of `MappingMetadata` is cheap, contained,
BWC-safe (the wire format need not change), and for a homogeneous tenant fleet collapses the
cluster-state mapping line item from N × 0.5 KB to one shared copy plus a reference per index —
about 50 GB → 0.8 GB per node at 100M indices. Do it. But it does not make 100M viable on its
own: the non-shareable parts of `IndexMetadata` dominate, and the parsed mapper graph dominates
on data nodes. True template *reference* (Tier B) breaks dynamic mapping, changes template
semantics from snapshot-at-create to retroactive, and is not recommended.

### 1.5 Remote cluster state already does most of the storage work

This was the most encouraging finding. Under `cluster.remote_store.state.enabled`:

- **`IndexMetadata` is already one blob per index**, at `index/<indexUUID>/metadata__...`, with
  optional hashed path prefixes to avoid object-store hot-spotting (`RemoteIndexMetadata.java:78-115`).
- **Uploads are already incremental and version-gated** — only indices whose `IndexMetadata.getVersion()`
  changed are re-uploaded; the rest are carried by reference from the previous manifest
  (`RemoteClusterStateService.java:436-455`).
- **A single-index fetch primitive already exists**: `RemoteIndexMetadataManager.getIndexMetadata(...)`
  (`:114-129`) reads exactly one blob for one index. It is package-private and used only for
  UUID-chain comparison. No public API, no transport action, no caller ever pulls one index on
  demand into a live cluster state.
- `RemoteWriteableEntityBlobStore` / `AbstractRemoteWritableEntityManager` is a clean, reusable
  SPI for a new remote-resident entity type. `IndexMetadataUploadListener` is an existing hook
  that receives only the changed-index list.
- Local Lucene persistence is likewise already per-index and incremental — one document per index,
  only changed indices rewritten (`PersistedClusterStateService.java:732-776`).

The gaps: **`ClusterMetadataManifest` lists every index twice (metadata and routing) and is
rewritten on every accepted cluster state**, so it is O(N) per version — tens of MB per state
change at high index counts. `GatewayMetaState.verifyManifestAndClusterState:808` asserts manifest
index count equals state index count, baking the full-set invariant in. `readClusterStateInParallel`
always materializes every index into `Metadata`. `RemoteClusterStateCache` holds exactly one whole
state, with no per-index cache. Publication downloads are blocking (TODO at
`PublicationTransportHandler.java:240`).

Also confirmed: `index.number_of_search_replicas > 0` forces `index.remote_store.enabled=true`
(`MetadataCreateIndexService.java:1275-1284`).

---

## 2. Reframing: three independent cost axes

The prior round's failure came from treating this as one problem. It is three, with different
fixes and different blast radii.

| Axis | Cost driver | Scales with | Fix class |
|---|---|---|---|
| **A. Cluster-manager compute** | full `Metadata`/`RoutingNodes`/`RoutingTable` rebuilds per change; single-threaded; unbatched creates | total indices, total shards | perf fixes, no architecture change |
| **B. Every-node residency** | full `IndexMetadata` + `indicesLookup` + `RoutingNodes` on every node | total indices | dedup, then non-residency |
| **C. Data-node per-resident-index** | parsed `MapperService` graph, engine, translog | *resident* indices only | already addressed by scale-to-zero |

Axis C is largely solved by the existing serverless-storage suspension work: a quiescent shard
costs nothing. Axis A is a set of contained performance fixes that benefit every OpenSearch
deployment, tenant-scale or not. Axis B is the architectural one, and it splits into a cheap half
(dedup) and a hard half (non-residency).

**Corrected after S2/S3/S6.** Axis C is *not* "largely solved" in the sense of being cheap — a
resident index costs a measured ~101 KB in parsed `MapperService` graph alone (S3), so scale-to-zero
is the load-bearing mechanism rather than an optimization. And the ranking between axes is wrong as
originally written: **Axis A binds first**. The allocator's cost (S6) is ~20 µs/shard in steady
state and superlinear on cold allocation, which caps a cluster at tens of thousands of *active
shards* — reached long before Axis B's residency cost matters.

Measured per-index residency (S2): today's full object is 3,668 B; a routing descriptor is 289 B
with no alias, 577 B with one. At 100M indices that is 27-54 GB, so non-residency is still required
at that scale — but it is the *second* constraint to hit, not the first. Dedup (S4) collapses the
mapping line item by up to 1000× in cluster state, and does nothing at all for the parsed graph on
data nodes (S3).

---

## 3. Core changes

Ordered by dependency. The first four are independently valuable, shippable as separate upstream
PRs, and benefit every deployment regardless of this plan.

### C1. Take the incremental path in `MetadataDiff.apply` (pure fix, no SPI)

Pass the previous `Metadata` into the builder in `MetadataDiff.apply` (`Metadata.java:1124`) so
`buildMetadataWithPreviousIndicesLookups` can be taken when the index set is unchanged. Today the
incremental path exists but is dead on the diff-apply path, which is the hot path on every
follower node.

Also: skip the six `Arrays.copyOf` when the index set is unchanged, make the constructor's
shard-count/`RoutingPool` loop incremental, and avoid the redundant second build in `patchVersions`.

Blast radius: `Metadata` only.

**Corrected after S7.** The correctness risk this section originally flagged — that alias changes
might slip past the reuse guard — is not real: aliases live inside `IndexMetadata`,
`IndexMetadata.equals` compares them, so any alias change makes the index map unequal and forces a
recompute. Verified.

A different, real trap was found instead. `Builder(Metadata)` seeds `this.indices` from the previous
metadata *and* sets `previousMetadata`, while `Builder#indices(Map)` is a `putAll` that cannot
express a deletion. So the obvious implementation — swapping `builder()` for `builder(part)` — would
leave deleted indices resident on every node, a correctness bug rather than a perf regression
(pinned by `MetadataLookupReuseSpikeTests#testSeedingBuilderFromPreviousResurrectsDeletedIndices`).
The fix must supply `previousMetadata` **without** seeding the index map: a new builder method or
setter.

**Still the right first PR**, and still worth doing on its own merits — but with that specific
implementation, not the naive one.

### C2. Content-addressed dedup for `MappingMetadata` and settings bodies

A canonicalization table in `Metadata.Builder`, keyed on `CompressedXContent`'s existing CRC32
hash with an equals check, so byte-identical mappings across indices share one instance. Apply at
`Builder#putMapping` and at the deserialization entry points (`IndexMetadata.java:1843`, `:2717`).
Wire format unchanged; each index still logically owns its mapping.

Extend the same treatment to `Settings`: tenant indices differ in only a few keys
(`index.uuid`, `index.creation_date`, `index.provided_name`), so a shared-prototype-plus-overlay
representation captures most of the win. This is a larger change than mapping dedup and should be
a separate PR behind it.

Optionally, later and BWC-gated: allow a diff to ship a hash reference instead of bytes when the
receiver already holds that mapping.

Blast radius: `Metadata.Builder`, `IndexMetadata` deserialization. No semantic change — mappings
remain per-index-owned and independently mutable.

### C3. Stop building cluster-wide `RoutingNodes` on data nodes

Add a local-node-scoped view so `IndicesClusterStateService` can get its local shards without
constructing the full inverse index. Either a `RoutingNodes.forNode(state, nodeId)` that walks
only what it needs, or have `IndicesClusterStateService` iterate the routing table filtered by
node without materializing `RoutingNodes` at all.

Blast radius: `ClusterState.getRoutingNodes()` is called from many places; this adds an
alternative rather than changing it. Data-node win is immediate and large.

### C4. Incremental `RoutingNodes` / `RoutingTable` rebuild

Follow `IndexMetadataUpdater`'s existing model: apply only changed shards rather than rebuilding.
This is the largest of the four and the one most likely to need its own design review, but it is
still a pure performance change with no new SPI and no semantic change.

### C5. Split `Metadata` into a resident directory and a hydrated index map

This is the architectural change, and it is where the prior round failed. The mechanism that
works is different from what that round proposed.

**Do not** make `Metadata.index(name)` return a union type or a lazily-resolving proxy. That was
the prior proposal, and it forces every one of hundreds of synchronous callers across `server/`
to cope with unresolved state.

**Instead**, split the two roles `Metadata` currently conflates:

1. **A compact `IndexDirectory`, resident on every node, carried in cluster state.** One entry per
   index, holding exactly what §1.3 enumerated as the non-hosting coordinator's minimum: name,
   UUID, the routing math fields, the flag set, `routingRequired` as a boolean, alias names with
   their routing values and filters, and the metadata version. This is the structure
   `indicesLookup` is built from, replacing `IndexAbstraction.Index`'s hard reference to full
   `IndexMetadata`. Wildcard and alias resolution, `OperationRouting`'s shard math, and
   `TransportBulkAction`'s `routingRequired` check are all satisfiable from this alone.
2. **A hydrated `Map<String, IndexMetadata>` that is node-local cache, not cluster state.** Full
   `IndexMetadata` for indices this node currently needs: any index it hosts a shard of, plus any
   index recently touched as coordinator. Authoritative copy is the per-index remote blob that
   `RemoteClusterStateService` **already writes** (§1.5), versioned, so a cached copy is validated
   by comparing against the directory entry's version.

`Metadata.index(name)` keeps returning `IndexMetadata` and keeps being synchronous. It returns the
hydrated object. The contract change is that it may return null for a cold index, and callers that
can touch cold indices must hydrate first.

**Hydration happens at the request boundary, not inside a getter.** `IndexNameExpressionResolver`
already runs first on every request and already resolves names to concrete indices. It resolves
from the directory (no hydration needed), and the resolved concrete set is then hydrated in one
batched async step before execution proceeds. Downstream code sees fully materialized
`IndexMetadata` and is unchanged. This mirrors the cold-shard activation pattern
`rfc-serverless-metadata-plane.md` §9 already establishes, and the batching mirrors
`can_match`-style pre-execution phases that already exist in search.

Cold indices are also **absent from `RoutingTable` entirely** — not present with unassigned
shards. A quiescent tenant has no shard copies anywhere, so there is nothing for the allocator to
consider, and `RoutingNodes`/balancer cost scales with the active set rather than the total. This
is the concrete realization of the existing RFC's "quiescent shard costs zero control-plane state",
and per S6 it is the single highest-value property in this whole plan, because the allocator is the
binding constraint.

**Corrected after S1.** "Absent" is not currently equivalent to "no shards to route to", and three
call sites convert graceful degradation into hard failure if an entry simply disappears:

- `OperationRouting.indexRoutingTable` (`:481-487`) throws `IndexNotFoundException`, so a search
  that today returns HTTP 200 with per-shard `NoShardAvailableActionException` would instead 404.
- `RoutingTable.shardRoutingTable(ShardId)` (`:164-175`) throws, so `TransportReplicationAction`'s
  `ReroutePhase` and `TransportBulkAction:741` would fail immediately instead of taking their
  existing retry-and-wait-for-allocation branch.
- `TransportBroadcastReplicationAction:170` dereferences `indicesRouting().get(index)` with no null
  check — NPE.

The broadcast-by-node family already treats absent as no-shards
(`RoutingTable.allShardsSatisfyingPredicate:329-332` skips missing indices explicitly), so this is
three specific small changes, not a pervasive problem. They are a prerequisite for C5, not
optional cleanup.

**Descriptor field set (S1-verified).** Beyond what this section originally listed, the audit added:
`defaultSearchPipelineId` (the one hot-path blocker, at `SearchPipelineService:496`), an
`index.frozen` boolean, per-member `(name, UUID, state)` on alias and data-stream entries,
per-`(alias, member)` `AliasMetadata`, and `ShardRouting.searchOnly`. Store aliases as a flat array
rather than a `HashMap`: S2 measured a single-entry `HashMap` doubling the descriptor from 289 B to
577 B.

### C6. Remote cluster state: shard the manifest, expose per-index fetch

- **Shard `ClusterMetadataManifest`.** Today it lists every index and is rewritten per version. Split
  into a small root manifest plus per-shard-of-namespace index manifests, so a version bump rewrites
  the root and only touched shards. Relax `GatewayMetaState.verifyManifestAndClusterState:808`'s
  full-set assertion accordingly.
- **Make single-index fetch a first-class, public, cached operation.** `RemoteIndexMetadataManager.getIndexMetadata`
  already does the work; it needs to be public, to have a bounded per-index cache alongside the
  existing whole-state `RemoteClusterStateCache`, and to be reachable as a transport action for
  nodes that read through a peer rather than the object store.
- **Bounded hydration budget**, modeled on the existing per-query activation budget: a cap on
  concurrent cold hydrations per request and per node, with an explicit "query too broad" failure
  rather than an unbounded stampede.

### C7. Batch create-index and update-settings

Give them real `ClusterStateTaskExecutor`s so N tenant creations collapse into few cluster-state
cycles, as mapping updates already do. Independent, contained, and directly relevant to a fleet
that provisions tenants continuously.

---

## 4. The plugin

Core provides mechanism; the plugin provides policy. Nothing tenant-specific belongs in `server/`.

**`metadata-plane` plugin**, engine-agnostic and opt-in per index via a setting independent of
`index.engine`, so a classic `InternalEngine` index can use it without adopting the
object-store engine:

- **Residency policy.** Decides which indices are hot versus cold, and when to evict a hydrated
  entry. Inputs: last access, whether a shard is locally assigned, memory pressure. Analogous to
  the existing `ShardSuspensionCoordinator` but for metadata rather than shards.
- **Hydration source.** Implements fetch-by-(uuid, version) against the remote store, using C6's
  public per-index API, with a bounded cache and single-flight coalescing so a hot cold-index does
  not stampede.
- **Directory maintenance.** Keeps the compact `IndexDirectory` entries current; on tenant creation
  writes the full `IndexMetadata` blob and publishes only the directory entry into cluster state.
- **Tenant provisioning fast path.** Bulk tenant creation that writes N metadata blobs directly and
  publishes one batched directory update, rather than N cluster-state cycles.
- **Shared-mapping support.** A per-tenant-class mapping registry so a fleet of tenants provisioned
  from one template land on byte-identical mappings and therefore collapse to a single instance
  under C2. This is where your template idea pays off, and it needs no template-reference semantics
  in core.

It composes with, and does not duplicate, `serverless-storage`: that plugin owns Class D (shard
truth and placement, via `ShardHead`/`ShardStateStore`/directory tier). This one owns Class C
(index metadata residency). A serverless-storage index would use both; a classic index could use
only this one.

---

## 5. What this reaches, and what it does not

**Rewritten after S2/S3/S6.** The original version of this section ranked the constraints wrong.

**The binding constraint is the allocator, not metadata residency.** S6 measured a steady-state
reroute at ~20 µs/shard (789 ms at 40k active shards) and cold allocation as superlinear, ~O(n^1.9)
(9 s at 40k shards, 53 s at 100k). A cluster is therefore limited to **tens of thousands of active
shards** — reached long before per-index residency matters. Cold-start time is the harsher of the
two limits and should be what sizes a cell.

**Reaches.** C1-C4 and C7 are pure efficiency and raise the practical ceiling of any cluster without
architectural commitment; C3 in particular removes a measured 64 ms-per-applied-state cost from
every data node at 40k shards. C2 plus shared mappings collapses the cluster-state mapping line item
by up to 1000× (S4). C5 plus C6 cuts per-index residency from 3,668 B to 289 B (S2).

**Does not reach.** Even with every change here, a cluster is capped by active-shard allocation
cost, so **cells are required, not optional**. The useful reframing from the measurements: a cell is
sized by *active shards* (order 20-50k), while the descriptor and hydration work is what lets each
cell **hold** millions of quiescent tenants. Those are different budgets, and conflating them is
what produced the earlier wrong ceiling.

**Scale-to-zero is therefore load-bearing, not an optimization.** S3 measured ~101 KB of parsed
`MapperService` graph per resident index — so residency is expensive on data nodes too — and S6
showed active shards are the scarcest resource on the cluster-manager. Keeping quiescent tenants out
of both the routing table and the resident set is what makes the whole design work.

Also unaddressed here: `AllocationService`'s global view (C4 makes it incremental but still
cluster-wide, and S6 suggests this deserves more attention than originally assigned), and
dormant-shard support for classic `InternalEngine` indices, which needs an engine-level lazy mode
independent of anything in this document.

---

## 6. Sequencing, and what needs proving first

1. **C1** — incremental `MetadataDiff.apply`. Small, self-contained, benefits everyone. Start here.
2. **C2 mapping dedup** — small, contained, large win for homogeneous fleets. Then settings dedup
   as a follow-on.
3. **C3** — local-node routing view. Contained, large data-node win.
4. **C7** — batch create-index/update-settings.
5. **C4** — incremental routing rebuild. Needs its own review.
6. **C6** — manifest sharding and public per-index fetch. Needed before C5 is useful.
7. **C5** — the directory/hydration split. Largest, and gated on the spike below.

**Updated after S1-S7.** The prerequisite this section originally named — can `IndexNameExpressionResolver`
and `OperationRouting` be satisfied from a compact entry — has been answered: **yes**, with one
hot-path addition (`defaultSearchPipelineId`) and a short list of extra fields, plus three specific
source changes so an absent routing entry degrades rather than fails. See S1.

Revised ordering, with what each now rests on:

1. **C1** — incremental `MetadataDiff.apply`. Still first. S7 verified the reuse condition is safe
   and found the trap: supply `previousMetadata` **without** seeding the index map.
2. **C3** — local-node routing view. S6 measured the win: 64 ms per applied cluster state at 40k
   shards, on every data node. Promoted above dedup.
3. **C2 mapping dedup** — S4 confirmed 1000× on homogeneous fleets, degrading as ≈`100/divergent%`.
   Pair with `dynamic: strict`. Note S3: this helps cluster state only, not data-node capacity.
4. **C7** — batch create-index/update-settings.
5. **C4** — incremental routing rebuild. S6 raises its priority: the allocator is the binding
   constraint, so this is worth more than originally assigned.
6. **C6** — manifest sharding and public per-index fetch. Needed before C5 is useful.
7. **C5** — the directory/hydration split, including S1's three routing-absence fixes.

**What still needs proving, in priority order:**

- **The allocator's scaling is the real ceiling** (S6). Nothing in this plan addresses it beyond C4.
  Whether allocation can be made incremental or domain-scoped is now the highest-value open
  question, and it is a bigger question than anything else here.
- **Tenant access distribution** (S5). The tiered-page design works under Zipf (72% hit rate at
  2.69 GB cache) and fails under uniform (10%). This should be checked against real tenant traffic
  before committing to the cache sizing.
- **Serialized descriptor size.** S2 measured deserialized heap (289 B); the wire form used for page
  sizing is estimated at ~120 B and has not been measured.

Steps 1-4 depend on none of these and can proceed in parallel.
