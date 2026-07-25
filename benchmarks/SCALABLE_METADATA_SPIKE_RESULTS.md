# Spike results S1-S7: scalable index and shard metadata

Seven spikes run to replace the estimated numbers in `rfc-scalable-index-metadata-plan.md` with
measured ones, after two earlier rounds in this investigation showed estimates breaking under
scrutiny. Every figure below came from running code in this repo. Where a spike contradicted what
the plan assumed, that is called out explicitly rather than quietly corrected.

Harnesses:
- `benchmarks/src/main/java/org/opensearch/benchmark/clusterstate/` — `RoutingDescriptor`,
  `TenantIndexRetainedHeapEstimate`, `DescriptorPagingSimulation`
- `server/src/test/java/org/opensearch/cluster/routing/allocation/AllocationCeilingSpikeTests.java`
- `server/src/test/java/org/opensearch/cluster/metadata/MappingDedupRealismSpikeTests.java`
- `server/src/test/java/org/opensearch/cluster/metadata/MetadataLookupReuseSpikeTests.java`
- `server/src/test/java/org/opensearch/index/mapper/MapperServiceHeapSpikeTests.java`

---

## S1 — Descriptor completeness audit

Classified every coordinating-path read of `metadata().index(...)`, `getIndicesLookup()`, and
`routingTable()` in `server/src/main/java` against a fixed candidate field set, so each site got a
hard verdict rather than a judgement call.

**`IndexMetadata` reads (33 sites):** 10 satisfiable, **1 needs-full on the hot path**, 20
needs-full on admin paths, 2 not-coordinator.

The single hot-path blocker is `SearchPipelineService.java:496`, which reads `getSettings()` to
evaluate `index.search.default_pipeline` for every concrete index of every search that does not
name a pipeline. Fixed by carrying one nullable string. Every other needs-full site reads
`getSettings()` on an admin API (`_settings`, get-index, resize, scale, remote-store stats,
tiering) — paths that tolerate a hydration round trip.

**`getIndicesLookup()` / `IndexAbstraction` (46 sites):** 30 satisfiable, 16 needs-full by current
shape but **14 of those read only small scalars**, and only 3 genuinely dereference a whole
`IndexMetadata` — all three already know their target index name and can fetch on demand.

The headline: `IndexNameExpressionResolver.expand()` (`INER:1296-1320`), the single reason wildcard
resolution pins all index metadata resident, needs only **`(name, state)` per member index**. The
hard reference from `IndexAbstraction.Index` to a full `IndexMetadata` is carrying two scalars'
worth of real demand.

**`routingTable()` (57 sites):** 19 satisfiable, 25 needs-derived, 13 not-coordinator. The
needs-derived set concentrates in exactly two places: `OperationRouting`'s preference machinery and
`_cat`/recovery reporting. `RotationShardShuffler` is just `CollectionUtils.rotate` over an
`AtomicInteger`, so a plain list plus a counter reproduces it; the attribute and weight maps are
memoized derivations of `(activeShards, DiscoveryNodes, awareness/weighted routing)`, recomputable
but currently cached.

**Fields the audit added that the first draft had missed:** `defaultSearchPipelineId`,
`index.frozen`, per-member `(name, UUID, state)` on alias and data-stream entries,
per-`(alias, member)` `AliasMetadata`, and `ShardRouting.searchOnly` (which
`Preference.SEARCH_REPLICA` depends on).

**The one genuine complication.** "Cold index absent from the routing table" is *not* equivalent to
"index present with all shards unassigned". Three call sites diverge, converting graceful
degradation into hard failure:

1. `OperationRouting.indexRoutingTable` (`:481-487`) throws `IndexNotFoundException` on a missing
   entry. Today an all-unassigned index yields empty iterators and a per-shard
   `NoShardAvailableActionException` (HTTP 200, partial failure); absence would 404 the search.
2. `RoutingTable.shardRoutingTable(ShardId)` (`:164-175`) throws. `TransportReplicationAction`'s
   `ReroutePhase` and `TransportBulkAction:741` currently see an UNASSIGNED `ShardRouting` and take
   the *retry-and-wait* branch; with the entry absent they fail immediately.
3. `TransportBroadcastReplicationAction:170` does `indicesRouting().get(index).getShards()` with no
   null check — NPE.

By contrast `RoutingTable.allShardsSatisfyingPredicate` (`:329-332`) already skips missing indices
explicitly, so the whole broadcast-by-node family already treats absent as no-shards. So the design
is viable but needs three small, specific source changes, not transparent absence.

---

## S2 — Real descriptor retained size

Measured with the same retained-heap method as the earlier rounds (build N, hold strong refs, force
GC, read delta), 200,000-object samples, OpenJDK 21, SerialGC.

| representation | retained/index | at 100M |
|---|---|---|
| full `IndexMetadata` + `RoutingTable` | 3,668 B | 341.6 GiB |
| realistic-compact (earlier round) | 1,097 B | 102.2 GiB |
| **routing descriptor, 1 alias** | **577 B** | 53.8 GiB |
| **routing descriptor, no alias** | **289 B** | 27.0 GiB |
| bare identity DTO (floor) | 161 B | 15.0 GiB |

**The plan estimated ~200 B. Actual is 289 B with no alias and 577 B with one** — low by 45% in the
best case and by nearly 3× in the aliased case.

A single alias doubles the descriptor. The cost is a one-entry `HashMap` (map object + table array +
node ≈ 160 B) plus the alias-name string and `AliasMetadata`. **Design consequence:** store aliases
as a flat array with a null sentinel rather than a `HashMap`; tenant indices addressed directly by
name typically have none, so this is roughly a free 2× on the dominant case.

**A distinction the plan conflated:** 289-577 B is *deserialized Java heap*, which governs cache
capacity. The object-store page holds the *serialized* form (~120 B), which governs GET size and
cost. Page sizing and cache sizing use different numbers.

---

## S6 — Allocation ceiling

20 nodes, 1 shard/index, 1 replica, recovery throttles removed so the measurement is the allocator's
own cost rather than how many shards it is permitted to start per round.

| indices | total shards | cold reroute | steady-state reroute | `RoutingNodes` rebuild |
|---|---|---|---|---|
| 1,000 | 2,000 | 436 ms | 51 ms | 2 ms |
| 5,000 | 10,000 | 1,250 ms | 237 ms | 39 ms |
| 20,000 | 40,000 | 9,120 ms | 789 ms | 64 ms |
| 50,000 | 100,000 | 53,656 ms | (run capped) | — |

**Cold allocation is superlinear**, roughly O(n^1.9): 2.5× the shards from 40k to 100k costs 5.9×
the time. 53 seconds for one reroute at 100k shards, on a single-threaded cluster-manager.

**Steady state is roughly linear**, ~20 µs/shard: 789 ms at 40k active shards, extrapolating to
~2 s at 100k and ~20 s at 1M.

**`RoutingNodes` rebuild alone is ~1.6 µs/shard** — 64 ms at 40k shards. That is the cost every
*data node* also pays per applied cluster state, because `ClusterState.getRoutingNodes()`
constructs it lazily and `IndicesClusterStateService` calls it eight times just to read the local
node. This is the C3 target and it is measurable, not theoretical.

**This contradicts the plan's ceiling claim in both directions.** The plan said metadata residency
capped a cluster at ~10M indices. In fact residency was never the binding constraint: the allocator
is, and it binds at **tens of thousands of active shards**, far below where residency would bite.
Cells are required, not optional — but they are sized by *active* shards, and each cell can hold
millions of quiescent tenants provided scale-to-zero keeps them out of the routing table entirely.
Cold-start time is the harsher of the two limits and is what should size a cell.

---

## S4 — Mapping dedup realism

1,000 tenant indices built from one mapping source, with differing index names, UUIDs and creation
dates.

- **Distinct by `equals()`: 1. Distinct by `hashCode()` (the CRC32 a dedup table would key on): 1.**
  Index identity provably does not leak into the stored mapping, so the identity check fires.
- Confirmed the instances are genuinely separate today, i.e. there is a real saving to capture.

Degradation as tenants diverge (a per-tenant field added to some fraction):

| tenants with a unique field | distinct mappings | dedup ratio |
|---|---|---|
| 0% | 1 | 1000× |
| 1% | 11 | 91× |
| 10% | 101 | 9.9× |
| 50% | 501 | 2.0× |
| 100% | 1000 | 1.0× |

The ratio is ≈ `100 / divergent%` — hyperbolic, so it degrades gracefully: a fleet where 10% of
tenants carry custom fields still gets ~10×. What kills it is **dynamic mapping**, since every
tenant that auto-adds a field forks off the shared instance. Pair dedup with `dynamic: strict` or a
fixed template and the win is real.

---

## S3 — Parsed `MapperService` graph cost

200 `MapperService` instances per tier, realistic tenant log/event mapping.

| fields | compressed (cluster state) | parsed graph (resident) | ratio |
|---|---|---|---|
| 20 | 188 B | 45 KB | 247× |
| 100 | 408 B | **101 KB** | 255× |
| 300 | 952 B | 237 KB | 255× |

**The plan's 30-80 KB estimate was low by 1.5-3×.** The ratio is stable at ~255× compressed, which
is a usable rule of thumb.

**Decisive negative result:** handing every index the *identical* `CompressedXContent` instance —
exactly what a dedup table produces — gives 103,758 B/index versus 103,844 B for separate instances.
A 0.08% difference. So mapping dedup helps **cluster state only**; data-node capacity is completely
unaffected, because the parsed graph is rebuilt per index regardless.

At ~101 KB per resident index, a node dedicating 8 GB to mapper graphs holds ~80k resident indices
before counting engine, translog, or Lucene structures. This is what makes scale-to-zero
load-bearing rather than an optimization.

*Caveat:* each `MapperService` here gets fresh `IndexAnalyzers`, matching real per-`IndexService`
construction, but some analyzer internals may be shared in a live node, so treat this as an
upper-ish bound.

---

## S5 — Descriptor paging: hit rate and write amplification

Pure simulation, 100M tenants, 2M sampled requests, using S2's measured 289 B heap / 120 B wire.

**Hit rate depends only on total cached descriptors, not on page size.** `1000 pages × 10k/page` and
`10000 pages × 1k/page` both give 72.2% at the same 2.69 GB. So page size is chosen for write
amplification and cache size for hit rate; the two knobs are independent.

| distribution | cache heap | hit rate | GET/s @10k RPS | $/mo |
|---|---|---|---|---|
| zipf | 2.69 GB | 72.2% | 2,782 | $2,885 |
| uniform | 2.69 GB | 10.0% | 9,003 | $9,334 |
| zipf | 0.27 GB | 53.8% | 4,624 | $4,794 |
| uniform | 0.27 GB | 1.0% | 9,899 | $10,263 |

**Uniform access defeats the design.** Under uniform access the cache is nearly useless until it
holds essentially everything. Real multi-tenant fleets are typically Zipf-ish, but this is the
assumption the whole tiered design rests on and it should be checked against real tenant traffic
before committing.

**Write amplification: the delta log's win is bandwidth, not request cost.** At 10k descriptors/page
and 10k updates/s, naive whole-page rewriting moves **7,234 MB/s**, versus small appends for a delta
log. But in pure S3 request cost the two are comparable (~$13k/mo), because naive rewriting touches
fewer *distinct* pages than there are updates. The binding constraint is that 7 GB/s of PUT traffic
blows through S3's ~3,500 PUT/s per-prefix limit long before cost matters — which is why hashed
prefixes (already present via `PathHashAlgorithm`) are mandatory rather than optional.

The update-rate assumption dominates: descriptor updates happen on *shard placement change*, not
per document, so a realistic large fleet is likely ~100/s (~$1,300/mo) rather than 10k/s.

---

## S7 — C1 incremental-reuse safety

`Metadata.Builder.build()` can reuse the previous `indicesLookup` and six derived arrays, or
recompute them. `MetadataDiff.apply` (`Metadata.java:1124`) uses the no-arg `builder()`, so
`previousMetadata` is null and the recompute path is *always* taken on every node for every
metadata-touching state.

Three results:

1. **The obvious fix is unsafe.** `Builder(Metadata)` seeds `this.indices` from the previous
   metadata *and* sets `previousMetadata`; `Builder#indices(Map)` is a `putAll` and cannot express a
   deletion. So swapping `builder()` for `builder(part)` in `MetadataDiff.apply` would leave deleted
   indices resident — a correctness bug, not a performance regression. Pinned by
   `testSeedingBuilderFromPreviousResurrectsDeletedIndices`.
2. **The alias risk flagged when C1 was proposed is not real.** Aliases live inside `IndexMetadata`
   and `IndexMetadata.equals` compares them, so any alias change makes the index map unequal and
   forces a recompute. Verified end to end: the new alias appears and the old one is gone.
3. **Reuse is equivalent when the index set is genuinely unchanged** — identical lookup key set and
   identical derived arrays.

So C1 is worth doing and is safe, but requires supplying `previousMetadata` *without* seeding the
indices map — a new builder method or setter, not `builder(part)`.

---

## What the plan got wrong

| plan claim | measured | effect |
|---|---|---|
| descriptor ~200 B | 289 B no-alias, 577 B with one alias | paging math re-based; aliases need a flat array |
| ~10M indices/cluster, metadata-limited | allocator binds first, at tens of thousands of *active shards* | cells required, sized by active shards not tenants |
| "cells become optional" (said mid-discussion) | wrong | withdrawn |
| parsed mapper graph 30-80 KB | 45-237 KB, ~255× compressed | data-node capacity is tighter than assumed |
| C1 is a trivially safe first PR | safe only with a non-seeding builder path | still first, but needs a specific implementation |
| dedup helps broadly | cluster state only, 0.08% effect on parsed graph | scope narrowed, value confirmed for its actual scope |

## What holds up

- The descriptor concept survives a complete call-site audit, with a small, enumerated set of
  additions and exactly one hot-path fix.
- Wildcard resolution needs only `(name, state)` per member — the residency pin is trivially thin.
- Mapping dedup is real (1000× on identical fleets) and degrades gracefully.
- Remote cluster state already provides per-index blobs, incremental version-gated upload, and a
  single-index fetch primitive.
- Scale-to-zero is confirmed as the load-bearing mechanism: at ~101 KB resident per index and an
  allocator that binds at tens of thousands of active shards, keeping quiescent tenants out of both
  the routing table and the resident set is what makes any of this work.
