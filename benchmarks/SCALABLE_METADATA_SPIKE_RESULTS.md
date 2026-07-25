# Spike results S1-S9: scalable index and shard metadata

Nine spikes run to replace the estimated numbers in `rfc-scalable-index-metadata-plan.md` with
measured ones, after two earlier rounds in this investigation showed estimates breaking under
scrutiny. Every figure below came from running code in this repo. Where a spike contradicted what
the plan assumed, that is called out explicitly rather than quietly corrected.

Harnesses:
- `benchmarks/src/main/java/org/opensearch/benchmark/clusterstate/` — `RoutingDescriptor`,
  `TenantIndexRetainedHeapEstimate`, `DescriptorPagingSimulation`, `DescriptorWireSizeEstimate`
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
capacity. The object-store page holds the *serialized* form, which governs GET size and cost. Page
sizing and cache sizing use different numbers; S8 measures the serialized side.

---

## S6 — Allocation ceiling

20 nodes, 1 shard/index, 1 replica, recovery throttles removed so the measurement is the allocator's
own cost rather than how many shards it is permitted to start per round.

**Corrected after S9.** The first run left assertions on. Gradle enables `-ea -esa`
(`OpenSearchTestBasePlugin.java:133`); production `jvm.options` does not, and the assertion paths
here are expensive (`RoutingNodes.assertShardStats` does two full shard passes allocating a
`HashSet` per `ShardId`; `RoutingNode#invariant` does three stream-and-collect passes per node).
**Assertions roughly double the measured cost.** Production-representative figures are the `-da`
column; the `-ea` column is kept to show the size of the distortion.

| shards | cold `-ea` | cold `-da` (2 runs) | steady `-ea` | steady `-da` (2 runs) | `RoutingNodes` `-ea` | `RoutingNodes` `-da` |
|---|---|---|---|---|---|---|
| 2,000 | 436 ms | 670 / 661 ms | 51 ms | **25 / 34 ms** | 2 ms | 1-2 ms |
| 10,000 | 1,250 ms | 531 / 1,389 ms | 237 ms | **75 / 71 ms** | 39 ms | 5-6 ms |
| 40,000 | 9,120 ms | 4,560 / 3,907 ms | 789 ms | **445 / 355 ms** | 64 ms | 43-52 ms |
| 100,000 | 53,656 ms | (exceeds suite timeout) | — | — | — | — |

**Treat these as order-of-magnitude, not precise.** Two `-da` runs of the same harness differ by up
to 2.6× on cold allocation at 10k shards (531 vs 1,389 ms), which is single-run noise on a shared
machine, not signal. Steady-state is markedly more stable (within ~25% across runs) and is the
figure to rely on. The 2,000-shard tier is dominated by JIT warmup in both directions and should not
be read as meaningful. The 100,000 tier exceeds the 20-minute suite timeout — driving 100k shards to
STARTED takes several rounds of an already-superlinear cold reroute — so it was measured once and
then dropped from the harness to keep it repeatable; its 53.7 s figure carries assertions and is
therefore also roughly 2× high.

**Cold allocation is superlinear** and, even corrected, is seconds-scale: ~4 s at 40k shards on a
single-threaded cluster-manager, with the 100k measurement (53.7 s, assertions on) suggesting tens
of seconds there. This is a recovery-time and mass-provisioning concern rather than a steady-state
one, and its run-to-run variance is too wide to fit a confident exponent to.

**Steady state is ~9-11 µs/shard** (355-445 ms at 40k), about half the originally reported figure,
and itself mildly superlinear at the top end (4× the shards from 10k to 40k costs ~5× the time).

**`RoutingNodes` rebuild is ~1.3 µs/shard** — 52 ms at 40k. That is the cost every *data node* also
pays per applied cluster state, because `ClusterState.getRoutingNodes()` constructs it lazily and
`IndicesClusterStateService` calls it eight times just to read the local node. It is ~12% of a
steady-state reroute, not the dominant term (see S9), but on data nodes it is pure waste.

**This contradicts the plan's ceiling claim in both directions.** The plan said metadata residency
capped a cluster at ~10M indices. In fact residency was never the binding constraint: the allocator
is, and it binds at the order of **100k active shards** — where a steady-state reroute is ~1 s and
cold allocation tens of seconds — far below where residency would bite. Cells are required, not
optional, but they are sized by *active* shards, and each cell can hold millions of quiescent
tenants provided scale-to-zero keeps them out of the routing table entirely. **Cold-start time, not
steady-state cost, is what should size a cell**: a cell whose steady-state reroute is a comfortable
445 ms still takes 4.6 s to cold-allocate, and that gap widens superlinearly.

Note that S9 identifies a concrete optimization worth an estimated 30-50% of the steady-state
figure, so these numbers are a measurement of the current implementation, not a hard floor.

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

## S8 — Serialized descriptor wire size

S2 measured retained heap, which governs cache capacity. Page size and GET cost depend on the
serialized form, which S5 assumed was ~120 B without measuring. Measured with
`DescriptorWireSizeEstimate`, 1,000 descriptors per page, DEFLATE at `BEST_SPEED`:

| form | uncompressed wire | in a 1,000-descriptor page |
|---|---|---|
| descriptor, 1 alias | 197.5 B | **36.0 B/index** (5.5× compression) |
| descriptor, no alias | 146.7 B | **31.4 B/index** (4.7×) |
| full `IndexMetadata` | 275.7 B | — |

Two corrections:

**The ~120 B assumption was low uncompressed but pessimistic once paged.** Pages of near-identical
tenant descriptors compress ~5×, so effective wire cost is 31-36 B/index and a 1,000-descriptor page
is ~36 KB, not 120 KB. This does not change S5's conclusions — its cost model was request-count
driven and bytes were never the binding term — but it makes larger pages more attractive than
modelled.

**The descriptor's value is heap residency, not storage or transfer.** On the wire the descriptor
beats full `IndexMetadata` by only 1.4× (197.5 B vs 275.7 B), against a **12.7×** heap difference
(289 B vs 3,668 B). Almost the entire benefit is Java object overhead avoided, not data avoided.
Design consequence: the object-store tier can store full `IndexMetadata` blobs and hydrate from
them directly, with no separate descriptor blob — one fewer artifact to keep consistent — while
nodes still hold only descriptors *in heap*.

*Caveat:* the `IndexMetadata` measured here carries no mapping. A realistic tenant index with a
100-field mapping adds ~408 B compressed, widening the wire gap to ~3.5×. The direction holds; the
magnitude is workload-dependent.

---

## S9 — Can allocation be made incremental or domain-scoped?

S6 established the allocator as the binding constraint, making this the highest-value open question.
Investigated against the real code.

**Where the steady-state cost actually goes.** `reroute()` short-circuits *before* the RoutingTable
rebuild (`AllocationService.java:573` returns the original `ClusterState` when nothing changed), so
`buildResult`/`RoutingTable.Builder.updateNodes` are **not** in the measured figure. The dominant
terms are:

1. `balanceByWeights()` (`LocalShardsBalancer.java:348-382`) — for every index, for every node, a
   full `deciders.canAllocate(indexMetadata, node, allocation)` call **before** any delta check. At
   40k indices × 20 nodes that is ~800k full decider-chain invocations even when perfectly balanced.
2. `moveShards()` (`:578-668`) — no early exit; every started shard gets `canMoveAway` plus a full
   `canRemain` decider chain.
3. `new RoutingNodes(...)` — only ~8% (64 ms of 789 ms at 40k).
4. `adaptAutoExpandReplicas` (`AllocationService.java:558`) constructs a **second**, read-only
   `RoutingNodes` and scans every index parsing the auto-expand setting.

**A flaw in the S6 measurement.** Gradle test runs enable assertions (`-ea -esa`, set by
`OpenSearchTestBasePlugin.java:133`); production `jvm.options` does not. The S6 figures therefore
include `RoutingNodes.assertShardStats` (two full shard passes plus a `HashSet` per `ShardId`,
`RoutingNodes.java:1251-1310`) and `RoutingNode::invariant` (three stream+collect passes per node,
`RoutingNode.java:499-524`). See the corrected table in S6 above.

**Incremental `RoutingNodes`: feasible in principle, blocked by ownership not algorithm.** The
derived aggregates are already maintained incrementally (`recoveriesPerNode`, `relocatingShards`,
`inactivePrimaryCount` via `updateRecoveryCounts`, `RoutingNodes.java:192-240`), `assignedShards` is
naturally diff-addressable, and `ShardRouting` object identity survives a RoutingTable rebuild so a
diff can be identity-based. What blocks reuse: allocation **mutates `RoutingNodes` in place**
(`initializeShard`/`relocateShard`/`startShard`/`failShard` at `:556, 584, 613, 677`), and
`AllocationService` is deliberately stateless — `reroute(ClusterState, reason)` accepts an arbitrary
state, and no field holds the previous `RoutingNodes`. `RoutingTableIncrementalDiff` already computes
exactly the needed diff, but for remote-state publication, *after* allocation.

**Domain-scoped allocation: partition nodes, not indices.** Per-`shardId` deciders are domain-safe
(`SameShardAllocationDecider`, `AwarenessAllocationDecider`, `FilterAllocationDecider`). What breaks
under index-partitioned domains is all per-*node* aggregate state: the weight function itself
(`theta0 * (node.numShards() - avgShardsPerNode())`, `BalancedShardsAllocator.java:662-666`) would
have each domain drive the same node toward its own average and oscillate; plus
`ShardsLimitAllocationDecider`'s cluster-total-per-node, `DiskThresholdDecider.java:203-205`, and
`ThrottlingAllocationDecider`'s global recovery counters. The existing `RoutingPool` precedent works
precisely because the **node sets** are disjoint, not the index sets — that is the shape any further
domain scheme must take.

**The most promising concrete optimization.** Hoist the delta check above the `relevantNodes` decider
loop in `balanceByWeights()` (`LocalShardsBalancer.java:361-381`). `buildWeightOrderedIndices()`
already computes `deltas[i]` per index over all nodes (`:509-512`) and then **discards them**. Since
`weight(node, index)` does not depend on which node subset is being sorted, the all-node delta is an
upper bound on the relevant-node delta — so an index below threshold provably cannot move, and can be
skipped. This eliminates up to indices × nodes decider-chain calls in the balanced case (~800k at
40k/20). Estimated 30-50% of steady-state reroute, to be confirmed by profiling. Second-cheapest:
guard `adaptAutoExpandReplicas` on whether any index actually sets `auto_expand_replicas`.

**Dead ends identified.** Incremental RoutingTable rebuild for steady state (already short-circuited
at `:573`; it only helps the small-delta `applyStartedShards` path). Sampling a subset of indices in
`balanceByWeights` — the ordering at `:485-497` exists specifically to prevent over-allocation onto
new nodes, and dropping indices reintroduces that bug; the delta-skip above is safe *because* it only
drops indices that provably cannot move. And the allocator timeout is not a brake at all: at 100k
shards it converts one 53 s reroute into a loop of 20 s reroutes that never converge.

---

## What was implemented off the back of these spikes

Four core changes, each measured and each with a test verified to actually catch its failure mode
(for a behaviour-preserving optimization a green suite proves nothing on its own -- it passes with
and without the change -- so each was deliberately broken to confirm the guard fails, then restored):

| change | what it removes | evidence |
|---|---|---|
| `MetadataDiff.apply` takes the reuse path | full `indicesLookup` rebuild on every node, every metadata change | S7 found the naive fix resurrects deleted indices; fixed via a non-seeding builder path |
| `RoutingNodes.localRoutingNode` | cluster-wide `RoutingNodes` allocation on every data node, every applied state | S6 measured 43-52 ms/state at 40k shards |
| `balanceByWeights` weight-spread skip | ~800k redundant decider calls per reroute | steady-state 445/355 ms -> 287 ms at 40k shards |
| auto-expand-replicas guard | a *second* cluster-wide `RoutingNodes` build + per-index settings parse, every reroute | forcing the guard on fails 3 of 5 `AutoExpandReplicasTests` |
| batched index creation (C7) | N cluster-state cycles for N index creations, collapsed to one | breaking the fold fails `CreateIndexBatchingTests` |

All five are pre-existing waste in current OpenSearch rather than scaling-only concerns; the
tenant-scale investigation simply made them visible.

Two lessons from the C7 change are worth carrying forward. First, its focused suites passed 2,400
tests while a wider sweep found 10 failures: `ClusterStateChanges` mocked only the singular
`submitStateUpdateTask`, so batched submissions were silently dropped. Run a wider net than the
change appears to touch. Second, the first batching test was **vacuous** -- breaking the fold did
not fail it, because that path submits one task per call and the batch is always size one. Only
driving the executor directly with a real multi-task batch tested anything. For a behaviour-
preserving change, a green suite proves nothing until you have broken the code and watched the test
fail.

## Recalibration: C2's headline number does not survive S6

S4 measured mapping dedup at up to 1000x on cluster state, and the plan quoted that as ~50 GB -> under
1 GB. That figure assumes 100M indices. S6 then established the allocator caps a cluster around 100k
*active shards*, so 100M indices is not reachable in one cluster and the saving must be read at
attainable scale:

| indices | mapping bytes/node (470 B each, S4) | saved by dedup |
|---|---|---|
| 10,000 | 4.5 MB | ~4.5 MB |
| 40,000 | 17.9 MB | ~17.9 MB |
| 100,000 | 44.8 MB | ~44.8 MB |
| 100,000,000 | 43.8 GB | ~43.8 GB (not reachable) |

So C2 is worth roughly **tens of MB per node** at reachable scale -- real, and it applies to every
node, but not the order-of-magnitude win the headline implied. Combined with S3's finding that dedup
does nothing for the parsed `MapperService` graph (0.08% effect), this moves C2 below C7
(provisioning throughput, which matters at any scale) in priority. Recorded here because the
1000x figure would otherwise justify a wire-format change on false pretences.

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
  allocator that binds around 100k active shards, keeping quiescent tenants out of both
  the routing table and the resident set is what makes any of this work.
