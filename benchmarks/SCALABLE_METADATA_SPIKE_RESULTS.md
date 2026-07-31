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

Six core changes, each measured and each with a test verified to actually catch its failure mode
(for a behaviour-preserving optimization a green suite proves nothing on its own -- it passes with
and without the change -- so each was deliberately broken to confirm the guard fails, then restored):

| change | what it removes | evidence |
|---|---|---|
| `MetadataDiff.apply` takes the reuse path | full `indicesLookup` rebuild on every node, every metadata change | S7 found the naive fix resurrects deleted indices; fixed via a non-seeding builder path |
| `RoutingNodes.localRoutingNode` | cluster-wide `RoutingNodes` allocation on every data node, every applied state | S6 measured 43-52 ms/state at 40k shards |
| `balanceByWeights` weight-spread skip | ~800k redundant decider calls per reroute | steady-state 445/355 ms -> 287 ms at 40k shards |
| auto-expand-replicas guard | a *second* cluster-wide `RoutingNodes` build + per-index settings parse, every reroute | forcing the guard on fails 3 of 5 `AutoExpandReplicasTests` |
| batched index creation (C7) | N cluster-state cycles for N index creations, collapsed to one | breaking the fold fails `CreateIndexBatchingTests` |
| `IndexMetadataHolder` storage split (C5) | the requirement that every index in cluster state be fully materialized | 2,944 -> 698 B/index deferred, no measurable cost materialized; see below |

The first five are pre-existing waste in current OpenSearch rather than scaling-only concerns; the
tenant-scale investigation simply made them visible. C5 is the exception -- it is the one change here
that exists only for the tenant-scale case, and it changes no behaviour on its own.

Two lessons from the C7 change are worth carrying forward. First, its focused suites passed 2,400
tests while a wider sweep found 10 failures: `ClusterStateChanges` mocked only the singular
`submitStateUpdateTask`, so batched submissions were silently dropped. Run a wider net than the
change appears to touch. Second, the first batching test was **vacuous** -- breaking the fold did
not fail it, because that path submits one task per call and the batch is always size one. Only
driving the executor directly with a real multi-task batch tested anything. For a behaviour-
preserving change, a green suite proves nothing until you have broken the code and watched the test
fail.

## Cumulative effect of the shipped changes, and a correction

40k shards, steady-state reroute, assertions disabled:

| | samples | mean |
|---|---|---|
| baseline | 445, 355 ms | 400 ms |
| after all shipped changes | 221, 276, 410 ms | 302 ms |

**~24% mean improvement, not the ~45% first reported.** That earlier figure came from a single
favourable sample; the run-to-run spread here is 1.9x (221-410 ms on identical code), which is wide
enough to swamp the effect being measured. Reporting the minimum was cherry-picking, and is recorded
here as a correction rather than quietly amended.

The practical lesson for anyone using `AllocationCeilingSpikeTests`: it resolves order-of-magnitude
differences (the superlinear cold-allocation curve, the assertions-on/off 2x) but not changes of
tens of percent. Those need many samples or a different harness.

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
node, but not the order-of-magnitude win the headline implied. Expressed as a share of a
cluster-manager heap, which is what actually decides whether it is worth building:

| indices | saving | % of 16 GB heap | % of 32 GB heap |
|---|---|---|---|
| 10,000 | 4.5 MB | 0.03% | 0.01% |
| 40,000 | 17.9 MB | 0.11% | 0.05% |
| 100,000 | 44.8 MB | 0.27% | 0.14% |

**Verdict: do not build it.** Even the wire-format-free variant (interning identical
`CompressedXContent` instances at construction) means putting a global mutable cache into a core
class, with the attendant concurrency and lifetime questions, to reclaim a quarter of a percent of
heap in the best case reachable and a hundredth of a percent typically. The full `mappingsByHash`
version additionally carries BWC cost. This is recorded as a measured decision rather than a
judgement call so it does not get re-argued from the 1000x figure. Combined with S3's finding that dedup
does nothing for the parsed `MapperService` graph (0.08% effect), this moves C2 below C7
(provisioning throughput, which matters at any scale) in priority. Recorded here because the
1000x figure would otherwise justify a wire-format change on false pretences.

## S10 -- C5's open mechanism question, answered

`rfc-serverless-control-cell-diet.md`'s adversarial review left C5 undecided: making `Metadata` stop
holding a fully-materialized `IndexMetadata` per index needs its storage to become a union of the
real object and a lazily-resolving stub, and it was not established whether that union can be
free for indices that never opt in. `StubResolutionSpike` asks exactly that, in isolation, touching
nothing in `server/`.

**Heap: strongly favourable.**

| | retained | per index |
|---|---|---|
| fully materialized | 395.7 MB | 2,074 B |
| all stubs, unresolved | 27.9 MB | **146 B** |

A **14.2x** reduction, with zero resolutions triggered -- stubs genuinely stay lazy.

**Access cost: small but not zero.** Reading an *already-resolved* entry through the union, over four
runs of 20M lookups each: +17%, +3.2%, +6.2%, +6.1%. The first is an outlier; the settled figure is
**~5-6%**, about 0.2 ns per lookup (3.4 -> 3.6 ns). One volatile read and a null check, on the path
every index lookup in OpenSearch takes.

**What this decides.** The amendment's constraint 1 -- "zero behaviour or cost change for indices
that don't opt in" -- is **not met**, and as literally written would reject this design. So C5's
decision is no longer "is there a mechanism"; it is:

- accept ~5-6% on resolved index lookups in exchange for 14.2x on metadata heap, relaxing
  constraint 1 to "negligible" rather than "zero"; or
- find a shape that avoids the indirection on the resolved path (harder -- the indirection *is* the
  mechanism); or
- decline C5 and accept that per-index residency stays as it is.

Whether 0.2 ns/lookup matters depends on lookup volume per request, which this spike does not
measure and which should be established before choosing. But the mechanism question itself is
closed: it works, it is cheap, and it is not free.

**Sizing the implementation, since "large" was doing a lot of work.** `Metadata.index(name)` can
resolve internally and keep returning `IndexMetadata`, so its **120** callers are untouched. What
does change is everything reaching the raw map: **41** call sites in `server/` main, **51** in tests
and the test framework, **9** across plugins and modules, plus `iterator()`, serialization and the
diff path -- and **57** internal uses inside `Metadata.java` itself. Call it ~158 external sites plus
the internals.

**And the obvious de-risking move does not work.** The tempting first slice is to introduce the union
with every entry pre-resolved, so no stub exists and behaviour is provably identical. But that
delivers **zero benefit on its own** -- all entries materialized means no heap saved -- while adding
indirection to the most central class in the codebase. Shipped standalone it is pure cost with
deferred payoff, and pure cost permanently if the follow-on never lands. So C5 does not decompose
into a safe, independently-valuable first step; it wants to be done as one reviewed piece of work
with the trade already accepted.

## C5 -- implemented, and two of S10's numbers corrected

S10 measured a union in which every entry is a wrapper object holding the `IndexMetadata`. Two of its
conclusions were artifacts of that shape rather than of C5:

- resolved lookups cost +5-6%;
- "C5 does not decompose into a safe, independently-valuable first step", because introducing the
  indirection with everything pre-resolved would be pure cost.

The shape that shipped has no wrapper. `IndexMetadata` implements the new `IndexMetadataHolder`
interface itself and returns `this` from `get()`, so a materialized index is what sits in the map --
nothing to allocate, nothing extra to traverse. That makes the pre-resolved state free, which is
exactly the property S10 said C5 lacked, and it is what allows the change to ship ahead of C6.

**Cost on a materialized index: below the noise floor.** `HolderLookupCostBenchmark`, 15 alternating
rounds of 20M lookups, both arms in the same JVM:

| run | direct `map.get(...)` | through `holder.get()` | delta |
|---|---|---|---|
| 1 | 3.240 ns | 3.178 ns | -0.063 ns (-1.9%) |
| 2 | 3.160 ns | 3.157 ns | -0.003 ns (-0.1%) |

Round-to-round spread was 0.25-0.49 ns, so neither delta means anything except "smaller than the
noise". The first version of this benchmark compared `Metadata.index` across two JVMs, one on the
pre-change tree and one after: 5.30 ns before, 5.13 ns after, with individual rounds ranging
4.87-7.03 ns for identical code. Same conclusion, but with the spread roughly 10x the effect being
measured, so the within-process A/B is the number to cite and the cross-JVM one is not evidence of
anything.

**Saving: 4.2x, not 14.2x.** S10 measured a bare `HashMap` of minimal stubs and got 2,074 -> 146
B/index. `DeferredMetadataHeapEstimate` measures a real `Metadata` holding real `LazyIndexMetadata`,
100k indices with one alias each -- the shipped thing rather than an upper bound:

| | retained | per index |
|---|---|---|
| materialized | 280.8 MB | 2,944 B |
| deferred | 66.6 MB | 698 B |

The gap between 146 B and 698 B is what S10's isolation left out, in two parts. The descriptor has to
carry everything `Metadata`'s build path reads -- aliases, the hidden and system flags, the shard
count, the routing-pool inputs -- not just a name and a loader. And `Metadata` keeps per-index state
that deferral does not touch at all: an `IndexAbstraction` per index and per alias, entries in the
derived name arrays, and the map and `indicesLookup` entries themselves.

**What stays lazy.** Building metadata, rebuilding the derived name arrays, building `indicesLookup`,
resolving names and aliases, computing a diff, and applying a diff all leave an untouched index
untouched. Diff computation compares holders, so an index both cluster states share is settled by
reference without loading; diff application walks the deletes/diffs/upserts directly and loads only
what actually changed. `DeferredIndexMetadataTests` asserts each of these with a loader that counts
calls, and each assertion was confirmed to fail when the corresponding production change is undone.

**What does not.** Serialization writes resolved metadata, so the wire format is byte-identical and
writing out a full `Metadata` loads everything -- correct, since that is a full-state publication.
Data-stream backing indices are materialized when the lookup is built, because
`IndexAbstraction.DataStream` is `@PublicApi` and takes `IndexMetadata`; concrete indices and aliases,
which is nearly all of them, are not.

**What this is not.** No core path installs a deferred index, so nothing changes for an existing
cluster. The 4.2x is available to a metadata store that can fetch one index at a time -- C6 -- and is
not realized by this change on its own. What this change buys is that C6 no longer needs to touch
`Metadata`: `Metadata.Builder#putStub` is the seam, and `LazyIndexMetadata` is a working
implementation of it.

**Since superseded on that last point.** C6's first two pieces are now implemented: the index descriptor
rides in the cluster metadata manifest at `CODEC_V5`, and the full-state read path installs a deferred
holder rather than fetching the blob. So there is a caller, behind two off-by-default settings
(`...index_metadata.descriptor.enabled` on the writer, `...index_metadata.defer.enabled` on the reader).
What that changes is the cost of a **full-state read** -- a node joining or a cluster-manager restarting
no longer fetches one blob per index in the cluster. The diff path was already unaffected, since an
unchanged index's holder is carried forward by reference and never needed a descriptor.

## What C6's descriptor costs the manifest

The manifest is the one file rewritten in full on **every** cluster state version, so anything added to
it is a recurring write cost rather than a one-off. C6 roughly doubles each index entry, which is worth
knowing before turning the descriptor on at scale. `ManifestDescriptorSizeEstimate` measures it.

**Uncompressed, one entry:**

| aliases | without | with | increase |
|---|---|---|---|
| 0 | 149 B | 271 B | +82% |
| 1 | 149 B | 307 B | +106% |
| 3 | 149 B | 381 B | +156% |

**Compressed, which is what actually gets written.** 100k entries, one alias each, DEFLATE:

| | without | with | increase |
|---|---|---|---|
| manifest | 0.5 MB | 0.8 MB | **+64%** |

Two things about that +64%. It is much better than the uncompressed +106%, because the entries share a
name prefix, repeat every field name, and mostly repeat the same booleans. And it is much *worse* than
the +17% a first version of this measurement produced, which reused a single descriptor across all
100k entries and so compressed a redundancy no real fleet has. Distinct alias names per index is the
number to plan from.

**What it means.** At 100k indices the manifest goes from about 0.5 MB to about 0.8 MB per cluster
state version. The descriptor is affordable; what is not obviously affordable is the 0.5 MB that was
already there, since both scale linearly with index count and both are rewritten on every version. C6
makes a pre-existing O(N)-per-version cost about 1.6x worse rather than introducing a new one.

That raises manifest sharding -- C6's third piece, previously described here as gating nothing -- from
optional to the thing that decides whether either half is usable at 100k+ indices. It is still
independent of the descriptor work and can be done separately.

## C3a -- what manifest sharding saves, before writing any of it

C2 attached a stop condition to its go decision: the saving is bounded by how many indices change per
cluster state version, not by index count. `ManifestShardingWriteAmplificationEstimate` measures it.
Nothing needs implementing first -- bytes written are fully determined by the design (top-level
manifest plus every shard holding at least one changed index), so simulating the hash partition is
exact.

100,000 indices with C6 descriptors, DEFLATE per blob. Unsharded is **882 KB** on every version.

| changed indices | 64 shards | 256 shards | 1,024 shards | 4,096 shards |
|---|---|---|---|---|
| 1 | 55x less | **125x less** | 61x less | 16x less |
| 10 | 5.8x less | **22x less** | 37x less | 15x less |
| 100 | 9.5% MORE | 2.4x less | **7.4x less** | 9.3x less |
| 1,000 | 9.5% MORE | 7.2% MORE | 22% MORE | **1.9x less** |
| 10,000 | 9.5% MORE | 7.2% MORE | 25% MORE | 92% MORE |

**Verdict: go, and C2's default of 64 was wrong.** 64 saturates at 100 changed indices -- every shard
is touched, and the sharded write is then 9.5% *worse* than not sharding, because N/S entries compress
slightly worse per entry than N do. That saturation is a coupon-collector effect and it arrives much
earlier than "changed indices ≈ shard count" intuition suggests.

**256 is the right default.** It gives 125x at one changed index and 22x at ten, which is the steady
state this is for, and still wins at 100. Raising it further trades the common case away: at 4,096
shards a single changed index costs 55 KB, because the top-level manifest listing 4,096 references is
itself the floor. That floor is the reason the curve is not monotonic in shard count, and it is why a
measurement was worth doing rather than reasoning about the partition alone.

**What this does not settle.** The right shard count depends on churn, and nobody has measured churn on
a real fleet. 256 is chosen for a steady state of single-digit changed indices per version. A cluster
doing bulk index creation lives at the right-hand end of the table, where sharding costs a few percent
-- acceptable, but it means the setting exists for a reason and D2's "off by default" stands until a
real churn distribution is known.

## A6 -- what a quiescent tenant costs the allocator today

The A4 spike found that scale-to-zero does not remove an index from the routing table. It evicts the
shards and holds them down with a decider that says no, so they stay present and `UNASSIGNED` and are
re-evaluated on every reroute. The entire case for making cold indices routing-absent rests on what
that costs, and nobody had measured it.

`ColdIndexRerouteCostSpikeTests`. 500 active indices, 20 nodes, one replica, assertions disabled.
`index.routing.allocation.enable: none` stands in for `SuspendedShardAllocationDecider`: same shape,
no plugin needed. Two samples:

| cold indices | held down, steady reroute | absent, steady reroute |
|---|---|---|
| 0 | 9 / 12 ms | 6 / 6 ms |
| 2,000 | 25 / 24 ms | 7 / 11 ms |
| 10,000 | 154 / 171 ms | 9 / 9 ms |

**Held-down cost is linear in cold indices; absent is flat.** Roughly 15 ms per 1,000 cold indices per
reroute in today's shape, against nothing measurable when the index is not in the routing table. At
10,000 cold indices that is a 17x difference on the steady-state reroute, and the reroute is paid on
the cluster-manager on every cluster state change.

The `RoutingNodes` rebuild tracks it as expected (1 ms -> 6-8 ms held down, 0-1 ms absent), because it
is O(shards in the routing table) and the held-down shards are in it.

**What this settles.** Phase A's premise holds and is worth the work: cold-absence removes essentially
all of the cold-tenant allocator cost rather than reducing it. Extrapolating the linear fit, 100k cold
indices would be roughly 1.5 s of steady-state reroute per cluster state change, which is not a
workable control plane. That is the wall scale-to-zero currently runs into, and it is separate from
the ~100k *active* shard ceiling S6 measured.

**What it does not settle.** The stand-in is a decider that refuses allocation, not
`SuspendedShardAllocationDecider` itself, and the active set is small. The shape of the curve is the
result, not the constant.

### A6 second half -- the saving tracks quiescent *tenants*, not quiescent shards

The comparison above is all-or-nothing: every cold index is either held down or absent. Real fleets
are not. A5.2 only prunes an index that is cold in every shard and every configured role, because the
routing entry is per-index while suspension is per-shard-per-role. An index with one still-hot shard
keeps its entry indefinitely, and its suspended shards go on being held down forever.

So the question an operator actually has is what fraction of *tenants* go fully quiescent. Cold
population fixed at 10,000, varying only that fraction:

| fully cold | steady reroute | shards in routing |
|---|---|---|
| 0% | 369 ms | 21,000 |
| 50% | 195 ms | 11,000 |
| 90% | 41 ms | 3,000 |
| 100% | 17 ms | 1,000 |

**Linear in the fraction, with no threshold effect in either direction.** Half the tenants fully
quiescent gives about half the saving; there is no point at which pruning starts paying off sharply,
and no point past which it stops helping. That is the useful shape, because it means the feature can
be evaluated on a partial rollout and the result extrapolates.

The absolute numbers in this run are higher than the table above (369 ms at 0% against 154-171 ms for
the same shape) because the two runs were taken on a differently loaded machine. Compare ratios across
rows within a run, not milliseconds across runs; the 100% column reproduces the roughly 20x of the
all-or-nothing comparison.

**What this changes.** The 17x headline is the best case, not the expected case. A fleet where
suspension is spread thinly across many partly-idle indices saves nothing at all, and would still pay
the full held-down cost while carrying all of A5's lifecycle risk. Whether that fleet shape occurs is
a workload question this cannot answer, and it belongs in the D2 defaults decision.

## What the plan got wrong

| plan claim | measured | effect |
|---|---|---|
| descriptor ~200 B | 289 B no-alias, 577 B with one alias | paging math re-based; aliases need a flat array |
| ~10M indices/cluster, metadata-limited | allocator binds first, at tens of thousands of *active shards* | cells required, sized by active shards not tenants |
| "cells become optional" (said mid-discussion) | wrong | withdrawn |
| parsed mapper graph 30-80 KB | 45-237 KB, ~255× compressed | data-node capacity is tighter than assumed |
| C1 is a trivially safe first PR | safe only with a non-seeding builder path | still first, but needs a specific implementation |
| dedup helps broadly | cluster state only, 0.08% effect on parsed graph | scope narrowed, value confirmed for its actual scope |
| S10: stub union costs +5-6% on resolved lookups | 0%, once `IndexMetadata` is its own holder | C5 shippable ahead of C6 |
| S10: deferral saves 14.2x | 4.2x in a real `Metadata` | still large, but the headline was an isolated-map upper bound |

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

## S11: shard-count scaling, and why every earlier number understated the problem

Every estimate above fixed the shard count at 1, with two at 3. That quietly made "per index" and "per
shard" the same number. The stated target is 100M indices at 3-30 shards each, which is 300M to 3B
shards, so they are not the same number and the per-shard dimension had never been measured.

`ShardScalingRetainedHeapEstimate` sweeps it. Retained heap, 100k indices per point, assertions off:

| shards | full (IndexMetadata + RoutingTable) | no routing entry | descriptor only |
|---|---|---|---|
| 1 | 3,652 B | 2,387 B | 175 B |
| 3 | 5,193 B | 2,600 B | 195 B |
| 10 | 10,540 B | 3,296 B | 192 B |
| 30 | 26,594 B | 5,872 B | 193 B |

Extrapolated to 100M indices at 30 shards: **2,477 GiB** full, **547 GiB** with the routing entry gone,
**18 GiB** descriptor-only.

**The descriptor is flat.** It carries `totalNumberOfShards` as an `int`, and the measurement confirms
nothing reachable from it scales per shard -- 175 B at one shard, 193 B at thirty. That was the
assumption C5 and C6 were built on and it now has a number behind it rather than a reading of the field
list.

**Routing absence is necessary and not sufficient.** Dropping the routing entry is the single largest
structural saving, and it grows with shard count: 1.5x at one shard, 4.5x at thirty. But 547 GiB is
still not a heap. Phase A was scoped as the enabling change for scale-to-zero; at this target it is a
precondition for deferral rather than an alternative to it.

**What changed in the conclusion.** Earlier framing treated C5/C6 deferral as a large optimization on
top of a workable design. At 100M indices with real shard counts it is the only representation that
fits at all, and the gap widens as shards grow -- 21x at one shard, 138x at thirty. The 100M target is
not reachable by making the resident representation smaller; it is reachable only by not making it
resident.

**Cell sizing, re-based.** At 18 GiB for 100M descriptor-only, a 10M-index cell costs about 1.8 GiB of
index metadata. That is affordable. The binding constraint stays where S6 put it -- the allocator, at
tens of thousands of *active* shards -- which at 3-30 shards per index means a cell's active tenant
count is bounded an order of magnitude below its resident tenant count.

**What this does not settle.** Replicas are zero here. Each in-sync copy adds an allocation id per
shard to `inSyncAllocationIds` and a `ShardRouting` to the routing table, so the `full` and
`norouting` columns are floors, not estimates. The descriptor column is unaffected by replica count for
the same reason it is unaffected by shard count.

### S11b: the 18 GiB figure was isolated-object, and the real one is 65 GiB

S11 measured `IndexDescriptor` on its own. That is the right way to ask "is the descriptor flat in
shard count" and the wrong way to ask "what does a tenant cost", because `Metadata` keeps per-index
state deferral never touches: an `IndexAbstraction` per index and per alias, entries in the derived
name arrays, and the map and `indicesLookup` entries themselves. S10 had already been corrected once
for exactly this (14.2x isolated -> 4.2x real), and S11 walked into the same trap.

`DeferredMetadataHeapEstimate`, now parameterized by shard count, measures a real `Metadata` with one
alias and one replica per index:

| shards | materialized | deferred | reduction |
|---|---|---|---|
| 1 | 2,743 B/index | 698 B/index | 3.9x |
| 3 | 2,939 B/index | 700 B/index | 4.2x |
| 30 | 6,213 B/index | 698 B/index | 8.9x |

**Deferred cost is flat in shard count** -- 698, 700, 698 -- which is the property the whole design
rests on, now measured in a real `Metadata` rather than inferred from an isolated descriptor. The
reduction grows with shards only because the materialized side grows; the deferred side does not move.

**100M indices cost about 65 GiB of resident index metadata**, not 18 GiB. Use 698 B/index, flat,
for all cell sizing. The 193 B descriptor figure is a component of that number, not a substitute for it.

**This does not change the direction, only the arithmetic.** Deferral is still the only representation
that fits, and it is still flat where every alternative is linear in shards. But 65 GiB does not fit in
one heap, so cells stop being a scaling nicety and become arithmetic: even a generous 16 GiB metadata
budget per cluster-manager holds about 24M tenants, which is four cells for 100M before any other
constraint is considered.

## S12: computed placement, and the ceiling S6 found does not apply to it

The target moved to 100M indices at up to 100 shards each, so up to 10B shards. S6 put the allocator's
ceiling at the order of 100k *active* shards. Those two numbers are five orders of magnitude apart, and
no amount of tuning closes that. The question is whether placement can stop being allocated and start
being computed.

First, the deferred cost holds at the new shard count. `DeferredMetadataHeapEstimate` at 100 shards:

| shards | materialized | deferred | reduction |
|---|---|---|---|
| 1 | 2,743 B/index | 698 B/index | 3.9x |
| 30 | 6,214 B/index | 698 B/index | 8.9x |
| 100 | 15,522 B/index | **698 B/index** | 22.2x |

Flat to three significant figures across a hundredfold change in shard count. 100M indices cost 65 GiB
deferred against 1,446 GiB materialized.

**`ComputedPlacementSpike` measures rendezvous (highest random weight) placement**, K=3 candidates per
shard, 1M shards sampled:

| nodes | lookup | distribution (min/max vs ideal) |
|---|---|---|
| 10 | 80 ns | 0.99x / 1.00x |
| 50 | 168 ns | 0.99x / 1.02x |
| 200 | 359 ns | 0.95x / 1.03x |

Membership change, measured as the fraction of shards whose primary moves and the fraction that lose
*every* previously-warm candidate:

| nodes | change | primary moves (ideal) | lost all K warm |
|---|---|---|---|
| 200 | +1 | 0.50% (0.50%) | **0.000%** |
| 200 | +100 | 33.29% (33.33%) | 3.614% |
| 200 | +200 | 49.96% (50.00%) | 12.373% |

**Placement quality is at the theoretical optimum.** Primary moves track `added/(N+added)` to two
decimal places at every size, and distribution stays within 5% of even at 200 nodes. There is no tuning
knob here to get wrong, which is the main practical advantage over a token ring.

**Incremental scaling is free.** A single join leaves *no* shard without a warm candidate, and that is
structural rather than lucky: one new node can displace at most one of K, so K-1 warm holders always
survive. Autoscaling one node at a time costs nothing in cold reads.

**Large jumps are the case that needs help.** Doubling the fleet leaves 12.4% of shards with no warm
candidate at all. That is the deploy, zone-recovery and capacity-doubling case, and it is the argument
for promoting the pre-warm task from optional to required: it was optional when placement was
allocator-driven and moves were rare, but under computed placement every large scale event triggers it.

**What this does to the ceiling.** The comparison is not lookup-versus-lookup. A stored routing table
read is a hash map hit, call it 20-50 ns, so computed placement is several times *slower* per request
at 359 ns. That is irrelevant next to a millisecond-scale search. What matters is that the allocator
must do a global pass over every shard against every node, superlinearly, and computed placement never
does one. The 10B-shard global pass that S6's curve says is impossible is not made faster here; it is
never executed.

**What this does not settle.** A hash does not know that one tenant takes a thousand times the traffic,
nor that a node's cache is full. K=3 gives room to choose among candidates rather than solving it, and
the choosing needs a signal: OpenSearch's existing adaptive replica selection ranks by service time and
queue depth, and a cold node reads as a slow node, so it should drift traffic toward warm replicas on
its own. That inference is untested here. Writers are unaffected either way, since a writer must be
single-owner and `ShardHead` CAS already fences that in the object store rather than in cluster state.

## S13: the global name index, the one part that cannot be partitioned

Everything else in the partitioned design scales by hashing on the index: descriptors are fetched on
demand and cached by whoever the hash names, and placement is computed rather than stored (S12).
Wildcard and alias resolution cannot work that way. Answering `logs-*` requires knowing every index
name, which is global by construction. Either one tier holds all 100M names, or every wildcard scatters
to every partition.

`NameIndexSpike` measures whether holding them is affordable, 2M names sampled:

| representation | per name | at 100M |
|---|---|---|
| `HashMap<String, byte[]>`, the shape core uses | 145.5 B | 13.6 GiB |
| compact: sorted name blob + offsets, raw 16-byte UUIDs, state byte | **45.0 B** | **4.2 GiB** |

**4.2 GiB holds the entire name space for 100M indices**, which fits in a single node's heap with room
to spare. The gap between the two rows is almost entirely per-entry object overhead -- a `String` header,
its hash field, its backing array, and a map node, none of which survive when names live in one sorted
byte array addressed by offsets.

Prefix resolution against that structure is a binary search followed by a forward scan:

| query | matches | time |
|---|---|---|
| `tenant-0000*` | 65,536 | 3.2 ms |
| `zzzz-no-such-prefix*` | 0 | 9.2 us |

**Cost is proportional to matches, not to the size of the index.** A miss costs microseconds against a
2M-name structure, and a wildcard returning 65k indices costs milliseconds. That is the property that
makes a single global tier viable rather than a bottleneck.

**What this settles.** The highest-risk item in the partitioned design was that wildcards resist
partitioning and might force either a scatter to every partition or a restriction on the query language.
Neither is necessary. A dedicated name-index tier is small enough to replicate rather than shard, which
also removes it as a scaling concern: replicate for availability, not for capacity.

**What this does not settle.** Only prefix wildcards were measured, because they are what a sorted
structure answers directly. Leading wildcards (`*-logs`) degenerate to a full scan of 100M names and
need either a second structure indexed on reversed names or an explicit restriction. Aliases are not
modelled here at all -- an alias is a name pointing at a set of indices, so the same structure serves it,
but the fan-out on resolution was not measured. Updates are also unmeasured: the compact form is built
sorted and is not designed for insertion, so index creation needs either periodic rebuild plus a small
overlay of recent changes, or a different structure entirely.

## S14 (C11): computed placement measured against the allocator it replaces

S12 measured the placement *algorithm* in isolation and found it optimal. This measures the thing that
actually replaces the allocator: building routing entries for a growing index population.

`ComputedPlacementCostTests`, 50 nodes, 10 shards per index, K=3:

| indices | shards | total | per shard |
|---|---|---|---|
| 1,000 | 10,000 | 42 ms | 4,241 ns |
| 4,000 | 40,000 | 171 ms | 4,296 ns |
| 16,000 | 160,000 | 558 ms | 3,491 ns |

**Per-shard cost is flat.** The ratio across a 16x population increase is 0.82, and the slight fall is
JIT warmup rather than a real improvement. S6's allocator over a comparable range went from 25 ms at
2,000 shards to 445 ms at 40,000 -- a 4.5x rise in per-shard cost -- which is the superlinear curve that
puts 10B shards out of reach.

**Against S6 at the same shard count**: 40,000 shards cost the allocator 4,560 ms of cold allocation and
cost this 171 ms. That is 27x, but the multiple is the least interesting part of the result.

**A single index costs the same regardless of cluster size**: 35,277 ns alone, 32,723 ns after 16,000
other indices exist. The allocator cannot say this, because its pass is over the whole cluster, and it is
this property rather than the constant factor that removes the ceiling.

**What this does not claim.** It is evidence about the algorithm, not about a running cluster; no cluster
was started. And 27x understates the real difference in a way worth being precise about: building N
entries at once is the pessimistic case, and under this design it never happens. A serverless index
publishes no routing entry, so there is nothing to allocate at startup and nothing to reallocate on a
membership change. The honest statement is not that the global pass got faster but that it stopped
existing, and the flat per-shard curve is what makes that credible rather than the ratio.

## S15 (G1): publication latency, and the batching decision it settles

The plan called this the one unmeasured number that changes a decision: whether wake and sleep need the
`ClusterStateTaskExecutor` batching that C7 gave create-index. They do, by between one and two orders of
magnitude.

Measured with `PublicationLatencyMeasurementIT`, which submits N cluster state updates and times them to
completion. An integration test rather than a single-JVM spike, because publication is a property of
coordination rather than of a data structure: the elected manager computes the state, sends it to every
node, and waits for acknowledgements.

### The numbers

Five runs. The framework randomises the node count, so cluster size is recorded per run rather than
chosen. Per-publication figures are at batch=100.

| nodes | unbatched, per publication | batched, per task | publications for 100 tasks | ratio |
|---|---|---|---|---|
| 1 | 6.3 ms | 0.33 ms | 100 vs 2 | 19x |
| 2 | 51.7 ms | 0.96 ms | 100 vs 2 | 54x |
| 3 | 12.8 ms | 0.37 ms | 100 vs 2 | 35x |
| 4 | 11.1 ms | 0.24 ms | 100 vs 2 | 46x |
| 4 | 11.9 ms | 0.22 ms | 100 vs 2 | 54x |

The absolute spread is wide, and the 2-node run at 51.7 ms is an outlier from a loaded machine rather
than a property of two nodes. It does not matter to the decision: what is consistent across every run is
the shape. Unbatched cost is linear in the number of tasks because each one publishes. Batched cost is
nearly flat because a hundred tasks collapse into two publications, so total time barely moves between a
batch of 10 and a batch of 100.

### What it means for wake and sleep

`TransportReactivateShardsAction` and `ShardSuspensionCoordinator` submit plain `ClusterStateUpdateTask`s
with no executor, so each shard woken or slept costs a full publication, measured here at roughly 6 to 13
ms on an idle small cluster. Waking a thousand shards is a thousand publications. Batched, the same
thousand would be a handful.

**Decision: both need an executor.** This is not a marginal optimisation to weigh against complexity, and
the mechanism already exists and is already used by create-index.

### Priority starvation is real, and worse than the latency

A NORMAL task submitted behind 300 already-queued URGENT tasks waited:

| nodes | wait |
|---|---|
| 1 | 2,339 ms |
| 2 | 14,807 ms |
| 3 | 2,934 ms |
| 4 | 3,898 ms |

The plan predicted this, from reactivation running at `Priority.URGENT` and suspension at
`Priority.NORMAL`. Confirmed: seconds, not milliseconds, and it grows with the pressure.

The first version of this measurement submitted the NORMAL task *first* and reported 24 ms, which says
nothing at all: the queue was empty when it arrived. The claim is about a sleep waiting behind sustained
wake traffic, so the wake traffic has to already be queued. Worth recording because the corrected number
is 100x the original and the original looked perfectly reasonable.

Batching addresses this too, and more directly than a priority change would. Suspension starves because
each wake occupies the queue for a full publication; collapsing wakes into batches shortens the queue
rather than reordering it.

### Why the numbers can be believed

The harness asserts what it measured: every task executed, elapsed time non-zero, and the cluster state
version advanced by exactly the number of publications expected. That last one is what distinguishes the
two modes rather than trusting the timer, and it is asserted rather than reported: unbatched must advance
the version once per task, batched must advance it fewer times than it has tasks.

Then the harness was mutated. Adding a 20 ms sleep to `ClusterManagerService#publish` moved unbatched
per-publication from about 12 ms to 35 ms, while batched at 100 tasks stayed at 85 ms, because it still
publishes twice. Both halves of that are the point: the timer responds to publication cost, and batching's
advantage is precisely that it publishes less often. Reverted afterwards.

## S16 (F1): what the allocation state actually costs, and which half of Area F is real

Area F proposes emptying `inSyncAllocationIds` and `primaryTerms` for serverless indices, on the grounds
that the object store and `ShardHead` already answer what they exist to answer. Its own risk note calls
this the change most likely to produce a subtle correctness bug for the least benefit. The audit and the
measurement disagree with each other about which half of that sentence to worry about.

### The audit: `primaryTerms` is not redundant

Every reader of `IndexMetadata#primaryTerm(int)`, classified:

| reader | verdict |
|---|---|
| `TransportReplicationAction:1107` | **required**, sends the term with every write |
| `IndicesClusterStateService:760` | **required**, reads it to open a shard |
| `RestoreService:585` | **required** for restore |
| `MetadataCreateIndexService:1694` | required, C18's initial-term assignment |
| `IndexMetadataUpdater:387` | allocator path, serverless-irrelevant |
| `ClusterState:374`, `IndexMetadata:2640` | debug string and serializer, irrelevant |
| `ShardStateAction:470,483,756` | validation on state transitions, serverless-irrelevant |

The plan's premise does not hold here. Core reads the metadata term on the write path, not the shard
head, and this is not theoretical: C18 hit the literal error `primary term must be positive but was [0]`
from exactly this path, which is why the term is now set at creation. Emptying `primaryTerms` would break
writes rather than remove waste. Doing it would mean first teaching the write path to take the term from
the shard head, which is a much larger change than F.2 describes.

### The audit: `inSyncAllocationIds` is emptiable

| reader | verdict |
|---|---|
| `IndexMetadataUpdater`, `PrimaryShardAllocator` | serverless-irrelevant, allocator maintenance |
| `IndexRoutingTable` validation | serverless-irrelevant, validates a published entry a computed index has none of |
| `IndicesClusterStateService:870` | already handled by C18's `computedAwareInSyncIds` |
| `ClusterState:375` | debug string |
| `ShardStateAction:502` | safe, with a behaviour change worth naming |
| `SegmentReplicationSourceService:266` | safe, because of a union |

Two needed probing rather than assuming. `ShardStateAction:502` is the shard-*failed* path: with the set
empty, a failed copy resolves as "does not exist anymore" instead of being marked stale. No promotion
safety is lost, since C18 made promotion a function of the placement. `SegmentReplicationSourceService`
takes the **union** of the metadata set with the runtime replication tracker's in-sync ids, so emptying
the metadata half leaves the authoritative half. The caveat: a shard not in primary mode contributes no
tracker ids, so for such a shard the union would be empty and its handlers cancelled.

### The measurement: much larger than the plan assumed

`AllocationStateFieldCostEstimate`, 100,000 indices, one replica so two allocation ids per shard, three
runs at 3 shards and one each at 10 and 30.

| shards | with in-sync ids | empty | saving | share of total |
|---|---|---|---|---|
| 3 | 3,998 B/index | 2,939 B/index | 1,050 to 1,060 B/index | 26.4% |
| 10 | 7,163 B/index | 3,638 B/index | 3,524 B/index | 49.2% |
| 30 | 16,810 B/index | 6,231 B/index | 10,579 B/index | 62.9% |

The three runs at 3 shards spread by 10 B, so the figure is stable.

The saving is not flat per index: it is per shard, and the stated target is 3 to 30 shards. At the top of
that range `inSyncAllocationIds` is **63% of what an index's metadata retains**. Extrapolated to 100M
indices at 30 shards it is roughly a terabyte; at 3 shards, about 105 GB.

**So the plan's risk sentence is half right.** It is the change most likely to produce a subtle
correctness bug, and F2-before-F4 should stay non-negotiable. It is not for the least benefit. On the
`inSyncAllocationIds` half it is the largest single metadata saving measured in this project.

### C11: the ceiling, re-measured now that Area C is done

`DeferredMetadataHeapEstimate`, unchanged harness, for comparison against the earlier figures:

| shards | materialized | deferred | reduction |
|---|---|---|---|
| 3 | 2,944 B/index | 698 B/index | 4.2x |
| 30 | 6,214 B/index | 698 B/index | 8.9x |

The deferred figure is flat at 698 B/index regardless of shard count, which is the property the
architecture depends on and which S11 warned had never been checked across shard counts. It holds.

Note what this means next to S16 above: the deferred path already avoids the in-sync cost, because an
unread index never materializes it. Area F's saving therefore applies to the **materialized** working
set, the indices actually in use, and not to the 100M at rest. That narrows where the terabyte lands
without making it less real.

## S17: batching wake and sleep, and what it did to the starvation

S15 measured the cost and made the decision; this is the change and the re-measurement.
`TransportReactivateShardsAction` and both `ShardSuspensionCoordinator` submissions now go through a
`ClusterStateTaskExecutor` shaped like `MetadataCreateIndexService#createIndexExecutor`.

### The starvation, measured before and after in the same run

Four runs, each measuring both shapes against the same cluster so the comparison is not across machines
or node counts.

| nodes | NORMAL behind 300 plain URGENT | NORMAL behind 300 batched URGENT | ratio |
|---|---|---|---|
| 4 | 2,734.9 ms | 82.2 ms | 33x |
| 2 | 2,697.5 ms | 38.8 ms | 70x |
| 2 | 2,490.8 ms | 22.9 ms | 109x |
| 3 | 2,715.5 ms | 38.9 ms | 70x |

The unbatched figure is stable at 2.5 to 2.7 seconds. The batched figure is 23 to 82 ms.

**So the theory held and no priority change is needed.** Suspension was not starving because URGENT
outranks NORMAL; it was starving because each of the 300 wakes held the queue for a whole publication.
Batching shortens the queue, and `Priority.URGENT` on reactivation against `Priority.NORMAL` on
suspension can stay exactly as it is. That matters because a priority change would have been a behaviour
change with nothing measured behind it.

### What is asserted, and why not a timer

The unit tests assert the property rather than the speed: twenty-five suspensions must resolve to one
state, every task must come back as a success or a failure, and folding must accumulate rather than
letting the last task win. A timing assertion would be flakier and would not distinguish a batch that
folded correctly from one that silently dropped tasks.

That last one is the real hazard here. A dropped suspension is a shard marked suspended in cluster state
that nothing ever evicts, so it stays assigned and holds a node. Fast and wrong would look exactly like
fast and right.

Proven by mutation: making the executor apply each task to the *original* state instead of the
accumulating one, which is the plausible way to write this wrong, fails both tests. Twenty-five tasks
then suspend one shard.

### Cost of the change

Batching moves the per-task "did I change anything" decision from `clusterStateProcessed`, which a batch
reports once for every task in it, into the executor, which must record it per task while the transform
runs. Without that, one real suspension in a batch would make every task in the batch evict, and one real
reactivation would make every request in the batch log and reroute. This is the part of the change most
likely to be got wrong by someone copying the shape without noticing why the flag is there.

## S18 (G2a): index creation is superlinear in the population, which no area on the plan fixes

G2 asks for a synthetic 1M-index cluster. Before building it, this measured whether a million is
reachable. It is not, and why it is not turns out to matter more than the test would have.

### The curve

Indices created with one shard, no replicas, bounded to 100 in flight, on a 2-node test cluster.

| population reached | batch | per index | projected 1M |
|---|---|---|---|
| 200 | 200 | 7.38 ms | 2.1 hours |
| 1,000 | 800 | 10.29 ms | 2.9 hours |
| 3,000 | 2,000 | 34.57 ms | 9.6 hours |
| 6,000 | 3,000 | 98.75 ms | 27.4 hours |

Per-index cost grew 13x between 200 and 6,000 indices, and the rate was still worsening at the last
point, so 27 hours is optimistic rather than an estimate of the answer.

**The plan's premise for G2 does not hold.** It says 1M is chosen "with the per-index costs known to be
flat so extrapolation is defensible". What C11 measured as flat is metadata *heap*: 698 B/index deferred at
both 3 and 30 shards. Creation *time* is not flat. The two are independent and were conflated.

### Two wrong answers found on the way, both worth recording

The first attempt issued each batch concurrently and the cluster fell over at 2,000 simultaneous creations
with nodes disconnecting. Reporting that as a capacity limit would have been wrong in the direction that
makes the architecture look worse than it is: it measured the harness's tolerance for concurrent requests.
Bounding in-flight requests to 100 separated the two.

The second wall was `cluster.max_shards_per_node`, which defaults to 1,000, so three data nodes refuse the
3,001st single-shard index with "this cluster currently has [3000]/[3000] maximum shards open". A
configurable default, not a fundamental limit, and worth noting on its own: it is the ceiling S12 says
computed placement removes, met here as a hard validation error. Reaching a million indices requires
raising it by three orders of magnitude.

Neither would have been visible without probing, and either could have been reported as the finding.

### The cause, and why it is architectural

`Metadata.Builder.build()` takes its fast path only when `indices.equals(previousMetadata.indices)`.
Creating an index changes that map by definition, so every create runs
`buildMetadataWithRecomputedIndicesLookups()` and sweeps every index in the cluster to rebuild six name
arrays and the sorted `indicesLookup`. Creating the millionth index does a million units of work unrelated
to it. Even the fast path copies six N-length arrays.

This is stated as the likely cause rather than a proven one: it matches the shape and the code, and the
next step is to profile `build()` against population size rather than to act on the reading. Everything in
this project argued from reading has been wrong at least once.

**No area currently on the plan removes it.** A and E and F each reduce bytes per index; `LazyIndexMetadata`
takes an unread index from 2,944 B to 698 B. But a stub is still an entry in the map, and `build()`
iterates holders, so 100M stubs is still a 100M-element sweep. Bytes were never the binding constraint for
creation throughput.

That gap is what `plan-area-h-metadata-off-cluster-state.md` addresses, and S18 is the measurement that
justifies opening it.

## S19 (H0a and the Area H spike): the cause confirmed, and the replacement measured

S18 measured index creation degrading with population and named `Metadata.Builder.build()` as the likely
cause from reading the code. This confirms it and measures the alternative.

### The cause, measured in isolation

`Metadata.builder(existing).put(one index).build()`, which is exactly what creation does, against
population. Best of twenty repeats, in-JVM so it reaches sizes a test cluster cannot.

| indices | add one index | rebuild, no change | forced sweep |
|---|---|---|---|
| 1,000 | 0.67 ms | 0.33 ms | 0.33 ms |
| 10,000 | 8.74 ms | 2.44 ms | 2.38 ms |
| 50,000 | 54.44 ms | 15.90 ms | 14.67 ms |
| 100,000 | **107.67 ms** | 31.54 ms | 33.22 ms |

Adding one index to a hundred thousand costs 107 ms of metadata rebuild alone, on the cluster manager's
single state-update thread, before consensus and before publication. That caps creation at roughly nine
per second at that population, and it degrades from there. At a million it is about a second per create.

**The fast path is O(N) too.** A rebuild that changes nothing still costs 31.5 ms at a hundred thousand,
because it copies six arrays of length N. So every metadata change pays proportional to the whole
population, not only creation. That is broader than S18 claimed.

### The replacement, measured in the same harness

Writing a descriptor document with `op_type=create` instead of creating an index, same cluster, same
populations, in one run so the comparison is not across machines.

| population | index creation | descriptor write |
|---|---|---|
| 200 | 6.51 ms | 1.64 ms |
| 1,000 | 9.76 ms | 0.43 ms |
| 3,000 | 37.24 ms | 0.26 ms |
| 6,000 | 91.66 ms | 0.16 ms |
| projected 1M | 25.5 hours | 2.7 minutes |

Index creation degrades fourteenfold across the range. Descriptor writes do not degrade. The apparent
improvement is warmup amortising over larger batches rather than anything superlinear in the good
direction, and it should be read as flat.

Uniqueness is asserted rather than assumed: a second `create` against an existing id throws
`VersionConflictEngineException`, which is the put-if-absent guarantee Area H's creation path depends on.
A create that silently overwrote would have looked equally flat and proved nothing.

### What this settles

Area H's premise holds. The superlinear term is the cluster state metadata rebuild, and replacing the
cluster state entry with a document in an index removes it. The spike does not yet prove the rest of Area
H, which is resolution, wildcards, deletion, and the forty-one enumerations. It proves the foundation:
creation can be flat, and the mechanism that makes it flat is already in the product.

The kill criterion set before the spike was "if the curve is not flat, stop". It is flat.

## S20 (H0b and H1): where the per-change cost goes, and what the audit found

### Ranking the terms

S19 named `Metadata.build()` as the cause of creation degrading with population. That was too strong on
one reading and correct on another, so both are recorded.

Adding one index to an existing population, best of ten, single threaded:

| indices | metadata build | routing build | diff | total |
|---|---|---|---|---|
| 1,000 | 1.61 ms | 0.13 ms | 0.47 ms | 2.21 ms |
| 10,000 | 14.34 ms | 0.83 ms | 2.14 ms | 17.31 ms |
| 50,000 | 69.31 ms | 7.03 ms | 14.46 ms | 90.80 ms |

Among the costs of a cluster state change, metadata build is 76 percent, diffing 16, routing 8. Metadata
and diff both grow about ninetyfold for a fiftyfold population increase.

**The correction, and the correction to the correction.** After S19 the note was made that build() at
G2a's population of 6,000 is roughly 4 to 5 ms while G2a measured 91.66 ms per index, so build() looked
like 5 percent of the problem. That comparison was invalid: G2a measures end-to-end throughput with a
hundred creations in flight through a batching executor, which includes queueing on a single-threaded
manager, publication, and the temporary IndexService built to validate mappings. H0b measures one
uncontended change. Against like for like, metadata build dominates. The lesson is that two of one's own
measurements can be as misleading as a reading if they measure different things.

**Diffing is O(N) too**, at 14.5 ms per change at fifty thousand indices, which matters because diffing is
what publication does on every state change regardless of what changed.

### The audit

31 direct enumerations in `server`, and the distribution is the finding:

| group | count | disposition under Area H |
|---|---|---|
| gateway and remote state persistence | 16 | eliminated: they enumerate cluster state, which would hold no indices |
| per-cluster-state-change listeners | 2 | eliminated, and these were O(N) on every change |
| user facing: cat, pagination, snapshots | ~5 | convertible to descriptor queries |
| already refused for computed indices | 1 | done, via C28 |
| local or CLI scope | ~3 | out of scope |

`ClusterChangedEvent.indicesCreated()` was initially counted among the per-change costs and has no
production caller at all, so it is excluded.

Two findings changed Area H's scope.

**There is no point-lookup fast path, and that turned out to matter less than it sounds.** `concreteSingleIndex`
does delegate to `concreteIndices`, so a plain name enters the expression pipeline. But
`WildcardExpressionResolver.innerResolve` calls `aliasOrIndexExists`, which is a single
`getIndicesLookup().get(expression)`, and returns early on a hit. A plain name therefore costs one hash
lookup rather than an enumeration, and the first version of this note was wrong to imply otherwise.

The real obstacle is what that lookup consults. Both `aliasOrIndexExists` and the `concreteResolvedIndices`
loop read `metadata.getIndicesLookup()`, which is the in-memory structure Area H removes. They need to fall
back to the descriptor index when the map misses, which is the seam pattern
`AbsentIndexRoutingSuppliers.resolve` established in Area C and proved across nine call sites: try
published, then supplied.

**The plugin has periodic enumerations.** `ShardSuspensionCoordinator`, `ReaderCacheAffinityRecorder` and
`InPlaceMergeTriggerCoordinator` each iterate every index on a scheduler tick, not on a request or a state
change. That is O(total indices) recurring forever, and Area H does not fix it. Worse, emptying the map
would make them iterate nothing and silently do no work, which is exactly the failure mode this project
has hit eight times. They need converting independently.

## S21 (H2a and H2f): the descriptor, and whether it can be read at scale

S19 proved descriptor writes stay flat where index creation degrades. It said nothing about reads, and
Area H's read path carries a risk its write path does not.

### The descriptor

99 bytes serialized for a realistic index name, against `IndexMetadata`'s measured 2,944 B. A hundred
million descriptors is therefore about a ten gigabyte index, half what the plan estimated, and it lives on
disk in an index rather than in heap on every cluster-manager-eligible node.

Its field set was audited rather than chosen: `ComputedRoutingTable` reads the index, the uuid, the shard
count and the search-only replica count, and `ComputedPlacementGate` reads the serverless flag. Each is
asserted individually, so a field quietly dropped fails at the type rather than in a routing decision.

### Reading it

Five shards, three nodes, fifty realtime GETs per sample.

| descriptors | point lookup | prefix hits | prefix query |
|---|---|---|---|
| 1,000 | 0.68 ms | 111 | 127.4 ms |
| 10,000 | 0.48 ms | 1,111 | 23.0 ms |
| 50,000 | 0.61 ms | 10,000 | 21.7 ms |

**Point lookup is flat**: 0.888 ms at a thousand descriptors and 0.783 ms at fifty thousand. That is the
property the whole area depends on, since resolution happens on every request and cannot scale with the
number of indices in the system.

**Wildcards did not turn out to be the problem the design review expected.** OpenSearch routes by hash
where S3 partitions by range, so a prefix query is a scatter-gather across every shard rather than a range
scan over the partitions covering the prefix. The fear was that this made wildcards expensive. Measured,
it is about 22 ms regardless of population, with the 127 ms first sample being warmup rather than a
signal.

### What this does not establish

Five shards, not the hundred a hundred million descriptors would need. The per-shard work is a term
dictionary seek and does not grow with population, which is what the flat numbers show, but the
coordination cost of the fan-out grows with shard count and that is untested here.

The prefix query ran with `size=0`, so it counted matches rather than returning names. Real wildcard
resolution needs the names, and fetching ten thousand of them costs more than counting them. That is the
next thing to measure rather than a limitation of the design.

## S22 (H3 and H5): both uncleared ceilings, measured on the gated path

Area H's two claims, each measured against the thing it replaces, in the same run and on the same machine.
That last part matters: comparing S19's cluster measurement against S20's in-JVM measurement produced a
wrong conclusion once, and the fix is to measure both arms together rather than to reason about the gap.

### Ceiling 3, creation throughput

Adding one index to an existing population, best of ten.

| existing indices | through cluster state | descriptor only | ratio |
|---|---|---|---|
| 1,000 | 1.463 ms | 0.001 ms | 2,240x |
| 10,000 | 9.819 ms | 0.001 ms | 16,842x |
| 50,000 | 57.777 ms | 0.001 ms | 101,542x |

Descriptor-only creation is 0.0006 ms at a thousand indices and 0.0005 ms at fifty thousand, a ratio of
0.90. Flat, and the gap widens with population because the other arm grows rather than because this one
shrinks.

### Ceiling 2, residency

A thousand gated creations leave zero metadata entries and zero routing entries, while all thousand are
recorded as descriptors. Gated and ungated indices coexist in one cluster state with only the ungated
resident.

Asserted as a count rather than as retained heap: zero is a stronger claim than an improvement, cheaper to
measure, and independent of a heap estimator being right about what it walks.

| | per index | fits in an 8 GB metadata budget |
|---|---|---|
| materialized | 2,944 B | 2.9M indices |
| deferred, C11 | 698 B | 12.3M indices |
| gated | 0 B | bounded by an index on disk, not by heap |

### The limit this ships with

A gated index has no metadata entry, and every mapping update path resolves its target through
`Metadata#getIndexSafe`, which throws when the index is absent. So a gated index cannot accept a document
carrying a new field: dynamic mapping fails rather than degrading.

That is H4c, and the probe inverted its priority. It was filed as an optimisation and is actually a
prerequisite: without it the gate is usable only for indices whose mapping is fully known at creation.

### One regression failure, investigated and unrelated

`AutoExpandSearchReplicasIT` hangs and is killed by the suite timeout. With `Metadata.java` reverted to
before the descriptor hook existed it hangs at 1199.703s against 1199.706s with it, three milliseconds
apart on a twenty minute timeout. Pulled into the run for the first time by a widened glob, and recorded
rather than adopted.

## S23 (H7b): descriptor cost against the descriptor index's own shard count

S21 measured lookups against descriptor population, found them flat, and flagged its own limitation: five
shards, where a hundred million descriptors needs closer to a hundred. Per-shard work is a term dictionary
seek and does not grow with population, which is what those flat numbers showed. What grows with shard
count is the coordination of a scatter-gather, and that was invisible to a measurement that varied
population.

Population held constant at twenty thousand so shard count is the only variable, six nodes.

| shards | point lookup | prefix hits | prefix query |
|---|---|---|---|
| 1 | 1.193 ms | 10,000 | 138.8 ms |
| 5 | 0.685 ms | 10,000 | 17.1 ms |
| 20 | 0.548 ms | 10,000 | 58.0 ms |
| 60 | 0.486 ms | 10,000 | 29.2 ms |

**The point lookup is flat and that is the load-bearing result**: 0.476 ms with one shard against 0.435 ms
with sixty, a ratio of 0.91. A point lookup routes by id to exactly one shard, so it should not care how
many shards exist, and it does not. Resolution scales.

**The wildcard shows no growth trend, and the honest reading is that the fan-out cost is not measurable
here rather than that it is absent.** 17, 58 and 29 milliseconds at five, twenty and sixty shards is
variance rather than a curve, and the 138.8 ms at one shard is warmup: it is the first query of the run.
Whatever the coordination cost of fanning out to sixty shards is, it sits below the noise of this harness.

That is weaker than "wildcards are free at a hundred shards", and it is what was measured. Extrapolating a
trend from numbers that do not show one would be the same mistake as G2's assumption that per-index costs
were flat because a different quantity was flat.

The test asserts `response.getTotalShards()` equals the configured shard count, so a wildcard that quietly
searched one shard would fail rather than look like excellent scaling.

## S24 (H7c): what a wildcard costs when it has to return the names

S21 and S23 both measured prefix queries with `size=0` and both flagged it: that counts matches rather
than returning them, and real resolution needs the names. This measures the difference, and the difference
is large enough that the earlier numbers should not be quoted for resolution cost.

Twenty thousand descriptors, five shards, paged with `search_after` at a thousand per page rather than one
large `size`, because `index.max_result_window` caps a single response at ten thousand and a real resolver
faces an unbounded match set.

| matches | counting | first page | every name |
|---|---|---|---|
| 100 | 121.4 ms | 40.1 ms | 70.1 ms |
| 1,000 | 14.0 ms | 46.3 ms | 47.2 ms |
| 10,000 | 22.3 ms | 55.8 ms | **373.2 ms** |

Warmed, at ten thousand matches: **counting 5.9 ms against returning names 327.5 ms, a ratio of 55**. The
121 ms in the first row is the first query of the run rather than a hundred-match effect, which is the
same warmup artefact S21 and S23 each reported before annotating it.

**So the ~22 ms figure previously recorded understates wildcard resolution by roughly fifteen times.** A
bounded wildcard, one page, stays in the 40 to 56 ms band regardless of how many match. An unbounded one
scales with the match set: ten thousand names is 327 ms, and a hundred thousand would be about 3.3
seconds.

### What this decides

Wildcard resolution needs pagination in its contract, or a bound on the match set. That is not a design
flaw, since resolving a hundred thousand index names is inherently proportional work and no storage
arrangement makes it free, but it is a semantic that belongs next to the freshness decision rather than
being met when a tenant runs `logs-*` against a hundred thousand indices.

The per-page cost is the number to design against: about 33 ms per thousand names, flat across match set
size. A resolver that streams pages pays that per page; one that materialises the whole list pays it
multiplied.

The count and the returned list are asserted equal, so a page loop that silently stopped early would fail
rather than report an attractively small number.

## S25 (H9b): the suspension uuid lookup, and what memoising it buys

H1d recorded that `ShardSuspensionCoordinator.findByUuid` scans every index and is called per candidate
shard per tick, so a tick suspending a hundred shards scanned the whole cluster a hundred times. It
attached no number, which left the severity a matter of opinion. This attaches one.

`Metadata` is immutable and shared, so the uuid map can be memoised against the instance and rebuilt only
when the cluster state changes.

| indices | first resolution | ninety-nine more |
|---|---|---|
| 50,000 | 20.4 ms | 7.0 ms total, about 0.07 ms each |

Unmemoised, a hundred-shard tick at fifty thousand indices would be a hundred scans, roughly two seconds
of the coordinator doing nothing but looking things up. Memoised it is about 27 ms, and the assertion is
that ninety-nine further resolutions cost less than twenty times the first rather than ninety-nine times.

**What this does not do.** The first resolution against a new cluster state still walks every index, so
the cost is paid once per state version rather than once per shard. At a hundred million indices that
first walk is still a hundred million entries, so this is a real improvement and not the answer.

The answer is not consulting metadata here at all, which is H9c. Suspension state currently lives inside
`IndexMetadata` through `SuspendedShardsMetadata`, which is both why the lookup is needed and why a gated
index cannot be suspended at all (S24's companion finding, H9a). Moving that state to the descriptor
removes the lookup and fixes the gated case with one change.

## S26 (H10): what gated creation actually costs, as opposed to what it avoids

S22 reported descriptor-only creation at 0.0005 ms, flat against population, against 57.777 ms through
cluster state at fifty thousand indices. That number is real and it is not creation. It measures the cost
of *not* doing a cluster state update, in-JVM, with no descriptor written anywhere. Actual gated creation
writes a descriptor document with `op_type=create`, and that write had never been measured.

The distinction decides a different question from the one S22 answers. Publication cost is what stops a
cluster from functioning; write cost is what stops a fleet from ever being filled. The plan had been
quoting a number that describes neither.

Measured concurrently, 500 requests in flight, descriptors in a 5-shard index.

| created | elapsed | per index | indices/sec |
|---|---|---|---|
| 5,000 | 1.16 s | 0.2325 ms | 4,301 |
| 20,000 | 1.34 s | 0.0892 ms | 11,207 |
| 50,000 | 1.46 s | 0.0486 ms | 20,577 |

At the last measured rate, 100M indices is roughly 1.4 hours of sustained writing on one small cluster.
The answer to "can the fleet be filled" is hours, not months, and creation throughput is therefore not a
ceiling.

### The rate improves with population, which is warmup rather than a discovery

Creating 10,000 descriptors from empty ran at 13,100/s; another 10,000 onto an existing 40,000 ran at
20,790/s, a ratio of 1.59. The improvement is JIT and index warmup dominating the first batch, not
something getting faster as it grows.

What the second measurement is for is the opposite claim, and it is the one asserted: the rate does not
*collapse* with population. A creation cost that degraded would mean the fleet cannot be filled regardless
of where the rate starts, because the last million indices would cost more than the first.

### What this does not say

It is a small cluster, so the absolute rate is the shape of the cost and not a capacity figure. It measures
descriptor writes, which is what gated creation does, and it deliberately does not materialize any shards,
which is what makes gated creation cheap in the first place. An index whose shards are later woken pays
that cost then, and scale-to-zero (H9c, H9d) is what keeps the population that is awake far below the
population that exists.

## S27 (H14): the descriptor path at a million indices

Every Area H figure before this topped out at fifty thousand. The curves were flat where flatness was the
claim, which is the best evidence available without a fleet, and flat to 50k is not flat to 100M. The
review that closed the last cycle named this run as the single most valuable next step for exactly that
reason.

Both populations measured on the same machine in the same run, since comparing across runs produced a
wrong conclusion once already in this project.

| population | creation (indices/sec) | point lookup (ms) |
|---|---|---|
| 50,000 | 11,934 | 0.4243 |
| 1,000,000 | 18,853 | 0.4070 |

**Lookup ratio 0.96x across a twentyfold population increase.** That is the number this run existed to
produce. **Refined by S28 and S29.** S28 found 1.88x over the next decade, and S29 showed that growth is segment
count rather than population: at equal segment counts, one million and ten million differ by 1.05x. The
flatness reported here is real, and it holds at scale only where merge policy keeps segment count bounded. A point lookup that grew with population would mean the descriptor index had become the new
ceiling, which is the single failure that would invalidate the whole approach, and it does not.

Creation appears 1.58x faster at the larger population. That is warmup dominating the first batch, the
same artefact S26 recorded, not something getting faster as it grows. The claim asserted is that it does
not collapse.

### It took five runs, and four of them were this file's fault

Worth recording because each failure looked like the finding the test exists to look for, and reporting a
harness limit as a scaling ceiling would have been worse than not running it.

1. **Out of memory.** The test set `indices.memory.index_buffer_size` to 512 MB with a thousand requests in
   flight. Every node of an `internalClusterTest` shares one JVM, so that buffer was allocated per node
   inside a single 3 GB heap.
2. **Out of memory again**, which is the useful one: the first diagnosis was correct and insufficient. The
   remaining cost was the test framework's own mock plugins, whose leak-tracking directories and engines
   retain per-operation state precisely so correctness tests can catch leaks. That accounting is what
   cannot hold a million documents.
3. **`Unsupported http.type []`.** Emptying the mock list entirely leaves the node unable to start. The
   transport, HTTP and seed plugins have to stay; only the accounting ones come out.
4. **The population check passed on ten thousand documents.** `getTotalHits()` is capped at 10,000 by
   default, so the assertion that every descriptor was present would have "verified" a million-document
   index by counting ten thousand of them. Fixed with `trackTotalHits(true)`.

The fourth is the one to remember. It did not fail loudly, it very nearly succeeded quietly, which is this
area's signature failure appearing in the test written to measure it.

### What it still does not say

A million is twenty times more evidence for the same extrapolation, not a demonstration at a hundred
million. It also runs against a small in-JVM cluster with the framework's leak detection disabled, so it
measures the shape of the cost rather than a capacity figure.

## S28 (H21): ten million indices, and the flatness claim was wrong

The previous review said closing the gap between "no known ceiling" and "demonstrated" needed a fleet.
That is true of a hundred million and it was not true of ten, so ten was run.

| population | creation (indices/sec) | point lookup (ms) |
|---|---|---|
| 1,000,000 | 22,805 | 0.3809 |
| 10,000,000 | 23,843 | 0.7147 |

**Creation is flat.** 1.05x across a tenfold population increase, consistent with S26 and S27. At this rate
a hundred million indices is about 70 minutes of writing.

> **Superseded in part by S29.** The lookup growth reported below is real but is driven by segment count,
> not by population. At equal segment counts the same decade costs 1.05x. Read S29 before quoting the
> 1.88x figure.

### The lookup is not flat, and every previous claim that it was came from too narrow a range

S27 measured 0.96x from fifty thousand to a million and concluded flatness. Across the next decade the
same measurement gives **1.88x**. The earlier number was not wrong, it was taken over a range where the
growth had not yet shown itself.

That matters more than the absolute figures, because the extrapolation to 100M on this plan has been
resting on "flat" rather than on "grows slowly". Those are different claims and only the second is
supported.

Extrapolating the observed decade-over-decade factor, 100M lands near 1.3 ms. That is still an acceptable
cost for a metadata point lookup, so the conclusion survives, but it survives as arithmetic on a growth
rate rather than as an appeal to a curve that does not move.

The growth is consistent with a term lookup over a larger dictionary and more segments, which is sublinear
and expected. What was not expected is that it was invisible below a million, which is exactly why the run
was worth doing.

### What this run needed, which is part of the result

Ten million did not fit the default 3 GB test heap, and raising it needs **both**
`-Dtests.heap.size=8g` and `-Poptions.forkOptions.memoryMaximumSize=8g`. Setting only the first produces
`Initial heap size set to a larger value than the maximum heap size`, because `gradle.properties` pins the
maximum and Gradle appends it after the plugin's arguments.

### What it still does not say

Ten million is one order of magnitude from the target rather than two. It is not a demonstration at a
hundred million, and the single-JVM cluster with leak detection disabled remains a shape measurement
rather than a capacity figure.

## S29 (H22): the lookup growth is merge policy, and at equal segment counts it is flat

S28 measured point lookup rising 1.88x from one million descriptors to ten million and concluded the
extrapolation to 100M had to rest on a growth rate rather than a flat curve. That conclusion is wrong, and
this is what replaces it.

Two causes were consistent with S28's number and they have opposite consequences. A get by id consults
every segment that could hold the id, so more segments means more work per lookup, and that is controllable.
A larger term dictionary costs more to search, logarithmically, and that is not. Force merging separates
them cleanly because it collapses segments without shrinking the dictionary.

| population | segments | lookup before | lookup after | ratio |
|---|---|---|---|---|
| 1,000,000 | 29 → 5 | 0.3755 ms | 0.3044 ms | 0.81 |
| 10,000,000 | 39 → 5 | 0.5314 ms | 0.3199 ms | 0.60 |

**The row that matters is the comparison between the two after-columns.** At five segments, one million
descriptors cost 0.3044 ms and ten million cost 0.3199 ms. That is **1.05x across a tenfold population
increase**, which is flat by any reading.

So the growth S28 found is an artefact of merge policy rather than a property of the index. The term
dictionary contribution across that decade is roughly five percent, not eighty-eight.

### What this changes

**100M lookup is a design parameter, not a fact to be accepted.** Keeping segment count bounded, by merge
policy or by a rollover scheme that keeps each descriptor index small, holds lookup near 0.32 ms
irrespective of population. The plan's extrapolation is back on a flat curve, but for a stated reason and
with an operational obligation attached rather than as an assumption.

**The obligation is real and belongs next to the refresh interval.** H18 made the descriptor index's
refresh interval API-visible because it bounds wildcard staleness. This makes its merge policy equally
load-bearing, because it bounds lookup latency. Both are now things a deployment can get wrong in a way
that looks like the architecture failing.

### Why the one million arm could not settle it, which was my error

The same experiment at one million alone gave 0.81 and was reported as inconclusive. It could not be
conclusive: the growth being explained happens between one million and ten million, so the experiment had
to run where the effect lives. The justification given at the time, that shape does not need size, was
wrong for this question.

### What it still does not say

Both arms are single-JVM clusters. The claim is that segment count rather than population drives lookup,
measured across one decade. It is not a demonstration at a hundred million, and a deployment that lets
segments grow unbounded will see S28's curve rather than this one.

## S30 (H24): the scaling curve, and a projection checked against an independent point

Measured locally at four populations rather than attempting the target, which is the right shape of
evidence: what decides the design is the slope of the cost, and a slope needs several points.

Segments held at one per shard in the merged column, because S29 established that lookup tracks segment
count rather than population. A single curve on naturally-merged indices would extrapolate merge policy
while appearing to extrapolate scale.

| population | creation/sec | segments | lookup (ms) | merged segments | merged lookup (ms) |
|---|---|---|---|---|---|
| 100,000 | 19,487 | 32 | 0.3159 | 5 | 0.2805 |
| 250,000 | 25,663 | 24 | 0.2698 | 5 | 0.2965 |
| 500,000 | 26,341 | 23 | 0.2820 | 5 | 0.3197 |
| 1,000,000 | 26,554 | 25 | 0.2910 | 5 | 0.3360 |

Merged lookup rises monotonically at **1.20x per decade**. Creation is flat once warm: the first
checkpoint's 19,487 is residual warmup, and the last three sit within 3.5 percent of each other.

### The projection, and the independent point that checks it

Applying 1.20x twice past a million projects **0.482 ms at a hundred million**.

That projection can be tested rather than merely stated, because S29 measured merged lookup at ten million
on a separate run: **0.3199 ms**. This curve predicts 0.403 ms there. The projection therefore
**overshoots the one point where it can be checked**, by about 26 percent, which makes 0.482 ms at a
hundred million a conservative upper bound rather than a best guess.

Taking S29's own two merged points, 0.3044 ms at one million and 0.3199 ms at ten, gives 1.05x per decade
across that decade. Projecting from that slope instead lands near **0.35 ms**.

So the honest range for merged lookup at a hundred million is roughly **0.35 to 0.48 ms**, with the
independent check favouring the lower end. The slope flattening as population grows is what a term lookup
should do, which is a further reason to treat the upper figure as a bound.

### The first attempt at this measurement was wrong and passed

Recorded because it is the most dangerous failure in this sequence. The first run warmed with 30 gets
against a measured pass of 300, so the earliest checkpoint absorbed cold-JIT cost the later ones did not.
The result was a curve where lookup *improved* with population, 0.44x per decade, projecting 0.013 ms at a
hundred million. Every assertion passed, the output was well formed, and the absurd number arrived with an
honest-sounding caveat attached.

Three changes: warmup now costs exactly what the measurement costs at every checkpoint, a throwaway index
absorbs the run's class loading before the first checkpoint, and an inversion guard fails the test if
merged lookup falls with population. A falling curve is warmup leaking into the measurement, never a
discovery, and the guard is what catches it without anyone reading the numbers.

### What this does not say

Four points across one decade, projected across two more. The assumption that the slope holds is exactly
what a run at scale would test and this does not. What it does have, which no earlier extrapolation here
did, is a validation point outside its own range.

## P1 to P8: the hot-path review, and the one mistake it kept finding

A pass over every per-request path the wiring added, benchmarking first and fixing second. Five
improvements, two correctness gaps, and one mistake made three times.

### Measured and fixed

| finding | before | after |
|---|---|---|
| P1, P2: suspension lookup, 16 threads | 9.1M reads/sec | 1,037M reads/sec |
| P3: store reads for a ten-new-field document | 10 | 1 |
| P5: placement plus filter, 100 shards, one asleep | 8,728 ns | 84.1 ns |
| P8: reads for fifty resolutions of one name | 50 | 1 |

P1's structure did not merely fail to scale, it ran backwards: 57.4M reads per second on one thread down to
9.1M on sixteen, a lock convoy on a path every request takes. H12 had chosen access ordering to protect the
active set from a sweep, and that argument never needed read ordering, because the map holds only suspended
indices and reading an unsuspended one inserts nothing.

### The mistake, three times

Every one of these was a cache that existed and was consulted too late to prevent anything.

1. **P3.** The mapping guard sat after the store read it existed to avoid, so ten new fields cost ten reads.
2. **P5, first attempt.** Keyed a memo by identity against a caller-supplied function that allocates per
   call, so it missed every time and cost *more* than no memo: 159,499 ns against 8,728 ns. Caught only
   because the benchmark ran after the change as well as before.
3. **P7.** Checked its synthesis cache after the descriptor read that populates it, which is what led to P8
   finding the descriptor read had no cache at all despite the seam documenting that it should.

The generalisable form: a guard that needs the expensive value in order to decide cannot be placed after
the expensive call. Where the check genuinely requires the result, the answer is a time window rather than
a value comparison, which is what P3 and P8 both ended up using.

### Correctness found while measuring

**P6, P7.** A gated index had no routing at all. Resolution converts a name to metadata before calling
placement, gating means there is no metadata, and ownership answered false for null. Nameable since W3,
gated since W12, served by nothing, and nothing thrown. Closed by passing the name so the descriptor can
supply what placement needs. I had scoped this as requiring a widened seam across 72 registration sites; it
required neither, which is worth remembering the next time a fix is deferred on estimated blast radius.

### Not changed, deliberately

`MappingRefreshOnDemand` has the same access-ordered structure and is not constructed in production, so
tuning it would be tuning dead code. Its ordering is also genuinely load-bearing in a way the suspension
registry's was not: reads there populate the map, so a sweep really can evict the working set.

The per-field cost in `DocumentParser` is one reference comparison against a local, short-circuiting before
any volatile read. Measured by inspection rather than benchmark, and recorded as such.

## S31 (P9): real gated creation is 235 per second, not 20,577

S26 reported 20,577 creations per second and S30 about 26,500. Neither called `prepareCreate` for the
indices it counted. Both wrote descriptor documents with `client.index` and timed that, so both measured
descriptor write throughput and neither measured index creation.

That is the same defect S22 had, where 0.0005 ms turned out to be the cost of *not* publishing rather than
the cost of creating. S26 was written to replace S22 and reproduced its shape at one remove: it measured
the storage operation creation performs instead of the operation it avoids, and still not creation.

Measured through the real API, both arms in one cluster, twenty indices each:

| arm | creations/sec |
|---|---|
| ordinary | 16 |
| gated | 235 |
| ordinary again, warmup control | 25 |

**Gated creation is 14.4x ordinary**, so the gate does what it was built to do: it skips the `Metadata`
rebuild S20 measured at 69 ms per change at fifty thousand indices, and it skips the publication.

**And it is 87x slower than the figure the plan has been quoting.** At 235 per second a hundred million
indices is about 4.9 days, not the 1.4 hours S26 implied.

### Why it is not faster

The gate returns the cluster state unchanged, but it does so from inside `CreateIndexTask`, an
`AckedClusterStateUpdateTask` submitted to the single-threaded cluster state executor. So a gated creation
still takes a cluster manager slot, and the whole settings and template pipeline runs before the result is
discarded. What the gate removes is the rebuild and the publication. What it does not remove is the
serialisation point, which is the thing that bounds the rate.

That is the guard-after-the-work shape found three times on the read paths (P3, P5's first attempt, P8),
appearing a fourth time and at the largest scale yet.

### What the absolute numbers are worth

The ordinary arm is 16 to 25 per second because this cluster runs with remote cluster state enabled, which
serverless requires, so every ordinary creation publishes to a remote store. Both arms pay that, so the
14.4x ratio and the two-orders-of-magnitude gap against the quoted figure are the findings. The absolute
rates are specific to a single-JVM test cluster and should not be quoted as capacity.

## S32 (P10): skipping the cluster state queue changed nothing, so the hypothesis was wrong

S31 found gated creation at 235 per second and blamed the queue: the gate returns the cluster state
unchanged, but it does so from inside `CreateIndexTask` on the single-threaded cluster state executor, so
every gated creation still took a turn.

H3 says that queue is unnecessary for a gated index, because the descriptor write with `op_type=create` is
the uniqueness gate rather than the cluster state entry. So the fast path was built: check the request's
own settings, take a cluster state snapshot for template resolution, run the pipeline, and if the gate
fires, answer without ever submitting a task.

**It fired and it did not help.** Twenty hits, zero misses, and gated creation moved from 235 to 245 per
second, which is noise on twenty samples.

So the queue was not the bottleneck. What remains in a gated creation is the pipeline itself: settings
aggregation, template resolution, validation and the temporary `IndexMetadata` build, plus the transport
and action layers above it. The descriptor write is already asynchronous (W4) so it is not blocking either.

**Reverted.** An optimisation with no measurable benefit on the path that creates indices is not worth its
risk, and a bug there means an index that does not exist or exists twice. The measurement is kept and the
hypothesis is recorded as disproved.

### Why this is worth writing down

The reasoning was sound and the answer was still wrong. Everything about the shape of the code said the
serialisation point had to be the cap, and it was not. That is the same lesson as P5's first attempt, where
identity keying looked equivalent to the identity keying one line above it and was an 18x regression: on a
hot path, a hypothesis about where time goes is worth exactly as much as the measurement that follows it.

Profiling the creation pipeline is the next step, and it is a different task from this one.


## S33 (T13): publication cost does not grow with node count, over the range that can be measured

Three separate answers in this project's design discussions rested on the claim that once index metadata
leaves cluster state, the remaining O(N) publication becomes the ceiling at thousands of nodes. The claim
was labelled unmeasured each time and reasoned from anyway, including in a recommendation to move node
membership to etcd. `ClusterStatePublicationCostIT` measures it.

A persistent cluster settings update is close to the smallest real cluster state change available: a few
bytes of diff, no reroute, no index metadata. Median of nine, three runs of the whole test.

| data nodes | run A | run B | run C |
|---|---|---|---|
| 1 | 13.69 ms | 16.99 ms | 26.60 ms |
| 2 | 13.41 ms | 14.42 ms | 26.53 ms |
| 4 | 14.69 ms | 15.47 ms | 24.22 ms |
| 6 | 15.71 ms | 16.45 ms | 23.00 ms |
| **ratio at 6** | **1.15x** | **0.97x** | **0.86x** |

The slope reverses direction between runs. Variance at a fixed node count spans 13.7 to 26.6 ms, wider than
anything six times the nodes does. Node join measured 180, 105, 104, 96, 106 and 79 ms against one through
six existing nodes, with the first carrying JVM warmup, so it shows no growth either.

### What this does and does not settle

The claim is not supported. It is also not refuted, and keeping those apart is the point. Every node here is
in one JVM over loopback, and the two effects that would make the claim true are exactly the ones a local run
cannot produce: real network fan-out, and a tail set by the slowest of a thousand nodes rather than the
slowest of six. So this is a floor, and the honest state of the claim is unproven.

### The constant is the real result

A cluster state change costs roughly 14 to 27 ms whatever the node count. That is a per-change floor paid by
anything touching cluster state, and it cross-checks S31 from the opposite direction: gated creation at 235
per second is 4.3 ms per index, less than a single publication costs. Gating therefore demonstrably skips
the round trip rather than making it cheaper, which S31 alone could not show. Two measurements taken for
different reasons agreeing is worth more than either on its own.

### Why this is worth writing down

An unmeasured number quoted three times starts to read like a finding. Nothing about it changed between the
first assertion and the third except repetition, and the measurement it was standing in for took under an
hour once someone wrote it instead of citing it.

## S34 (T14): 235 per second was the harness, not the system

S31 measured gated creation at 235 per second and this plan has quoted it ever since, including as the
reason a hundred million indices takes 4.9 days. That figure was taken with five requests in flight, and a
number measured under fixed pressure is a ceiling only if something serialises.

Two measurements already suggested nothing did. S32 removed the cluster state queue entirely and throughput
did not move. S33 found a publication costs 14 to 27 ms while a gated creation costs 4.3 ms, so gating skips
the round trip rather than queueing behind it. What remains is per-request pipeline work, which should scale
with concurrent requests until something shared saturates.

| in flight | run A | run B |
|---|---|---|
| 1 | 189/sec | 145/sec |
| 5 | 222/sec | 317/sec |
| 20 | 443/sec | 386/sec |
| 50 | 499/sec | 492/sec |
| 100 | — | 520/sec |
| 200 | — | 566/sec |

The five in flight row reproduces S31, which is what makes the rest credible: same harness, only the
pressure changed. Throughput saturates around 500 to 570 per second, and four times the concurrency from
fifty to two hundred buys 1.15x, so that is a genuine asymptote rather than a point on a line.

Individual figures move by a third between runs at the same concurrency. The asymptote survives repetition;
the individual numbers do not.

### What it changes

A hundred million indices is roughly **2.1 days at 550 per second**, not 4.9 days at 235. The plan overstates
creation cost by about 2.3x and should be corrected.

It is not a reprieve. Two days is still a bulk migration rather than an operation, and whether the asymptote
rises with cluster size is a separate question one cluster cannot answer.

### Why this is worth writing down

S31 was itself a correction. It replaced S26's 20,577 per second after finding S26 measured descriptor
document writes rather than index creation, and it was right to. But it then fixed the concurrency at
whatever the test happened to use and the resulting number was quoted as a property of the system for the
rest of the project. A measurement is of a system under conditions, and the conditions are part of the
result. S26 got the operation wrong; S31 got the operation right and the conditions unstated.

## S35 (T15): every creation figure in this document was measured on an instrumented JVM

The question was where creation time goes, so `GatedCreationProfileIT` runs three thousand gated creations
at a hundred in flight, long enough for Java Flight Recorder to collect a real profile. The first thing the
profile said was about the measurement rather than the system.

Of 1,045 execution samples: **18.7 percent touch the Java security manager, 8.6 percent touch the jacoco
coverage agent**. The hottest single leaf frame in the entire recording is `ArraysSupport.mismatch` at 11
percent, which is file permission path comparison. Neither agent exists in production.

| configuration | creations/sec | |
|---|---|---|
| jacoco + security manager | 536 | what S26, S31 and S34 were all measured under |
| security manager off | 690 | 1.29x |
| both off | **859** | **1.60x** |

So the creation numbers in this document understate production by roughly **1.6x**. A hundred million
indices is about **1.35 days** at 859 per second, against the 4.9 days S31 established and the 2.1 days S34
corrected it to. That is the third value for the same quantity, and the first one measured on a JVM
resembling the one that will run it.

### Where the time goes, once the agents are subtracted

The busiest thread by a wide margin is `clusterManagerService#updateTask` at 27.2 percent of all samples,
and there is exactly one of it. Within that thread:

| | share of that thread |
|---|---|
| jacoco / security manager | 14.8% |
| index metadata and settings construction | 13.4% |
| `withTempIndexService` | 4.6% |
| mapping parse | 1.4% |
| `aggregateIndexSettings` | 1.4% |

`withTempIndexService` was the hypothesis this profile was built to test, because
`applyCreateIndexWithTemporaryService` constructs a real `IndexService` per creation and its own comment
says so. At 4.6 percent it is real and it is not the cost. That is the second hypothesis about this path to
die on contact with a profiler after S32, both times because reading the code makes the expensive-looking
thing look expensive.

### An open contradiction

S32 removed the cluster state queue entirely and throughput did not move, which says the queue is not the
limiter. This profile says the single cluster manager task thread is the busiest thing in the system by a
factor of two over anything else.

Both can be true if S32's null result was taken at five requests in flight, below the knee S34 later found
near fifty. Until that is retested at saturation, "the queue is not the bottleneck" rests on a measurement
made where nothing was contended. **This is the next experiment**, and it is also the one that decides
whether batching creations into fewer cluster state tasks is worth building.

### On batching, before anyone builds it

Batching the descriptor write is not worth doing and this can be settled from existing numbers rather than
new ones. S26 measured a descriptor document write at 20,577 per second, about 0.049 ms. A gated creation
at 859 per second is 1.16 ms. The storage write is roughly 4 percent of creation, so a perfect batcher
removes at most 4 percent.

Batching at the cluster state task level is a different proposition and is currently unevaluated, pending
the S32 retest above.

### Why this is worth writing down

Three measurements of gated creation throughput, three different answers, and each was a correction of the
last: S26 measured the wrong operation, S31 measured the right operation under unstated conditions, S34
found the conditions were the ceiling. This one found that all three ran under two agents that do not ship.
The pattern is not carelessness about arithmetic; it is that a number, once written down, stops carrying the
conditions that produced it.

## S36 (T16): the clean profile, and why the remaining wins are a semantic decision

S35 found the profile was 27 percent test infrastructure. Re-profiled with the security manager and the
jacoco agent both off, which is the only profile worth optimising against: 523 samples, **zero**
contamination.

### Is the cluster manager thread actually the bottleneck

`jdk.ThreadCPULoad` answers it without inference. The cluster manager task thread runs at **3.76 percent of
total CPU on a twenty core machine**, where one fully busy thread reads 5.0 percent. So it is about **75
percent busy**: the leading constraint by a factor of two over anything else, and not saturated.

That resolves the S32 contradiction without needing the concurrency explanation S35 proposed. Removing work
from a thread that has headroom gives sub-linear gains, so S32's null result is exactly what a 75 percent
busy serialisation point predicts. The clean concurrency sweep agrees:

| in flight | 1 | 5 | 20 | 50 | 100 | 200 |
|---|---|---|---|---|---|---|
| creations/sec | 243 | 617 | 475 | 805 | **872** | 833 |

Saturating near 850 per second between fifty and a hundred, declining slightly beyond.

### Where the time goes

| | share of the cluster manager thread |
|---|---|
| settings subsystem | ~20% |
| temporary IndexService and analysis | ~15-20% |
| descriptor write submission | ~10% |

The first entry is the surprise and the contaminated profile hid it: validating index settings against the
scoped settings registry costs more than building the throwaway `IndexService` that the code makes so
visible. `withTempIndexService` was the hypothesis two profiles were built to test and it is second.

### Why nothing was optimised here

Each remaining target has a problem measurement will not solve.

- **Skip the temporary IndexService for gated indices.** Defensible: a gated index's mappings go to the
  mapping store rather than cluster state, and T7 already refuses to gate an index carrying alias filters.
  But it moves mapping validation from creation time to first use. That is a semantic trade, not an
  optimisation, and it belongs to whoever owns the mapping validation contract.
- **Cache built analyzers.** Core OpenSearch behaviour affecting every index creation, not just gated ones.
- **Move the descriptor write submission off the cluster manager thread.** Safe, worth perhaps five percent,
  and needs a `ThreadPool` plumbed through `DescriptorGate.install` to get it.

### Where creation actually stands

**~850 per second, so a hundred million indices in about 1.35 days.** Down from 4.9 days (S31), via 2.1
(S34), via 1.35 (S35). The three corrections were: wrong operation, unstated conditions, instrumented JVM.

### On batching, settled

Batching descriptor writes buys at most 4 percent and is not worth building. Batching creations into fewer
cluster state tasks was the open question, and the 75 percent figure closes it: there is roughly a third
more headroom on that thread, not a multiple.

## S37 (T17): a gated creation acknowledges before its descriptor lands

Found while looking for the last few percent of creation throughput, by reading the path the profiler
pointed at. `MetadataCreateIndexService.clusterStateCreateIndex` says of the gated branch:

> The descriptor write is the creation, and it is the thing that must succeed.
>
> Failure semantics invert from H2b's here. During dual write a lost descriptor cost a comparison; now it
> costs the index.

It then calls `IndexDescriptorPublisher.publish`, which reports whether a publisher was **invoked**, not
whether the write **landed**. The publisher routes a live descriptor to `DescriptorStore.putAsync`, which
submits and returns, logging failure at warn.

Probed rather than argued, by closing the descriptor index so the write cannot land:

```
T17: gated creation of [doomed-idx] returned acknowledged=true
     while the descriptor index was closed
```

The client is told the index was created. There is no cluster state entry, because that is what gating
means, and no descriptor, because the write failed. **The index exists nowhere and the client believes it
exists.**

### Why it is this way

W4 produced it. The publish hook runs on the cluster state thread while a state is being built, and a
blocking descriptor write there deadlocked the node rather than failing. `putAsync` was the answer to that
deadlock, and it silently traded away the durability the gated branch later came to depend on. Both
decisions were locally correct; the second one changed what the first one meant.

### The fix, which is not in this commit

The descriptor write has to move off the cluster state task and onto the request path, which is what H3
describes: `create()` with `op_type=create` is the uniqueness gate and the creation, so it belongs where it
can block and report failure. Two consequences fall out of that, and both are improvements:

- the acknowledgement becomes truthful
- `op_type=create` actually gates uniqueness, which the current path does not use at all, since `publish`
  routes to a plain put

That is a change to how index creation is sequenced. It needs its own design rather than being appended to
a performance pass, so the test is committed under `AwaitsFix` and the defect stays reproducible.

### Status

**This is a blocker for enabling gated creation**, ahead of any remaining throughput work. Creation is at
roughly 850 per second and the last available optimisations are worth a few percent each; correctness of the
acknowledgement is worth more than all of them.

### Why this is worth writing down

The performance investigation found it. Three profiles looking for milliseconds walked past this line, and
it only surfaced when the question became "what does this code actually do" rather than "how long does it
take". Optimisation work reads code adversarially and at a level of detail that review does not, which is
worth remembering the next time a performance pass is deprioritised.

## S38 (T21): a sleeping tenant's first request costs about 41 ms

T20 established that an empty awake shard costs 118 KB and three file descriptors, so an index per tenant
only works if most tenants are asleep. Scale to zero moves that cost rather than removing it: a sleeping
tenant is free until it is not, and then somebody waits. That wait is a per-tenant service level, paid on
every visit by a tenant who queries once an hour.

`ServerlessStorageShardSuspensionIT` already proved the mechanism works. It never timed it, because proving
a mechanism and sizing it are different jobs.

| round | wake latency |
|---|---|
| 0 | 52.3 ms (carries warmup) |
| 1 | 41.5 ms |
| 2 | 36.0 ms |
| 3 | 43.5 ms |
| 4 | 40.9 ms |

**Median 41.5 ms** excluding the first. One nearly empty shard, local recovery, idle cluster, so a floor: a
real tenant's shard recovers more data, and on a busy cluster it queues behind other reroutes.

41 ms of coordination overhead is small enough that the wake path is not the obstacle. What a real tenant
pays on top is recovery of their actual data, which is a function of their index size rather than of this
mechanism.

## S39 (T22): the read path assumed uniform access, and tenant traffic is not uniform

Earlier reasoning about the read path at many coordinators concluded the descriptor cache would be nearly
useless: fifty thousand entries against a hundred million indices is 0.05 percent, so effectively every read
pays the miss. That rested on an assumption never stated as one, that tenants are accessed uniformly.

| capacity | uniform | zipf 0.8 | zipf 1.0 | zipf 1.2 |
|---|---|---|---|---|
| 20 (0.5%) | 0.5% | 6.7% | 20.2% | 40.9% |
| 100 (2.5%) | 2.4% | 18.5% | 39.7% | 62.8% |
| 200 (5.0%) | 4.8% | 26.2% | 49.2% | 71.1% |
| 500 (12.5%) | 11.9% | 40.1% | 62.0% | 80.7% |
| 1000 (25%) | 23.5% | 53.7% | 72.9% | 86.8% |

The uniform column tracks capacity exactly, which is the arithmetic the earlier reasoning did and is correct
for uniform access. At Zipf 1.0, the distribution multi-tenant traffic usually resembles, the same 0.5
percent capacity returns 20 percent rather than 0.5, and 5 percent returns 49 percent.

**The property that matters at scale is that Zipf hit rate follows the absolute cache size, not the
fraction.** A cache holding fifty thousand tenants earns whatever share of traffic the top fifty thousand
tenants generate, and that share barely moves whether the population is four thousand or a hundred million.
So the 0.05 percent framing was the wrong denominator, not a pessimistic estimate.

Measured at four thousand tenants with capacities from 0.5 to 25 percent. Extending to a hundred million
with a fifty thousand entry cache is an order of magnitude below the smallest capacity measured here, so the
shape transfers and the number should be re-measured before being quoted.

Re-measured during T28 at ten thousand accesses per arm rather than forty thousand, because eight hundred
thousand reads made this benchmark expensive enough to exceed the suite budget it shares. Every cell agreed
with the table above to within about a percentage point, which is what a ratio estimated from ten thousand
samples over four thousand tenants should do.

## S40 (T25): a wildcard cannot see a gated index, and mostly does not say so

`WildcardExpressionResolver.matches` reads `metadata.getIndicesLookup()` in all three of its branches:
match-all takes the whole lookup, a suffix wildcard takes a `subMap` of it, any other pattern filters it.
A gated index is absent from that map by construction, and `AbsentIndexDescriptorSuppliers` has no pattern
seam, only exact-name `supply`, `exists` and a bounded `page`.

Measured against five gated tenants, resolved against an empty cluster state so the descriptor store is the
only possible source of an answer:

| expression | options | result |
|---|---|---|
| `tenant-*` | default (`allowNoIndices=true`) | 0 names, no error |
| `*` | default | 0 names, no error |
| `tenant-*` | `allowNoIndices=false` | `IndexNotFoundException` |
| `tenant-000` (exact) | any | resolves, control |

The severity is in the second column rather than the first. Under the options nearly every client uses, a
tenant running `tenant-*` is told there are no matching indices rather than that the question cannot be
answered, which is the same shape of failure as H19's plausible wrong cluster stats and T23's eight
acknowledged creations of one name.

## S41 (T26): a capped prefix wildcard is bounded only with an index sort and no hit tracking

The repair everyone reaches for is a prefix seam over `findNamesByPrefix` with a cap on how many names a
pattern may expand to, enforced by asking for `cap + 1` and refusing when that many come back. That is only
bounded if a query with `size = cap + 1` costs the same whether the prefix matches a thousand names or a
hundred million, which Lucene does not give for free: ranking by name over a prefix query normally visits
every match.

Single shard, one segment, `size=101`, median of nine rounds, arrival order shuffled:

| population | plain | index-sorted | index-sorted, `track_total_hits=false` | narrow prefix (control) |
|---|---|---|---|---|
| 50,000 | 17.97 ms | 9.52 ms | 6.34 ms | 7.96 ms |
| 200,000 | 29.48 ms | 11.20 ms | 3.28 ms | 3.12 ms |
| 800,000 | **49.72 ms** | 11.57 ms | **4.15 ms** | 3.56 ms |

**Both settings are required and neither is sufficient.** Index sorting alone still grows, because an exact
hit total cannot be produced without visiting every match. Dropping hit tracking alone leaves the collector
ranking in an order the segments are not stored in. With both, a prefix matching the entire population costs
what a prefix matching ten names costs.

The plain column is the cost of the design without this: 16x the population for 2.8x the time, which
extrapolates to roughly six seconds at a hundred million. The terminated column shows no trend at all.

A first run at 5k, 20k and 80k reported every arm flat and would have been quoted as "the cap is free".
Every arm sat within a millisecond or two of the transport round trip, so a linear scan of eighty thousand
documents was hidden inside the floor. The populations here are chosen so a scan cannot hide.

### S41b: what the index sort costs the write path

Creation throughput is a headline number at roughly 850 per second, so a flat wildcard bought by halving
creation would be a bad trade made quietly.

| arm | rate |
|---|---|
| plain | 15,795 docs/s |
| index-sorted | 16,841 docs/s |

**0.94x, which is to say no penalty.** The first run of this wrote names in ascending order and reported the
same answer, and that run should not have been quoted: feeding a sorted index already-sorted input is the
one case where sorting at flush is free. The numbers above shuffle arrival order, which is what descriptor
writes actually do, since tenants are created in an order unrelated to their names.

## S42 (T27): gated listing returns one page and reports it as the whole population

H16 made pagination able to see gated indices by merging one pager page into the cluster state page, and the
asymmetry it rests on is sound. What was never driven is the loop: every test of the merge asked for one
page. Twelve gated indices, pages of four, creation dates deliberately inverse to name order:

| walk | pages | distinct names | order returned |
|---|---|---|---|
| ascending | 1 | **4 of 12** | `paged-003, 002, 001, 000` |
| descending | 1 | **4 of 12** | `paged-000, 001, 002, 003` |
| ordinary indices, same walk (control) | 3 | 12 of 12 | correct |

Three defects, each sufficient on its own:

1. **The next-page token is computed from the cluster state page alone.** With every index gated that count
   is zero, zero is not greater than the page size, and the token is null. A null token is how this API says
   "that was everything", so an operator listing a hundred million indices is shown ten and told there are
   no more.
2. **The pager and the merge disagree about order.** `mergeGatedIndices` orders by `(creationDate, name)`
   and states that both sides arrive already in page order. `DescriptorGate.pagerFor` orders by `name` alone
   and resumes with `search_after(afterName)`, discarding the `afterCreationDate` it is handed. The page is
   therefore selected by name and then ordered by date, which is visible above as four names chosen
   alphabetically and returned in reverse.
3. **Descending fetches the ascending first page and reverses it**, which reverses the first page rather
   than producing the last one. Both walks return the same four names.

Creation dates run opposite to name order on purpose. With both orders agreeing, a pager sorting by name and
a merge sorting by date produce identical pages and defect 2 is invisible, which is presumably how it
survived H16 and T5.

Defect 1 is what hides the other two, since a walk that stops after one page never reaches a second page to
be wrong about.

## S43 (T28): the wildcard contract, built

S40 measured that a wildcard matches no gated index and mostly says nothing about it. S41 measured that a
capped prefix expansion is bounded only with an index sort and no hit tracking. This is what shipped, and
two parts of it disagree with the design that preceded it.

| pattern | over gated indices |
|---|---|
| `tenant-42-*`, at or under the cap | expands |
| `tenant-*`, over the cap | refused, naming the cap and the setting that controls it |
| `*-logs`, `a*b`, `tenant-?` | refused, naming the trailing-wildcard rule |
| `*`, `_all` | cluster state only, does not expand |
| exact name | resolves, unchanged |

### What the build changed about the design

**Match-all had to be exempted, and it took two attempts to learn why.** The design treated `*` as a prefix
with an empty prefix, so it would work on a small cluster and be refused on a large one. Measured, capping
`_all` made cluster health fail once the population passed the cap, and then hung the test cluster until the
suite timed out at twenty minutes, because the framework's own consistency checks ask the same question. The
second attempt exempted only empty expression lists, on the theory that an explicit `*` is a user
enumerating while an empty list is an API default. Measured, cluster health and index stats arrive with an
explicit `_all` anyway. The resolver sees an expression, not a caller.

So match-all keeps its cluster state meaning. The cost is stated rather than hidden: on a cluster small
enough that `*` would have worked, `*` silently omits gated indices. That is S40's defect narrowed to one
pattern and made deliberate, and it is the price of not letting a wildcard cap decide whether cluster health
answers.

**The resolver needed two call sites, not four.** The plan was to teach each of `matches()`'s three branches
about gated indices separately. The gated side has no `IndexAbstraction`, no aliases and no data streams to
reconcile, so it is a second source of names folded in beside `expand()` rather than a fourth branch. One
call covers every pattern and the prefix-or-refuse decision lives in one method. The second site is
`resolveEmptyOrTrivialWildcard`, which `_all` reaches without passing through the loop at all.

### S43b: two costs that were not there before, and one that was

A wildcard matching nothing now issues **0 descriptor point reads**, where before it issued one. Every
expression passes through `aliasOrIndexExists`, which consults the store on a miss, and an index name cannot
contain a star, so that read could only ever miss. Harmless from H8a until T28 made wildcards a normal path.
Counted rather than reasoned about.

A wildcard matching five gated tenants issues five reads, and those are the feature: each resolved name
needs a descriptor to carry its uuid or the request cannot reach the shard. The first version of that test
asserted zero against a matching prefix and failed, which is the test being wrong rather than the code.

`ExceptionSerializationTests` had been failing since T12, which introduced `DescriptorUnavailableException`
without registering it. An unregistered exception loses its type crossing the wire, which defeats the entire
purpose of a type that exists so a caller can tell "unknown" from "absent": the distinction held on one node
and was erased between two. Found only by running the full server suite rather than the descriptor subset.

### S43c: what the index sort costs the store's own workload

S41b measured index sorting as free on the write path and that number was quoted to justify making it a
contract. It was measured with bulk requests and no reads, which is not how the descriptor store behaves.
Re-measured under the shape that matters:

| operation | plain | sorted | ratio |
|---|---|---|---|
| 2,000 single-document writes | 505/s | 581/s | 0.87x |
| 5,000 realtime GETs by id | 1,099/s | 1,155/s | 0.95x |

No penalty on either, and faster on both.

Worth recording how close this came to the wrong conclusion. The T22 skew benchmark started exceeding its
suite timeout, an A/B removing the index sort appeared to fix it, and the sort was very nearly reverted on
that basis. The table above contradicted it, and re-running the benchmark in isolation with the sort in
place passed in nine minutes. The benchmark was simply expensive enough that a loaded suite tipped it over,
and it now runs a quarter of the accesses. **One run per arm is not an A/B**, which is the same lesson S26,
S31 and S35 each taught about a different quantity.

## S44 (T29): an alias on a gated index was silently dead, and the fix is not to resolve it

T7 closed the list of reasons an index keeps its cluster state entry and let a *plain* alias through, because
`IndexDescriptor` has a field for the name. The field is written, serialized, sized and round-tripped, and
`IndexDescriptor`'s own javadoc says aliases are carried "because resolution needs them to answer without
materializing". Nothing read it. Seventh mechanism in this area found correct and unreachable.

| query against a gated index with alias `tenant-alias` | result |
|---|---|
| the alias name, lenient options | **0 names, no error** |
| the alias name, strict options | `IndexNotFoundException` |
| `tenant-*`, the alias namespace | 0 names |
| the descriptor carries the alias (premise) | yes |
| the same alias on an ordinary index (control) | resolves |

### The question that changed the answer

The obvious repair is to resolve aliases by searching the `aliases` field, which T28's machinery already
makes easy. Before building it, one more thing was measured: whether an alias can be **changed** on a gated
index.

It cannot. Every alias operation is a cluster state update that looks the index up in `Metadata` and rewrites
its `IndexMetadata`, and a gated index is not in `Metadata`. Adding an alias to one does not fail cleanly, it
times out with `ClusterManagerNotDiscoveredException`.

So resolution would have served an alias that could be set once at creation and never added, removed or
repointed, with a refresh-bound visibility window on top. That is a partial feature inviting exactly the
usage it cannot support, and an alias is the worst place for that: a client is given an alias precisely so
the index behind it can change.

**So `DescriptorRepresentable` refuses to gate any index that declares an alias**, widening T7's four alias
rules into one. An aliased index keeps its cluster state entry and behaves exactly as it always has.

The four narrower rules were about a descriptor *dropping* something: a filter, routing, a write index flag,
a hidden flag. The new rule is about something different, and the distinction is worth keeping: a plain alias
loses nothing on the way into a descriptor. It is refused because nothing can ever change it.

### What this costs

An index with an alias is not gated, so it counts against the cluster state budget the whole design exists to
protect. If tenant indices routinely carry aliases, gating does not apply to them and the population that can
reach a hundred million is only the alias-free part. That is a product constraint rather than a defect, and
it is now visible at creation in a log line naming aliases as the reason, rather than as an alias that
resolves to nothing.

Supporting aliases properly needs alias names to become a second key space in the descriptor store, with
their own uniqueness against index names and their own write path for add, remove and repoint. That is a
feature, not a repair, and nothing here forecloses it.

## Re-scoping the target from a hundred million to ten million

A hundred million was never runnable on the machine this work is done on, and every 100M figure in this
document is an extrapolation. Ten million is the target now. This records what that changes, and the short
answer is that it changes the arithmetic and none of the decisions.

### What is already measured at exactly ten million

| quantity | at 10M | where |
|---|---|---|
| descriptor point lookup | 0.32 ms at controlled segment counts | S29 |
| descriptor point lookup, uncontrolled segments | 0.71 ms | S28 |
| descriptor document write rate | 23,843/s | S28 |
| descriptor index size | about 1.8 GiB | S9, re-based |

S28 and S29 populated ten million real descriptors. That is a real run, not a projection, and it is the
dimension the whole design rests on.

### What ten million costs that a hundred million did not

Real gated index creation through the API is about 850 per second (S31, corrected by S35 for an
instrumented JVM). So the populations differ by more than a factor of ten in wall-clock patience:

| population | creation time at 850/s |
|---|---|
| 10,000,000 | about 3.3 hours |
| 100,000,000 | about 1.35 days |

Three hours is a run that can actually be done here, which is the point of the re-scope.

### Which decisions would reverse at ten million, and none do

- **Gating itself.** Without it, index metadata is roughly 70 GB per node at 100M, so about **7 GB per node
  at 10M**. Still far past what a cluster manager can hold, so the residency problem is not solved by
  shrinking the target. Everything from H2 onward stands.
- **The wildcard cap (T28).** It bounds fan-out, which is set by how many shards one request wakes, not by
  the population. Unchanged at any target.
- **The descriptor index sort (T28).** Extrapolating S41's 49.7 ms at 800k, an unsorted capped prefix query
  costs roughly 600 ms at 10M against a flat few milliseconds sorted. Still decisive.
- **Refusing aliases (T29), the byte-budgeted cache (T4b), tombstones, uniqueness (T18).** All correctness
  or memory decisions with no population term.

### What is still not measured at ten million

- **End to end.** No search or bulk request has been measured against a gated index at any population. This
  is the largest remaining gap and it is not a scale problem, so it does not need ten million to start.
- **Real gated creation at 10M**, as opposed to document writes at 10M. About 3.3 hours.
- **Wildcard expansion at 10M.** S41 measured to 800k, where the terminated arm was flat. Flat is a claim
  that should be checked once at the target rather than extrapolated, which is the mistake S28 caught S27
  making about lookup.

## S45 (T30): the gated path works end to end, and does not honour a shard count

Every measurement before this was a component. Creation, resolution, placement, wake, listing, wildcards and
mappings each had numbers, and no request had ever been indexed into a gated index or searched out of one.
That gap was the one thing that could have invalidated the rest, since every component can be right while
the composition serves nothing.

Twenty gated tenants created through the real API, fifty documents each:

| stage | cost |
|---|---|
| create | 32.0 ms/index |
| write | 343.6 ms/tenant, 50 documents |
| search | 6.9 ms/tenant |
| **documents found** | **1,000 of 1,000** |

**The composition works, for indices that start gated and do not stay gated.** S50 corrects this: the
tenants were auto-created into cluster state by their first write, so the documents below were served by
ordinary indices. The path is real and the result is real; what it does not demonstrate is a *gated* index
serving a write.

The per-stage numbers are a floor rather than a benchmark. One node, one local disk, an empty cluster, and
a test JVM carrying jacoco. They are recorded to show the path composes and to give the shape of where time
goes, not to be quoted as production latency.

### The defect it found

The wildcard search reported touching **100 shards for 20 indices**, each of which had asked for one. Chased
with a control, and the control is what made it a finding rather than a puzzle:

| | asked | descriptor recorded | search touched |
|---|---|---|---|
| gated index | 1 | 3, then 5, then 10 across runs | same as the descriptor |
| ordinary index, same cluster, same request | 1 | n/a | 1, with `number_of_shards=1` in its settings |

**That conclusion was wrong, and S48 withdraws it.** Asking for one, two, four and seven shards records
exactly those, in three isolated runs and again after writing a document. The probe above varied only the
observation while holding the request fixed at one, which cannot separate "the setting is ignored" from
"something else is being counted". What survives is narrower and unexplained: see S48.

This is not cosmetic. Every residency figure in this work is per shard: T20 measured 118 KB and 3.06 file
descriptors per awake shard, and T28's expansion cap of a hundred was chosen from how many shards one
request wakes. If a tenant asking for one shard gets ten, each of those figures is out by that factor per
tenant, and the descriptor faithfully records the wrong number, so the error is durable rather than
transient.

Committed under `AwaitsFix` with the control alongside it, so the next session starts from a reproducer
rather than from this paragraph. The cause was not located: the plugin's index setting provider only injects
a shard count for data-stream backing indices with no explicit value, so something else on the gated
creation path is losing the request's settings.

## S46 (T32): batching has nothing left to amortise, and creation is CPU-bound

### Batching, answered directly rather than from headroom

S36 closed the batching question with an inference: the cluster manager thread is about 75 percent busy, so
there is a third more headroom rather than a multiple. That is an argument about CPU, and batching is not
about CPU. A batching executor collapses N cluster state tasks into one *publication*, which is why S15
measured it worth one to two orders of magnitude for wake and sleep. So the question is how many
publications a creation costs, and the cluster state version answers it exactly:

| creation | cluster state versions advanced, per index |
|---|---|
| ordinary | 2.16 |
| gated | **0.00** |

**A gated creation costs zero publications, so batching has nothing to collapse.** Creations already run
through a batching executor, so the mechanism is present and idle. This is structural rather than a matter
of degree, and it does not change with population, node count or in-flight depth.

The other two candidates were already dead: P10 removed the cluster state queue entirely and moved 235 to
245 per second, which is noise, and S36 measured batching descriptor writes at four percent.

### Where the CPU and I/O actually go

Profiled with Java Flight Recorder at `settings=profile`, jacoco disabled and the security manager off,
since S35 established that both contaminate this measurement. Sixty-one seconds, 2,160 execution samples.

**There is essentially no I/O.** Over the whole recording: **zero** socket read or write events, 39 file
reads and 5 file writes. These events fire only above a 10 to 20 ms threshold, so the honest statement is
that no individual I/O operation was slow, not that no I/O happened. Off-CPU time supports the same reading:
the largest parked threads are transport workers at 47.8 and 42.3 seconds out of 61, which is idle waiting
for work rather than blocking, and the cluster manager thread does not appear in the parked list at all.

**The CPU, counting only OpenSearch threads (533 samples):**

| thread role | share of OpenSearch CPU |
|---|---|
| `clusterManagerService#updateTask` | 43.5% |
| `write` pool | 27.6% |
| `transport_worker` | 16.9% |
| other OpenSearch | 10.7% |
| Lucene merge | 1.3% |

The cluster manager thread is the single largest consumer, which agrees with S36 and with the 75 percent
busy figure. The write pool at 27.6 percent is the descriptor write, and it is larger than any previous
profile suggested, which is consistent with T18 having put a real indexing request on the acknowledgement
path.

### A flag raised and withdrawn

A first, contaminated recording showed the descriptor index's Lucene merge thread at about 95 percent of one
core, and it was flagged as a possible tension with W8's merge policy: `segments_per_tier=4` was chosen by
S29 and S30 to bound lookup latency, and a policy that costs a core during creation would mean two spikes
each optimised a metric without seeing the other's cost.

**It is not a tension.** That figure came from a single one-second `ThreadCPULoad` window, so it was a peak.
Across the clean recording merge accounts for 1.3 percent of OpenSearch CPU, 7 samples out of 533. Merging
is bursty and brief, and the merge policy costs creation nothing worth naming.

### What the profile does not support

Frame-level attribution. The JSON export associates method names with the wrong types, producing impossible
frames such as `java.lang.String.executeAndMaintainThreadName`, and the JDK's own pretty printer crashes on
the same recording with a `StringIndexOutOfBoundsException`. Thread-level attribution and the duration
events are sound; method percentages from this recording are not, and none are quoted above.

The other limitation is sample budget: 74.4 percent of all samples are test harness threads, so the
OpenSearch numbers are ratios within 533 samples. They are strong enough to separate CPU from I/O and to
rank thread roles, and too thin to rank methods.

### Throughput, re-measured after T18

| run | creations/sec |
|---|---|
| 1 | 323 |
| 2 | 432 |
| 3 | 360 |

Median 360, against the 536 S35 recorded under the same instrumented conditions. The spread across three
runs is 34 percent, so this suggests T18's acknowledgement now waiting on a descriptor write cost something
without establishing how much. Worth an A/B against the pre-T18 commit before being quoted, and **not** on
one run per arm, which is the mistake S43c already records.

## S47 (T33): what Elasticsearch does about this, and why it does not transfer

Two efforts in Elasticsearch are the obvious places to look, and neither addresses the constraint measured
in S46. Recording that is the point: the temptation was to import a known optimisation, and the reason not
to is specific.

**Batched master task queues** (elastic/elasticsearch#92021, closing #81626 "Pending task batching can be a
bottleneck"). Replaces one `PriorityBlockingQueue<Runnable>` with per-priority queues, per-executor typed
queues created by the client, and avoids enqueueing on the thread pool unless work exists. Their
`MasterService` documentation now forbids unbatched tasks in new production code, calling them a source of
performance and stability bugs.

**It optimises publication, and T32 measured our gated creation at zero publications per index.** The
bottleneck their work removes is one we already have at zero. Creations here already run through a batching
executor, and batching cannot collapse what does not exist.

**Stateless Elasticsearch** (Elastic Cloud Serverless, GA on AWS December 2024, ACM SoCC 2025). Offloads
index data, translog and cluster state to an object store, collapsing four data tiers into two.

**It changes where cluster state is persisted, not how large it is.** A hundred million indices is still a
hundred million metadata entries held by the elected master, wherever the bytes are durably stored. This
work removes the per-index entry instead, which is the more aggressive move and the one the population size
requires. The same limit applies to OpenSearch RFC #17957, which proposes externalising cluster state to
etcd.

So on this specific constraint there is nothing to copy. That is worth stating plainly rather than leaving
the impression the research was skipped.

### What gating already bought, and what is left

`GatedCreationThroughputIT`, both arms in one cluster, twenty indices each:

| arm | creations/sec |
|---|---|
| ordinary | 10 |
| gated | **213** |
| ordinary again, warmup control | 19 |

**Gated creation is about 21x ordinary.** So skipping publication and the `Metadata` rebuild is where the
large factor already came from, and it has been collected. What remains at a few hundred per second is the
per-index creation pipeline, which S46 measured as 43.5 percent of OpenSearch CPU on the single
`clusterManagerService#updateTask` thread.

### The ranked options, with what is already known about each

| option | share of the thread | status |
|---|---|---|
| run gated creation off the cluster manager thread entirely | ~43.5% becomes parallel | the only option with a multiple in it; H3 designed it, T18 deferred it because throughput was not then the question |
| skip `withTempIndexService` for gated indices | 15-20% | a semantic trade, moving mapping validation to first use |
| move the descriptor write submission off the thread | ~10%, S36 estimated ~5% net | safe, needs a `ThreadPool` through `DescriptorGate.install` |
| cache settings validation | ~20% of the profile | **dead**: measured at 0.14 percent benefit |

The ceiling for the first option is set by what the descriptor write itself sustains, which S28 measured at
23,843 documents per second against a few hundred creations per second now. That is the gap worth an
architectural change, and it is the same gap H3 named before any of this was built.

## S48 (T31): the shard count claim, withdrawn, then superseded by S49

S45 reported that a gated index does not get the shard count its request asked for, and named a cause: the
test framework's randomised index template winning over the explicit setting. **Both the claim and the cause
were wrong.**

Asked for four distinct counts instead of one:

| asked | recorded at creation | recorded after writing a document |
|---|---|---|
| 1 | 1 | 1 |
| 2 | 2 | 2 |
| 4 | 4 | 4 |
| 7 | 7 | 7 |

Every value honoured, in three isolated runs, and unchanged after a write, which was the leading hypothesis
for what might corrupt it afterwards.

**The original probe could not have distinguished the two possibilities.** It held the request fixed at one
shard and watched the observation move (three, then five, then ten), which is consistent with the setting
being ignored and equally consistent with something other than the index's shard count being read. Varying
the request is what separates them, and that was not done before the finding was written up and committed.

### What does survive, and is still unexplained

Running the whole class in one cluster, in a single run:

| index | asked | recorded |
|---|---|---|
| `shards-asked-1/2/4/7` | 1, 2, 4, 7 | 1, 2, 4, 7 |
| `shardcount-probe` | 1 | **9** |
| `e2e-tenant-*`, via wildcard search | 1 | **8 per tenant** |

Different indices in the same cluster disagreeing rules out a per-suite random template, which would give one
value to all of them. Writing a document does not cause it. It is order-dependent, it reproduces only when
the class runs as a whole, and no mechanism has been found.

It still matters, because every residency figure here is per shard (T20: 118 KB and 3.06 file descriptors)
and T28's expansion cap was chosen from how many shards one request wakes. But it is now an open question
with an honest description rather than a diagnosis, and the reproducer stays under `AwaitsFix` with the
multi-value control beside it so the next attempt starts from both halves.

**The method lesson, which is the third instance today.** The index sort was nearly reverted on a
single-run A/B that a per-operation measurement then contradicted. The T22 timeout was blamed on that same
sort and turned out to be suite contention. This one held the input fixed and varied only the reading. All
three would have been caught by the same discipline: change the thing you are attributing the effect to, and
more than once.

## S49 (T31, third attempt): gating does not survive the first write

S45 said a gated index does not get the shard count it asked for, and named a cause. S48 withdrew that as
unexplained. Both were wrong, and the thing underneath is the most serious finding in this work.

Varying the requested count and re-reading the descriptor after writing one document:

| asked | recorded at creation | recorded after a write |
|---|---|---|
| 1 | 1 | **9** |
| 2 | 2 | **9** |
| 4 | 4 | **9** |
| 7 | 7 | **9** |

Four indices collapsing to a single value is a cluster default rather than a per-index setting, which is
what pointed at the mechanism. Asked directly:

```
gatedness-probe
  at creation:   in cluster state = false   descriptor shards = 1
  after a write: in cluster state = TRUE    descriptor shards = 1
```

**A write puts the gated index back into cluster state.** `IndexDescriptorPublisher.publish` fires only from
`Metadata.Builder.put`, so a descriptor rewritten during a write is the signature of the index entering
metadata and being rebuilt there from defaults. The shard count was the visible symptom and is the smaller
half of the problem.

### Why this outranks everything else currently open

The entire design is that a gated index has no cluster state entry. If writing to one restores the entry,
then gating holds only until first use, and a population of a hundred million *written* indices is a
hundred million entries, which is precisely the cost H2 onward exists to remove. Every residency number in
this document describes a population that has been created and not yet used.

It also explains an oddity nobody chased: the T30 wildcard search reporting 100, 160 and 180 shards for
twenty tenants across runs. Those tenants had been written to, so they were back in cluster state with
default shard counts.

### What is not yet known

Which write-path component does it. Auto-creation on the write is the obvious suspect, since the index is
absent from metadata and an indexing request that cannot find one there creates it, but that is a reading of
the code and this area has punished three such readings today alone. The next step is to find the caller,
not to assume it.

### The method note, since this is the third diagnosis of one symptom

Attempt one held the request fixed and varied only the observation, and produced a confident wrong cause.
Attempt two varied the request, found it honoured, and concluded there was no defect, which was right about
the shard count and wrong about there being nothing there. Attempt three asked what the system's own
invariant was, rather than what the number was, and the invariant is what broke. **Measuring the quantity
you noticed is weaker than measuring the property the design guarantees.**

## S50 (T34): there is no gated write path, and auto-creation was hiding it

S49 found that a write puts a gated index back into cluster state. T34 asked which component does it, and
the answer runs deeper than the component.

**The mechanism.** `AutoCreateIndex.shouldAutoCreate` asks
`IndexNameExpressionResolver.hasIndexAbstraction`, which reads `state.metadata().getIndicesLookup()` and
never consults the descriptor seam. H8a taught `aliasOrIndexExists` to consult it and did not teach this
sibling. So a write to a gated index is told no such index exists, and auto-creates one.

**The fix that was not one.** Routing that check through `AbsentIndexDescriptorSuppliers.exists`, exactly as
its sibling does, is a three-line change and it is correct. Applied, writes stop being auto-created and
start failing instead:

```
IndexNotFoundException: no such index [e2e-tenant-000]
  at Metadata.getIndexSafe(Metadata.java:837)
  at TransportBulkAction$BulkOperation.addFailureIfIndexIsUnavailable(TransportBulkAction.java:968)
```

`TransportBulkAction` needs the index's `IndexMetadata` from cluster state to route a document. A gated
index has none. **There is no gated write path at all**, and auto-creation was supplying one by silently
un-gating the index on its first write.

The change was reverted. It converts a silent design failure into a hard failure without providing the
missing path, which is worse for anyone using this today, and the real work is the path itself.

### What this corrects

S45 reported the end-to-end composition working: a thousand documents written and found across twenty
tenants. Those documents are real and were served correctly. They were served by **ordinary indices**,
because each tenant was auto-created into cluster state by its own first write. So the demonstrated result
is that an index which starts gated and is then un-gated by writing to it works end to end, which is not the
claim that was made.

Read and search by exact name over a genuinely gated index are still demonstrated, by W9, DescriptorGateIT
and the T25 and T28 wildcard work, none of which write documents.

### What it costs, and what it needs

Every residency figure in this document describes a population that has been created and not written to. A
tenant that receives one document today rejoins cluster state at full cost, so the hundred-million and
ten-million projections describe an idle fleet rather than a working one.

The missing piece is the write equivalent of what P6 and P7 built for placement: `TransportBulkAction` and
whatever else calls `getIndexSafe` need to obtain an `IndexMetadata` for a gated index from its descriptor,
which `IndexDescriptor.toIndexMetadata` already synthesises for placement. That is a real piece of work and
it is now the top of the list, ahead of both the cluster manager thread and the ten million run, because
both of those describe a system whose indices do not stay gated once used.

## S51 (T35): what the gated write path actually requires, mapped by building it

T35 attempted the write path and did not finish it. What it produced is the inventory, which is worth more
than the partial code and is why the code was reverted rather than left in the tree.

**Method: fix one call site, run, read the next stack.** Each failure names the next assumption that a
written index lives in cluster state. In order:

| # | site | what it needs | fix |
|---|---|---|---|
| 1 | `AutoCreateIndex.shouldAutoCreate` via `hasIndexAbstraction` | existence | route through `AbsentIndexDescriptorSuppliers.exists` |
| 2 | `TransportBulkAction.addFailureIfIndexIsUnavailable` | `getState()` | descriptor carries state |
| 3 | `TransportBulkAction.doRun`, index/create branch | `mapping()`, `getCreationVersion()` | synthesised metadata; mapping is legitimately absent (H4c) |
| 4 | `TransportBulkAction.doRun`, append-only branch | `isAppendOnlyIndex()` | synthesised metadata |
| 5 | `TransportBulkAction.addFailureIfAppendOnlyIndexAndOpsDeleteOrUpdate` | `isAppendOnlyIndex()` | synthesised metadata |
| 6 | `TransportBulkAction.doRun`, data stream guard | `IndexAbstraction` | absent means no parent data stream, and T29 refuses to gate anything that could be one |
| 7 | `OperationRouting.indexMetadata` | shard count to hash against | synthesised metadata |
| 8 | `RoutingTable.shardRoutingTable` via `OperationRouting.shards` | the shard's routing entry | **not reached**; the existing `AbsentIndexRoutingSuppliers` fallback beside it did not answer |

Sites 1 through 7 were written and each moved the failure to the next one. Site 8 is where it stopped, and
it is the interesting one: the fallback for it already exists, added by P6 and P7, and its own comment says
it is there so "a computed index answers searches and fails writes and gets" cannot happen. It did not
answer here, so either the computed placement disagrees with the descriptor's shard count or the supplier
declines for a reason not yet established. That is the next question, and it should be asked with a probe
rather than by reading.

### Why it was reverted rather than committed part-done

Site 1 is the crutch. Removing it stops auto-creation from un-gating the index, and until site 8 works,
writes fail instead. Committing sites 2 through 7 without site 1 leaves code that cannot execute, because
auto-creation still fills in the metadata they exist to synthesise, which is a seventh instance of the
"correct and unreachable" pattern this area keeps producing. Committing all of them breaks writes. Neither
is a good state to leave a shared tree in, so the branch keeps working behaviour and this document keeps the
map.

### The shape of the finding

**Seven call sites on one request path each independently assume an index is in cluster state**, and that is
after P6, P7 and H8a had already done this for placement and resolution. The descriptor design removes a
per-index entry that a great deal of code treats as unconditional, and the write path was simply never
walked. The count is the useful part of this spike: it says the remaining work is mechanical but not small,
and that it needs an end-to-end test to drive it rather than reasoning about which callers matter.

## S52 (T36 and T35 continued): the map, extended, and the wall behind it

S51 stopped at site 8 and called it unexplained. It is explained, and it was not a defect.

**T36: the routing fallback declined because computed placement was never on.**
`COMPUTED_PLACEMENT_ENABLED_SETTING` (`serverless_storage.computed_placement.enabled`) defaults to false,
and no test in this class ever set it. Without it nothing supplies a routing table for a gated index, so
`AbsentIndexRoutingSuppliers.supply` correctly returned null. The seam was right; the harness was
incomplete.

That also settles how every earlier run of this class passed: with placement off, **nothing gated could
have served a request at all**, so the thousand documents S45 reported were necessarily served by the
ordinary indices auto-creation had put back into cluster state.

### The full site list, with placement enabled

Ten call sites on the write path assume an index is in cluster state:

| # | site | needs |
|---|---|---|
| 1 | `AutoCreateIndex.shouldAutoCreate` via `hasIndexAbstraction` | existence |
| 2 | `TransportBulkAction.addFailureIfIndexIsUnavailable` | `getState()` |
| 3 | `TransportBulkAction.doRun`, index/create branch | `mapping()`, `getCreationVersion()` |
| 4 | `TransportBulkAction.doRun`, append-only branch | `isAppendOnlyIndex()` |
| 5 | `TransportBulkAction.addFailureIfAppendOnlyIndexAndOpsDeleteOrUpdate` | `isAppendOnlyIndex()` |
| 6 | `TransportBulkAction.doRun`, data stream guard | `IndexAbstraction` |
| 7 | `OperationRouting.indexMetadata` | shard count to hash against |
| 8 | `OperationRouting.shards` routing table | computed placement, once enabled |
| 9 | `TransportBulkAction.executeBulk`, adaptive shard selection | `isAppendOnlyIndex()` |
| 10 | `TransportReplicationAction.ReroutePhase.doRun` | `IndexMetadata` for the shard |

All ten were written, and each moved the failure to the next. **Past all ten, the write reaches:**

```
UnavailableShardsException: [e2e-tenant-000][0] primary shard isn't assigned to a known node
```

### The wall, and why this stopped here

That is a different subsystem. Metadata resolution is now complete enough for the write to be routed, and
the shard it routes to does not exist on any node: computed placement produces a routing table, but nothing
has materialised the shard. That is the scale-to-zero and wake machinery, which T21 measured at 41.5 ms for
a shard that had been created and suspended, not one that has never existed.

So the gated write path is three problems, not one:

1. **Metadata residency assumptions**, ten call sites, all now known and all mechanically fixable.
2. **Placement**, which exists and is off by default, and which no test had enabled.
3. **Shard materialisation on first write**, which is unexplored. A gated index has never had a shard
   allocated, and the wake path assumes a shard that exists and is asleep.

Reverted again, for the reason S51 gives: with site 1 the writes fail, without it the other nine are
unreachable. The value here is the map, and the map now reaches the wall rather than stopping at a puzzle.

**What it means for the projections.** Nothing in this document has ever measured a gated index serving a
write, and now the reason is precise rather than suspected. The residency figures describe a population that
is created, resolvable, listable, wildcard-matchable and searchable-by-name, and that has never taken a
document. Whether the design holds under writes is still open, and item 3 is the part nobody has looked at.
