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

