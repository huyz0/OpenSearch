# Remaining work on scalable index metadata

Task breakdown for what is left after C1, C3, C5, C6 and C7 landed. Written to be worked one item at
a time. Background, measurements and the reasoning behind each decision are in
`rfc-scalable-index-metadata-plan.md` and `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md`.

## Conventions for every task here

- A task is done when the change compiles, has a test, **and the test has been confirmed to fail with
  the change reverted**. For behaviour-preserving work a green suite proves nothing on its own; it
  passes with and without the change. Break the production code deliberately, watch the test fail,
  restore.
- Run a wider test net than the change appears to touch. C7 passed 2,400 focused tests and a wider
  sweep found 10 failures.
- Never run a build while another gradle run is in flight. Both corrupt the shared build directory and
  produce failures that look real and are not.
- Measure with assertions disabled (`-da -dsa`). Gradle enables `-ea -esa`; production does not, and
  the difference was 2x on one measurement.
- Do not push without being asked.

---

# Phase A. Cold indices absent from the routing table

**Why first.** S6 measured the allocator binding at roughly 100k active shards, well before metadata
residency binds. This is the property that makes a quiescent tenant genuinely free: no routing entry,
so nothing for the allocator to consider. It is the highest-value item whose prerequisites are known
and small.

**Risk.** A2 and A3 change the contract of methods the search and write paths depend on. They are only
correct once an index can legitimately be absent from routing, which is what A5 establishes. Do A1
first for the audit, and do not land A2/A3 before A5 has a design.

**Status.** A1 done, and A7 -- the category it uncovered -- done except for two follow-ups. Sixteen
call sites that would have thrown `NullPointerException` on an index present in metadata and absent
from routing are now guarded. That work stands on its own: four of them are reachable by an ordinary
request today, and the rest are latent. Nothing yet makes an index routing-absent, which is A4 and A5.

| task | state |
|---|---|
| A1 audit | done, `rfc-routing-absence-audit.md` |
| A7.1 tiering health | done, tested |
| A7.2 four request paths | done; one tested, three inspection-only |
| A7.3 snapshot shard status | done, inspection-only; sibling deferred to A7.7 |
| A7.4 resize/merge/allocation | done, tested; latent rather than live |
| A7.5 clusterless shard-started | reviewed, no change needed |
| A7.6 integration coverage | open |
| A7.7 snapshot generation preconditions | done, tested |
| A5.1 cold-vs-gone | done, tested |
| A5.2 + A5.3 prune + recreate | done, tested, off by default |
| A5.4 decider still needed? | decided: yes, unchanged |
| A2 search degradation | done, tested |
| A3 shardRoutingTableOrNull | done, tested |
| A4 spike | done |
| A6 | done, both halves |

### A1. Audit every caller that assumes a routing entry exists -- DONE

See `rfc-routing-absence-audit.md`. 58 call sites classified. Three results changed this phase:

- **A3 shrinks a lot.** `TransportReplicationAction:1041` and `TransportBulkAction:741` already have
  the `primary == null` retry-and-wait branch the plan wanted them to reach. Only the lookup throws
  first. So A3 adds a null-returning variant used by those two, rather than changing the contract of
  `shardRoutingTable(ShardId)` across all 21 callers.
- **A2 stands as written** and is a real behaviour change.
- **A7 is new**, and is most of the actual work: twelve call sites that would throw
  `NullPointerException` on a cold index, none of which the plan named.

### A7. Fix the twelve unguarded dereferences found by A1

From `rfc-routing-absence-audit.md` category 2. Independent of whether cold indices ever ship: these
are latent defects today, reachable whenever metadata and routing disagree, exactly like the
`TransportBroadcastReplicationAction` one already fixed.

Do them in this order, most reachable first:

- A7.1 -- DONE, though not where expected. `ClusterIndexHealth` is `@PublicApi` and `ClusterStateHealth`
  already *skips* an index with no routing entry rather than reporting on it, so teaching the
  constructor to accept null would have invented a health semantic for a state core deliberately
  excludes. Guarded the two callers instead, matching that convention:
  `TieringRequestValidator:141` reports not-healthy, `TieringServiceValidator:189` rejects with
  `INDEX_RED_STATUS`. Both tests reproduce the original NPE when the guard is removed.
- A7.2 -- DONE. All four guarded. `TransportUpgradeAction` has a unit test that reproduces the
  original NullPointerException. The other three are `shards()` overrides that a unit test cannot
  reach without disproportionate scaffolding (see `rfc-routing-absence-audit.md`); they are covered by
  inspection, and A7.6 below exists to close that.
- A7.6 Integration coverage for the three `shards()` guards: stand up a node, put an index in metadata
  without a routing entry, and assert analyze, get-field-mappings and update report no shard available
  or retry rather than throwing. Needed anyway once cold indices exist.
- A7.3 -- PARTLY DONE. `SnapshotsService:3443` needed no decision: that method already marks a shard
  `MISSING` when the index's metadata is absent, and an index with no routing entry has no shards to
  snapshot either, so it takes the same branch. `SnapshotsService:3511` is left alone on purpose --
  it does not check `indexMetadata` either, so the question is what its callers guarantee rather than
  what absent routing means. See A7.7.
- A7.5 -- REVIEWED, no change. `LocalShardStateAction:53` is clusterless-mode only and runs while a
  shard of that index is being marked started, so the index cannot be absent from routing there. A
  guard that cannot fire and cannot be tested is worse than the note.
- A7.7 -- DONE, and the answer was to delete the dependency rather than guard it. The caller,
  `createSnapshotV2`, builds its index list from `metadata().indices().keySet()` and never filters on
  routing, so the method's precondition was "every index in metadata has a routing entry" -- exactly
  what cold-absence removes, reachable by an ordinary create-snapshot call. But the routing table was
  never actually needed: the only thing read out of it was `indexRoutingTable.shard(i).shardId()`, and
  only `.id()` of that is used, which is `i`. The lookup laundered the loop counter into itself. The
  parameter is gone, so the requirement is gone rather than guarded -- better than the alternative,
  since a guard that skipped such an index would have silently left it out of the snapshot. A probe
  against the pre-fix code confirmed the `NullPointerException` was real.
- A7.4 -- DONE. All four guarded. Unlike the request paths these are **not reachable today**: they need
  a closed or write-blocked source, and closing keeps the routing entry via `addAsFromOpenToClose`.
  Latent guards rather than live defects, and the audit says so.


### A2. Search degrades instead of throwing -- DONE

`computeTargetedShards` no longer routes through the throwing lookup. It reads metadata first, so an
index in neither metadata nor routing still gets `IndexNotFoundException`, and only an index that
exists without a routing entry takes the new path.

**Synthesised empty shards, not a skip**, and that was the real decision. Skipping the index would
have left the search with nothing to route to and returned HTTP 200 with zero hits for an index that
exists and holds data -- a silent wrong answer, the same failure mode A5.1 was written to prevent.
Instead the index gets one shard iterator per shard in metadata, each with no copies, which is
byte-for-byte what an index whose shards merely happen to be unassigned already produces. A third
test asserts that equivalence directly, because it is the justification: absence is being made to
look like a state core already handles rather than being given a behaviour of its own.

Three tests; the one that targets the change fails with it reverted and the two controls pass either
way. The `*routing*` and `*search*` suites pass (16 min).

### A3. A null-returning sibling, and two callers moved to it -- DONE

`shardRoutingTableOrNull(ShardId)` added; `TransportReplicationAction.ReroutePhase` and
`TransportBulkAction` use it. A1 was right that this is small: both already had the "primary is not
allocated yet, wait and retry" branch one statement below the lookup, and the throwing lookup simply
never let them reach it.

**The first attempt was too broad and an existing test caught it.** Returning null for an unknown
*shard* of a *present* index turned a permanent error into retry-until-timeout, so
`testUnknownIndexOrShardOnReroute` got `UnavailableShardsException` where it expected
`ShardNotFoundException` -- the wrong error, after wasting the whole timeout. The contract is now
narrow: null when and only when the index has no routing entry. An unknown shard still throws
`ShardNotFoundException`, a mismatched index UUID still throws `IndexNotFoundException`. Absence of
the index from routing is the only behaviour that changed.

Four unit tests on the sibling plus one on `ReroutePhase` that mirrors `testNotStartedPrimary`
assertion-for-assertion, since the claim is that a cold index behaves identically to an unallocated
primary. It fails with the change reverted. The `*bulk*`, `*replication*` and `*routing*` suites pass
(11 min).

### A4. Spike: how does an index become routing-absent? -- DONE

See `rfc-routing-absence-mechanism.md`. Scale-to-zero does **not** produce this state; it produces the
opposite, deliberately. A suspended shard stays in the routing table as `UNASSIGNED` so
`SuspendedShardAllocationDecider` can keep returning `NO` for it, once per candidate node, on every
reroute. That is the cost cold-absence removes, and it scales with suspended shards times nodes.

The blocker is not a missing mechanism, it is that **the routing table is currently the completion
signal for reactivation**: `ShardReactivationActionFilter` iterates
`state.routingTable().index(indexName)` in three places to decide whether the primary is started and
the search replicas are back.

Recommendation for A5: recreate the entry in the same cluster-state update that clears the suspended
flag, so the completion check works unchanged and the mechanism stays where the rest of scale-to-zero
lives. Two alternatives and why they are worse are in the spike.

Two tasks came out of it:

- **A6 gains a first half.** Measure the *current* curve -- reroute time against suspended-shard count
  with today's mechanism -- before measuring the improvement. S6 measured 40k active shards; nobody has
  measured 40k suspended ones, and that number is the prize.
- **A8 is new.** Sweep the plugin for the same assumption A1 swept core for. A1 found sixteen sites in
  `server/src/main`; the plugin is smaller but it is the component that would create this state, so its
  own dereferences matter more rather than less.

### A5. Implement the mechanism chosen in A4

Split into four, because the pieces have very different risk. A5.1 is the one that makes the rest
safe, and it lands first and alone.

**A5.1 -- DONE.** `ShardReactivationActionFilter` distinguishes cold from gone. `readerCopyNotYetStarted`
now answers *true* for an index present in metadata and absent from routing (the caller has already
established metadata presence, so absence there can only mean cold), and `allFullyReactivated` answers
*false* rather than skipping the entry. The `indexMetadata == null` branch above it keeps meaning
"gone", so a deleted index still releases the wait instead of stalling to the timeout -- that pairing
is what the two new tests pin down, and both fail with the change reverted.

One case is deliberately left to A5.3 rather than guarded speculatively: an index with zero
search-only replicas short-circuits before the routing read, so a cold writer-only index whose marker
has cleared but whose routing entry has not yet been recreated would proceed. **A5.3 must clear the
marker and recreate the entry in the same cluster-state update**, which is what closes it. If that
ordering is ever relaxed, this becomes a live gap.

**A5.2 and A5.3 -- DONE, landed together.** They were planned as separate tasks and cannot be: an
entry removed by one and never recreated by the other leaves every cold index permanently unservable,
so neither is shippable alone. Both sit behind
`serverless_storage.scale_to_zero.prune_routing_entry`, default false.

`ShardSuspensionCoordinator` prunes from the already-suspended reconciliation branch rather than from
`clusterStateProcessed`, because eviction is an asynchronous reroute -- at the moment the suspend flag
commits the copies are still assigned, so a prune attempt there would always find the condition unmet.
Pruning requires the *whole* index to be cold: the entry is per-index while suspension is
per-shard-per-role, so a partially suspended index that lost its entry would take its still-serving
shards down with it.

`TransportReactivateShardsAction` recreates the entry in the same update that clears the marker, and
recreation is deliberately *not* gated on the setting -- an entry pruned while pruning was enabled
must still come back if it is turned off afterwards.

**The test caught a data-loss bug the call site did not look like it had.** The obvious helper,
`RoutingTable.Builder#addAsRecovery`, picks the recovery source from `inSyncAllocationIds` and falls
back to `EmptyStoreRecoverySource` when the set is empty. It is empty here: the eviction that made the
index cold goes through `CancelAllocationCommand`, which calls `RoutingAllocation#removeAllocationId`
on the cancelled copy. So reactivation would have silently resurrected a scaled-to-zero index as a
brand new empty one. The entry is now built explicitly with `ExistingStoreRecoverySource`, which is
correct despite the empty in-sync set because the data lives in the object store and
`ServerlessStorageExistingShardsAllocator` exists precisely to allocate such a shard.

**A5.4 -- DECIDED, no change. `SuspendedShardAllocationDecider` stays exactly as it is.** The A4
spike expected pruning to make it "mostly dead for suspended shards". It does not, for three
independent reasons, any one of which is sufficient:

1. **It is what makes eviction stick.** Eviction is a `CancelAllocationCommand`, which unassigns the
   copy; the reroute that follows immediately runs `allocateUnassigned`. Without `canAllocate` saying
   `NO`, `ServerlessStorageExistingShardsAllocator` would assign the shard straight back on that same
   pass. Suspension would not merely be slower to take effect, it would never take effect at all --
   and this holds for every shard between eviction and the prune that follows some ticks later.
2. **A partially suspended index is never pruned.** The routing entry is per-index and suspension is
   per-shard-per-role, so an index with one hot shard keeps its entry indefinitely and its suspended
   shards are held down by the decider alone, permanently. This is not an edge case; it is what any
   index whose shards idle at different times looks like.
3. **Pruning is off by default.** With the setting off the decider is the entire mechanism.

**Corollary worth recording, because it bounds A6's number.** The 17x applies to *fully* cold indices.
A partially suspended index still pays the held-down per-suspended-shard reroute cost, so the realised
saving in a fleet depends on how many tenants go fully quiescent rather than how many shards do.
A6's second half should measure a mixed fleet, not only the all-or-nothing shape it measured first.

**A6 second half -- DONE, and it caps the headline.** With the cold population fixed at 10,000 and
only the fully-quiescent fraction varying, steady reroute runs 369 / 195 / 41 / 17 ms at 0 / 50 / 90 /
100 percent. Linear in the fraction, no threshold in either direction, so a partial rollout
extrapolates. The 17x is the best case rather than the expected one: a fleet where suspension is
spread thinly across many partly-idle indices saves nothing while still carrying all of A5's
lifecycle risk. Whether that fleet shape occurs is a workload question, and it now belongs to D2.
- Check whether `SuspendedShardAllocationDecider` is still needed for the window between marking and
  eviction, now that the allocator stops seeing the shard afterwards.

Gated on A8, which establishes the full plugin-side call-site list the way A1 did for core.

### A8. Sweep the plugin for the routing-entry assumption -- DONE

Four routing-entry reads in `plugins/serverless-storage`, against core's fifty-eight:

| site | verdict |
|---|---|
| `WriterPublicationNotifier:89` | already guarded, both index and shard |
| `ShardSuspensionCoordinator:295` | **was unguarded, now fixed** -- eviction skips a shard whose index has no routing table |
| `ShardReactivationActionFilter:226` (`readerCopyNotYetStarted`) | guarded by `hasIndex`, but semantically wrong for a cold index |
| `ShardReactivationActionFilter:306` (`allFullyReactivated`) | same |

**This corrected the A4 spike, which had claimed three unguarded sites in the reactivation filter.**
Both are guarded. What they are not is *correct*: absence currently reads as "nothing to do" and "this
one is done", so removing the routing entry would make reactivation silently report success for an
index serving nothing. That is a worse failure than the exception the spike predicted, and it moves
those two sites from A5's "tolerate absence" list to its "distinguish cold from gone" list.

### A6. Measure -- FIRST HALF DONE

`ColdIndexRerouteCostSpikeTests`, recorded in `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md`. Today's
held-down shape costs roughly 15 ms of steady-state reroute per 1,000 cold indices and is linear in
them; routing-absent is flat. At 10,000 cold indices that is 154-171 ms against 9 ms, a 17x
difference, paid on the cluster-manager on every cluster state change.

So Phase A's premise holds, and the prize is quantified: cold-absence removes essentially all of the
cold-tenant allocator cost rather than trimming it. Extrapolated, 100k cold indices is about 1.5 s per
reroute today.

Remaining half: re-measure with the real mechanism once A5 lands, and against a larger active set, to
confirm the constant rather than just the shape.

---

# Phase B. The allocator ceiling

**Why.** S6 measured it as the binding constraint and the plan calls it "a bigger question than
anything else here". Phase A reduces what the allocator sees; this asks whether the allocator itself
can be made to scale. It is research first. Do not start with an implementation task.

### B1. Profile reroute at increasing active-shard counts

Extend the S6 harness to 40k, 100k and 200k active shards, assertions disabled. Break the time down by
phase: `RoutingNodes` construction, decider evaluation, balancer iterations. S6 established the
balancer dominates; get the curve, not one point.

### B2. Can allocation be domain-scoped? -- ANSWERED, and the question dissolves

**Yes, and it reduces to a deployment decision rather than an allocator change.**

The precedent is real and stronger than the plan assumed. `RoutingPool` is not a filter inside one
balancing pass; there are two balancers, `LocalShardsBalancer` and `RemoteShardsBalancer`, and the
local one builds its index list by filtering `LOCAL_ONLY` before doing any weight work at all
(`buildWeightOrderedIndices`). So scoped balancing already exists in core and is load-bearing.

**Why it works is the whole answer.** Every constraint the plan listed as possibly cluster-wide --
`ShardsLimitAllocationDecider`'s total-shards-per-node, `DiskThresholdDecider`'s watermarks, awareness
and allocation filtering -- is **per node**, not per cluster. A per-node constraint is trivially
satisfied within a scope whenever the scopes do not share nodes. That is exactly why `RoutingPool`
works: warm and non-warm node sets are disjoint by construction.

So the constraint list the task asked for is empty, conditionally:

| constraint | scope | safe to domain-scope? |
|---|---|---|
| total shards per node | per node | yes, if domains do not share nodes |
| disk watermarks | per node | yes, if domains do not share nodes |
| awareness / filtering | per node | yes, if domains do not share nodes |
| balancer weights | per domain by construction | yes |

**And that condition is the finding.** Domain-scoped allocation is sound if and only if domains have
disjoint node sets. Domains with disjoint nodes are not a logical partition of one cluster; they are
separate clusters sharing a control plane, which is what the metadata-plane RFC already calls cells.

So there is no allocator work here. A cell's allocator scales because a cell is a cluster, and its
active-shard count is whatever the cell was sized for. Building a *logical* domain scope inside one
cluster, over shared nodes, is unsound for every per-node constraint above and would have to
reintroduce cluster-wide accounting for each -- which is the cost the scoping was meant to avoid.

B4 should record this as "the ceiling is structural, cells are the answer" unless B3 finds something
that changes it.

### B3. Can the balancer be incremental? -- ANSWERED: not by caching weights

**No, and the weight function says why in one line.**

```java
float weightShard = node.numShards() - balancer.avgShardsPerNode();      // total shards on the node
float weightIndex = node.numShards(index) - balancer.avgShardsPerNode(index);
return theta0 * weightShard + theta1 * weightIndex;
```

The first term is the node's **total** shard count, across every index. So a node's weight for index A
depends on how many shards of B, C and D it holds. Relocating a single shard of any index changes
`numShards()` on two nodes, which invalidates the cached weight of **every index** on those two nodes,
plus `avgShardsPerNode` globally. Invalidation is not sparse; one move dirties O(indices) entries. A
cache whose every write invalidates most of itself is not a cache.

**Core already knows this, and the evidence is in the code.** `balanceByWeights` recomputes
`weightSpreadAcrossAllNodes(index)` fresh for each index inside a single pass, with a comment saying
why: earlier indices in the same loop may have relocated shards, so a spread captured beforehand would
be stale and could under-report. If maintenance is already unsafe *within* one pass, maintaining
across passes is strictly harder, and the same term is the reason.

**What does work is the shape that already shipped.** The weight-spread skip does not cache a result;
it proves cheaply that an index cannot yield a relocation and skips the expensive per-node decider scan
entirely. That composes safely because it is recomputed each time, and it is where any further work
belongs -- more provably-empty skips, not memoized weights.

**And the biggest such skip is A5.** An index absent from the routing table is not iterated at all,
which is why A6 measured cold-absence removing essentially the whole cold-tenant cost rather than
trimming it. Phase A already took the win Phase B was looking for, for the tenants this project cares
about.

### B4. Verdict -- the ceiling is structural, cells are the answer

B2 and B3 both say no, in different ways that reinforce each other, so per this task's own instruction
there is no implementation task here.

- **B2:** domain-scoped allocation is sound exactly when domains do not share nodes, because every
  constraint involved is per-node. Domains with disjoint nodes are separate clusters sharing a control
  plane, which is what cells already are. There is no allocator change in it.
- **B3:** weights cannot be maintained across reroutes, because the weight function's leading term is
  the node's total shard count and couples every index on a node. One relocation invalidates O(indices)
  cached weights, and core already recomputes within a single pass for exactly this reason.

**So the ~100k active-shard ceiling S6 measured is structural.** It is a property of a balancer that
must consider every shard against every node with a globally-coupled weight, and neither scoping nor
caching removes that within one cluster.

That is a useful answer rather than a disappointing one, because the thing this plan set out to fix
was never the active-shard ceiling. It was the cost of *quiescent* tenants, and Phase A removed that
directly: an index with no routing entry is not iterated at all. A6 measured 369 ms against 17 ms of
steady-state reroute at 10,000 cold indices. Cells then bound the active-shard count per cluster, which
is what they were for.

**B1 is now optional.** It would refine the shape of the curve between 40k and 200k active shards, but
no version of that curve changes the verdict: B2 and B3 rule out the two candidate mitigations
independently of where exactly the knee sits. Worth doing if someone needs the number for cell sizing;
not worth doing to decide this.

**Folding in E2.** The incremental routing rebuild belongs here rather than in the decisions section.
It is the one remaining idea in this space that is not ruled out -- it needs a change signal from every
producer of a `RoutingTable`, which is a core-wide invariant and a maintainer's call. Its payoff is 35
to 67 ms of a 300 ms reroute at 40k *active* shards, so it is squarely a Phase B item and does nothing
for quiescent tenants.

---

# Phase C. Manifest sharding

**Why.** The manifest is rewritten whole on every cluster state version. At 100k indices that is about
0.5 MB compressed before C6's descriptor and about 0.8 MB after, and both scale linearly with index
count. This decides whether C5 and C6 are usable at that scale.

**Already audited.** See the C6 section of `rfc-scalable-index-metadata-plan.md`. Two findings carry
into these tasks: the manifest genuinely does list all N indices every version, and
`RemoteClusterStateCleanupManager` computes `filesToKeep` as a union over retained manifests, so
sharding must teach it to resolve index blob names transitively through shard blobs.

### C1. Make the sweep refuse to run on a manifest it cannot read -- DONE, and not as written

**The task as specified cannot come first.** "Fetch each shard blob to add the index names it
references" needs a shard blob format, and that is C2's job. Writing the resolver now would mean
inventing the format inside the cleanup path, which is the wrong place to decide it.

What *can* come first, and is the safety C1 exists for, is a guard. The sweep works by subtraction:
retained manifests contribute to `filesToKeep`, and anything a stale manifest references that is not
in that set is deleted. That is sound only while references can be enumerated. A manifest written by a
newer codec still **parses** here -- the parser dispatch falls back to the newest version the node
knows -- but its unknown fields come back empty, so its references read as *none* and the subtraction
deletes blobs that are still in use.

Sharding is precisely that kind of change: it moves the index list out of `getIndices()` and behind
shard blobs, so a node running today's code against a sharded manifest computes an empty keep-set for
every index in the cluster. During a rolling upgrade that is a repository-wide delete.

So `deleteClusterMetadata` now records the highest codec version it saw across both the retained and
the stale manifests, and returns without deleting anything if it exceeds
`MANIFEST_CURRENT_CODEC_VERSION`. Recorded inside the existing loops rather than in a pre-scan: a
separate pass would fetch every manifest twice, and nothing is deleted until both loops finish anyway.
Costs nothing until such a manifest exists.

C3 still has to add the transitive resolution when it introduces shard blobs. This makes getting that
wrong non-catastrophic instead of catastrophic, which is what "deliberately first" was for.

Test confirmed to fail without the guard; the 27-test cleanup suite and the wider `gateway.remote`
suite pass.

### C2. Design the shard scheme -- DONE

See `rfc-manifest-sharding-design.md`. Verdict is go, with one condition.

Decisions: partition on `murmurhash3(index UUID) mod shardCount`; shard count **fixed**, declared in
the top-level manifest, defaulting to 64, and never derived from index count; shard blobs immutable
and carried forward by reference when unchanged, which is where the entire saving comes from.

The shard-count decision is the C6 scar applied again. Making the codec depend on the manifest's
contents let a cluster flap between versions as indices came and went, and was reverted. Deriving
shard count from index count is the same mistake with a worse ending: crossing the threshold rewrites
every shard, so a cluster hovering near a boundary repeatedly rewrites the whole manifest -- exactly
the cost sharding exists to remove.

**The condition: C3 must measure the write amplification reduction before C4 and C5 are written.** The
saving is bounded by changed indices per version, not by index count; if most versions change enough
indices to touch most shards, it collapses and the rest of Phase C is not worth its cleanup risk. That
measurement is cheap, and it is the last point where this can be stopped cheaply.

### C3. Write path

Write only shards whose contents changed; carry the rest forward by reference. This is where the win
is, so measure the write amplification reduction as part of the task rather than after.

### C4. Read path

Read the top-level manifest, then the shards. For a full-state read that is all shards; check whether
anything wants a single index without reading all of them.

### C5. Codec bump

Follow the pattern C6 used and the four bumps before it: new `CODEC_V6`, its own parser, an entry in
`VERSION_TO_CODEC_MAPPING`, a `CLUSTER_METADATA_MANIFEST_FORMAT_V5` for reading what is already in
repositories, and `MANIFEST_CURRENT_CODEC_VERSION` moved unconditionally. Do not make the codec depend
on the data in the manifest; that was tried during C6 and reverted, because it lets a cluster flap
between versions as indices come and go.

### C6. Measure

Manifest bytes written per cluster state version, sharded against not, at 10k / 100k / 1M indices.
Distinct per-index data, since compressing identical entries flatters the result by a lot.

---

# Phase D. Wire it up and prove it end to end

**Why.** C5 and C6 are inert by default and nothing in the serverless plugin uses them.
`Metadata.Builder#putStub` currently has only test callers.

### D1. Integration test with both settings on

Bring up a cluster with `...index_metadata.descriptor.enabled` and `...index_metadata.defer.enabled`
set, create N indices, restart or join a node, and assert it does not fetch every index blob. Count
blob reads; do not infer from timing.

### D2. Decide the serverless defaults

Whether the plugin turns both on by default, and what the operational story is for a cluster that
enables them on an existing repository. Note the descriptor is backfilled onto carried-forward entries
during publication, so enabling it does reach existing indices, but only once a cluster state version
is published after the setting changes.

### D3. Decide whether the plugin needs its own store

If serverless-storage is meant to own index metadata rather than use core's remote cluster state, that
is what `putStub` is for and it needs a task of its own. If not, `putStub` stays a plugin-facing seam
with no in-tree caller, which is fine but should be a stated decision rather than an accident.

---

# Phase E. Decisions to close out

These are not implementation work. Each is a call to make and record.

### E1. Mapping dedup: NOT DOING, and here is the number that made it look worth doing

**Decision: do not implement mapping deduplication.** Recorded here so the 1000x does not resurface.

S4 measured 1000x compression on a fleet of identical mappings. That figure is real and it is also
close to meaningless, for three independent reasons, any one of which is disqualifying:

1. **It measures the wrong thing.** S3 measured what dedup does to the parsed `MapperService` graph,
   which is what actually occupies a data node: **0.08%**. The compressed cluster-state bytes dedup
   targets are roughly 1/255th of the resident cost. Deduplicating them leaves the number that matters
   untouched.
2. **The absolute size stopped being alarming.** The ceiling recalibration puts mapping bytes at tens
   of MB per node at reachable scale, not the tens of GB the original framing implied. S6 established
   that the allocator binds first, at roughly 100k active shards, well before mapping residency does.
   Dedup optimises a resource that is not the constraint.
3. **The 1000x is an artefact of the input.** It requires a fleet of *identical* mappings. Every
   measurement in this project that assumed uniformity across tenants has been wrong in the same
   direction -- the descriptor compression measurement flattered itself 4x by reusing one descriptor
   across 100k entries until distinct per-index aliases were used. A tenant fleet with genuinely
   identical mappings is a benchmark, not a deployment.

Against that, the correct implementation is a wire-format change to how mappings travel in cluster
state, with backwards-compatibility cost on every node in a mixed-version cluster. That is a large,
permanent, cross-version commitment bought with a 0.08% improvement in the resource that binds.

**What would reopen it.** A measurement showing parsed-graph residency, not compressed bytes, is the
binding constraint on data nodes at a scale the allocator can actually reach. S3 and S6 together say
it is not. Nothing short of overturning both should restart this.

### E2. Incremental routing rebuild: PARKED, with the question made answerable

Attempted and reverted. The finding generalizes: `RoutingChangesObserver` observes allocation changes,
but routing tables also change outside allocation, so a change set derived from it is not a sound
over-approximation. A correct version needs a change signal from every producer of routing tables.

**Parked rather than closed**, because unlike E1 this is not a bad idea, it is a good idea whose
correct implementation costs more than one contributor should commit unilaterally. Writing down the
decision it needs, so it can be answered rather than rediscovered:

> Is core willing to require every producer of a `RoutingTable` to declare what it changed?

That is the whole question. Everything else follows: with the invariant, the rebuild is
straightforward and the existing revert becomes sound; without it, no version is correct, and the
attempt should not be repeated.

**What this session changed about the payoff, in both directions.** A6 measured steady-state reroute
directly and confirmed the balancer dominates -- the ~35 to 67 ms of a ~300 ms reroute at 40k shards
stands. But A6 also showed that the cold-tenant portion of that cost, which is the part this project
cares about, is removed entirely by A5's routing absence rather than trimmed by a faster rebuild. So
the case for E2 is now narrower than when it was attempted: it helps clusters with many *active*
shards, which is S6's ceiling and Phase B's subject, and does nothing for the quiescent-tenant problem
this plan was written for.

That makes E2 properly Phase B's concern rather than Phase C's, and B4 should fold it into its verdict
instead of leaving it as a standalone item here.

### E3. S1 sites 1 and 2 -- MOOT, Phase A happened

This existed to force an explicit "not doing" if cold-absent routing was abandoned, so that A2 and A3
did not sit open forever as changes to live search and write behaviour with no benefit behind them.

Phase A was pursued and A5 landed the mechanism, so the conditional never fired and both are done. The
concern it encoded was still the right one, and it shaped how they shipped: neither is a bare
degradation. A2 synthesises empty shards rather than skipping the index, so a cold index produces
per-shard failures rather than a silent 200 with zero hits, and a test asserts that equals the
behaviour of an index whose shards merely happen to be unassigned. A3's null is returned only when the
index has no routing entry, leaving `ShardNotFoundException` and `IndexNotFoundException` exactly where
they were.

**The residual worth naming:** A2 and A3 are live for every cluster, while the mechanism that creates
routing-absent indices is behind a setting that defaults to off. So today they are pure tolerance for a
state nothing produces. That is deliberate -- it is the same order A7 used, and it is what makes A5.2
safe to enable -- but it does mean the first cluster to turn pruning on is the first to exercise them
together. D1's integration test is where that gets covered, not here.

---

# Suggested order

1. A1 (audit, no risk, gates the rest)
2. A4 (spike, decides whether Phase A is small or large)
3. C1 (cleanup safety, independent of everything, removes the scariest part of Phase C early)
4. A2, A3, A5, A6 or B1 through B4, depending on what A4 says
5. C2 through C6
6. D1 through D3
7. E1 through E3 whenever the relevant phase resolves

B can run in parallel with A and C; it is research and shares no code.
