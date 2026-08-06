# Architecture for 100M indices with index/search and compute/storage separation

## What this answers

Three goals, stated together because they turn out to be the same design:

1. **Index/search separation.** Writing and searching run on different nodes with different engines.
2. **Compute/storage separation.** Data of record lives in object storage; nodes hold cache.
3. **100M indices, 3 to 100 shards each.** Up to 10 billion shards.

Goals 1 and 2 are built and running. Goal 3 is the one that needed evidence, and this document records
what the measurements say and what remains to build.

The short version: goal 3 is reachable, the remaining work is well-scoped, and the thing that makes it
work is that goals 1 and 2 already removed the constraints that would otherwise block it.

**Read the dates on the evidence.** This document was written when the metadata plane was a design with
spikes behind it. Most of it is now built, and two of its central proposals were replaced rather than
implemented: the name index tier was deleted, and the descriptor system index it was meant to unblock was
removed without it. Sections describing measurements are kept as measurements; sections describing plans
have been rewritten to say what happened. A design document that quietly keeps recommending work that was
already abandoned is worse than no document, because it reads as current.

## The constraint that shapes everything

OpenSearch keeps a description of every index in memory on every node, and places every shard through a
single cost-based allocator whose output lives in one global routing table, published by one elected
coordinator.

That combination is what none of the systems that scale to billions of things use. Cassandra computes
placement from a token ring. Vespa computes it from an ideal-state function. DynamoDB stores it, but in
a partitioned metadata service rather than one replicated object. OpenSearch is the outlier, and every
ceiling measured over this work traces back to that one design fact.

Measured ceilings, for reference:

| what | measured | source |
|---|---|---|
| allocator, steady-state reroute | ~9-11 us/shard, ~1 s at 100k active shards | S6 |
| allocator, cold allocation | 4.6 s at 40k shards, tens of seconds at 100k, superlinear | S6 |
| resident metadata, materialized | 15,522 B/index at 100 shards | S12 |

100k active shards against a 10 billion shard target is five orders of magnitude. That is not a tuning
problem.

## Why serverless-only changes the answer

Five things force explicit, stored placement in stock OpenSearch. Serverless-only removes four:

| constraint | under serverless-only |
|---|---|
| shard bytes sit on one node's disk | gone: bytes are in object storage, any node can serve |
| disk watermarks, balance by data size | mostly gone: local disk is cache, eviction is safe |
| shards are wildly heterogeneous in size | gone: a node holds cache, not the whole shard |
| replicas recover peer-to-peer from the primary | gone: readers materialize from the object store |
| `inSyncAllocationIds` tracks which copy has the data | gone: `ShardHead` CAS already decides who may write |

The last row is the important one and it is easy to miss. `inSyncAllocationIds` is per-shard state
carried in `IndexMetadata` to answer "which copy is safe to promote." In this architecture that question
is already answered elsewhere: the object store holds the data, and the lease and term split in
`ShardHead` decides who may write. **Shard authority already moved out of cluster state.** Cluster
state's copy is redundant and has simply not been deleted yet.

Once placement need not be stored, it can be computed, and once it is computed the allocator leaves the
path entirely.

## The evidence

### Metadata cost is flat in shard count

`DeferredMetadataHeapEstimate`, real `Metadata`, one alias and one replica per index:

| shards | materialized | deferred |
|---|---|---|
| 1 | 2,743 B/index | 698 B/index |
| 30 | 6,214 B/index | 698 B/index |
| 100 | 15,522 B/index | **698 B/index** |

Flat to three significant figures across a hundredfold change in shard count. At 100M indices that is
65 GiB deferred against 1,446 GiB materialized. This is what C5 and C6 bought, and the property holds at
the new shard target without modification.

### Computed placement is at the theoretical optimum

`ComputedPlacementSpike`, rendezvous hashing (highest random weight), K=3 candidates per shard:

| nodes | lookup | distribution vs ideal |
|---|---|---|
| 10 | 80 ns | 0.99x - 1.00x |
| 50 | 168 ns | 0.99x - 1.02x |
| 200 | 359 ns | 0.95x - 1.03x |

Membership change, as fraction of shards whose primary moves and fraction losing every warm candidate:

| nodes | change | primary moves (ideal) | lost all K warm |
|---|---|---|---|
| 200 | +1 | 0.50% (0.50%) | **0.000%** |
| 200 | +100 | 33.29% (33.33%) | 3.61% |
| 200 | +200 | 49.96% (50.00%) | 12.37% |

Primary moves track `added/(N+added)` to two decimal places at every size. There is no tuning knob to get
wrong, which is the practical advantage over a token ring with vnodes.

Adding one node leaves **no** shard without a warm candidate, and structurally rather than by luck: one
new node displaces at most one of K, so K-1 warm holders always survive. Incremental autoscaling is free.

Doubling the fleet leaves 12.4% of shards with no warm candidate. That is the deploy and
zone-recovery case, and it is why pre-warm before rotation moves from optional to required.

The comparison that matters is not lookup against lookup. A stored routing table read is a hash map hit
at perhaps 20-50 ns, so computed placement is several times slower per request, which is irrelevant next
to a millisecond search. What matters is that the allocator must do a superlinear global pass and
computed placement never does one. The pass S6's curve says is impossible is not made faster. It is never
executed.

### The global name index was measured, built, and then deleted

`NameIndexSpike` measured 2M names at 45.0 B each, so 4.2 GiB at 100M, with prefix resolution as a binary
search plus a forward scan: 3.2 ms for a wildcard returning 65,536 indices, 9.2 us for one returning none.
It was the highest-risk item in the design, because wildcards are inherently global and the alternatives
were scattering every wildcard to every partition or restricting the query language.

**None of that is in the system.** S5 deleted the tier and S8 removed the descriptor system index without
it, because a wildcard turned out to be answerable from one bounded `ListObjectsV2` against the descriptor
prefix with a `maxKeys` cap. That is a single request whose cost is set by the cap rather than by the
population, which is what the tier existed to provide, and it needs no second copy of the name set to keep
consistent.

The measurements are kept because they are true and because they record what the alternative would have
cost. They are not a description of anything running.

## The architecture

```
                 client
                   |
      [ LB, consistent hash on index ]        Envoy maglev / NGINX ketama / HAProxy
                   |
      [ coordinating-only nodes ]             descriptor cache, request resolution
                   |                          wildcards: one bounded LIST, capped
           |                  |
      [ writer nodes ]   [ search nodes ]
       single-owner       top-K candidates
       lease-fenced       chosen by ARS
              |                  |
     [ object storage: descriptors, tombstones, segments, manifests, WAL, shard heads ]
```

**Routing is hash-derived at two levels, for two different reasons.**

At the LB, `hash(index)` picks a coordinator so that coordinator's descriptor cache stays warm. This is
cache affinity, not ownership. Any coordinator can serve any index because C5/C6 made descriptors
lazily fetchable from the manifest, so a miss costs one object-store read rather than a failure. That
distinction matters: it means no ownership handoff protocol, no fencing on membership change, and
graceful degradation when the hash and reality disagree.

At the coordinator, `rendezvous(index, shard)` gives K candidate search nodes. Adaptive replica
selection, which already exists in `ResponseCollectorService` and `OperationRouting`, picks among them by
service time and queue depth. A cold node reads as a slow node, so ARS should drift traffic toward warm
replicas without being told about caches at all.

**Writers and readers are deliberately asymmetric.** A writer must be single-owner per shard, so it
hashes to one node and `ShardHead` CAS fences correctness. If the hash briefly disagrees with reality
after a membership change, the lease makes the wrong node fail cleanly rather than corrupt. Readers have
no such constraint, which is exactly why top-K applies to them and where the cache spend is.

## What is built, what is designed, what is unknown

**Built and proven.**

- Index/search separation: `WriterEngineFactory` and `ReaderEngineFactory` give the two roles genuinely
  different engines, not one engine in two modes.
- Compute/storage separation: segments, manifests, WAL and shard heads in object storage; readers
  materialize lazily.
- Deferred metadata: descriptor carried in the manifest, index loaded on first touch. Proven end to end.
- Write fencing outside cluster state: `ShardHead` CAS with the lease and term split.
- Routing absence tolerated by core: an index can be in metadata and absent from the routing table.
- Cache affinity recording, and an allocator fast path that uses it.

**Built since this document was first written.**

- Gated indices: an index with no cluster state entry at all, its record a descriptor in object storage.
  Creation is one conditional PUT, resolution one GET, deletion a tombstone PUT, and a wildcard one
  bounded `ListObjectsV2`. The descriptor system index that used to back it is gone.
- Computed placement, installed and reachable. A gated index publishes no routing entry and every node
  derives the same answer from the node list.
- Admission off the cluster state update thread, for both creation and deletion. Creation went from 1,194
  to 10,505 per second at concurrency 16, deletion from 407 to 646 in a controlled A/B.
- On-demand shard opening, and idle eviction to close the loop: a node opens a gated shard because a
  request arrived and gives it back when nothing has touched it, so residency tracks the working set.

**Measured, with the numbers that matter.**

| | |
|---|---|
| heap per open gated index | 150,888 B |
| file descriptors per open gated index | 3.0 |
| open gated indices per node, 31 GiB heap, half to residency | ~110,000 |
| creates per second, one 20-core box, concurrency 16, no declared mapping | 10,505 |
| creates per second, with a declared mapping, concurrency 8 | 2x to 10x slower than the unmapped arm beside it, depending on warm-up; at least ~80% of it the index-backed mapping store, of which T24 removed about a seventh |
| deletes per second, same | 646 |

**Unknown, and honestly so.**

- How far idle eviction lags under CPU pressure. Expected residency is arrival rate times the idle
  window and a quiet run measures exactly that; a run on a loaded box measured seven times higher. The
  sweep runs on `GENERIC` and closing an index flushes, so eviction competes with the traffic that
  caused it, which is the wrong way round.
- **What a declared mapping costs a creation is the index-backed mapping store, and what remains is
  which part of that store.** The bypass that produced 10,505 declined on any non-empty mapping, so a
  mapped index -- the common case since mappings began to be carried -- took the slow path through a
  `synchronized` block. A mapping whose fields declare nothing but a type is now validated against the
  field type registry instead, an unlocked lookup that is complete for that shape, which moved the ratio
  from about 14x to about 10x. The "so the lock was a quarter of it" that followed subtracted two
  single-run ratios, and T23 has since measured that ratio moving by more than that on warm-up alone, so
  treat the lock's share as unquantified rather than as a quarter.

  T23 attributed it by adding a third arm: the same mapped creations with an in-memory
  `MappingGenerationStore.Store` registered in place of the index-backed one. Each run reports the
  store's share twice, taking the index-backed arm from the front of the round and from a repeat of the
  same arm at the end, so a reader can see whether the answer depended on where the arm ran. Five runs:

  | run | share, front arm | share, repeat | in-memory arm, control over arm |
  |---|---|---|---|
  | 1 | 99% | 99% | 1.03x |
  | 2 | 115% | 109% | 0.84x |
  | 3 | 83% | 83% | 1.18x |
  | 4 | 95% | 94% | 1.07x |
  | 5 | 101% | 101% | 0.98x |

  Over 100% is not a typo. The residual came out negative in runs 2 and 5 because the in-memory arm
  finished *faster* than the unmapped control (the last column is control over arm, so below 1 means the
  mapped arm won), which cannot be true of a mapping that costs something. So the honest statement is a floor and a
  bound: the index-backed store is at least about 80% of what a declared mapping costs, and the residual
  -- parsing, merging, validation, the descriptor write's share -- is not resolvable at this precision.

  **Two limits on that, both real.** The measurement cannot separate the store's round trips from the
  local work in the same implementation: building the indexing request, deriving the field type counts,
  and checking that the mapping index exists. It prices removing the index-backed store, not the network
  specifically. And only the index-backed arm has a positional control; the in-memory and unmapped arms
  always run second and third and are never transposed, so the residual carries whatever the drift
  between those two positions is worth.

  **T25: removing the creation's read is worth something, and less than the arithmetic suggests.**
  T24 removed one of the store's two round trips, the read whose answer can only be "absent".
  Measured in a worktree at the parent commit against HEAD, the two sides run in adjacent pairs so
  a pair shares a machine state, eight pairs: the median mapped-to-unmapped ratio in the settled
  round fell from 2.82x to 2.42x, and the median per-creation penalty by 15%. Both framings of the
  same runs agree on direction and disagree on size, 15% against 22%, which is the honest width.

  **It does not reach significance in that batch on its own: HEAD won 5 pairs of 8.** Across four
  batches run over the evening -- 36 pairs in total, every batch's median favouring HEAD -- HEAD
  won 27, a two-sided sign test at p = 0.004. So the effect is real and it is small enough that
  eight pairs on this box cannot see it. Anyone re-running this should expect a null result as
  often as not.

  Two confounds were found by measuring them rather than by argument, and both are gone from the
  final batch. The first design ran the parent side first in every pair, and the second slot is
  measurably slower, so the whole order effect landed on HEAD; order now alternates by pair. The
  worktree also sat on tmpfs while HEAD sat on ext4, giving the parent side faster storage on the
  I/O-heavy arm being compared; both sides are now on ext4. Both biases had been working against
  the finding, which is the direction that flatters it least.

  A seventh to a fifth, from deleting one of two round trips, says the two are not equal. What is
  left is the swap, plus the local work in the same implementation that no arm here separates from
  it.

  **The multiple itself is not a constant and should not be quoted as one.** The repeat arm is what
  makes that visible: within the measured round the same arm's throughput differed by up to 1.9x between
  its two positions, and by up to 3x in the unwarmed round, while the share computed from either end
  moved by at most 6 points. Over the 36 runs T25 added, the ratio in a settled round ran 1.4x to
  4.1x, and 4.3x to 10.6x in an unwarmed one, at concurrency 8. The share is the finding; the multiple depends on how
  warm the mapping index is.
- `.opensearch-index-mappings` is a second system index. One shared index rather than one per tenant, so
  far cheaper than what was removed, but a fixed-geometry funnel on the mapping write path.
- Cluster-state publication latency against cluster size, which decides whether wake and sleep need
  batching. Estimated at 50-200 ms; not measured.
- Whether ARS's latency signal is a good enough proxy for cache warmth, or whether it oscillates and
  needs the direct signal from `ReaderCacheAffinityRecorder` blended in.
- Leading wildcards (`*-logs`) cannot be served by a prefix listing and need either a reversed keyspace
  or an explicit restriction.
- Alias resolution fan-out, unmeasured. A gated index may carry no alias at all today.
- Hot-tenant skew. A hash cannot know one tenant takes a thousand times the traffic. K=3 gives room to
  choose rather than solving it.

## Order of work

1. **Bound eviction under load.** The residency ceiling is the whole argument for reaching ten billion
   shards, and it currently weakens exactly when a node is busiest. Decide between a dedicated thread, a
   work budget per pass, and eviction driven by memory pressure rather than a timer.
2. **Cut what the index-backed mapping store costs a creation.** Attribution is done and the cheap half
   is taken: T23 measured the store at 83% or more of what a declared mapping costs, and T24 removed the
   read a creation issues for a UUID that cannot yet have one, which T25 measured at a seventh to a fifth
   of the penalty. The swap is what is left, and it is a write against the shared fixed-geometry index
   listed below, which is the next thing to measure rather than the next thing to assume.
3. **Resolve-and-forward hop.** Small, no core changes, makes multi-index requests work under hash
   routing. Lets the LB configuration ship.
4. **Pre-warm before rotation.** Under computed placement every large scale event puts some shards on
   cold nodes, and the 12.4% figure at fleet-doubling is the number to design against.
5. **Manifest sharding aligned with the routing hash**, so a coordinator warms its whole partition in one
   read.
6. **Publication latency measurement**, and wake/sleep batching only if it confirms the need.

## The one decision this does not make

Whether this is a fork or an upstream contribution. Computed placement replaces OpenSearch's placement
model for serverless indices rather than optimizing it, and that is a hard sell upstream while being the
obviously right answer for a serverless-only system. The codec namespace question (reserving a high
integer range, or a distinct blob codec name) is downstream of that choice and is straightforward either
way.
