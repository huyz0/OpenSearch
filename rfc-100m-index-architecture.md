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

### The global name index fits on one node

`NameIndexSpike`, 2M names sampled:

| representation | per name | at 100M |
|---|---|---|
| `HashMap<String, byte[]>` | 145.5 B | 13.6 GiB |
| sorted name blob + offsets, raw UUIDs, state byte | **45.0 B** | **4.2 GiB** |

Prefix resolution is a binary search plus a forward scan, so cost tracks matches rather than index count:
3.2 ms for a wildcard returning 65,536 indices, 9.2 us for one returning none.

This was the highest-risk item in the design, because wildcards are inherently global and the fallbacks
were scattering every wildcard to every partition or restricting the query language. Neither is needed.
At 4.2 GiB the name tier is replicated for availability rather than sharded for capacity.

## The architecture

```
                 client
                   |
      [ LB, consistent hash on index ]        Envoy maglev / NGINX ketama / HAProxy
                   |
      [ coordinating-only nodes ]             descriptor cache, request resolution
           |                  |
   [ name index tier ]   [ writer nodes ]  [ search nodes ]
    4.2 GiB, replicated   single-owner      top-K candidates
    wildcards, aliases    lease-fenced      chosen by ARS
                              |                  |
                    [ object storage: segments, manifests, WAL, shard heads ]
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

**Designed and measured, not built.**

- Computed placement replacing the allocator for serverless indices. Measured optimal; the code change
  is to make `IndexRoutingTable` derived and lazy per index rather than stored and published, reusing the
  holder seam C5 introduced for `IndexMetadata`.
- The name index tier. Measured at 4.2 GiB with microsecond prefix resolution; no implementation exists.
- Hash routing at the LB. Configuration rather than code, but the resolve-and-forward hop for
  multi-index requests (`_bulk`, `_msearch`, wildcards) does need building.

**Unknown, and honestly so.**

- Cluster-state publication latency against cluster size, which decides whether wake and sleep need
  batching the way create-index did in C7. Estimated at 50-200 ms; not measured.
- Whether ARS's latency signal is a good enough proxy for cache warmth, or whether it oscillates and
  needs the direct signal from `ReaderCacheAffinityRecorder` blended in.
- Leading wildcards (`*-logs`) degenerate to a full scan of the name index and need either a
  reversed-name structure or an explicit restriction.
- Alias resolution fan-out, unmeasured.
- Name index updates: the compact form is built sorted and does not support insertion, so index creation
  needs periodic rebuild plus an overlay of recent changes, or a different structure.
- Hot-tenant skew. A hash cannot know one tenant takes a thousand times the traffic. K=3 gives room to
  choose rather than solving it.

## Order of work

1. **Name index tier.** Highest risk retired, no dependencies, and it unblocks wildcards for everything
   else. Build the compact structure, the update path, and the replicated service around it.
2. **Resolve-and-forward hop.** Small, no core changes, makes multi-index requests work under hash
   routing. Lets the LB configuration ship.
3. **Computed lazy routing table.** The large one. Apply C5's holder pattern to `IndexRoutingTable` so
   it is derived from (nodes, descriptor, hash) on first access per index rather than published.
4. **Pre-warm before rotation.** Promoted from optional. Under computed placement every large scale
   event puts some shards on cold nodes, and the 12.4% figure at fleet-doubling is the number to design
   against.
5. **Manifest sharding aligned with the routing hash.** C3b and C4, with the shard function matched to
   the coordinator hash so a coordinator warms its whole partition in one read.
6. **Publication latency measurement**, and wake/sleep batching only if it confirms the need.

## The one decision this does not make

Whether this is a fork or an upstream contribution. Computed placement replaces OpenSearch's placement
model for serverless indices rather than optimizing it, and that is a hard sell upstream while being the
obviously right answer for a serverless-only system. The codec namespace question (reserving a high
integer range, or a distinct blob codec name) is downstream of that choice and is straightforward either
way.
