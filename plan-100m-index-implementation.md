# Implementation plan: 100M indices with index/search and compute/storage separation

Status: in progress. Evidence recorded in `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md` (S1 to S55) and
`rfc-100m-index-architecture.md`.

> **Read the seventh review at the end of this file before quoting any number from it.** The descriptor
> system index this plan reasons about was deleted on 2026-08-05 and replaced by object storage, and the
> creation-throughput figure quoted throughout has been corrected four times since it was written. Parts 0
> through 6 and the first six reviews are preserved as written, superseded reasoning included, which is this
> project's practice -- but the ceiling tables in them are stale in ways their own text cannot show you.
>
> Ground this file against the tree before extending it. `docs/rounds/STATE.md` carries a correction of the
> same shape about its own task numbers, and the instruction it ends with applies here: re-run the check
> yourself rather than propagating a table forward again.

---

# Part 0: The three ceilings, and which are cleared

Added after S15 to S19, because the plan had grown into eight lettered areas and the structure underneath
them was no longer visible. There are only three things that stop this design reaching 100M indices. Each
area is an attack on one of them, and reading the plan as three ceilings rather than eight workstreams is
what makes the ordering obvious.

| ceiling | what it is | measured | status |
|---|---|---|---|
| **1. Placement** | the allocator, the routing table, per-node shard limits | S6: cold allocation superlinear, 4.6 s at 40k shards. `cluster.max_shards_per_node` refuses at 3,000 on a 3-node cluster (S18) | **cleared by Area C** |
| **2. Residency** | metadata bytes held on every cluster-manager-eligible node | 2,944 B/index materialized, 698 B deferred (C11). 100M x 698 B = 70 GB, on every such node | **not cleared** |
| **3. Throughput** | work per cluster state change, proportional to the whole population | 107 ms to add one index at a population of 100k, and 31 ms even when nothing changes (S19) | **not cleared** |

**Ceiling 2 in one line.** At a realistic 8 GB metadata budget, the materialized design holds ~2.7M
indices and the deferred design ~11.5M. Deferral bought one order of magnitude; the target needs two. That
is arithmetic on C11's measured figures rather than a separate measurement, and it is the reason Area A's
deferral is necessary but not sufficient.

**Ceiling 3 in one line.** Creation cost grows with the existing population because `Metadata.build()`
rebuilds derived structures over every index on every change. At 100k indices one create costs 107 ms of
single-threaded work before consensus even begins.

**The two uncleared ceilings have one cause and one fix.** Both exist because cluster state contains one
entry per index. Area H removes the entry, which is why it clears both, and why the areas that make the
entry *cheaper* do not clear either.

## The architecture the ceilings imply

```
cluster state  ->  nodes, membership, templates, settings         O(nodes), kilobytes
descriptors    ->  an OpenSearch index: name -> uuid, shards      point lookup and prefix scan
mappings       ->  object store, indices/{uuid}/metadata/{gen}    fetched on demand, cached
placement      ->  hash(uuid) over published membership           derived, nothing stored
```

Nothing in that picture is per-index-per-node. That is the whole design, and it is the shape S3 uses to
serve 10^14 objects with no global map: a sorted, partitioned, disk-backed index where memory is cache
rather than the source of truth.

## What the lettered areas mean under this reading

- **C, placement.** Done. Cleared ceiling 1 and made the descriptor sufficient for routing.
- **H, metadata off cluster state.** The spine. The only area that attacks ceilings 2 and 3.
- **A, name index tier.** Not a peer of H. It is H's substrate: the descriptor store *is* the name index.
- **F, cluster state diet.** Cancel if H lands. F1 already found `primaryTerms` unremovable, and with no
  cluster state entry there is nothing left to diet.
- **E, manifest sharding.** Re-scope. Most of its motivation is persisting a large cluster state, which
  H makes small.
- **B and D, routing affinity and cache warmth.** More important after H, not less: H trades a resident
  map for a lookup plus a cache, and these two are what keep that cache warm.
- **G, validation.** The gate at the end. G1 and G2a are done and are what produced Part 0.

So the plan is one spine, C then H, with B and D as consequences and G as the proof.

---

# Part 1: Requirements

## 1.1 Functional requirements

**R1. Index and search separation.** Writing and searching run on separate nodes with separate engines,
not one engine in two modes. A search node must never need a writable engine, and a writer must never be
required to serve queries.

**R2. Compute and storage separation.** Object storage holds the data of record: segments, manifests,
WAL, and shard coordination state. Node-local disk holds cache only. Losing a node loses no data and
requires no recovery from a peer.

**R3. Scale.** 100 million indices in the system. Each index has between 3 and 100 shards, so the design
must hold up to 10 billion shards. Shard count per index is changeable at runtime (split and merge
already exist).

**R4. Wildcard and alias resolution.** `logs-*` and alias lookups must keep working across the whole
name space, not only within one partition.

**R5. Elasticity.** Node count changes up and down without operator intervention and without a
correctness event. Adding a node must not cause a fleet-wide cache invalidation.

## 1.2 Non-functional requirements

**N1. No global pass.** No operation may be O(total shards) or O(total indices) on a request path or on
a cluster-state change. This is the requirement that rules out the current allocator.

**N2. Idle tenants are near-free.** A tenant that is not being read or written should cost bounded
memory and zero recurring CPU.

**N3. Graceful degradation on routing staleness.** When the routing hash and reality disagree, the
system serves correctly and more slowly. It does not fail, and it does not corrupt.

**N4. Single-writer safety.** Exactly one writer per shard may commit, enforced independently of any
routing decision.

**N5. Bounded blast radius on scale events.** A membership change must move a bounded fraction of
placement, not reshuffle everything.

## 1.3 Explicitly out of scope

- Supporting non-serverless (local-disk, peer-recovered) indices in the same cluster. The design assumes
  every index is serverless. Mixed mode is a separate problem and would reintroduce the constraints this
  plan removes.
- Cross-cluster replication.
- Making any of this acceptable to upstream OpenSearch. See Part 5 on the fork decision.

## 1.4 Measured constraints this plan works within

| constraint | value | source |
|---|---|---|
| allocator steady-state reroute | ~9-11 us/shard, ~1 s at 100k active shards | S6 |
| allocator cold allocation | 4.6 s at 40k shards, superlinear | S6 |
| materialized metadata | 15,522 B/index at 100 shards | S12 |
| deferred metadata | 698 B/index, flat at 1, 30 and 100 shards | S12 |
| rendezvous lookup | 80 ns at 10 nodes, 359 ns at 200 nodes | S12 |
| compact name index | 45 B/name, 4.2 GiB at 100M | S13 |

---

# Part 2: Desired architecture

## 2.1 Tiers

```
                          client
                            |
              [ load balancer, consistent hash on index name ]
                            |
                  [ coordinating-only nodes ]
                  descriptor cache, request resolution,
                  computed placement, scatter-gather
                   /                |               \
    [ name index tier ]     [ writer nodes ]    [ search nodes ]
    all 100M names          one owner/shard      K candidates/shard
    wildcards, aliases      lease-fenced         chosen by ARS
    replicated                    |                    |
                            [ object storage ]
                    segments, manifests, WAL, shard heads
```

## 2.2 Where authority lives

This is the part worth being precise about, because most of the design follows from it.

| state | authority | cached where |
|---|---|---|
| index descriptor (name, uuid, settings, mappings ref) | object storage, in the manifest | coordinator, by hash affinity |
| shard data | object storage, in bundles | search node local disk, by hash affinity |
| who may write shard S | `ShardHead` register, by CAS | nowhere; always read for a lease decision |
| shard placement | nobody; it is computed | not stored at all |
| index name space | name index tier | replicated in full |
| node membership | cluster state, via existing consensus | every node; it is O(nodes) |

The row that matters most is "shard placement: nobody." Today this lives in the routing table, is
produced by the allocator, and is published in cluster state. Removing it is the change that lifts the
ceiling.

## 2.3 Two hashes, two purposes

**LB to coordinator, `hash(index_name)`.** This is cache affinity, not ownership. Any coordinator can
serve any index, because a descriptor is lazily fetchable from the manifest (built in C5 and C6). A
misroute costs one object-store read. There is no handoff protocol and no fencing, because nothing is
owned.

**Coordinator to search node, `rendezvous(index_uuid, shard_id)` top-K.** Also cache affinity. K
candidates give load-balancing freedom and let a warm holder keep serving while a new node fills.
Adaptive replica selection picks among the K.

**Coordinator to writer node, `rendezvous(index_uuid, shard_id)` top-1.** Correctness comes from
`ShardHead` CAS, not the hash. The hash is only a hint about where to try first.

## 2.4 Why this meets the requirements

- N1 (no global pass): placement is computed per shard on demand. Nothing enumerates all shards.
- N2 (idle near-free): an untouched index costs 698 B of descriptor plus 45 B in the name index. Nothing
  polls it, nothing allocates it, no cluster-state change mentions it.
- N3 (graceful staleness): a wrong coordinator fetches and serves; a wrong search node fetches and
  serves; a wrong writer is rejected by the lease.
- N4 (single-writer): unchanged, already enforced by `ShardHead`.
- N5 (bounded blast radius): measured at exactly `added/(N+added)` for rendezvous.

---

# Part 3: Summary of the suggestion

**The insight.** Requirements R1 and R2 are already built, and building them removed four of the five
constraints that force OpenSearch to store shard placement explicitly. The fifth, `inSyncAllocationIds`,
became redundant when `ShardHead` CAS took over write authority; cluster state's copy simply has not been
deleted. Once placement need not be stored, it can be computed, and once computed the allocator leaves
the request path entirely. That is what makes R3 reachable.

**The single largest change.** `IndexRoutingTable` becomes derived and lazy per index rather than stored
and published, reusing the exact holder pattern C5 introduced for `IndexMetadata`. Core code calling
`state.routingTable().index(name)` continues to work unchanged; the answer is computed on first access
rather than read from published state.

**What gets deleted rather than built.** The published routing table for serverless indices, and
`inSyncAllocationIds`. Both are redundant.

**What gets demoted.** The scale-to-zero machinery stops being load-bearing. Under computed placement,
waking a tenant is not a cluster-state update: a request arrives, the coordinator computes who serves it,
and that node opens a reader from object storage. Scale-to-zero becomes a compute-cost optimization
rather than a metadata mechanism. This is worth knowing before investing further in it.

**What is genuinely new work.** The name index tier, and the resolve-and-forward hop for multi-index
requests. Everything else is modification or deletion.

---

# Part 4: High-level plan

Seven areas. The order is chosen so that risk is retired early and nothing waits on the fork decision
longer than necessary.

| # | area | size | depends on | retires |
|---|---|---|---|---|
| A | Name index tier | large | none | R4, the highest-risk unknown |
| B | Request routing and forwarding | medium | A | R5 partially, unblocks LB config |
| C | Computed placement | large | B | N1, R3, the allocator ceiling |
| D | Cache warmth and rotation | medium | C | N5, cold-start cost |
| E | Manifest sharding and codec namespace | medium | none | write amplification at 100M |
| F | Cluster state diet | small | C | residual per-shard state |
| G | Validation and measurement | medium | C, D | confidence in the whole |

Areas A and E are independent of everything else and of each other, so they can run in parallel from day
one. C is the critical path.

---

# Part 5: Detailed plan by area

## Area A: Name index tier

### A.0 Goal

A replicated service holding every index name in the system, answering exact lookup, prefix wildcard,
and alias resolution, without any node needing the full metadata.

### A.1 Why this first

It was the highest-risk item in the design. Wildcards are inherently global and resist the hashing that
makes everything else scale. The two fallbacks were unattractive: scatter every wildcard to every
partition, or restrict the query language. S13 showed neither is necessary, but nothing is built.

It also has no dependencies and does not depend on the fork decision, so it is the safest thing to start.

### A.2 What exists

`NameIndexSpike` in `benchmarks/` measures the representation and proves the sizing. That is all. There
is no service, no update path, and no integration.

Measured: 45 B/name compact against 145.5 B as a `HashMap`, so 4.2 GiB against 13.6 GiB at 100M. Prefix
resolution is a binary search plus forward scan, costing 3.2 ms for 65,536 matches and 9.2 us for a miss.

### A.3 Tasks

**A1. Production name index structure.** Promote the spike's `CompactNameIndex` into a real class. Sorted
name blob, offset array, raw 16-byte UUIDs, state byte. Add: exact lookup, prefix scan, and an iterator
that does not materialize the whole match set (a wildcard matching 10M indices must stream).

**A2. Update path.** The compact form is built sorted and cannot accept insertions. Design and build the
overlay: a small mutable structure holding recent creates and deletes, consulted alongside the compact
base, with periodic rebuild and atomic swap. Decide and document the rebuild trigger (size ratio, time,
or both) and the memory cost of holding two bases during a swap.

**A3. Alias support.** An alias is a name pointing at a set of indices. Extend the structure to hold
alias entries and their targets. Measure resolution fan-out, which S13 explicitly did not.

**A4. Leading wildcard decision.** `*-logs` degenerates to a full scan of 100M names. Choose one:
   (a) a second structure over reversed names, doubling memory to about 8.4 GiB;
   (b) an explicit restriction, rejecting leading wildcards with a clear error;
   (c) accept the scan and bound it with a timeout.
   Measure (a) before choosing. Record the decision and its rationale.

**A5. Service wrapper.** A transport action for lookup and resolution, and the node role or plugin
component that hosts it. Replication is full-copy, so this is a read-only replicated service with a
single writer applying updates.

**A6. Persistence and bootstrap.** Where the name index lives when the tier restarts. Options: rebuild
from the manifest on start (slow, simple, no new durable state) or checkpoint to object storage
(faster, another thing to keep consistent). Measure rebuild time from a 100M-entry manifest before
choosing.

**A7. Integration.** Route wildcard and alias resolution from the coordinator to this tier instead of to
`Metadata.indicesLookup`. This is where it meets core, and where the surface area is largest.

### A.4 Acceptance criteria

- Holds 100M names within 5 GiB, verified by a scaled test.
- Exact lookup under 10 us, prefix resolution proportional to match count.
- Creates and deletes visible to resolution within a stated bound.
- Restart to serving within a stated bound.
- Every claim above has a test that fails when the corresponding mechanism is disabled.

### A.5 Risks

- **Update path is the hard part, not the read path.** The read structure is proven; insertion into a
  sorted blob is not, and the overlay-plus-rebuild pattern has a memory spike at swap time.
- **Alias fan-out is unmeasured.** An alias over 1M indices may be common in a tenant-per-index world,
  and resolution cost is unknown.
- **Consistency model needs stating.** If a create is not instantly visible to a wildcard, say so
  explicitly rather than discovering it in production.

---

## Area B: Request routing and forwarding

### B.0 Goal

Requests reach a coordinator whose descriptor cache is warm, and multi-index requests work anyway.

### B.1 Why

The hash only helps if requests actually arrive at the affine coordinator. Single-index paths like
`/{index}/_search` can be hashed by the load balancer. `_bulk`, `_msearch`, wildcards, and aliases cannot,
because the target is in the body or needs resolution first.

### B.2 What exists

Nothing. Coordinating-only nodes exist as a role and need no change; the routing in front of them does
not exist.

### B.3 Tasks

**B1. Load balancer configuration.** Consistent hashing on the index. Maglev on Envoy, or `hash ...
consistent` on NGINX, or `balance hash` with `hash-type consistent` on HAProxy. Never `hash % N`, which
reshuffles nearly every key on a membership change.

**B2. Hash key extraction.** Two options, and prefer the first: have clients set a header such as
`x-os-index`, which every LB hashes trivially; or extract from the path by regex, which must survive
every URL form OpenSearch accepts, including `/_search` with no index, `/a,b,c/_search` with several, and
`/index/_doc/id`. Build the header path and keep regex as fallback.

**B3. Backend discovery.** The LB must learn when coordinators join and leave, or the ring goes stale.
EDS or DNS SD on Envoy, endpoints on Kubernetes, the Runtime API on HAProxy. Note that NGINX open source
needs a config reload for upstream changes, which may rule it out.

**B4. Resolve-and-forward hop.** For requests the LB cannot hash, the receiving coordinator resolves
names (using Area A), then forwards to the coordinator the hash names. One extra hop, no new resolution
logic. This is the main piece of code in this area.

**B5. Forwarding loop protection.** A forwarded request must not be forwarded again. Mark it, the way
`WritePartitionRoutingActionFilter` already marks re-entrant dispatch with a `ThreadContext` marker.
Reuse that pattern rather than inventing a second one.

**B6. Fallback policy.** Decide what happens when the affine coordinator is unreachable. Prefer failing
or queueing over silently round-robining, because a fallback that breaks affinity does so exactly when
the system is least able to absorb the cache loss. Make it a setting, default to strict.

### B.4 Acceptance criteria

- Single-index requests reach the same coordinator across repeated calls.
- Multi-index requests succeed, with at most one forward hop.
- A coordinator restart moves only its share of affinity.
- Loop protection has a test that fails without the marker.

### B.5 Risks

- **Path regex fragility** if the header approach is not available, because OpenSearch's URL space is
  large and evolving.
- **Double hop latency** on `_msearch`, which is already latency-sensitive.

---

## Area C: Computed placement

### C.0 Goal

Shard placement is derived from a hash on demand, per index, rather than allocated globally and published
in cluster state.

### C.1 Why

This is the change that lifts the ceiling. S6 measured the allocator binding at the order of 100k active
shards with superlinear cold allocation. The target is up to 10 billion shards. No tuning closes five
orders of magnitude; the global pass has to stop happening.

S12 measured the replacement and found it at the theoretical optimum: primary moves track
`added/(N+added)` exactly, distribution within 5% of even, 80 to 359 ns per lookup.

### C.2 What exists

- `IndexMetadataHolder`, `LazyIndexMetadata`, and `Metadata.Builder#putStub`: the lazy-holder pattern this
  will copy, already proven for metadata.
- Phase A: core already tolerates an index present in metadata and absent from the routing table, which
  is the weaker version of what this needs.
- `ComputedPlacementSpike`: the algorithm, measured, not integrated.
- `ResponseCollectorService` and `OperationRouting`'s adaptive replica selection: the mechanism for
  choosing among K candidates, already shipped and on by default.

### C.3 Tasks

**C1. Placement function.** Promote rendezvous top-K from the spike into a production class. Inputs are
the node list and `(index_uuid, shard_id)`; output is K ordered candidates. Must be deterministic across
nodes and versions, so pin the hash function and add a test that its output does not change.

**C2. Node set snapshot.** The placement function needs a stable view of eligible nodes. Define what
"eligible" means (role, health, not draining) and where the snapshot comes from, since disagreement
between two coordinators about the node set means disagreement about placement.

**C3. Holder seam on `IndexRoutingTable`.** Mirror C5's approach: an interface the concrete type
implements, so nothing changes for existing callers, plus a lazy implementation that computes on first
access.

**C4. Computed `IndexRoutingTable`.** Build shard routing entries from the placement function rather than
from allocator output. Writers get top-1 as primary; search nodes get the remaining K-1 as search-only
replicas. Recovery source is `ExistingStoreRecoverySource`, since the data is in object storage. Note the
trap A5 already hit: `addAsRecovery` picks its source from `inSyncAllocationIds` and will silently choose
`EmptyStoreRecoverySource` when those are absent, which resurrects an index as blank. Build the entry
explicitly.

**C5. Gate on index type.** Only serverless indices take this path. Non-serverless indices, if any remain,
keep the allocator. A setting, defaulted off, exactly as C5 and C6 shipped.

**C6. Bypass the allocator.** Serverless indices must not enter `AllocationService.reroute` at all. Check
what `SuspendedShardAllocationDecider` and `ServerlessStorageExistingShardsAllocator` still need to do,
and remove what becomes dead.

**C7. Search path integration.** Feed the K candidates to adaptive replica selection instead of the
routing table's replica list. Verify `OperationRouting` accepts hash-derived candidates without
modification, which is the assumption this area rests on and has not been checked.

**C8. Writer path integration.** Top-1 as a hint; `ShardHead` CAS decides. Confirm a writer that loses the
lease fails cleanly and the request retries against the new owner.

### C.4 Acceptance criteria

- A cluster with serverless indices performs no global reroute.
- Placement for a given shard is identical on every coordinator.
- A search request reaches one of the K candidates.
- A writer that is not the lease holder is rejected, not permitted to write.
- Recovery source is never `EmptyStoreRecoverySource` for an index with data. This needs an explicit test,
  given A5's history.

### C.5 Risks

- **Blast radius.** Phase A found 16 call sites that dereference a routing lookup; far more read it.
  Making it computed rather than absent is a smaller change than removing it, but the surface is still
  large.
- **Code that writes to the routing table**, or observes shard state transitions from INITIALIZING to
  STARTED, has no obvious equivalent under computed placement. This is the most likely thing to break and
  deserves an audit before C4, not after.
- **Hot-tenant skew.** A hash cannot know one tenant takes a thousand times the traffic. K=3 gives room to
  choose but does not solve it. Consider an explicit override list for known-hot indices.

---

## Area D: Cache warmth and rotation

### D.0 Goal

Scale events do not cause a cold-read storm.

### D.1 Why

S12 measured this precisely. Adding one node leaves no shard without a warm candidate, structurally,
because one node can displace at most one of K. But doubling the fleet leaves 12.4% of shards with no
warm candidate at all. Under the old allocator-driven placement, moves were rare and pre-warm was
optional. Under computed placement every large scale event triggers this, which is why the existing
"Phase 3 (optional): pre-warm before rotation" task is promoted to required.

### D.2 What exists

- `ReaderCacheAffinityMetadata` and `ReaderCacheAffinityRecorder`: a direct warmth signal.
- Node drain mode, from the node-autoscaling work.
- `SustainedCandidateTracker`: the anti-flap pattern used by split and merge triggers.

### D.3 Tasks

**D1. Pre-warm before rotation.** When the node set is about to change, fetch the shards that will move
onto their new candidates before shifting traffic. Bound the concurrency so pre-warm does not itself
cause an object-store storm.

**A real correctness gap found and fixed (2026-08-14), after D1 itself had already shipped.** A
multi-agent survey of the whole plan, run to pick the next task once Area E landed, ranked this the top
candidate for a reason worth stating plainly: `ReaderShardPreWarmCoordinator` enumerated only
`event.state().metadata().indices()`, and a gated index (Area H) has no entry there at all -- that is
what gating means. So the entire gated population, which is the actual target this whole project exists
to serve, was silently pre-warmed for never. No error, no log line, nothing to notice -- the mechanism
looked wired (D1's own IT already proved it dispatches for an *ordinary* computed-placement index) and
did nothing for the population the plan's own Part 0 names as the point of Area H.

Fixed by giving gated indices a second, differently-shaped pass rather than trying to make the first one
see them: `IndicesClusterStateService#onDemandOpenIndices()`, a new small accessor over the same
`openedOnDemand` registry T39 already maintains, exposes each node's own bounded, currently-open gated
indices -- the identical "per-node working set, not a population scan" scope `GatedIndexPrewarmer`
already established for the same reason. That pass runs on every node (not only the cluster manager,
since the working set is inherently node-local and small), with dispatch restricted to whichever node
rendezvous currently names the shard's primary candidate, so exactly one node ever sends the request
for a given shard.

One more real bug surfaced building the actual IT: a node simultaneously the primary dispatcher and
newly eligible for its own shard would dispatch a request to itself, which takes a local fast path that
re-enters `clusterService.state()` from inside the very cluster-state-applier callback that is running
it -- an assertion failure. Fixed by skipping the local node id in the newly-eligible set before
dispatching.

Verified with a real break-the-fix cycle on the new gated pass specifically (commenting it out
reproduced "got 0" in the new `testAGatedIndexsShardIsAlsoPreWarmed`, restoring it went green again),
plus a broader regression sweep (`IndicesClusterStateServiceRandomUpdatesTests`,
`GatedIndexPrewarmerTests`, and every class under `org.opensearch.serverless.storage.placement.*` and
`...readerengine.*` -- 20 classes, 0 failures). Full write-up in the commit itself.

**D2. Scale hysteresis.** Reuse `SustainedCandidateTracker` rather than inventing a third scaling policy.
A brief traffic spike must not reshuffle affinity. Set the sustained window from the measured pre-warm
duration, not from a guess.

**D3. Prefer-warm among K.** When candidates are equal by ARS, prefer one that held the shard before the
last membership change. This is what converts the 0% single-join figure from a theoretical property into
an actual one.

**D4. ARS sufficiency check.** The plan assumes ARS's latency signal proxies cache warmth adequately, on
the reasoning that a cold node reads as a slow node. Test it. If it oscillates, blend in
`ReaderCacheAffinityRecorder`'s direct signal.

**Tested (2026-08-13), and the real finding differs from what the plan predicted.**
`AdaptiveReplicaSelectionCacheWarmthProxyTests` exercises core's own real `ResponseCollectorService`
(the actual mechanism `IndexShardRoutingTable`'s candidate ranking uses -- traced directly:
`NodeRankComparator` sorts ascending on `ComputedNodeStats#rank`, lower wins) with illustrative
warm/cold response-time magnitudes (5 ms vs. 50 ms, a 10x separation meant to represent a local
cache hit vs. an object-store re-fetch, not measured against a real workload).

- **Oscillation under noise, the plan's own named concern, does not reproduce at this magnitude.** A
  single anomalous sample on an otherwise-warm node (representing a GC pause or network blip, not a
  real cache-state change) does not flip the ranking against a consistently cold node -- core's
  existing EWMA smoothing (alpha 0.3) absorbs it.
- **The real gap is a blind spot, not oscillation: a node ARS has never received a sample for has no
  statistics at all**, not a "cold" ranking -- `getNodeStatistics` returns empty. A shard just
  reallocated onto a previously-untouched-by-it node (exactly D1's pre-warm scenario: a scale-up or
  rebalance) is indistinguishable, to ARS alone, from a node that has always been warm for it, because
  there is no signal yet, not a bad one.

This still supports the plan's own conditional -- blend in `ReaderCacheAffinityRecorder`'s direct
signal -- but for a different, more specific reason than "oscillation": ARS needs to be seeded (or
supplemented) at the exact cold-start moment D1 cares about, not corrected for noisy disagreement it
already handles reasonably on its own.

**D5. Incremental scaling policy.** Prefer adding one node at a time over large jumps, given the measured
difference (0% against 12.4%). Encode this as policy, not as documentation.

**Investigated (2026-08-13): this plugin has nothing to encode the policy into.** `NodeCapacitySignalService`
is a signal emitter, not an actuator -- `latestSignal()` reports capacity state for an external operator
or autoscaler to read, the same "signal exists, no auto action" boundary this project already draws
deliberately elsewhere (e.g. `ShardSplitCandidatesAction`; the auto-split-controller work explicitly refused
to build target-index auto-provisioning without human sign-off for the same reason). This plugin does not
provision cloud nodes and has no seam that would decide *how many* nodes to add at once -- that decision is
made entirely outside this codebase. "Encode this as policy" would mean encoding it into whatever external
system actually adds nodes, which is out of this repository's reach. What this codebase *can* and does do is
make the consequence of a large jump bounded rather than unbounded: `ReaderShardPreWarmCoordinator` (D1,
built this session) caps how many shards it pre-warms per cluster-state event
(`serverless_storage.reader_pre_warm.max_per_event`), so even an operator ignoring this recommendation and
adding many nodes at once degrades to "some shards pay an ordinary cold read" rather than an object-store
request storm. The recommendation itself belongs in deployment documentation for whoever operates the
autoscaler, not in code this plugin owns.

### D.4 Acceptance criteria

- A single node join causes no measurable increase in object-store reads.
- A fleet doubling with pre-warm enabled causes a bounded, measured read increase.
- Hysteresis prevents a transient spike from triggering a scale event.

### D.5 Risks

- **Pre-warm cost at scale.** Warming 12.4% of 10 billion shards is not a small operation, and may need
  to be partial (warm the hot subset) rather than complete.

---

## Area E: Manifest sharding and codec namespace

### E.0 Goal

Cluster-state uploads stop rewriting all N index entries, and the fork's on-disk format cannot be
confused with upstream's.

### E.1 Why

MS.1 confirmed every cluster-state version rewrites every index entry. At 100M indices that is
untenable write amplification. C3a measured sharding at 125x saving with one changed index at 256
shards, and corrected the design's original 64 (which saturated and became 9.5% worse than not sharding).

### E.2 What exists

- `UploadedManifestShard`: the reference type, built and tested. First slice of C3b.
- `ClusterMetadataManifest` CODEC_V5, `MANIFEST_CURRENT_CODEC_VERSION`, and the exact-match dispatch in
  `RemoteClusterMetadataManifest.getClusterMetadataManifestBlobStoreFormat()` which throws on an unknown
  codec.
- The C1 cleanup guard, which skips the GC sweep when it sees a codec above what it understands.

### E.3 Tasks

**E1. Codec namespace.** Reserve a high integer range for the fork, starting at 10000. Verified safe: the
read dispatch is exact-match with a throw at the end, so an upstream node reading a fork manifest fails
cleanly before deserializing anything, rather than misreading it as a cluster with zero indices.

**E2. `onOrAfterCodecVersion` audit.** That method is a `>=` test, so a codec of 10000 silently takes every
V1 to V4 branch in `toXContent`. That is probably intended (the fork format as a V5 superset) but is
currently true by accident. Make it explicit and pin it with a test, or those branches will drift on the
next merge.

**E3. Assert and write path.** `assert max(CODEC_VERSIONS) == MANIFEST_CURRENT_CODEC_VERSION` means adding
the new codec to the array flips every node to writing it. For a fork this is correct and simplifies
matters: no dormant landing, no setting to flip, no two-release rollout. Document explicitly that the
fork's repository is then not readable by stock OpenSearch, as a deliberate choice.

**E4. Error message.** The dispatch failure says "corrupted" for what is really "foreign." Reword it in
the fork.

**E5. C3b write path.** Write the sharded manifest. Must also add the transitive cleanup resolution that
C1 deliberately deferred, since the GC sweep computes `filesToKeep` by subtraction over retained
manifests and a sharded manifest changes what "referenced" means.

**E6. C4 read path.** Read the sharded manifest, fetching only the shards needed.

**Built (2026-08-14), E5 and E6 together with the cleanup resolution E5 calls for.** Both had to land as
one slice: a write path with no read path is unreadable to every existing caller, and a write/read path
with no cleanup fix is unsafe to ever turn on. `ManifestShardFunction` (murmurhash3(indexUUID) mod
shardCount) plus `IndexMetadataManifestSharder` (the carry-forward/rewrite/drop-empty planning step) do
C3b's partitioning; `ManifestShardContent`/`RemoteManifestShard` are the shard blob's own read/write
entity, reusing `UploadedManifestShard` as the reference type it was already built to be. `ClusterMetadataManifest`
gained `CODEC_V6` (`manifestShardCount`, `indexMetadataShards`), with `CODEC_V5` kept readable so existing
unsharded manifests are not orphaned by the bump -- consistent with E3's "no dormant landing" policy:
every node flips to writing V6 immediately once this lands.
`RemoteManifestManager#resolveIndices` is the one seam that decides inline-vs-sharded; off by default
(`cluster.remote_store.state.manifest.shard_count = 0`), matching the rest of this plan's default-off
discipline for unmeasured mechanisms.

The cleanup resolution turned out to have more call sites than E5's own text anticipated: not just the GC
sweep, but `RemoteClusterStateService` (`writeIncrementalMetadata`'s own diff against the previous
manifest, `markLastStateAsCommitted`, `getClusterStateForManifest`, `getClusterStateUsingDiff`, checksum
validation, `isMetadataEqual`) and `GatewayMetaState#verifyManifestAndClusterState` all called
`manifest.getIndices()` directly and would each have silently computed an empty index list against a
sharded manifest -- found by a repo-wide grep, not by re-reading the RFC's own list of call sites, which
did not enumerate all of them. All fixed to route through `resolveIndices`.

**The E.5 risk below ("Cleanup correctness... deserves the fuzzing treatment") got exactly that, and the
first attempt at it produced a false negative worth recording.** Reintroducing the bug
(`clusterMetadataManifest.getIndices()` in the GC sweep's active-manifest loop) did not fail
`ManifestShardingIT#testCleanupSweepDoesNotDeleteLiveDataUnderSharding`'s original restart-based
assertion, even with the fix genuinely absent -- `internalCluster().fullRestart()` does not reliably force
every index's metadata to be freshly re-read from the remote repository in that test's topology, so the
check was not actually exercising the failure it was meant to catch. Strengthened it with a direct,
no-round-trip check instead: capture the blob name of an index the test's own write traffic never
touches, and assert with `BlobContainer#blobExists` that it still exists immediately after the sweep, not
after a restart. Re-run against the reintroduced bug with that check in place failed exactly as predicted;
restoring the fix went green again. This is the version now in the tree -- the acceptance criterion below
("The GC sweep does not delete blobs referenced by a sharded manifest") is verified against a real
repository, not assumed from the subtraction logic reading correctly.

**E7. Align shard function with the routing hash.** If the manifest shard function matches the coordinator
hash, a coordinator warms its entire partition in one read. This is a free win from two independent
designs lining up, and needs to be built deliberately rather than discovered.

**Deferred, not built (2026-08-14).** `ManifestShardFunction`'s own javadoc documents why: the two hashes
solve different problems over different domains (a fixed shard count vs. `RendezvousShardPlacement`'s
dynamic node-count rendezvous hashing), so forcing them to align would be inventing a design decision
this plan does not actually specify, the same discipline D5 applied when it found no actuator to encode
its own recommendation into. Left open rather than force-built.

**E8. Re-measure.** C3a's 256-shard figure came from a simulation, not the implementation. Re-run against
the real write path.

**Measured (2026-08-14) against the real write path, and the real number is a lot smaller than the
simulation's headline figure -- for a reason the simulation's own text already flagged.**
`ManifestShardingWriteAmplificationIT` calls the genuine `RemoteManifestManager#uploadManifest` against a
real FS-backed repository (not the simulation's arithmetic), diffs real blob listings before and after one
settings update, and sums real, real-compressor bytes. At 1,000 real indices and 256 shards -- two orders
of magnitude below the RFC's 100,000-index scale, since creating 100,000 real indices is not something an
integration test can do in reasonable time -- one changed index wrote **656,322 bytes unsharded vs.
134,617 bytes sharded, a 4.9x reduction**, not 125x.

The gap is not a bug in either number, it is scale: C3a's 125x figure is for N=100,000, S=256 (~390 real
index entries per shard); this measurement's N=1,000 at the same S=256 has only ~4 entries per shard.
Unsharded bytes scale with total index count; sharded bytes for one changed index scale with entries per
shard plus a small constant per-shard-reference overhead in the top-level manifest -- so the achievable
ratio is bounded above by roughly S itself, and a shard holding only ~4 entries has far less to save than
one holding ~390, on top of C3a's own noted penalty that small blobs compress worse per entry than large
ones. **This is not evidence the RFC's 100,000-index figure is wrong** -- it is a different point on the
same curve, consistent with the mechanism, not a contradiction of it -- but it is also not confirmation of
125x, and the 125x number should not be repeated as validated against the real implementation without a
run at closer to the RFC's own scale. `ManifestShardingWriteAmplificationIT`'s own javadoc documents this
scale gap so it is not lost the next time this number is cited.

### E.4 Acceptance criteria

- One changed index rewrites one manifest shard plus the top-level manifest, not all N entries.
- An upstream node reading a fork manifest fails with a clear, non-corrupting error.
- The GC sweep does not delete blobs referenced by a sharded manifest.

### E.5 Risks

- **Cleanup correctness.** GC deletes by subtraction, so any gap in the transitive resolution deletes live
  data. This deserves the fuzzing treatment the earlier GC work got.

---

## Area F: Cluster state diet

### F.0 Goal

Delete the per-shard state in cluster state that computed placement makes redundant.

### F.1 Why

`inSyncAllocationIds` exists to answer which copy is safe to promote. In this architecture the object
store holds the data and `ShardHead` CAS decides who may write, so the question is already answered
elsewhere. Same for `primaryTerms`, which the shard head also carries. Carrying both is not merely waste;
it is two sources of truth that can disagree.

### F.2 Tasks

**F1. Audit.** Enumerate every reader of `inSyncAllocationIds` and `primaryTerms` and classify each as
serverless-irrelevant or not.

**F2. Make them empty for serverless indices**, behind the same gate as Area C, and confirm nothing
breaks. Do this before deleting anything.

**F3. Measure.** Re-run `DeferredMetadataHeapEstimate` to confirm the 698 B/index figure drops, and by how
much.

**F4. Remove.** Only after F2 has soaked.

### F.3 Risks

- This is the area most likely to produce a subtle correctness bug for the least benefit. It is last for
  that reason, and F2 before F4 is not optional.

---

## Area G: Validation and measurement

### G.0 Tasks

**G1. Publication latency.** The one unmeasured number that changes a decision. It determines whether
wake and sleep need the `ClusterStateTaskExecutor` batching that C7 gave create-index. Measure against
cluster size and batch size. Note that `TransportReactivateShardsAction` and `ShardSuspensionCoordinator`
both submit plain `ClusterStateUpdateTask`s today with no executor, and that reactivation runs at
`Priority.URGENT` against suspension's `NORMAL`, so under sustained wake pressure suspension can starve
and the active set ratchets up.

**G2. Scaled integration test.** A cluster with a synthetic 1M-index population, exercising create,
search, wildcard, scale up, scale down, and node failure. 1M rather than 100M so it can run in CI, with
the per-index costs known to be flat so extrapolation is defensible.

**G3. Cold-start measurement.** Time from cluster start to serving, with a large index population. S6
found cold allocation superlinear and it was the figure that should size a cell; under computed placement
it should become flat, and that claim needs a number.

**Measured (2026-08-14), real end-to-end, and the claim holds at this scale.**
`ComputedPlacementColdStartIT` times a real `client().admin().indices().create` acknowledgement -- the
same shape of measurement S6 made for the allocator this replaces -- against a real, growing background
population, at checkpoints [0, 300, 1,200]. Deliberately smaller than S6's 40,000-shard scale or the
plan's 100M-index target: real creation is a real cluster-state publication, not S14/C11's now-deleted
`ComputedPlacementCostTests`' free in-memory routing computation, so an integration test reaches a much
smaller population in reasonable time -- a real number here is a genuine data point, not proof flatness
holds all the way to 100M. Two runs, same shape both times: population 0 costs far more than 300 or
1,200 (190ms/148ms, JIT and first-request warmup -- S6 itself notes the identical distortion at its own
smallest tier), then flat rather than growing across the 4x population increase from there (84ms then
85ms; 29ms then 37ms). Deliberately asserts nothing about the numbers themselves -- see T40's own "a
test asserting on elapsed time is deleted, a test asserting on a count is kept" rule -- only that each
timed create completed and took non-zero time; the numbers above are read from `logger.warn` output, not
from a threshold the test enforces.

Narrower than this item's own "cluster start to serving" framing in one respect, stated in the test's
own javadoc: it times the create acknowledgement -- the point `ComputedRoutingTable` has already marked
the new shard started as part of that same computation, matching what S6 measured for the allocator --
not a first real search or write against the index afterward, which remains unmeasured.

**G4. Chaos.** Node loss during pre-warm, LB and coordinator disagreement about the node set, and a
partitioned name index tier. N3 claims graceful degradation and that claim needs evidence.

Two of the three named disruptions now have real coverage, built through the real plugin bootstrap path
rather than the hand-wired `installBlobBackedDescriptorPlane()` harness most gated tests in this plugin
use (`ServerlessStoragePreWarmChaosIT` sets `SERVERLESS_STORAGE_NODE_ENABLED_SETTING` for real, the same
path `BlobBackedDescriptorIT` proved once already; this is the second proof point, under conditions --
node death mid dispatch -- the first was never subjected to). `ServerlessStoragePreWarmChaosIT` kills a
data node while `ReaderShardPreWarmCoordinator`'s gated pass has active dispatch in flight for a real
gated index, then asserts the cluster stabilizes, pre-chaos documents remain searchable, and one more
growth round after the kill still produces a real dispatch -- not just "the process is still up."
`ServerlessStorageClusterManagerFailoverChaosIT` kills the elected cluster-manager mid-dispatch for an
ordinary (non-gated) index and asserts the newly elected replacement resumes dispatch with no special
hand-off. Both pass. The third named disruption, "a partitioned name index tier," and "LB disagreement
about the node set" specifically, remain deliberately out of scope: neither has a concrete mechanism in
this codebase to disrupt yet (no simulated LB, and Area A's name index tier has no partition-injection
seam this plugin owns), so building either now would be guessing at a scenario rather than testing one --
the same discipline E7/D5/H1d's `InPlaceMergeTriggerCoordinator` deferral already applied.

Building the node-loss test surfaced a real, separate finding, deliberately not fixed in this pass: a
gated shard's primary is re-derived by rendezvous hashing over the current node list and opened on
demand, not failed over by the ordinary allocator, and after its host node dies that reassignment can
take longer than several minutes -- or possibly hang outright -- in this environment. Reproduced against
more than one target shard across different runs (not one unlucky shard), and survived every plausible
test-side explanation: draining the cluster-state task queue for real
(`waitForEvents(Priority.LANGUID)`, not the vacuous `assertBusy(() -> assertNotNull(count))` idiom used
elsewhere in this plugin, which never actually waits because `assertNotNull` on an autoboxed `long`
never throws) before the write, and request timeouts up to three minutes, made no difference. The
shipped test works around this by only asserting that documents written *before* the chaos remain
searchable (a search tolerates a still-unavailable shard; it does not throw the way a write to one
specific unavailable shard does), deliberately not asserting a fresh write succeeds immediately after a
node death. Whether this is a genuine gap in gated shard failover or an artifact of this test
environment's shard-open concurrency is unresolved and worth its own investigation; deferred here for
the same reason `InPlaceMergeTriggerCoordinator` was deferred under H1d -- fixing it was not this
chunk's question, and force-fixing an unscoped finding under a chaos-coverage task would be exactly the
"guessing at a scenario" this item's own scope discipline warns against.

Both chaos scenarios are separate top-level `internalClusterTest` classes rather than two methods on one
class, and that split is load-bearing, not stylistic: OpenSearch's IT runner forks one JVM per test
class and reuses it across that class's methods, and `DescriptorGate` -- the component the node-loss
test's real bootstrap path installs -- is a JVM-wide static singleton. Two methods that each boot real
`ServerlessStoragePlugin` nodes in one shared JVM, one of which installs real gated-index machinery, hit
exactly the "components correct in isolation, never proven integrated" trap this plugin has hit more
than once this cycle -- except here the trap was in the test harness itself, not the production code.
Splitting into two classes (confirmed via test timestamps to run in genuinely separate, concurrent JVM
forks) fixed a real failure that only appeared when both ran together and never appeared running either
in isolation.

**G5. Re-run the full spike suite** after Area C lands, since several figures were measured against the
current architecture and will change.

---

# Part 6: What this plan does not decide

**Fork or upstream.** Computed placement replaces OpenSearch's placement model for serverless indices
rather than optimizing it. That is the right answer for a serverless-only system and a hard sell
upstream, since it would need to coexist with every local-disk deployment. Areas A, B, E and G are
useful either way. Area C is the one that forces the choice, and Area F follows C.

**Workload shape.** Three numbers would sharpen the sizing and none of them are derivable from the code:
peak concurrent active tenants, how often a tenant goes quiet and returns, and how long a returning
tenant may wait. The plan is deliberately built not to depend on them, since computed placement removes
the active-shard ceiling that made them critical. They still determine capacity, and Area G's tests need
a shape to test against.

**Hot-tenant policy.** K=3 gives room to spread a hot shard but nothing decides when a tenant is hot
enough to warrant an override. Needs a product answer.

## Review: where the 100M goal actually stands

Written after Area H's task list emptied, against the three ceilings rather than against the task list,
because a finished task list is not evidence of a cleared ceiling.

### Ceiling 1, placement: cleared

Area C. Nothing here changed it and nothing found since has challenged it.

### Ceiling 2, residency: cleared in mechanism, at the scale measured

A thousand gated creations leave zero metadata entries and zero routing entries (S22). Zero is the right
kind of claim: it does not degrade with population the way an improvement would.

Two things nearly undid it, both found in one review pass and neither by a failing test. The mapping cache
held an entry with a full field map for every index a node had ever served (H11). The suspension registry
held an entry for every sleeping shard, which under scale-to-zero is most of the cluster (H12). Both moved
the ceiling from the cluster manager to the data nodes rather than removing it, and both were invisible:
nothing was slow, nothing threw, a node just used more memory the longer it ran.

That pattern is the finding worth carrying forward. Removing per-index state from one place creates
pressure to cache it in another, and the replacement is not audited the way the original was.

### Ceiling 3, throughput: cleared, and the number that was quoted was the wrong one

Gated creation bypasses `Metadata.build()`: 101,542x at fifty thousand indices, flat against population
(S22). End to end, including the descriptor write that S22 did not measure, 20,577 indices per second,
which puts filling 100M at roughly 1.4 hours on one small cluster (S26). Creation throughput is not a
ceiling, and it was the plausible next one.

### What is not established

**Nothing has run above fifty thousand indices.** Every measurement here extrapolates. The curves are flat
where flatness is the claim, which is the strongest evidence available without a fleet, but flat to 50k is
not flat to 100M and should not be quoted as though it were.

**The enumeration surface is not fully converted.** Of the 41 direct enumerations, 16 vanish with the map
and about 5 need conversion. Not all conversions are done, and an unconverted enumeration on a request path
is a full scan of the index population.

**Wildcards have no pagination contract.** Returning ten thousand names costs 55x counting them (327.5 ms
against 5.9 ms, S24). A tenant with a hundred thousand indices running `logs-*` is a different operation
from anything measured, and the contract question belongs next to the freshness question rather than being
discovered in production.

**Suspension has no durable record** (H13). `IndexDescriptor` carries the field and nothing writes it, so a
full-cluster restart wakes every sleeping shard. They sleep again on the next idle tick, so this is a
thundering-herd risk at restart rather than data loss, but the design is described in two places as though
the durable half exists.

**Two product decisions remain open**: whether serverless-only is a formal precondition, and fork versus
upstream.

### Honest answer

The architecture no longer has a known ceiling below 100M, which is a different and weaker statement than
having reached 100M. Every mechanism the plan calls for exists and is tested at the scale a test cluster
allows. What separates this from a demonstrated result is a run at a population two to three orders of
magnitude larger than anything measured, and that is now the single most valuable thing to do next.

## Second review: what changed, and the answer is still no

The first review said the remaining work was breadth rather than architecture, and named two open product
decisions. Both claims were wrong, and how they were wrong is the most useful thing this cycle produced.

### Neither open item was a product decision

**Pagination (H16, H17).** The choice looked like resolving pages through the descriptor index versus
refusing gated clusters, pending an ordering the descriptor index could produce cheaply. S24 had already
measured that ordering months of work earlier: paging by sorted name with `search_after`, about 33 ms per
thousand. Nothing was waiting on anyone. Gated indices now appear in paginated listings, folded in as a
merge of two sorted runs whose cost is ordinary-index-count plus page-size, with the hundred million in
neither term.

**Wildcard freshness (H18).** Deferred three times. It is not a decision at all: the two resolution paths
read the same descriptors by different means, so the store decides. An exact name is fetched by id, which
reads through the translog and is realtime. A wildcard is a search, which sees refreshed segments. Measured
both arms with refresh disabled: nameable immediately, wildcard-visible only after a refresh. The
asymmetry lands the right way round, since the client that must not be told an index is missing is the one
that just created it.

The lesson is not about these two items. It is that "product decision" was being used to describe
questions whose evidence had already been gathered, and the cost of that mislabel is a cycle.

### Breadth turned up architecture twice

Sweeping request paths for enumerations found two, not the tidy residue the first review implied. H16 in
pagination, closed. **H19 in cluster stats**, pinned: `MappingStats.of` walks every index, so at 100M the
walk is the request, and gated indices contribute nothing to the counts. That one is the least visible
instance of the area's signature failure yet, because a statistic that silently omits a population returns
a plausible number rather than an obvious gap.

### The ceilings, restated

| ceiling | state |
|---|---|
| placement | cleared by Area C |
| residency | cleared; H11 and H12 closed two leaks that had rebuilt it on the data nodes |
| throughput | cleared; 20,577 creations/sec (S26), lookup flat 50k to 1M at ratio 0.96 (S27) |

### What is genuinely left

1. **The MappingStats aggregate** (H19). Pinned, not built. The only piece of implementation work
   remaining that is known and specified.
2. **A demonstration above one million.** S27 is twenty times more evidence than anything before it and it
   is still two orders of magnitude short of the target. This needs a fleet, not a test cluster.
3. **The serverless-only precondition.** Genuinely a product decision, unlike the two that were mislabelled
   as such, because it is a question about what the product promises rather than about what the code can
   do.

### Answer

Still no, on narrower grounds than last time. There is no known ceiling below 100M and one known
unimplemented gap above it. The largest population ever exercised is one million. That gap between "no
known ceiling" and "demonstrated" is not closable by more work of this kind, and saying otherwise would be
the same error as calling a measured question a decision.

## Third review: the goal is not reached, and now I can say precisely what is missing

The previous review claimed the remaining distance was "not closable by more work of this kind". That was
overstated, and the correction is the substance of this cycle: it was true of a hundred million and false
of ten, and running ten changed a load-bearing claim.

### What this cycle closed

- **H20**, the last known unimplemented gap. Mapping stats now include the gated population through an
  aggregate that offers no way to iterate indices, so the repair that would fix correctness while
  restoring the population-sized cost is inexpressible rather than merely discouraged.
- **H21 / S28**, the demonstration pushed from one million to ten.

### What ten million changed

Creation is flat: 23,843 per second at 10M against 22,805 at 1M, putting a hundred million at about 70
minutes of writing.

**The lookup is not flat, and the plan had been resting on the claim that it was.** S27 measured 0.96x from
fifty thousand to a million. The next decade gives 1.88x. The earlier figure was correct for its range and
I generalised past it. Extrapolating the observed decade factor puts 100M near 1.3 ms, which is still an
acceptable metadata lookup, so the architecture survives, but it survives as arithmetic on a growth rate
rather than as a flat curve. Anything downstream that quotes "flat" needs rewording.

### The ceilings, third pass

| ceiling | state |
|---|---|
| placement | cleared by Area C |
| residency | cleared; H11 and H12 closed two leaks that had rebuilt it on the data nodes |
| throughput, creation | cleared and flat to 10M |
| throughput, lookup | acceptable but **growing**, 1.88x per decade, ~1.3 ms projected at 100M |

### What is left, and it is now short and specific

1. **A run at a hundred million.** Ten million needed an 8 GB heap on a single-JVM cluster. A hundred
   million is a fleet exercise, not a test-suite one. This is the only thing standing between the current
   state and the goal, and no amount of work in this repository substitutes for it.
2. **The serverless-only precondition.** A genuine product decision, unlike the two that were mislabelled
   as such and turned out to be settled by measurements already taken.

### Answer

No, and for the first time the reason is a single missing measurement rather than a list. Every mechanism
the plan calls for exists, is tested, and is mutation tested. Every enumeration found on a request path is
either converted or pinned with a named fix. The largest population exercised is ten million, one order of
magnitude short, and the curve that carries the extrapolation is known to grow rather than assumed to be
flat.

Claiming the goal is reached would require asserting that 1.3 ms at 100M holds without having measured a
decade of it, which is exactly the generalisation S28 caught me making one decade lower down.

## Fourth review: the last technical unknown is closed, and the goal is still one measurement away

### What this cycle produced

**S29 reversed S28's central conclusion.** S28 found lookup growing 1.88x per decade and I concluded the
extrapolation to 100M had to rest on a growth rate. H22 separated the two candidate causes by force
merging, which collapses segments without shrinking the term dictionary. At five segments, one million
descriptors cost 0.3044 ms and ten million cost 0.3199 ms: **1.05x across a tenfold increase**. The growth
was merge policy. The term dictionary contributes about five percent per decade, not eighty-eight.

So the descriptor index does not become the new ceiling, and 100M lookup is a design parameter rather than
a fact to be accepted.

### The ceilings, fourth pass

| ceiling | state |
|---|---|
| placement | cleared by Area C |
| residency | cleared; H11 and H12 closed two leaks that had rebuilt it on the data nodes |
| throughput, creation | cleared and flat to 10M, about 70 minutes to write 100M |
| throughput, lookup | cleared **conditionally**: flat at bounded segment count, 1.88x per decade if unbounded |

### Two operational parameters that are now load-bearing

Both were discovered rather than designed, and both can be got wrong in a way that looks like the
architecture failing rather than like a misconfiguration.

1. **Refresh interval** (H18) bounds wildcard staleness. Disabling it, which the measurement harnesses do,
   makes wildcards permanently stale.
2. **Merge policy** (S29) bounds lookup latency. Letting segments grow unbounded reproduces S28's curve.

These belong in the deployment contract next to each other. Neither is a tuning preference.

### What is left

1. **A run at a hundred million.** Ten million needed an 8 GB heap on a single-JVM cluster. This is a fleet
   exercise and nothing in this repository substitutes for it.
2. **The serverless-only precondition.** A product decision, and the only genuine one remaining.

### Answer

No, and the reason is now a single missing measurement with every technical unknown around it closed.
Every mechanism exists, is tested and is mutation tested. Every enumeration found on a request path is
converted or pinned with a named fix. Both curves that carry the extrapolation have been measured across a
decade rather than assumed, and the one that appeared to grow has been explained and shown controllable.

What would change the answer is a hundred million indices on real hardware. That is the whole of the
remaining gap, and it is a smaller and better characterised gap than at any previous review.

## Fifth review: the goal answered by extrapolation from a measured curve

Scope was set by the user: plan for a hundred million, measure locally at populations that fit, and
project. That is what the previous reviews should have done instead of treating the endpoint as something
to brute-force.

### The answer

**The design carries to a hundred million on the evidence available, with two conditions attached.**

| ceiling | evidence |
|---|---|
| placement | cleared by Area C |
| residency | gated indices leave zero cluster state entries (S22); H11 and H12 closed two leaks that had rebuilt it on the data nodes |
| creation throughput | flat at ~26,500/sec across 100K to 1M (S30), ~20,600/sec end to end (S26); 100M is roughly 70 minutes of writing |
| lookup latency | 0.35 to 0.48 ms projected at 100M (S30), against 0.34 ms measured at 1M |

The lookup projection is the one that matters and it is the one with a check outside its own range. S30's
curve predicts 0.403 ms at ten million; S29 measured 0.3199 ms there on a separate run. The projection
overshoots where it can be tested, so the upper figure is a bound rather than an estimate.

### The two conditions, both discovered rather than designed

1. **Merge policy** bounds lookup latency (S29, S30). Segments left unbounded reproduce the 1.88x per
   decade curve S28 first saw. Held at one per shard, the same decade costs 1.05x.
2. **Refresh interval** bounds wildcard staleness (H18). Exact names are realtime because a get by id reads
   the translog; wildcards are searches and see refreshed segments only.

Both are deployment parameters that can be got wrong in a way that looks like the architecture failing.
They belong in the contract, not in a tuning guide.

### What remains genuinely open

**The serverless-only precondition**, which is a product decision about what the system promises rather
than a question the code can answer.

Everything else that was open at the fourth review is closed. The mechanisms exist, are tested and are
mutation tested. Every enumeration found on a request path is converted or pinned with a named fix. Both
cost curves have been measured across a decade and one of them validated a decade beyond that.

### The caveat that stays attached

Four points across one decade, projected across two more, is arithmetic on a measured slope. A run at a
hundred million would test whether the slope holds, and nothing here substitutes for it. What this has
that no earlier extrapolation in this project had is a validation point outside the measured range, and a
slope that flattens rather than steepens as population grows.

## Sixth review: the machinery is connected, and that is a different claim from before

The five earlier reviews all reported Area H's mechanisms as done. They were, in the sense that each was
built, tested and mutation tested. None of them was reachable from a node that had started. That gap is
what the W series closed, and finding it changes how the earlier reviews should be read rather than adding
to them.

### What was actually true before this cycle

Zero production registrations for `AbsentIndexDescriptorSuppliers`, `IndexDescriptorPublisher`,
`MappingGenerationStore` and `GatedMappingStatsAggregator`. `MappingRefreshOnDemand` constructed nowhere.
The suspension registry never installed. No descriptor index in existence, so every measurement in the area
had been reading from a fixture the tests built themselves. And `DescriptorOnlyCreation.register` never
called, so no index was ever gated and the whole apparatus served a population of zero.

### What the wiring produced that isolated testing could not

Four defects that existed only once the seams were connected:

1. **Self-reference recursion.** The descriptor supplier makes the store's own reads resolve through the
   supplier that reads the store. It closes only when the descriptor index is absent from cluster state,
   which is a fresh cluster. `StackOverflowError` on the first resolution miss. Found by a control test,
   not the feature test, which had already created a descriptor and so never reached the supplier.
2. **Cluster-state-thread deadlock.** The publish hook runs inside `Metadata` construction, so a blocking
   descriptor write waits on an index operation that needs a cluster state. The suite hung for ten minutes
   rather than failing.
3. **Aggregation tension.** Not indexing user field names is necessary, since they are unbounded, and it
   makes mappings unaggregatable, which cluster stats needs. Resolved with a bounded `{type, count}`
   projection, because field types are a fixed vocabulary.
4. **The stamping point that did not exist.** H6c's lazy refresh assumed a request carries the generation
   it expects. Nothing does. The replacement, triggering on a mapping miss, is cheaper than the original.

### The pattern worth carrying forward

Every cycle's real output was a corrected assumption, not the code. H6c assumed a stamping point. W10
proposed a reconciliation that H4b had already rejected three lines above the code being extended. S27's
flat curve did not survive the next decade, and S28's growth turned out to be merge policy rather than
population. A test labelled "pre-existing flakiness" was a locale bug.

The discipline that caught each of them was the same: write the control, count rather than assert, mutate
the change and check the test dies, and re-read the decision next to the gap before proposing to fix it.

### Where the goal stands

100M is answered by extrapolation from a measured and independently checked curve, not by a run at scale:
creation flat at ~26,500/sec, merged lookup projecting to 0.35 to 0.48 ms with the 10M point falling below
the projection. Two operational parameters are load-bearing and were discovered rather than designed, merge
policy and refresh interval.

What remains is a run at a hundred million on real hardware, and the serverless-only precondition, which is
a product decision rather than a measurement. Neither is closable by more work of this kind, and this time
that statement is made with every seam connected rather than with a set of unreachable mechanisms behind
it.

## Seventh review: the substrate under all six reviews was replaced, and none of them says so

Written 2026-08-15 by grounding this document against the tree rather than against its own task list, which
is the check the first six reviews did not do. Every one of them is internally sound and reasons about
machinery that has since been deleted. **Nothing below revises a conclusion by argument. It records that the
thing the conclusion was measured against no longer exists.**

The reviews are left standing as written, because this project's practice is to preserve superseded
reasoning rather than erase it. What follows is what a reader has to know before quoting any of it.

### The descriptor system index was deleted, and it is what S24 through S30 measured

`46cfb963519` (2026-08-05) removed the descriptor system index. The descriptor plane is now object storage
on both halves: a point read is a GET on `descriptors/`, a create is a conditional PUT, and a prefix is one
bounded `listBlobsByPrefixInSortedOrder` capped at 100 (`DescriptorGate.DEFAULT_WILDCARD_EXPANSION_LIMIT`),
over which `UnsupportedWildcardException` refuses rather than truncating. `BlobDescriptorBackend` and
`DescriptorEnumerator` are the implementations; `DescriptorStore` and the `descriptor.backend` setting are
gone, so an object store is a hard requirement of the plugin rather than an option.

Everything the fifth and sixth reviews rest on was measured against the index that was removed:

| figure | quoted above as | what it measured |
|---|---|---|
| lookup 0.35 to 0.48 ms projected at 100M (S30) | the load-bearing extrapolation | a term-dictionary lookup in an OpenSearch index |
| 1.05x per decade at bounded segment count (S29) | the reason lookup is "cleared conditionally" | merge policy on that index |
| paging by sorted name, ~33 ms/thousand (S24) | what settled pagination | `search_after` on that index |
| wildcards refresh-bound (H18) | one of two load-bearing operational parameters | get-vs-search on that index |

**So both "operational parameters that are now load-bearing" are no longer parameters of the shipped
system.** Merge policy bounds nothing here: there is no descriptor index to merge. Refresh interval bounds
nothing here: a wildcard is a LIST, not a search. `DescriptorFreshnessContractIT` still passes, and reading
it shows why that is not reassurance -- it creates an ordinary index literally named `descriptors` and
measures OpenSearch's own get-versus-search semantics. It is a true statement about an index, and no longer
a statement about this design.

What replaces them is not measured. Object-store list consistency is now what bounds wildcard freshness, and
nothing in this repository has measured it, because nothing in this repository has run against a real object
store over a network (`ServerlessStorageS3FixtureIT` uses the in-process fixture; `LatencyProfile` is a
simulation).

### The creation figure has been corrected four times, and this document quotes the first one

The sixth review says "creation flat at ~26,500/sec". That number, and S26's 20,577/sec, were both refuted
inside `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md` before the review that quotes them was a week old.
The chain, all of it recorded there:

| | figure | what was wrong with it |
|---|---|---|
| S26 / S30 | 20,577 to 26,500/sec | never called `prepareCreate`; measured descriptor document writes, not creation (S31) |
| S31 | 235/sec | right operation, unstated conditions -- fixed at five requests in flight (S34) |
| S34 | ~550/sec asymptote | measured under jacoco and the security manager, neither of which ships (S35) |
| S35 | 859/sec, agents off | the last of this lineage, and the first on a JVM resembling production |

**And S35 is not the current answer either, because a second lineage overtook it.** S26 through S35 all
measured the descriptor-index era. `rfc-100m-index-architecture.md` carries a later set taken after the blob
switch, and its finding is sharper than a throughput number:

| | figure |
|---|---|
| gated creation, **no** declared mapping | 6,011 to 10,505/sec |
| gated creation, **with** a declared mapping | **275/sec** |

The gap is a `synchronized` block. The fast path that produces the headline rate skips building a throwaway
`IndexService` inside `IndicesService.createIndexService`, and it declines on any non-empty mapping. Once
T11/T13/T15 made a mapped gated index the ordinary kind, the fast path and the ordinary case became mutually
exclusive. **So the figure that describes the population anyone would actually create is 275 per second, and
100M of them is about four days.** Filed as T21; the direction of the fix is validating against a mapper
service built outside the lock, since the lock cannot move without changing concurrency for ordinary indices
and R1 forbids that.

The honest summary is not "creation is X". It is that creation throughput has been measured six times, the
answer has moved by two orders of magnitude in both directions, and the two things that decide it are whether
a mapping is declared and whether the profiler's own agents are loaded. Any number quoted without both
conditions attached will be wrong again within a cycle. What this document says -- flat, 70 minutes, no
ceiling -- is wrong on all three counts.

S35 also leaves an open contradiction worth carrying: S32 found removing the cluster state queue changed
nothing, while S35's profile finds the single cluster-manager task thread is the busiest thing in the system.
Both can hold if S32's null result was taken below the knee S34 found near fifty in flight. Retesting that at
saturation is the next experiment, and it decides whether batching creations is worth building.

### H17's pagination merge was reverted, deliberately

The second review reports pagination closed by folding gated indices in as a merge of two sorted runs.
`b178ef673aa` (2026-08-05) removed it, with reasons worth reading in the commit: honouring
`(creationDate, name)` in both directions from a bucket needs two more keyspaces kept consistent on every
create and delete, to support a cursor walk of a hundred million that nobody completes. `IndexPaginationStrategy`
is byte-identical to upstream again, which removes a modified core file rather than adding one.

The consequence is that **gated indices are not in paginated listings and are not meant to be**. Inventory at
this scale belongs in `DescriptorEnumerator`'s parallel prefix listing or an object-store inventory report,
not on a request path. This is a good decision recorded in the right place and contradicted by this document.

### Area A is dead, not pending

`c8cf356c2b9` (2026-08-03, S5) deleted the name index. There is no `nameindex` package. Part 0 already reads
Area A as "H's substrate", and that is now literally true: the descriptor keyspace is the whole of it.
`plan-area-a-name-index.md` describes work that will not be done and now says so at the top.

### One thing the reviews claimed as closed that had come undone again

H20's mapping stats aggregate stopped working on 2026-08-08 and nobody noticed until this pass.
`894ac942432` moved mappings into the descriptor -- correctly -- and registered `DescriptorBackedMappingStore`
in place of the store the plugin builds, leaving `IndexBackedMappingStatsAggregator` reading
`.opensearch-index-mappings`, an index whose only writer had just been unregistered. The aggregation failed,
was swallowed at debug, and `_cluster/stats` reported the ordinary population's field types as the whole
cluster's. Eleventh instance of this area's signature failure and, again, the least visible kind.

Two things about how it survived are the useful part. `DescriptorGate.install` kept accepting the mapping
store as a parameter and stopped reading it, so the plugin assembled a store and a cluster-state listener and
discarded both -- detectable by asking which arguments a method never reads, which is now how it was found.
And every test of the aggregate hand-built its own `IndexBackedMappingStore`, so the whole suite stayed green
against a store production did not register. That is the same defect the descriptor-index deletion commit
called out in the descriptor tests three days earlier, repeated in the mapping tests.

Fixed by `StatsProjectingMappingStore`: the descriptor stays authoritative and the mapping index becomes a
write-behind projection that exists only to keep the aggregate one search. The shared IT fixture now composes
the store the way the plugin does, and `GatedMappingStatsIT` writes through `MappingGenerationStore` rather
than around it, so the next version of this fails red.

### What the same sweep found next door, pinned rather than fixed

Asking "which arguments does this method never read?" over every production file this branch changed returns
21 hits once record constructors and subclass hooks are excluded, and most of the rest are upstream base-class
signatures. One is not.

**`MetadataUpdateSettingsService.updateGatedSettings` never reads its request.** The caller validates the
submitted settings -- refresh interval, translog durability, the scoped-settings pass -- and then calls a
method that resolves each gated index's descriptor, re-publishes it unchanged via
`IndexDescriptorPublisher.updateGated`, and answers `acknowledged: true`. `request.settings()` is never
touched. So `PUT /gated-index/_settings` validates the caller's settings, applies none of them, and reports
success.

The commit that added it (`d90641f1f61`, 2026-08-08) is titled "Support dynamic settings updates off cluster
state thread for gated indices". It moved the work off the cluster state thread, which it did do, and did not
implement the update.

**Why it cannot simply be fixed here.** `IndexDescriptor`'s own javadoc says settings are deliberately absent
-- they were to live in the object store under a convention. Mappings were later moved into the descriptor
(T58) and settings were not, so there is currently nowhere to put them. That makes this a design question
rather than a patch, with two honest answers:

- **Refuse.** Reject a settings update on a gated index with a clear error, the way C14 and C28 refuse
  resharding and scaling and the way `DescriptorRepresentable` refuses to gate an aliased index. Cheap,
  immediate, and strictly better than acknowledging a no-op.
- **Carry settings on the descriptor**, as mappings now are, with the same CAS-on-generation shape.

The first is the right immediate move regardless, because the current behaviour is the one thing neither
answer wants: a silent success. It is deliberately not changed here, because turning a success into a failure
is a user-visible contract change and belongs to whoever owns the contract, not to the sweep that found it.

### The suites are not green, and that was not known

The full plugin integration suite has now been run end to end, which does not appear to have happened before
in this project's records. `:plugins:serverless-storage:internalClusterTest`, 28 minutes:

```
87 classes, 219 tests, 5 skipped, 8 failures
```

Every one of the eight was checked against an unmodified tree rather than assumed, because the run was made
with the mapping-stats fix in place and a failure list is worthless without that separation.

**Six are pre-existing and deterministic**, and several are the T43/T44/T48 guards written specifically to
keep the mapping path honest:

```
BlobBackedDescriptorIT              > testTheConfiguredMappingIndexShardCountIsUsed
BlobBackedDescriptorIT              > testOperationsAGatedIndexCannotSupportFailClearly
GatedCreateTimeMappingIT            > testAGatedCreationDoesNotReadAMappingThatCannotExist
GatedMappingIndexLossIT             > testAMappingWriteAfterTheMappingIndexIsDeletedFailsRatherThanRewriting
GatedMappingMissingWindowIT         > testAReadFailsInTheWindowAndSucceedsOnceResolutionCatchesUp
GatedMappingOffClusterStateThreadIT > testNeitherCreationNorPutMappingTouchesTheStoreFromAClusterStateThread
```

They cluster in one place, and the place is the one T58 changed: these are the tests of what a mapping store
must do when it cannot read, when its index is lost, and when it must stay off the cluster state thread.
Moving mappings into the descriptor moved the behaviour those guards describe without moving the guards.
That is the same root as the stats regression, seen from a third side.

**Two are flaky rather than broken**, and both were confirmed by repetition rather than by argument:

- `ServerlessStorageAffinityForwardingIT` fails on an unmodified tree too, and worse there --
  `testMixedGatedAndOrdinaryIndicesNeverForwards` and `testRequestAlreadyOnTheAffinityNodeIsNotForwarded`
  both fail on baseline against one of them with the fix in place.
- `ServerlessStoragePreWarmChaosIT#testClusterSurvivesANodeDyingDuringGatedPreWarmDispatch`, G4's own chaos
  test, fails about two runs in five in isolation. It kills a randomly chosen data node, so which shards go
  with it varies by seed, and G4's own text already records that a gated shard's primary can take minutes to
  be re-derived after its host dies. This is that finding surfacing as an intermittent test rather than a new
  defect.

`GatedIdleEvictionIT#testResidencyIsBoundedByArrivalRateRatherThanByPopulation` is flaky too, at about one
run in two in isolation, on a leftover-index assertion at teardown rather than in its own body.

The unit suites are green -- `:plugins:serverless-storage:test` at 236 classes / 1,275 tests and the core
seam tests at 50 classes / 2,416 tests, both zero failures -- which is what has kept this looking healthy.
The per-change discipline the recent commits describe is real, and it is per-change: it does not catch a test
that a different change broke three commits ago.

`spotlessCheck` also fails on an unmodified tree, on six files from the last week's commits.

**Triaging the six is the highest-value validation work available**, ahead of any new measurement, because
until they pass a green run proves nothing about what they cover -- and what they cover is the mapping path
that has now produced two silent defects in eight days.

### The six, triaged and fixed the same day

Five were tests still demanding the contract T58 replaced. Rewriting a failing test to match the code is
usually how a suite stops meaning anything, so each one was re-pointed at the property the old assertion was
protecting rather than deleted: a creation that must not read a mapping it cannot have now asserts it does
not touch the store at all; a lost mapping index now asserts the mapping survives it, because since T58 that
index is a stats projection and losing it costs a statistic; the threading proof now records the descriptor
backend, because the creation path stopped reaching the mapping store and a phase that records nothing
passes every thread check there is.

The sixth was not a test problem. `MappingGenerationStore.currentMapping(uuid, expected)` -- T59's guard
against reporting an index with declared fields as one with none -- checked only for a **null** answer. That
was exhaustive against the index-backed store, which either had a document or did not. A descriptor-backed
store answers for every index that resolves, so a missing mapping comes back as generation 0 with no fields:
the same wrong answer as a value instead of a null, past a guard written for the reference. Stated over the
generation now. **This is the fourth mechanism in this area found correct, tested, and unreachable**, and the
first one where the unreachability was introduced by a change (T58) rather than by never having been wired.
That is worth naming as its own failure mode: a guard does not have to be deleted to stop guarding -- it only
has to keep checking a condition the system no longer produces.

A seventh failure appeared once those six were fixed, and it was the fixture: the drain that waits for
write-behind projections only knew about stores the fixture itself built, so the classes that let the plugin
wire itself tore their cluster down with a projection still running ("shard is still locked", in a class with
nothing to do with mappings). `DescriptorGate` keeps the installed store so it can be drained -- in
`uninstall()` for the node-close case its own javadoc had claimed and never wired, and from the fixture for
the test one.

The three flaky classes all passed in that run, which is what intermittent failures do and settles nothing.
A second full run put two of them back, and both had been mislabelled:

**`GatedIdleEvictionIT` was a real race in core.** `removeIndices` walks the index map as it stood when the
loop began; idle eviction closes gated indices from a thread that deliberately does not hold the applier's
monitor, removing the index from the map first and clearing its `openedOnDemand` entry after. An eviction
landing mid-iteration therefore leaves an index that the loop can still see, that nothing claims, and that is
already gone -- and the loop's remaining checks read "absent from cluster state" as "the cluster manager
deleted it", ending at an assertion an unpublished index can never satisfy. The guard meant to catch this
asks `DescriptorOnlyCreation`, a registration that disappears when a node's gate uninstalls while the indices
it opened are still resident, which is why it surfaced at teardown and looked like a test artefact. The loop
now skips an index that is no longer in the map. Six consecutive green runs against one failure in three
before.

**The affinity one was one missing line.** Its symptom -- a write to a freshly created gated index never
becoming servable within sixty seconds -- reads like a deep property of on-demand shard opening, and was
`ServerlessStorageAffinityForwardingIT` failing to disable the framework's mock engine. This plugin supplies
an engine factory, `IndicesService.getEngineFactory` refuses when two plugins claim one index, and on the
seeds where the randomizer installs the mock one -- about one run in three -- no gated index can open on
that node. Gating turns that from a failure into a hang: there is no cluster state entry to tell the write
it will never be servable, so it retries until the suite times out. Seventy-one classes had the override and
three did not; it is on the shared base class now.

The third, G4's chaos test, stopped reproducing without being touched -- five green runs against two
failures in five before -- and is not claimed as fixed. It kills nodes while gated indices are open on
demand, which is the same window the eviction race lives in, so the fix above could account for it; nothing
here shows that it does.

**Worth noting as a pattern rather than as three fixes.** Two of the three were labelled flaky and neither
was: each had a single deterministic cause reached by a seed-dependent path. "Flaky" was the label that
stopped the investigation, and in both cases it was hiding a real defect -- one of them in core.

**The suite is green end to end for the first time**: 87 classes, 219 tests, 5 skipped, 0 failures, 7m 39s,
alongside `:plugins:serverless-storage:test` at 237/1,277 and the core seam tests at 85/1,069, with
`spotlessCheck` passing.

### Where the goal actually stands, seventh pass

| ceiling | state |
|---|---|
| placement | cleared by Area C. Unaffected by any of the above. |
| residency | cleared. Gated creations leave zero cluster state bytes, measured in bytes rather than versions (`GatedCreationClusterStateFootprintIT`), and that claim does not depend on the descriptor's storage medium. |
| throughput, creation | **275/sec for a mapped index**, the ordinary case, so about four days for 100M. 6,011 to 10,505/sec only for the unmapped population nobody creates. Bounded by a `synchronized` block the fast path avoids by declining mappings (T21). |
| throughput, lookup | **unmeasured on the shipped path.** Every figure quoted for it measured the deleted index. |

### What is actually left

1. **Re-measure lookup against the blob descriptor plane.** This is the load-bearing extrapolation for the
   whole design and it currently has no measurement behind it at all. Highest value item on this list by a
   wide margin.
2. **Measure against a real object store over a network.** Named as open since the delivery gap analysis was
   written and still open. Every latency claim depends on it, and the design's answer to "the descriptor
   index got slower" is now "the cache hides the object store", which is precisely what is unmeasured.
3. **Settle the wildcard freshness contract for a LIST rather than for a search.** H18's answer was correct
   for the index and does not transfer.
4. **The S32 retest at saturation**, which decides whether creation batching is worth building.
5. **A run at a hundred million**, still, and still a fleet exercise.
6. **The serverless-only precondition**, still the one genuine product decision.

### The lesson, which is the same one this project keeps finding one level up

The sixth review's own closing paragraph says every cycle's real output was a corrected assumption rather
than code, and lists four. It could not list the largest one, which was running underneath it: the reviews
were auditing the task list against the plan, and the plan against itself. Grounding either against the tree
is a different operation, and it is the one that finds a document arguing from deleted machinery.

`docs/rounds/STATE.md` already carries a correction of exactly this shape, dated 2026-08-12, about its own
task numbers. It ends with an instruction that generalises: re-run the check yourself and do not propagate
the table forward again without it. That instruction belongs at the top of this document too.

