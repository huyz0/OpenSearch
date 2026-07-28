# Implementation plan: 100M indices with index/search and compute/storage separation

Status: in progress. Evidence recorded in `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md` (S1 to S19) and
`rfc-100m-index-architecture.md`.

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

**D2. Scale hysteresis.** Reuse `SustainedCandidateTracker` rather than inventing a third scaling policy.
A brief traffic spike must not reshuffle affinity. Set the sustained window from the measured pre-warm
duration, not from a guess.

**D3. Prefer-warm among K.** When candidates are equal by ARS, prefer one that held the shard before the
last membership change. This is what converts the 0% single-join figure from a theoretical property into
an actual one.

**D4. ARS sufficiency check.** The plan assumes ARS's latency signal proxies cache warmth adequately, on
the reasoning that a cold node reads as a slow node. Test it. If it oscillates, blend in
`ReaderCacheAffinityRecorder`'s direct signal.

**D5. Incremental scaling policy.** Prefer adding one node at a time over large jumps, given the measured
difference (0% against 12.4%). Encode this as policy, not as documentation.

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

**E7. Align shard function with the routing hash.** If the manifest shard function matches the coordinator
hash, a coordinator warms its entire partition in one read. This is a free win from two independent
designs lining up, and needs to be built deliberately rather than discovered.

**E8. Re-measure.** C3a's 256-shard figure came from a simulation, not the implementation. Re-run against
the real write path.

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

**G4. Chaos.** Node loss during pre-warm, LB and coordinator disagreement about the node set, and a
partitioned name index tier. N3 claims graceful degradation and that claim needs evidence.

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

