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
- A7.2 Request paths: `TransportAnalyzeAction:142`, `TransportGetFieldMappingsIndexAction:115`,
  `TransportUpdateAction:214`, `TransportUpgradeAction:202`. All should report no shard available
  rather than NPE.
- A7.3 Snapshot paths: `SnapshotsService:3443`, `:3511`. Needs a decision, not just a guard: is a cold
  index snapshotted from its remote state, or skipped?
- A7.4 Resize, merge and allocation: `MetadataCreateIndexService:1991`, `DiskThresholdDecider:668`,
  `MetadataInPlaceMergeShardService:232`, `MetadataInPlaceSplitShardService:193`. Should reject, not
  NPE. Lower priority; a cold index probably should not reach them.
- A7.5 `LocalShardStateAction:53`, clusterless mode only. Confirm absence is possible before touching.

### A2. `OperationRouting.indexRoutingTable` degrades instead of throwing

Currently throws `IndexNotFoundException` when the routing entry is missing, turning a search that
would return HTTP 200 with per-shard `NoShardAvailableActionException` into a 404.

- Change it to treat absence as no shards to route to.
- Test: metadata has the index, routing does not, search returns per-shard failures rather than 404.
- Confirm the test fails with the change reverted.
- Watch for callers that *rely* on the exception to detect a missing index. A1 should have found them.

### A3. `RoutingTable.shardRoutingTable(ShardId)` degrades instead of throwing

`TransportReplicationAction.ReroutePhase` and `TransportBulkAction` should take their existing
retry-and-wait-for-allocation branch rather than failing immediately.

- Decide the shape first: return null, return empty, or add a separate `shardRoutingTableOrNull`. The
  third avoids changing an existing contract and is probably right, with callers migrated one at a
  time.
- Test: a bulk request against a metadata-only index retries rather than failing.
- Confirm the test fails with the change reverted.

### A4. Spike: how does an index become routing-absent?

Do not implement yet. Establish the mechanism:

- Does the serverless plugin's existing `ShardSuspensionCoordinator` / scale-to-zero already produce
  this state, so core only has to tolerate it? If so, A5 is mostly deletion of assumptions.
- Otherwise: what marks an index as cold, and who removes its `IndexRoutingTable`? Candidates are a
  cluster-state flag read by `RoutingTable.Builder`, or the allocator skipping it.
- What brings it back, and what is the latency of the first write to a cold index?

Output: a short design with one recommended mechanism, in this file or its own RFC.

### A5. Implement the mechanism chosen in A4

Gated on A1 through A4. Size unknown until A4 lands.

### A6. Measure

Build a cluster state with N routing-absent indices and M active ones. Confirm reroute cost scales
with M and not with N. Compare against the S6 baseline at 40k shards so the numbers are comparable.

---

# Phase B. The allocator ceiling

**Why.** S6 measured it as the binding constraint and the plan calls it "a bigger question than
anything else here". Phase A reduces what the allocator sees; this asks whether the allocator itself
can be made to scale. It is research first. Do not start with an implementation task.

### B1. Profile reroute at increasing active-shard counts

Extend the S6 harness to 40k, 100k and 200k active shards, assertions disabled. Break the time down by
phase: `RoutingNodes` construction, decider evaluation, balancer iterations. S6 established the
balancer dominates; get the curve, not one point.

### B2. Can allocation be domain-scoped?

`RoutingPool` is the existing precedent: LOCAL_ONLY and REMOTE_CAPABLE are balanced separately, and it
works because the node sets are disjoint. The question is whether tenant cells can be disjoint the
same way, and what breaks if they are not (shard movement across domains, cluster-wide constraints
like total shards per node, disk watermarks).

Output: a verdict with the specific constraints that are cluster-wide and therefore cannot be scoped.

### B3. Can the balancer be incremental?

The `balanceByWeights` weight-spread skip that shipped is a cheap version of this: skip indices that
provably cannot move. Ask whether the balancer can maintain weights across reroutes rather than
recomputing, and what invalidates them.

### B4. Write up and recommend

One of: a scoped design worth implementing, or a statement that the ceiling is structural and cells
are the answer. Either is a useful outcome. Do not manufacture an implementation task if B2 and B3 say
no.

---

# Phase C. Manifest sharding

**Why.** The manifest is rewritten whole on every cluster state version. At 100k indices that is about
0.5 MB compressed before C6's descriptor and about 0.8 MB after, and both scale linearly with index
count. This decides whether C5 and C6 are usable at that scale.

**Already audited.** See the C6 section of `rfc-scalable-index-metadata-plan.md`. Two findings carry
into these tasks: the manifest genuinely does list all N indices every version, and
`RemoteClusterStateCleanupManager` computes `filesToKeep` as a union over retained manifests, so
sharding must teach it to resolve index blob names transitively through shard blobs.

### C1. Extend cleanup for transitive reachability, before anything writes shards

Deliberately first. The delete path must be safe before any shard blob exists, not after.

- `deleteClusterMetadata` adds shard blob names to `filesToKeep`, and fetches each shard to add the
  index blob names it references.
- Test: a sweep with a retained manifest whose indices live in shard blobs must not delete those index
  blobs. Confirm it fails without the transitive pass, which is the corruption this prevents.

### C2. Design the shard scheme

Partition function (hash of index name is the obvious choice), shard count and how it changes, blob
naming, and how the top-level manifest references shards. Decide whether shard count is fixed or
scales with index count, and what happens when it changes.

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

### E1. C2 mapping dedup: decide against it, or justify it

S4 measured 1000x on identical fleets. S3 showed it does nothing for the parsed `MapperService` graph
(0.08%), and the ceiling recalibration puts it at tens of MB per node at reachable scale rather than
tens of GB. The correct implementation is a wire-format change with BWC cost. Record the decision so
the 1000x figure does not resurface as justification.

### E2. C4 incremental routing rebuild: maintainer call

Attempted and reverted. The finding generalizes: `RoutingChangesObserver` observes allocation changes,
but routing tables also change outside allocation, so a change set derived from it is not a sound
over-approximation. A correct version needs a change signal from every producer of routing tables. The
payoff is roughly 35 to 67 ms of a 300 ms steady-state reroute at 40k shards, where the balancer
dominates. This wants a maintainer's judgement rather than another unilateral attempt.

### E3. S1 sites 1 and 2, if Phase A does not happen

If cold-absent routing is not pursued, close A2 and A3 explicitly as "not doing" rather than leaving
them open. They are only correct in service of that property, and shipping them alone changes live
search and write behaviour for no benefit.

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
