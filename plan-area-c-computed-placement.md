# Area C task plan: computed placement

Parent: `plan-100m-index-implementation.md` Part 5, Area C. Evidence: S6 (allocator ceiling) and S12
(rendezvous placement measured at theoretical optimum) in `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md`.

## Why this area

S6 measured the allocator binding at the order of 100k active shards, with cold allocation superlinear.
The target is 100M indices at up to 100 shards each, so up to 10B shards. Five orders of magnitude is not
a tuning problem, so the global pass has to stop happening.

S12 measured the replacement: rendezvous hashing with K=3 candidates moves exactly `added/(N+added)` of
primaries at every fleet size, distributes within 5% of even, and costs 80 to 359 ns per lookup. The
design risk is retired. What is left is integration, and integration is where the risk now lives.

## C0: surface audit, done first because the plan depends on it

Counted in `server/src/main/java`:

| surface | count |
|---|---|
| files that **write** a `RoutingTable` | 20 |
| files that **read** `routingTable()` | 70 |
| files that use `RoutingNodes` | 26 |
| files touching shard state transitions | 8 |

The writers, grouped, because the grouping is what decides the approach:

- **Index lifecycle**: `MetadataCreateIndexService`, `MetadataDeleteIndexService`,
  `MetadataIndexStateService`, `MetadataUpdateSettingsService`.
- **Allocation**: `AllocationService`. The one that matters.
- **Resharding**: `MetadataInPlaceSplitShardService`, `MetadataInPlaceSplitShardCommitService`,
  `MetadataInPlaceMergeShardService`, `MetadataInPlaceMergeShardCommitService`.
- **Recovery and restore**: `RestoreService`, `RemoteStoreRestoreService`, `LocalAllocateDangledIndices`,
  `ClusterStateUpdaters`.
- **Shard state**: `LocalShardStateAction`.
- **Other**: `ScaleIndexClusterStateBuilder`, `TieringService`, `TransportClusterStateAction` (filtering),
  `ClusterManagerService`, `NoopRemoteRoutingTableService`, `OperationRouting` (the A2 degradation path).

**What the audit changes about the approach.** 70 readers against 20 writers means replacing the
*representation* is far cheaper than replacing the *interface*. Keep `RoutingTable` and
`IndexRoutingTable` as the types every reader already uses, and change where an entry comes from. That is
the same trade C5 made for `IndexMetadata`, and it is why the holder pattern is the right precedent
rather than a convenient one.

**The transition observers are concentrated, which is the good news.** `initializeShard`, `startShard` and
`relocateShard` are reached from `RoutingNodes`, `AllocationService`, the two balancers and
`MoveAllocationCommand` -- all allocator-internal, none of which run for a serverless index under computed
placement. Only `SnapshotsService` and `IndexService` observe transitions from outside the allocator, and
those two need reading closely rather than assuming.

## Tasks

Each is done when it compiles, has tests, **each test has been shown to fail without its production
change**, and the affected suites pass.

### Phase 1: the placement function

**C1. `RendezvousShardPlacement`.** Promote the algorithm from `ComputedPlacementSpike` into production.
- Input: an ordered node list and `(indexUuid, shardId)`. Output: K ordered candidates.
- The hash must be pinned and a test must assert its output does not change, because two coordinators
  disagreeing about placement is a split view of the cluster, not a performance problem.
- Keep the spike's partial-selection loop rather than sorting; N is in the hundreds and K is small.

**C2. Node set snapshot.** Define "eligible" (role, health, not draining) and where the snapshot comes
from. Two coordinators computing against different node lists produce different placement, so this is a
correctness input, not a convenience.

### Phase 2: the seam

**C3. `IndexRoutingTableHolder`.** Mirror C5 exactly: an interface the concrete type implements, so no
existing caller changes, plus a lazy implementation that computes on first access per index.

**C4. Computed `IndexRoutingTable`.** Build shard routing entries from C1 rather than from allocator
output. Writers get top-1 as primary, search nodes get the rest as search-only replicas.

**The known trap, from A5.** `addAsRecovery` picks its recovery source from `inSyncAllocationIds`, and
under computed placement those are absent, so it silently falls back to `EmptyStoreRecoverySource` and
resurrects a live index as blank. Build the entry explicitly with `ExistingStoreRecoverySource`, and write
the test that fails when it is not.

**C5. Gate on index type.** Only serverless indices take this path; everything else keeps the allocator.
A setting, defaulted off, exactly as C5 and C6 shipped.

**C6. Bypass the allocator.** A serverless index must not enter `AllocationService.reroute` at all. Check
what `SuspendedShardAllocationDecider` and `ServerlessStorageExistingShardsAllocator` still need to do and
remove what becomes dead.

### Phase 3: the request paths

**C7. Search.** Feed the K candidates to adaptive replica selection instead of the routing table's replica
list. Verify `OperationRouting` accepts hash-derived candidates unmodified -- this is the assumption the
area rests on and it has not been checked.

**C8. Write.** Top-1 as a hint, `ShardHead` CAS decides. A writer that is not the lease holder must fail
cleanly and the request retry against the new owner.

### Phase 4: what the audit says needs reading rather than assuming

**C9. `SnapshotsService` and `IndexService` transition observers.** The only two outside the allocator.
Read them, decide what a computed-placement index means for each, and test it.

**C10. The other nineteen writers.** For each, decide: no-op for serverless, unchanged, or needs work.
Resharding is the group most likely to need real work, since split and merge manipulate routing directly.

### Phase 5: measurement

**C11. Re-measure the ceiling.** S6's numbers were taken against the allocator. Re-run the same harness
with computed placement on and confirm cold allocation becomes flat rather than superlinear. This is the
claim the whole area exists to make, and it should be a number rather than an argument.

## Acceptance

- A cluster with serverless indices performs no global reroute.
- Placement for a given shard is identical on every coordinator.
- A search request reaches one of the K candidates.
- A writer that is not the lease holder is rejected rather than permitted to write.
- Recovery source is never `EmptyStoreRecoverySource` for an index with data, asserted directly.
- Cold-start time is flat in index count, measured.

## Risks

- **Blast radius.** 70 readers. Keeping the interface is what makes this tractable, and any change that
  forces readers to adapt should be treated as a design failure rather than as work.
- **The A5 trap.** It has bitten once, silently, and this area recreates the exact condition.
- **Hot-tenant skew.** A hash cannot know one tenant takes a thousand times the traffic. K=3 gives room to
  choose but does not solve it. An override list for known-hot indices may be needed.

## C9 answered: neither observer is a problem, and one was not an observer

The audit flagged `SnapshotsService` and `IndexService` as the only two places outside the allocator
that appeared to watch shard state transitions. Reading them:

**`IndexService` was a false positive.** The grep matched `startShardLevelRefreshTasks`, which names a
refresh scheduler and has nothing to do with `ShardRouting` state. There is no transition observer there
at all.

**`SnapshotsService` reads current state rather than watching a transition.** It asks
`primaryShard().started()`, `initializing()`, `relocating()` and `unassigned()` to decide what a
snapshot should record for a shard. Under computed placement a shard is STARTED whenever the cluster has
data nodes, so it takes the `started()` branch immediately and records the computed node.

That is not merely tolerable, it is better than the allocator case: today the snapshot has to wait
through INITIALIZING, and under computed placement there is nothing to wait for. The
`initializing() || relocating()` branch simply never fires, and the `unassigned()` path already exists
for the no-data-nodes window that `ComputedRoutingTable` produces.

**No change required.** The risk the audit raised does not survive reading the code, which is the
outcome an audit is supposed to be allowed to have.

## Status after this pass

| task | state |
|---|---|
| C0 surface audit | done |
| C1 `RendezvousShardPlacement` | done, 12 tests |
| C2 eligible node snapshot | done |
| C4 `ComputedRoutingTable` | done, 11 tests |
| C9 transition observers | done, no change required |
| C3 holder seam in `RoutingTable` | **not started, and larger than it looked** |
| C5 gate on index type | not started |
| C6 bypass the allocator | not started |
| C7 search path, C8 write path | not started |
| C10 the other nineteen writers | not started |
| C11 re-measure the ceiling | blocked on C3 |

### C3 is bigger than the plan assumed, and that is worth recording

The plan said "mirror C5 exactly". C5 changed `Metadata`'s internal map to holders, and the same move on
`RoutingTable` means changing `Map<String, IndexRoutingTable> indicesRouting` to hold suppliers. The
difference is what that map is entangled with: 38 internal references, and among them
`RoutingTableDiff`, `writeTo`, `writeVerifiableTo` and `DiffableUtils.diff`. `Metadata`'s holder change
was contained because the holder resolved before serialization; `RoutingTable` is diffed and serialized
on every cluster state publication, so a lazy entry has to decide what a diff of an unresolved entry
means.

That is a real design question rather than a mechanical port, and it is the next thing to answer:
either resolve on serialize (simple, loses the saving exactly when publication happens) or teach the
diff to compare unresolved entries by their inputs (keeps the saving, needs the inputs to be part of the
state). C5 took the first option for `Metadata` and it was the right call there; whether it is here
depends on whether publication is the hot path, which S6 suggests it is.

## C7 and C8 done, and they found the seam was half-installed

C3 hooked `indexRoutingTable`, which is what `searchShards` resolves through. `ComputedRoutingResolutionTests`
covered search and then failed on the write path, because `OperationRouting.shards()` -- the single-document
route used by index, get and update -- reaches `clusterState.getRoutingTable().shardRoutingTable(...)`
directly and never touches the hooked method.

**A computed index would have answered searches and failed writes.** That is worse than not supplying at
all: the failure surfaces as a missing index rather than as an unsupported configuration, so it would be
diagnosed as data loss before anyone suspected routing. The same fallback is now applied there, verified
by mutation.

Worth naming the lesson, because C3's own commit message got this wrong. "The seam is one hook at the
point where Phase A degrades" was true of the code I had read and false of the system: Phase A degraded
in more than one place, and I only hooked the one the first test exercised. The test that found it was
written for C8, not for C3.

C8 also confirms the intended division of labour: routing hands back a single primary as a hint and
`ShardHead` CAS decides. A wrong hint costs a retry, not correctness, which is what lets placement be
computed rather than agreed.

## C6 and C10: no work required, and why

**C6, bypass the allocator.** Under the shipped design there is nothing to bypass. A serverless index
publishes no routing entry, so `AllocationService` never sees one to allocate, no decider is consulted,
and no balancer considers it. The plan assumed the allocator would need to be told to skip these indices;
in fact it is never handed them. `SuspendedShardAllocationDecider` and
`ServerlessStorageExistingShardsAllocator` remain live for indices that still publish routing, which is
every non-serverless index, so neither becomes dead code.

**C10, the other nineteen writers.** All are writers of *published* routing, and a serverless index has
none for them to write. They fall into three groups, none of which needs changing:

- Index lifecycle (`MetadataCreateIndexService`, `MetadataDeleteIndexService`, `MetadataIndexStateService`,
  `MetadataUpdateSettingsService`) and recovery/restore (`RestoreService`, `RemoteStoreRestoreService`,
  `LocalAllocateDangledIndices`, `ClusterStateUpdaters`): they add or remove published entries. A
  serverless index simply has no entry added, and the supplier fills the gap on read.
- Resharding (the four `MetadataInPlace*` services): these manipulate routing for split and merge. They
  are the group the plan expected to need real work, and they still might -- but only once resharding runs
  against a computed-placement index, which requires the setting on, which C11 has not yet justified.
  Recorded as a dependency of turning the setting on rather than as work now.
- Everything else (`ScaleIndexClusterStateBuilder`, `TieringService`, `TransportClusterStateAction`,
  `ClusterManagerService`, `NoopRemoteRoutingTableService`, `LocalShardStateAction`, `AllocationService`,
  `OperationRouting`): filtering, transport plumbing, or the allocator itself.

**The honest form of this conclusion:** the nineteen writers are fine because computed placement does not
change what gets published, it changes what happens when nothing was. That is the same property that made
C3 and C5 small, and it traces back to Phase A.

## Final gap review

Every task C0 through C11 is addressed. Re-reading the area as a whole rather than task by task, three
things stand out that the per-task work did not surface.

**1. The area shrank because Phase A did the hard part.** C3 was planned as a restructuring of
`RoutingTable`'s internal map; it became an 80-line hook. C5 was planned as keeping serverless entries
out of the allocator's path; it became a predicate. C6 and C10 were planned as real work; they turned out
to need none. All four collapsed for the same reason: a serverless index publishes no routing entry, and
Phase A already made that legal. The plan treated Phase A as a prerequisite in the scheduling sense. It
was a prerequisite in the design sense, and this area is mostly the payoff.

**2. One defect, and it came from generalising a reading of the code.** C3's commit claimed "the seam is
one hook at the point where Phase A degrades". Phase A degrades in more than one place, and the write
path was left unhooked -- so a computed index answered searches and failed writes, which reads as data
loss rather than as misconfiguration. It was caught by a test written for C8, not for C3.

**3. What is genuinely untested.** Everything here is unit-level. No cluster has run with
`serverless_storage.computed_placement.enabled` set, so:

- **The setting has never been on in a live node.** C11 measured the algorithm, not the system.
- **Resharding is unresolved rather than resolved.** The four `MetadataInPlace*` services manipulate
  routing directly, and whether split and merge work against a computed-placement index cannot be
  settled without running them.
- **Recovery has never been exercised.** `ComputedRoutingTable` builds shards already STARTED. What
  happens when a node holding a computed shard restarts, and the index has no published routing to
  recover from, is reasoned about but not observed.
- **Hot-tenant skew has no answer.** K=3 gives room to choose; nothing chooses yet, because C7 feeds the
  entry to `OperationRouting` without adaptive replica selection ranking the candidates.

## Next cycle

| task | why |
|---|---|
| **C12** integration test with the setting on | the acceptance criterion, and the only thing that can find what unit tests cannot |
| **C13** node restart with a computed index | recovery is reasoned, not observed, and A5 showed what reasoning about recovery costs |
| **C14** resharding against a computed index | the one C10 dependency left open |
| **C15** wire ARS to rank the K candidates | C7 resolves to candidates but nothing chooses among them |
| **C16** hot-tenant override | needs a product answer before it needs code |

C12 and C13 are the ones that gate turning the setting on anywhere real. C14 gates using resharding with
it. C15 and C16 are optimisation rather than correctness.

## C12 ran, and found the mechanism is still unreachable

Two findings, one from writing the test and one from running it.

**Writing it found that the supplier was never invoked.** `MetadataCreateIndexService` calls `addAsNew`
for every index it creates, and a supplier only runs when an index has *no* published routing entry. So
C3, C4 and C5 were installed and dead. Every unit test passed because each one constructed the absence
it was testing. C6's earlier conclusion -- "nothing to bypass" -- was wrong: the real content of C6 is
preventing publication, and without it nothing else in the area executes. Fixed with a second
registration, `registerUnpublished`, kept separate from the supplier so that an index skipping
publication with no supplier installed is a configuration error rather than a silent outage.

**Running it found index creation hangs.** With routing unpublished, the create API waits for active
shards and counts them from the routing table. No entry means no active shards, the wait never
satisfies, and the suite times out at twenty minutes.

**So skipping publication is necessary and not sufficient.** Everything that counts active shards needs
the same supplier fallback that resolution now has, or creation has to stop waiting for an index whose
placement is computed. That is a second seam rather than a tweak to this one.

The test is marked `@AwaitsFix` rather than deleted: a hanging test in CI is worse than none, and the
finding is worth more than the assertion.

**What this says about the area.** C11 measured the algorithm and C12 shows the algorithm is not yet
reachable in a live cluster. The measurement stands; the claim that computed placement *works* does not
yet, and no amount of unit testing was going to reveal that, because unit tests construct the state they
assert against.

## Next cycle, revised

| task | why |
|---|---|
| **C17** active-shard counting consults the supplier | the blocker C12 found; nothing works until this does |
| **C13** node restart with a computed index | still unobserved, and A5 showed the price of reasoning about recovery |
| **C14** resharding against a computed index | unchanged |
| **C15** wire ARS to rank the K candidates | unchanged |
| **C16** hot-tenant override | needs a product answer |

C17 replaces C12 at the head of the queue. C12 itself becomes the test that passes when C17 lands.

## C17 landed, and the audit behind it found a worse problem than the hang

The blocker itself was small. `ActiveShardCount.enoughShardsActive(ClusterState, indices)` is the only
place in the codebase that counts active shards from a whole cluster state, and index creation waits on
it through `ActiveShardsObserver`. It read the routing table directly, found nothing, and either asserted
or dereferenced null depending on whether assertions were on. Making it resolve through the supplier is
one line.

Two things came out of doing it that were not in the plan.

**The pairing is now one function.** `AbsentIndexRoutingSuppliers.resolve(state, indexName)` returns the
published entry or the computed one. C3 hooked resolution and left the write path unhooked because the
pairing of "look up, then maybe supply" was open-coded at each site, and the second site was written by
someone who had read the first. Three call sites later, that was going to happen again. Having one
function to call is what makes the next site hard to get wrong, and it is cheap insurance against the
one defect this area has actually produced.

**Cluster health reported GREEN for indices with no shards anywhere.** `ClusterStateHealth` skipped an
index with no routing entry, which does not throw and does not hang. It removes the index from every
counter, so the cluster looks perfect. `ensureGreen()` in the test framework is a thin wrapper over that
health response, so every integration test written for the rest of this area would have passed without
exercising anything. That is worse than the hang: a hang is a finding, a false green is a false finding.
Both health constructors now resolve, and the active-shards percentage counts computed entries rather
than dividing by an empty routing table, which was also a NaN in the health response for a cluster whose
indices are all computed.

**What the test taught, again.** The first version of the integration test asserted `assertAcked` on
creation, and it passed with the fix reverted. Creation does not hang any more once the null branch
returns false instead of asserting: it waits out the thirty-second active-shards timeout and comes back
acknowledged with `shardsAcknowledged=false`, which `assertAcked` does not look at. The clock said it too,
6.3 seconds against 33.9. The assertion had to move to `isShardsAcknowledged`. A test that passes against
the mutation is not a test, and this one only got caught because the mutation was run.

## C18: the shards do not exist

The audit that C17 started asked which code counts shards. Asking which code *creates* them gives a
worse answer.

`RoutingNodes.localRoutingNode` builds the local node's shard list by looping the published routing
table, and all seven phases of `IndicesClusterStateService` take their work list from it. A computed
index is in none of them, so no `IndexService` and no `IndexShard` is ever created, on any node. There is
no second path: recovery from disk is a `RecoverySource` on a routing entry that already came from the
table, and dangling index import publishes an entry rather than bypassing one.

So a computed index today is a routing fiction. Coordinators resolve it, health counts it, creation
completes, and every request lands on a node that was never told to open the shard. A write fails with
`ShardNotFoundException`, which is a shard-not-available exception, so `TransportReplicationAction`
retries it until the request timeout rather than failing fast.

The seam is `localRoutingNode` and deliberately not the `RoutingNodes` constructor. The constructor is
what `AllocationService` and `ClusterState#getRoutingNodes` build, so adding computed shards there hands
these indices back to the allocator, which is the one thing this area exists to avoid. The local helper
is read only by the data node's own shard lifecycle. Hooking it materializes shards locally and leaves
allocation with nothing to allocate, which is the same split that made C6 and C10 turn out to need no
work.

Three obstacles are already visible from reading, and each needs a decision rather than a patch:

- `createOrUpdateShards` and `createShard` both assert the routing entry is INITIALIZING. A computed
  entry that is already STARTED cannot open a shard. So the coordinator view and the local view have to
  disagree on purpose: STARTED to whoever is routing, INITIALIZING to the node that has not opened it
  yet. The two registrations already separate those questions.
- `updateShard` calls `routingTable.shardRoutingTable(shardId)`, which throws `IndexNotFoundException`
  for a computed index, so the shard would be failed and removed on the next cluster state.
- `ShardStateAction`'s started-shard executor looks the shard up by allocation id, finds nothing, logs,
  and marks the task successful. The cluster manager will never move a computed shard to STARTED. The
  transition has to be local, which fits the premise of the area but is a state machine that does not
  exist yet.

C13 is blocked on this. Restarting a node to see whether a computed index recovers its data is not a
question that can be asked while the index has no data, because it has no shard.
