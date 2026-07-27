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

## C18 in progress: the shard opens, and it will not take a write

The seam works. A data node now creates the `IndexService` and the `IndexShard` for a computed index,
asserted by `ComputedPlacementShardLifecycleIT.testADataNodeOpensTheComputedShard`, which passes in
under a second having failed at thirty before the change. Writes still fail, and the way they fail is
the useful part.

**Five requirements came out of running the test, and reading had surfaced only one of them.**

1. **Allocation ids cannot be random.** `ShardRouting.initialize` mints a fresh one when none is given.
   An entry rebuilt on every cluster state would carry a new id every time, `updateShardState` throws
   when handed a different allocation, and `removeShards` tears the shard down. `ComputedShardRouting`
   derives it from `(indexUuid, shardId, nodeId)` and a test pins the value, for the reason C1 pins the
   placement hash: two nodes disagreeing about a shard's identity is a split view, not a slow one.
2. **The local view must say INITIALIZING while the coordinator's says STARTED.** `failMissingShards`
   fails any shard the local view calls active that the node does not already have, and `createIndices`
   then skips it through `failedShardsCache`. This is what the first run hit, and it produced **no
   node-side error at all**, which is why reading did not find it.
3. **`updateShard` must resolve.** It called `shardRoutingTable`, which throws for an unpublished index,
   and the catch failed and removed the shard.
4. **The write path had a third unhooked lookup.** `TransportReplicationAction.ReroutePhase` read the
   routing table directly, got null, and retried "primary shard is not active" until the request timed
   out. C3 missed the write path in `OperationRouting`; this is the same mistake one layer down, at a
   site nobody would call a routing decision. Three open-coded lookups have now produced three bugs, so
   `resolveShard` exists to stop there being a fourth.
5. **`inSyncAllocationIds` never arrives.** It is maintained by the cluster manager as shards start, and
   a computed shard's started message is discarded because the allocation id is in no published table.
   The set stays empty, `ReplicationTracker` never tracks the primary's own id, and primary mode never
   activates. Reading the in-sync set from the placement is the right shape and is not yet sufficient.

**Where it stands.** The shard opens and refuses writes with "shard is not in primary mode", because
`ReplicationTracker`'s checkpoint map is empty on the first call: the in-sync set reaching it is still
empty on a path other than the one that was changed. The write test is `@AwaitsFix` with the run-by-run
account; the shard-creation test stays enabled, because it asserts something that now works.

**Two results worth keeping precisely because they are negative.**

The version gate in `updateFromClusterManager`, which ignores an update unless the cluster state version
is strictly newer, looked like the cause. It is not: passing a higher version changed nothing. That was
reasoned and wrong, which is the third time in this project that reasoning lost to measurement.

Driving the START transition from `handleRecoveryDone` rather than from the next applied cluster state
is necessary and made things worse. Necessary, because on an idle cluster nothing publishes a new state
for a computed index, so a transition waiting for one waits forever, and that is invisible from reading
because the missing thing is an event rather than a line. Worse, because the attempt tripped the
tracker's invariant during recovery, which failed and removed the shard and regressed the sibling test
from passing to "no such shard". It has been reverted, and the necessity stands.

**Measured after the commit, and it changes the picture.** A logging probe in `computedAwareInSyncIds`
printed nothing across a full run with node logging captured. `updateShard` never runs for a computed
shard, because it only fires on a cluster state applied after the one that created the shard, and an
idle cluster publishes no such state. So `computedAwareInSyncIds` and `startComputedShardLocally` are
both correct and unreachable.

That is the third time this area has shipped a mechanism nothing could reach: the supplier before
`registerUnpublished`, the C1 to C11 run before C12, and now these two. The pattern is stable enough to
act on. Reachability is not something to establish by reading the call graph and agreeing with yourself;
a probe that prints nothing settles it in ninety seconds, and it settled this after the call-graph
argument had already reached the same conclusion less certainly.

**The shape of all of it.** Computed placement removes the cluster manager from the loop, and every
piece of state the cluster manager used to maintain as a side effect has to become a function of the
placement instead: allocation identity, the started transition, and the in-sync set. That is a bigger
claim than "routing is computed", and it is the claim the area is actually making.

## C18 done: a computed index takes a write

Three consecutive clean runs, seven tests across the two integration suites, none skipped. The write
test asserts the document comes back, with a one second budget on the write itself, which fails outright
if the request has to retry.

**The five things the cluster manager used to do.** Computed placement takes it out of the loop, so
everything it maintained as a side effect had to become a function of the placement. Only the first was
predicted by reading the code:

1. Allocation identity. `ComputedShardRouting` derives it from `(indexUuid, shardId, nodeId)`, pinned by
   a test, because a rebuilt entry with a fresh id makes `updateShardState` throw and `removeShards`
   tear the shard down on every cluster state.
2. The started transition, at recovery completion rather than on the next applied cluster state. There
   is no next state: nothing publishes one for a computed index, so a transition that waits for one
   waits forever. A probe on that path printed nothing at all, which is how this was found.
3. The in-sync set, read from the placement, because the started-shard message that normally populates
   it is discarded for an allocation id in no published table.
4. The primary term, set at creation in the branch that skips publication, because for a computed index
   creation is the assignment. Setting it node-side instead trades "primary term must be positive" for
   "term is only increased as part of primary promotion".
5. The refusal to act on an unresolvable placement. An empty in-sync set clears every checkpoint while
   setting a routing table that names an active allocation, and the shard is then failed and removed.
   Reachable in production when an index is deleted while a shard is still recovering.

**"Created" is a weaker promise for a computed index, and that is by construction.**
`waitForActiveShards` counts the computed table, which reports STARTED from the moment the placement can
be derived, which is before any node has opened the shard. So creation returns optimistically and a write
issued immediately afterwards retries for between one and five seconds. The test asserts the two facts
separately: wait for primary mode, then write with a one second budget. Whether creation should instead
wait for the owning node to confirm is a real question and is not answered here.

**The bug that was not in the product.** Most of the flakiness chased through this task was the test
harness clearing the static registrations in `@After` while a shard was still recovering, so the recovery
hook found no supplier. That is the documented "static registries leak across test suites" trap running
backwards: not leaking into other suites, but being torn down underneath the suite that needs them.
Shortening timeouts to iterate faster made it far more likely, which is why the failure moved every time
the timing changed. Registrations now live for the class.

**What the mutation says, precisely.** Removing the empty-in-sync guard fails two runs in three rather
than three in three, because the condition it protects is itself a race. Recorded rather than rounded up.

## C13 done: a computed index survives a restart, and it needed no production change

Twenty documents indexed, the whole cluster restarted, twenty documents still there by count, asserted
twice over. Recovery for a computed index had been reasoned about and never observed, which is what A5
cost last time, and observing it says the reasoning was right.

**No production change was required, and that is the finding.** C18's seams already do the work: the
local view hands back an INITIALIZING entry after restart, the node opens the shard against the data
directory it already has, and the start transition happens locally at recovery completion. This is the
third task in this area planned as real work that turned out to need none, after C6 and C10, and always
for the same reason. A computed index publishes no routing entry, so the machinery that would have
needed teaching is never handed one.

**The test asserts a count rather than a document.** A5 was silent data loss: a blank index is STARTED,
green and empty, so every structural check passes. One document can survive a partial recovery; twenty
by count is much harder to pass accidentally.

**One open question, recorded rather than glossed.** The recovery source is `EMPTY_STORE` at creation
and `EXISTING_STORE` after restart, which is correct in both cases, but the hook hardcodes `EMPTY_STORE`
and something in the server upgrades it. `ShardRouting.moveToUnassigned` performs exactly that
transformation for an active primary, but it sits on an allocator path a computed index should never
reach. Until the mechanism is named, "a computed shard recovers from its existing store" is measured
rather than guaranteed, and the assertion in the test is what would catch it changing.

**What C13 does not cover.** One node holding the shard restarts and finds its own data. A node that
comes back with an empty disk, or a shard whose computed placement moves to a different node while the
old one still holds the data, are both different questions and neither is answered here.

## Correction: C13 was not done, and a green suite was measuring nothing again

An edit meant to move the test registrations to class scope did half its job. It removed the per-test
`registerComputedPlacement()` calls and never added the `@Before` that was supposed to replace them,
while `@After` kept clearing the registrations after every test. Three tests then created an ordinary
index that merely happened to be named `computed-lifecycle`, and passed.

**What that invalidates.** C13's result entirely: the restart test was restarting an allocated index. It
also explains the anomaly recorded above as an open question, the recovery source moving from
`EMPTY_STORE` to `EXISTING_STORE` across a restart with no mechanism to account for it. An ordinary index
does exactly that. There was nothing to explain.

**What survives.** C17, C18's shard-creation test, which kept its registration call throughout, and the
write test, which still passes with the registrations restored. The production seams were all verified by
failures that could only happen with computed placement active, and those failures are unaffected.

**C13's real result, now that the test tests something.** A computed index does **not** survive a restart.
The documents are gone and the index comes back blank while looking healthy, which is the A5 trap firing
exactly where the plan predicted it. The local view supplies `EmptyStoreRecoverySource` because that is
what the placement function states, and on restart that is the wrong answer.

The fix is a design question rather than a patch. The recovery source cannot come from the placement
function: placement is computed identically on every node, and only the node holding the data knows
whether it has any. Either the node overrides the source at shard creation by consulting its own disk, or
the hook is given enough context to decide per node. That is the next task and it is C19.

**The durable fix for the harness.** `createComputedIndex` now asserts that the index it just created has
no published routing entry. The premise of the suite can no longer be silently lost, which matters more
than the specific bug: this is the second time in this area that a green suite was exercising nothing,
after the C1 to C11 run that C12 exposed.

## C14 done: resharding a computed index is rejected with a reason

Split and merge now reject an index whose routing is not published, and the message says why. Before
this, split threw `IndexNotFoundException` from a routing lookup, merge dereferenced a null index routing
table, and both commit services read the absence as "still in progress" and stayed pending forever.
"No such index" is a poor description of an index that plainly exists.

Making resharding work under computed placement is a redesign rather than a fix: the children would have
to be placed by the same function, and the operation would have to reach agreement without publishing
anything. Not attempted, and recorded as a decision rather than an omission.

Three unit tests, mutation-verified: both rejections fail without the guard, and the control confirms an
index nobody claims is still rejected on its own merits rather than by the new branch. Unit rather than
integration on purpose, since the guard is pure logic over a cluster state and the integration version
spent its time on cluster teardown.

## C19: the recovery source has to come from the node, and C13 is still not done

**C19, done.** A computed shard's recovery source can no longer come from the placement function. The
function is evaluated identically on every node from the same inputs, which is the point of the design
and also why it cannot answer this question: it does not know which node holds a store. Stating
`EMPTY_STORE` is right while an index is being created and catastrophic on restart.

The node corrects it at shard creation, using `ShardPath.loadShardPath` to ask whether it already holds
data, and only ever upgrades `EMPTY_STORE` to `EXISTING_STORE` for an index with no published routing.
Anything the cluster manager allocates keeps the source it was given. The interface question is
defaulted to false so implementations that do not manage local storage, including the test doubles, are
untouched. An unreadable path reports "data present" rather than "absent", because absent would recover
from an empty store, which is the outcome the call exists to prevent.

**C13 is reopened, and the blocker is bigger than the recovery source.** After a full restart with a
computed index present, the cluster does not recover: every node reports `state not recovered /
initialized` and no cluster manager is discovered. That is a gateway-level question, and the likely
suspect is state recovery expecting each index in metadata to have routing. `ComputedPlacementRestartIT`
carries the finding and is `@AwaitsFix`.

**It lives in its own class now.** The restart leaves the shared test cluster unusable, so keeping it
beside the other tests turned one real failure into three misleading ones, with the siblings failing on a
broken cluster rather than on anything they assert. Isolation here is not tidiness, it is the difference
between one signal and three false ones.

## C15 done: adaptive replica selection already ranks the computed candidates

No production change, and the reason is the same property that made C3 and C5 small. ARS is applied by
`OperationRouting#shardRoutings` to an `IndexShardRoutingTable`, and a computed entry *is* an
`IndexShardRoutingTable`. The ranking neither knows nor cares where the table came from. Keeping the
existing type rather than introducing a parallel one means existing behaviour composes for free.

Verified rather than assumed, which is the point: "ARS applies to the K candidates" was an assumption
the area rested on. Three computed candidates with the third node an order of magnitude slower, and the
slow node ranks last. Moving the slow node moves the ranking, so the assertion is measuring the ranking
and not the list order.

**The open decision this surfaces.** The test builds the K-1 non-primary candidates as ordinary
replicas, which is what makes them rankable. Whether they should be search-only copies outside the
replication group, as C4 sketched, or full members of it, is not settled here and changes what the
in-sync set from C18 has to contain. Recorded as a decision for whoever turns the setting on rather than
as an omission.

## C20 in progress: state recovery was quietly republishing computed routing

**A real bug, found by reading the recovery path rather than by a failure.**
`ClusterStateUpdaters.updateRoutingTable` rebuilds the routing table from metadata on every state
recovery, calling `addAsRecovery` for every index, with no equivalent of the guard index creation
applies. So a single recovery turns every computed index into an ordinary one whose shards are all
UNASSIGNED, while the node keeps contributing its own computed copy of the same shard id.

It does not throw and does not look like a failure. That is the profile of every defect this area has
produced: the supplier is consulted only when nothing is published, so republishing does not break
loudly, it just stops the mechanism from ever running again. **This is the fifth seam, and the rule is
now general: anywhere that decides what gets published has to respect the opt-out.** The four before it
were publication at creation, counting active shards, materialising shards locally, and resolving rather
than looking up.

Guarded, with a unit test, mutation-verified.

**A correction to the guess recorded with C13.** The `@AwaitsFix` note said state recovery expects each
index in metadata to have routing. That is wrong in the letter: nothing in the recovery path throws or
asserts on a missing entry, and `RoutingTable.validate` has no production caller and only checks the
routing to metadata direction anyway.

**And the guard fixed cluster recovery, which corrects a second guess made here.** The reasoning above
said the missing cluster manager could not be a routing problem, since election never reads the routing
table. Election indeed does not, but the republished routing was still the cause: it collided with the
copy each node contributes for itself, and cluster state application never got far enough for the
gateway to recover. With the guard in place the cluster comes back and the restart test runs to
completion. Correct in the letter, wrong in the conclusion, and running it settled in one attempt what
the argument had settled wrongly.

**What is left of C13 is the documents, and a probe cleared C19 of causing it.** The cluster recovers,
the shard recovers from its existing store, and it is empty: twenty indexed, zero found. The node-local
check runs twice and is right both times, `hasData=false` at creation so the shard stays EMPTY_STORE and
`hasData=true` after the restart so it becomes EXISTING_STORE. The node finds its data and asks for the
right recovery, which is exactly what C19 was built to do.

So the store is opened and comes back empty, and that points at history and translog rather than at
placement. The suspects are the three things computed placement changes about a shard's identity: the
primary term is synthesised at creation, the in-sync set is derived from the placement, and the shard
starts itself locally without the cluster manager. Any of those can lead recovery to bootstrap a new
history rather than replay the existing translog, and a new history on an existing store is a blank
index that looks perfectly healthy.

**Probed, and recovery is cleared entirely.** The recovery source is right both times, the
`cleanLuceneIndex` branch that silently discards an existing store never fires, and the pre-restart
recovery is a textbook new index with `si=null`. The cause is which node owns the shard.

**C13 is blocked on C2, not on recovery.** Placement in the test is
`shardId % dataNodes.size()` over the data nodes visible in the cluster state at that instant. During a
restart that list grows as nodes rejoin, ownership moves, and a node that gains the shard has none of
its data. It recovers empty, correctly by its own lights, because it really does have nothing. Different
timing gives different symptoms, which is why one run returned zero documents and the next could not
find the shard at all.

C2 said it in advance: "Two coordinators computing against different node lists produce different
placement, so this is a correctness input, not a convenience." A restart is that same problem in time
rather than in space. The same coordinator, computing against a changing node list, relocates shards
away from their data, and every new owner starts blank while looking healthy. Which is A5's signature
again, arriving from a direction the recovery source cannot defend against.

**What that means for the area.** The placement function needs a node set that is agreed and stable
rather than instantaneous. `RendezvousShardPlacement` from C1 minimises movement when membership really
does change, and the test should use it rather than modulo, but that only shrinks the blast radius: it
does not make the input stable, and a shard whose winning node is briefly absent still moves. Until C2
lands, a restart is not safe for a computed index, and no amount of work on recovery changes that.

## C2 done: the node set is agreed rather than observed

Placement no longer reads `DiscoveryNodes` as-of-now. It reads `ComputedPlacementMembership`, a sorted,
versioned, deduplicated node set published as `Metadata.Custom` with `API_AND_GATEWAY` context. The
gateway half is the requirement rather than a nicety: a membership that had to be rediscovered after a
restart would be empty exactly when the shards need it.

**The asymmetry is the design.** Nodes are added automatically on join, because a cluster that grows
should use the new capacity without ceremony. Nodes are never removed, because absence has two causes
that look identical from the cluster manager, restarting and gone for good, and guessing "gone" moves
every shard that node owned to a node with none of its data, which recovers empty while looking healthy.
Guessing "still here" only costs availability while it is away. `withNodes` has no counterpart and a test
asserts membership never shrinks. Decommission is deliberate, needs to move data first, and is not
implemented.

**Verified rather than assumed.** Node ids are persisted in the data path and reloaded on restart, so a
returning node is the same member holding the same data. Placement not changing when a node leaves is a
test, mutation-verified: reverting `eligibleNodes` to the live view fails it with the message it was
written for. That the maintainer actually runs in a live cluster is an assertion rather than a log probe,
because a probe only prints when a test fails and this area has three times shipped a mechanism that was
correct and never invoked.

**Two things this surfaced that are worth keeping.**

Growth is a placement change. Membership grows as nodes join, and with modulo every growth reshuffles
every shard; harmless before there is data and destructive after. That is what rendezvous hashing is for,
and the plugin path uses it over this same membership. The reasoning that "a membership that never
shrinks makes modulo as stable as rendezvous" is true only once the set has settled, which was too narrow
a claim when first made here.

The maintainer is edge-triggered on cluster state changes, so a cluster that installs a supplier and then
goes completely idle publishes nothing and keeps using the live view. A test has to force a change; a
real cluster generates them constantly. Publishing on registration would close the window and is the
obvious next refinement.

**Where C13 stands now.** Placement is stable across a restart and the test's placement assertion is
satisfied. The remaining failure is earlier and different: the documents are not searchable even before
the restart. That is a new symptom rather than the old one, and it is recorded as its own investigation
rather than folded into C13. The first suspect is the ordinary trigger index the test creates to force a
state change, since it publishes routing and is the first thing the membership sees.

## C21, C22 and C23: the seam that fails by succeeding

Three passes, one shape. A request path reads the routing table directly, finds nothing for an index
that publishes no entry, and degrades the way Phase A chose. What makes this family different from
everything else in the area is how it fails.

**Six confirmed cases, none of which threw.** Refresh reached no shards and reported success with
`successful_shards: 0`, which is what hid C21 for two sessions and made C13 look like a recovery bug.
Get field mappings returned a well-formed response describing an index with no fields. Stats reported no
shards and no documents, segments none, recovery nothing recovering. Force merge is the worst: it
accepted the instruction, reported success, and did no work at all.

Every other bug in this area announced itself with a hang, an exception, or a cluster that would not
form. This one returns 200. That is the whole reason it survived six passes over the same seam, and it
is why every test in these three tasks asserts a count rather than the absence of an exception.

### What the probes overturned

C22 predicted `TransportUpdateAction` would be the serious case, on the reasoning that an empty shard
iterator makes the caller wait for an allocation that never arrives, so the request hangs rather than
fails. The reasoning was sound and the conclusion was wrong. Bulk updates never enter that action, and
an update routed by document id resolves already through `OperationRouting`. The branch is reachable only
on a retry, because `TransportInstanceSingleOperationAction` assigns `request.shardId` *after* `shards()`
has already succeeded once. It is converted, with a unit test rather than an integration test, and the
difference is recorded in the suite instead of being smoothed over.

Meanwhile the two call sites with no special reasoning attached, analyze and get field mappings, were the
genuinely broken ones.

### Where the bulk fix could not go, and why

`RoutingTable.allShards` and its neighbours cannot resolve, for two structural reasons rather than one
stylistic one. They receive a routing table and never a `ClusterState`, while supplying an entry needs
the state and the index metadata. And the no-argument forms derive their index list from the routing
table's own key set, which a computed index is not in, so it cannot even be *named* there.

So the bulk counterpart lives beside `resolve` in `AbsentIndexRoutingSuppliers`, and nine callers use it.
This keeps supplier knowledge in the one class that owns it. It also leaves a real gap: anything calling
the no-argument form still cannot see a computed index, which is C26 and is why `_cat/shards` does not
list one.

### Two things a mechanical sweep got wrong

Nine call sites were converted by regex and two needed correction on reading.

Force merge filters one branch with `ShardRouting::primary`. The convenience overload would have dropped
that silently and merged replicas.

The fast path has to delegate to the *exact* method it replaces, not an equivalent one. Routing
`allShards(indices)` through `allShardsSatisfyingPredicate(indices, alwaysTrue)` is behaviourally
identical, since the first is implemented as the second, and still broke
`TransportRemoteStoreStatsActionTests`, which stubs `allShards(String[])` on a spy. Callers and tests
bind to methods rather than to behaviour, and a no-op path exists precisely to be indistinguishable.

### Status

C13 passed with no further work once C21 landed, over seven consecutive runs with fresh seeds. Every
theory the restart test had accumulated across two sessions, blank recovery and lost writes among them,
was downstream of a refresh that reached no shards.
