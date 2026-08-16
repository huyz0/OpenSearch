# STATE

Read this first, every time. It is the only source of truth for where work stands, and it
is written to survive context loss: nothing here depends on remembering a previous session.

Updated: 2026-08-16 (round 006 closed out below; GC candidate queue entry unchanged beneath it)

## 2026-08-16: round 006 closes out -- items 1b, 5 and 6 landed, one new finding left open

**1b, closed.** The third layer named in the last entry (`GatedShallowSnapshotIT`'s harness installed no
change log, so nothing appended and nothing tailed regardless of what production wired) is fixed:
`startDescriptorChangeTail` gives the test a real `BlobDescriptorChangeLog` and a real
`DescriptorChangeTailer` polling it, opted into only by the round-trip test. With that running, closing a
gated index appends `CLOSED`, the tailer applies it, `IndicesClusterStateService.releaseGatedIndex` releases
the shard, the lease lapses, and restore-in-place -- which refuses while a lease is held -- becomes
reachable. Confirmed both directions: reverting the wiring reproduces the exact `IllegalStateException` the
`AwaitsFix` named. `GatedShallowSnapshotIT` no longer carries one.

**Item 5, shipped for the case that matters most, found something real in the other.**
`IndexDeepSnapshotAction`/`ShardDeepSnapshotAction` turn the round's own spike
(`DeepSnapshotOrchestrationIT`) into a real transport action -- pin, copy through a `Store` over a
`LazyBundleDirectory` into the target repository via core's own `Repository#snapshotShard`, finalize once on
the cluster manager, release on both paths. Two bugs found and fixed while proving it through a real IT
rather than through the spike's manual wiring:

- A same-thread-pool self-deadlock: the shard action dispatched onto `GENERIC` and then blocked waiting on
  `SnapshotPinAction`, which also dispatches onto `GENERIC`. Reproduced as a genuine hang -- the suite's
  20-minute timeout fired with every thread parked. Fixed by chaining through `ActionListener` instead of
  blocking, the same pattern `TransportIndexSnapshotPinAction` already uses.
- `BlobStoreRepository#finalizeSnapshot` writes each index's own metadata via `clusterMetadata.index(name)`,
  which answers null for a gated index. Fixed by folding the already-resolved `IndexMetadata` into a local,
  unpublished copy of cluster metadata before the call.

**Proven**: an ordinary serverless index, deep-copied through the shipped action and restored by core's
ordinary `_restore` under a new name, every document back.

**Found and left `AwaitsFix`, not swept under**: a deep snapshot of a *gated* source completes -- copy and
finalize both succeed -- and the restored shard then never allocates, stuck at
`allocation_status[fetching_shard_data]` forever. Traced one layer further before stopping:
`PrimaryShardAllocator` refuses a snapshot-recovery shard until `SnapshotShardSizeInfo` answers, which
`InternalSnapshotsInfoService` only ever supplies by fetching `Repository#getShardSnapshotStatus` on the
cluster manager -- and that fetch, or the reroute it should trigger, never resolves for a snapshot finalized
outside `SnapshotsService`'s own in-memory state tracking, which this action's own javadoc already named as
a boundary it does not replicate. Computed placement being enabled on the node is the one difference between
the passing and the stuck test -- recorded because it narrows where to look next, not because the restored
index is itself gated (it deliberately is not, confirmed by the failure's own diagnostic). The test bounds
its own wait now (30s health check, then a diagnostic failure) rather than the indefinite hang that first
found this, so the suite stays fast either way.

**Item 6, built once the blocker was gone.** The plan's own obstacle -- "needs a per-index container
resolver first" -- turned out to already exist: `ShardCloner.ContainerResolver`, built for clone
lineage-chasing. `PinLedgerSweeper` reads every shard a ledger names through it and deletes the ledger only
once none of them still carry a live `PinRecord` under its pin id; an unreadable shard counts as still
holding the pin, not as absent, so a transient failure can never manufacture the leak this class exists to
close. `PinLedgerSweepTask` schedules it per index, off by default
(`serverless_storage.retention.pin_ledger_sweep_interval`), built in `getEngineFactory` for shard 0 only and
deduped by uuid. **Coverage is node-local by design, stated rather than implied**: an index whose shard 0
has never opened on this node is not swept, because there is no fleet-wide registry of which indices have a
ledger to drive this from, and building one is out of this item's own scope. Seven unit tests over real
`FsBlobStore` containers, including the property the class exists for (a live pin on a shard other than 0
must hold the ledger) and the fail-safe direction (an unreadable shard is never mistaken for a clear one).

**B1, closed on its engineering half.** The branch's own 1,200-plus commits already answer the shape
question: computed placement is built as a set of small, generally-useful core seams
(`AbsentIndexRoutingSuppliers`, `GatedIndexRelease`, `EngineFactory#localStoreIsStale`, and the rest §15
names) behind a plugin-installed gate that is off by default -- never a fork of core's allocator. That
pattern is validated by the code as it stands and needs no further decision to keep following. What is
genuinely still a human's to decide, and is not blocking anything in this round or the next: whether those
seams are ever proposed upstream to `opensearch-project/OpenSearch` as real PRs, or stay a long-lived
downstream patch set. That is a distribution/governance question, not an engineering one, and answering it
was never this session's to make unilaterally.

**Round 006's own sequencing table, final state**: items 1, 1b, 2, 2b, 3, 4 done; item 5 shipped with one
new, precisely-located `AwaitsFix`; item 6 built and node-local by design. Nothing in the round is silently
unaccounted for -- the one open item is a named, reproduced, partially-traced defect, not a gap.

## 2026-08-16: manifest GC gets an event-driven front door, so a warm-but-idle shard costs nothing

Not part of round 006 -- this came out of a cost conversation about `GcSchedulerTask`'s own shape: a real
`listBlobsByPrefix` call is priced in S3's write tier, not its read tier, and that task issues **two** of
them (manifest listing, bundle listing) on a fixed clock, on every warm reader shard, whether or not
anything changed since the last tick. At this branch's target population that is a real, recurring cost
driven by idle time on a fixed schedule, which is the worst possible pairing.

**What shipped**: `GcCandidate`/`BlobGcCandidateLog`/`GcCandidateTailer` (package `gc`) -- a fleet-wide,
S3-backed append log in the same bucketed-and-sharded shape `BlobDescriptorChangeLog` already proved (see
that class's own javadoc for the per-prefix write-throttling reasoning this reuses verbatim), except entries
retire by deletion rather than by a cursor, because a GC candidate has to be revisitable until its retention
window elapses, which a forward-only cursor cannot do. `ObjectStoreCommitHeadPublisher` appends one entry at
the exact moment a publish supersedes a prior head -- the one place that fact is known for free, without
relisting anything. `GcCandidateTailer` runs on the elected cluster manager alone (same reasoning as
descriptor-changelog pruning: one bounded listing, not paid for N times over), deletes what is past
retention and unpinned, drops the queue entry for what turns out to be durably pinned, and leaves the rest
for the next pass. `GcSchedulerTask` is completely unchanged and keeps running as the backstop; nothing here
replaces it, only front-runs it. Off by default (`serverless_storage.gc.candidate_tail_interval = -1`),
matching every other GC-adjacent setting's own shape.

**Two real bugs found during the doing, both caught by tests before they shipped**:

- **The lookback cannot equal the retention window.** If a pass only ever looked back one retention window,
  any tailer downtime longer than that (a cluster-manager failover, a rolling restart) would age a
  genuinely still-pending candidate's bucket out of every future pass' reach, silently and permanently.
  `GcCandidateTailer` takes `lookbackMillis` as a separate, larger, validated-at-construction knob
  (`serverless_storage.gc.candidate_lookback`, default 2 hours) rather than deriving it from the retention
  window.
- **A lease-only head is not a superseded manifest.** `acquireOrRenewLease` can put a placeholder `ShardHead`
  at generation 0 in place before any commit is ever published (the same sentinel `readLatestManifest`
  already treats as "nothing published yet"). The append condition originally read `currentHead != null`,
  which is true for that placeholder too -- appending a candidate naming a manifest that was never written.
  Harmless (the tailer's `NoSuchFileException` handling absorbs it as "already gone") but wrong, and found
  by `GcCandidateQueueIT` before it was found any other way: two writes were producing two candidates
  instead of one until this was fixed. The condition is `currentHead != null && currentGeneration > 0` now,
  with a unit regression test (`testAFirstPublicationAfterOnlyALeaseAcquisitionAppendsNoCandidate`) reproducing
  the lease-then-publish sequence directly, verified to fail without the fix.

**What this deliberately does not cover**, stated rather than discovered later: bundle orphan detection
stays exactly as periodic and per-shard as it already was -- `BundleReferenceCounter`'s own javadoc explains
why (bundle liveness is a reference count over a *set*, not a fact one publish event can hand over the way
manifest supersession can); a manifest written but never published as head (the losing side of a CAS race)
generates no candidate and remains `GcSchedulerTask`'s own responsibility; and a durably pinned generation
whose pin is later released does not currently re-append a fresh candidate for it -- `GcSchedulerTask`'s
sweep remains what eventually reclaims that case.

Tests: `GcCandidateTests`, `BlobGcCandidateLogTests`, `GcCandidateTailerTests` (unit, fake clock/containers),
two new cases on `ObjectStoreCommitHeadPublisherTests` (the append itself, and the lease-only-head
regression), `GcCandidateQueueIT` (real cluster: a real write's supersession is discoverable from the object
store alone at the exact location the plugin resolves, and a directly-constructed tailer against that same
location deletes what should be deleted and spares what is pinned). Full plugin unit suite and the touched
internalClusterTest groups (writer engine, snapshot, this feature's own IT) all green; the full
internalClusterTest suite was kicked off to confirm nothing else regressed.

## Correction, 2026-08-15: the suites were not green, and the plan argued from deleted machinery

**Both are now fixed** -- `internalClusterTest` is green end to end, and the plan carries its correction.
The section below is kept in the order it was found, because what it found is worth more than its
conclusion: the suite had eight failures, six of them deterministic, and three of the eight turned out to be
defects in the product rather than in the tests.

Two things found by grounding the documents against the tree rather than against each other. The full
write-up is the **seventh review** at the end of `plan-100m-index-implementation.md`; this is the part a
reader needs before picking up any task.

**1. `internalClusterTest` was red before anyone changed anything.** The full plugin suite has now been run
end to end -- 87 classes, 219 tests, 5 skipped, **8 failures**, 28 minutes -- which does not appear to have
happened before. Each failure was checked against an unmodified tree rather than assumed.

**Six are pre-existing and deterministic**, and they cluster in exactly the place T58 changed: the guards for
what a mapping store does when it cannot read, when its index is lost, and when it must stay off the cluster
state thread.

```
BlobBackedDescriptorIT              > testTheConfiguredMappingIndexShardCountIsUsed
BlobBackedDescriptorIT              > testOperationsAGatedIndexCannotSupportFailClearly
GatedCreateTimeMappingIT            > testAGatedCreationDoesNotReadAMappingThatCannotExist
GatedMappingIndexLossIT             > testAMappingWriteAfterTheMappingIndexIsDeletedFailsRatherThanRewriting
GatedMappingMissingWindowIT         > testAReadFailsInTheWindowAndSucceedsOnceResolutionCatchesUp
GatedMappingOffClusterStateThreadIT > testNeitherCreationNorPutMappingTouchesTheStoreFromAClusterStateThread
```

**Three looked flaky**, all confirmed by repetition rather than argument -- and two of the three turned out
not to be flaky at all, only intermittent, which is not the same thing. Both had a single deterministic
cause reached by a seed-dependent path, and both are fixed below.
`ServerlessStorageAffinityForwardingIT` (fails on an unmodified tree too, and worse there -- two of its tests
rather than one); `ServerlessStoragePreWarmChaosIT#testClusterSurvivesANodeDyingDuringGatedPreWarmDispatch`,
G4's own chaos test, about two runs in five, which is G4's own recorded "a gated shard's primary can take
minutes to be re-derived after its host dies" surfacing as an intermittent test; and
`GatedIdleEvictionIT#testResidencyIsBoundedByArrivalRateRatherThanByPopulation`, about one run in two, on a
leftover-index assertion at teardown rather than in its own body.

**The eviction one was a real race in core, and it is fixed.** Not a flaky test: `removeIndices` iterates
`indicesService`, which hands out the index map as it was when the loop started, and idle eviction closes
gated indices from another thread that deliberately does not hold the applier's monitor. The close removes
the index from the map first and clears its `openedOnDemand` entry after, so an eviction landing
mid-iteration leaves an index in the loop's stale snapshot that is no longer held on demand and no longer
there to remove -- and every check below reads that as "the cluster manager took this away", ending at an
assertion that it must have been deleted or the cluster be new. Neither is true of an index that was never
published. The gated guard that should have caught it asks `DescriptorOnlyCreation`, whose registration goes
away when a node's gate uninstalls while the indices it opened are still resident, which is why this
surfaced at teardown. `IndicesClusterStateService` now skips an index that is no longer in the map at all:
it is not that loop's to reason about, whoever closed it. **Six consecutive green runs against one failure
in three before it.**

**The affinity one was a missing line in one file.** `ServerlessStorageAffinityForwardingIT` never disabled
the framework's mock engine, and this plugin supplies an engine factory of its own:
`IndicesService.getEngineFactory` refuses when two plugins claim one index, so on the seeds where the
randomizer installs `MockEngineFactoryPlugin` -- about one run in three -- no gated index can open on that
node at all. For a gated index that is a hang rather than a failure: it has no cluster state entry, so
nothing tells the write it will never be servable, and it retries until the suite times out twenty minutes
later. Read as "a write to a freshly created gated index sometimes never becomes servable", which sounds
like a deep property of on-demand shard opening and was one line of test wiring. Seventy-one other classes
had the override; three did not. It is declared on `ServerlessStorageIntegTestCase` now, so the next test
cannot omit it.

**The chaos one stopped reproducing and is not claimed as fixed.** Five consecutive green runs against two
failures in five before, with no change made to it. The eviction race above could account for it -- that
test kills nodes while gated indices are open on demand, which is the same window -- but nothing here
demonstrates that, and the alternative explanation is that the box was quieter. Left on this list, with the
rate re-measured rather than the label repeated.

`DescriptorLifecycleIT#testAGatedIndexLivesItsWholeLifeOutsideClusterState` failed on the unmodified tree and
passes now, fixed incidentally: `DescriptorGate.supply` never recorded the uuid-to-name entry
`DescriptorBackedMappingStore` needs, so a node that had only *resolved* an index -- the ordinary case for the
node handling a write -- read a null mapping and then failed its swap sixteen times before reporting
"sustained contention" for what was a missing lookup.

The unit suites are green -- `:plugins:serverless-storage:test` at 236 classes / 1,275 tests / 0 failures, and
the core seam tests at 50 classes / 2,416 tests / 0 failures -- which is what has kept this from being
visible.

### All eight are now fixed, and three of them were production

Same day, same pass. **`internalClusterTest` is green end to end for the first time: 87 classes, 219 tests,
5 skipped, 0 failures, 7m 39s.** Alongside it `:plugins:serverless-storage:test` at 237/1,277 and the core
seam tests at 85/1,069, both zero, and `spotlessCheck` passes.

Five of the six deterministic failures were tests still demanding the contract T58 replaced. The sixth was a
*guard* T58 made unreachable, and the two remaining "flaky" classes were a race in core and a missing line
of test wiring -- neither of them flaky, both intermittent for a deterministic reason.

- **`GatedMappingMissingWindowIT` -- a production fix.** T59's check in
  `MappingGenerationStore.currentMapping(uuid, expected)` refused only a **null** answer, which was
  exhaustive while the index-backed store was registered: it had a document or it did not. T58 made the
  descriptor the store, and a descriptor that resolves always answers -- for an index with no mapping, with
  generation 0 and no fields. The identical wrong answer, arriving as a value rather than as a null, walked
  straight past the guard. It is stated over the generation now: the store must be able to answer at the
  generation the caller's descriptor claims, or it refuses. Two unit tests cover behind-the-descriptor and
  ahead-of-it (ahead is the ordinary stale-resolver case and must **not** fail).
- **`GatedMappingIndexLossIT`** asserted T48's contract, that losing `.opensearch-index-mappings` fails the
  next write. That index holds no mapping since T58; it is a stats projection. The assertion inverts --
  fields survive, writes succeed, the projection rebuilds itself from the next mapping change -- which makes
  it the live test of the watcher clearing `IndexBackedMappingStore`'s "the index exists" latch.
- **`GatedCreateTimeMappingIT`** asserted T41's "one swap through the store"; the mapping now rides the
  descriptor the creation was already writing, so the criterion is zero touches.
- **`GatedMappingOffClusterStateThreadIT`** watched only the mapping store, which creations no longer reach,
  so both creation phases recorded nothing -- and a phase that records nothing passes every thread check
  there is. It records the descriptor backend too, splitting blocking calls (held to the thread rule) from
  asynchronous ones (evidence the phase reached the plane at all).
- **`BlobBackedDescriptorIT`** (both) -- one demanded a refusal that close/open have genuinely supported
  since; the other raced the framework's index wipe.

The one failure left in that run was `GatedMappingMissingWindowIT` failing in *teardown* --
"shard is still locked" -- with its body green: the fixture's projection drain only knew about stores the
fixture itself built, so classes that let the plugin wire itself left a write running into a cluster being
torn down. `DescriptorGate` now keeps the installed store so it can be drained, in `uninstall()` for the
node-close case and from the fixture for the test one. That drain had been written with a javadoc naming two
callers and wired to one.

Two practical notes for whoever picks this up. Run the suite with `-Dtests.jvms=2 --max-workers=2`: at the
default fork count on this box the test workers crash outright ("Could not stop all services"), which is the
environment rather than the code. And `spotlessCheck` fails on an unmodified tree, on six files from the last
week's commits -- `./gradlew :plugins:serverless-storage:spotlessApply` fixes it.

### Creation throughput, re-measured 2026-08-15: it is a cliff, not a number

The 275/sec figure below is superseded. Two changes had moved under it without a re-measurement -- T18
taught the creation bypass to validate a plainly typed mapping from the mapper registry, and T58 took
`MappingGenerationStore` off the creation path entirely -- so what a declared mapping costs now depends on
its *shape*, not on whether it exists:

| arm (100 per arm, concurrency 8, second of two rounds) | rate | vs control |
|---|---|---|
| unmapped (control) | 2,259/sec | -- |
| plainly typed mapping | 2,095/sec | 1.08x |
| one field parameter added | 315/sec | 7.18x |

**The store was never the answer.** Not the deleted system index, not the blob CAS, not the cluster state
thread. It is `IndicesService.createIndexService`'s `synchronized`, entered to build a throwaway
`IndexService` for any mapping richer than a bare type name, at about 21.6 ms of serialized time per
creation. Everything else a mapping costs is under ten percent. Absolute rates are from a jacoco-loaded JVM
and are a floor; the ratio is the finding.

**The harness could not run, and its first repaired run found a regression rather than a number.**
`GatedMappedCreationCostIT` asserts one mapping-store swap per creation, which T58 ended, so it had been
broken since -- opt-in behind `-Dtests.mappingcost=true`, which is why a green suite never said so. Repaired
and re-armed by mapping shape, its first run stalled: the mapping stats projection saturated `GENERIC`, the
pool gated creation itself runs on, 127 of 132 threads parked inside it, and an arm of 300 mapped creations
that never finished. Projections are bounded at four in flight now. **Nothing in the suite creates mapped
gated indices concurrently at any scale**, which is the population this product exists for, and that gap is
what let a regression of this shape through a green run.

### One name can be claimed in both planes, and it is not a race — CLOSED

**Closed 2026-08-15 by partitioning the names, not by making the authorities agree.** `DescriptorGate::gatable`
refuses a name outside the `serverless_` namespace, so no descriptor can exist for one;
`clusterStateCreateIndex` refuses a name inside it, so no cluster state entry can exist for one. Neither
authority consults the other — which is what made this unfixable where it was found, because the consulting
would have been a blocking descriptor read on the cluster state thread — and both checks are string
comparisons, which are safe anywhere. `GatedAndOrdinaryNameCollisionIT` lost its `AwaitsFix` and now asserts
both directions plus the third one that stayed open longest: a namespaced index the gate declines is refused
outright rather than created ordinary, which is the road the collision actually travelled. The finding as
first written follows.



Found while checking a claim I had just written into `MetadataCreateIndexService` -- that an ordinary
creation consults the descriptor store. It does not. `validate` checks the routing table, the metadata and
the aliases, all of which are cluster state, and nothing on that path asks the descriptor store anything.

Measured rather than argued (`GatedAndOrdinaryNameCollisionIT`):

- **gated `x`, then ordinary `x`: both granted.** Sequentially, no concurrency. The descriptor stays live
  and cluster state gains an index of the same name with a different uuid.
- **ordinary `x`, then gated `x`: refused**, because a gated creation validates its name against cluster
  state like any other. Only one of the two orders is open, and it is the one where the authority holding
  the name is the one nobody asks.

**What it costs.** Resolution consults metadata before the supplier, so the ordinary index shadows the gated
one: the client that created the gated index was told it exists and can no longer address it. Nothing
reconciles the two, so this does not converge -- it is not eventual consistency. Delete the ordinary index
and the descriptor unshadows, so the name comes back as a different index with a different uuid and no data.
No documents are destroyed at any point, but a name silently changing identity is indistinguishable from
loss to whoever was writing to it.

**Why the obvious fix does not fit.** Teaching `validateIndexName` to ask the descriptor store puts a
blocking object-store read inside the cluster state update task, which is the deadlock W4 already paid for.
The check has to move to the request path, before the task is submitted.

Left failing-but-visible with `AwaitsFix` rather than weakened, the same way `GatedCreationDurabilityIT`
keeps T17. Nothing about the distribution change opened this and nothing about it widens this direction.

### The mapper-service lock, and the change log's one prefix

Both found by asking why creation is slow now that the descriptor system index is gone, and both fixed.

**The lock.** Validating any mapping richer than a bare type name meant building a throwaway `IndexService`
inside `IndicesService`'s monitor, so every concurrent creation on the node queued behind it. A mapping
needs a *mapper service*, not an index service, and it needs the monitor only across
`pluginsService.onIndexModule`, which is the one part that runs code this repository does not own.
`createMapperServiceForValidation` builds one that way and `MetadataCreateIndexService` gained a third road
between the fast path and the full one. `createIndexMapperService` is untouched, so ordinary-index
concurrency is unchanged. Measured, second of two rounds at concurrency 8:

| arm | before | after |
|---|---|---|
| unmapped (control) | 2,259/sec | 3,060/sec |
| plainly typed mapping | 2,095/sec | 2,639/sec |
| one field parameter | **315/sec** | **1,348/sec** |
| the lock, per creation | 21,618 us | 2,903 us |

Also gone as a reason to take the slow road: two mappings to merge. That was justified as "precedence is
what the index service implements", and it is not -- the merge is `mapperService.merge` once per mapping in
order. Template plus request, a very common shape, stops paying.

**The change log's prefix.** An object store rate-limits by key prefix, and every append in a minute went
under that minute's bucket: about 3,500 writes per second on S3, which is a quarter of what 100M in two
hours needs and a ceiling on the whole cluster however many nodes are creating. Appends are spread over
sixteen prefixes within a bucket now (~56,000/sec), readers discover shards by listing rather than knowing
the count, and the pre-sharding layout is still read. Nothing about the old version looked wrong: appends
never contended, and a prefix rate is invisible to every test in this repository because none of them runs
against an object store.

**Creation is distributed now.** `TransportCreateIndexAction.localExecute` returns true for a creation that
is *certainly* gated, so it runs on the node that received it instead of redirecting to the elected cluster
manager. Nothing about a gated creation needs that node: uniqueness is the register CAS, and the cluster
state need is a snapshot read every node has.

Certainly, not probably, and that is the whole of the design. Admission is allowed to be wrong -- the real
gate can still decline, and the fallback that handles it submits a cluster state update task, which only the
cluster manager can publish. So `certainlyGated` declines on anything that could make the gate refuse: an
alias on the request, a context, a data stream, a resize source, **and a template's alias**, which the
request does not carry and which is the commonest configuration in a tenant-per-index deployment. Anything
short of certain keeps today's behaviour.

**One consequence stated rather than discovered:** a gated creation checks the name against the cluster state
snapshot it can see, so off the cluster manager the window in which an ordinary index of the same name is not
yet visible grows from thread-scheduling lag to publication lag. The race is not new -- gated creation has
run off the state thread since T49, so the manager never serialised against its own publications either --
and the reverse direction is closed for a different reason than I first wrote here: not because ordinary
creation consults the descriptor store -- it does not, as the section above measures -- but because a gated
creation validates its name against cluster state like any other. Closing this direction needs a commit
across both stores.

**What it is worth cannot be measured here.** Every node in an internal cluster test shares one JVM on one
box, so distributing the work adds no CPU: the harness shows no regression and an improvement consistent
with removing a network hop, on a round whose own drift control says it had not settled. The effect this is
for -- a creation rate per node rather than per cluster -- needs a real multi-machine cluster, which is the
same measurement gap as everything else here.

**2. The 100M plan reasons about a descriptor system index that was deleted on 2026-08-05**
(`46cfb963519`), and quotes a creation-throughput figure that has been corrected repeatedly since
(20,577 -> 235 -> ~550 -> 859/sec across S26-S35, and then overtaken entirely by the post-blob measurements
in `rfc-100m-index-architecture.md`: **275/sec for a mapped index**, the ordinary case, against 6,011-10,505
for the unmapped population nobody creates. 100M is about four days of writing, not 70 minutes, and the gap
is a `synchronized` block the fast path avoids by declining any mapping -- filed as T21). Its
lookup-latency projection, the load-bearing number
for the whole design, measured the deleted index and has **no replacement measurement on the shipped blob
path**. Its two "load-bearing operational parameters", merge policy and refresh interval, are parameters of
that index and not of what ships.

Nothing about ceiling 1 or ceiling 2 changed. Placement is still cleared, and residency is still zero cluster
state bytes per gated creation, measured in bytes.

**Fixed in the same pass**: `_cluster/stats` had been silently omitting the gated population's field types
since 2026-08-08, because `894ac942432` registered `DescriptorBackedMappingStore` in place of the store the
plugin builds and left `IndexBackedMappingStatsAggregator` reading an index whose writer had just been
unregistered. `StatsProjectingMappingStore` restores it: descriptor authoritative, mapping index a
write-behind projection kept only so the aggregate stays one search.

**Two detectors worth reusing, both of which found real defects in one pass:**

- *Which arguments does a method never read?* `DescriptorGate.install` had been accepting the mapping store
  and ignoring it since T58. The same sweep found
  `MetadataUpdateSettingsService.updateGatedSettings` never reads its request either -- a settings update on
  a gated index validates the caller's settings, applies none of them, and answers `acknowledged: true`.
  Pinned, not fixed: `IndexDescriptor` has nowhere to put settings, so the fix is either to refuse the request
  or to carry settings on the descriptor as mappings now are, and that is a contract decision.
- *Does the test fixture build what the plugin builds?* Every mapping-stats test hand-built its own
  `IndexBackedMappingStore`, so the suite stayed green against a store production did not register. The shared
  fixture now composes the store the way `ServerlessStoragePlugin` does.

### Pins expire, so an abandoned one stops holding storage

Two-phase, which is what makes it work rather than merely exist. An index-wide pin puts every shard down
with a ten-minute expiry, and only once every shard is pinned does a second pass make them permanent. A
coordinator that stops halfway now leaves pins that lapse on their own, where before it left pins holding
generations against garbage collection forever -- rollback only ever ran when a shard *failed*, never when
the coordinator itself went away. The failure mode inverts, and inverts the right way for a mechanism whose
only job is to prevent deletion.

Confirming re-stamps the expiry rather than re-pinning, deliberately: a shard that committed between the two
phases would otherwise be re-pinned at a later generation, quietly turning one point in time into two.

**Expiry is honoured in the one place it can be.** `getPinnedManifestIds` is the only question garbage
collection asks of the registry, so a lapsed pin simply stops answering it and what it held becomes
collectable with nobody removing the record -- the record stays, so a sweep can still say who left it. The
instant is a parameter, not a clock read, so one sweep judges every pin against one moment and the behaviour
can be asserted without waiting for wall time.

**Two things this turned up.**

- **The register format is durable, so it needed a version.** Pins on disk were written before pins had an
  owner or an expiry, and reading five fields out of a three-field record does not fail cleanly: it reads
  into the next record and produces nonsense, which for the registry deciding what may be deleted is the
  worst available outcome. A marker distinguishes the shapes, and old pins are read as never expiring --
  the only safe reading of a pin taken when nothing could expire.
- **The CAS path discarded the change.** `mutate` skipped writing when the new set `.equals` the old, and
  `PinRecord`'s equality is its *identity* -- pin id, term, generation -- excluding owner and expiry so that
  adding the same pin twice is the documented no-op. So a set whose expiries had been re-stamped was equal
  to the set before it, and confirming a pin silently did nothing. Now identity, not equality: every
  mutation already returns `current` itself when it means no-op, so the check is both sufficient and exact.

Still open: nothing yet sweeps the ledgers of pins that lapsed, so an abandoned pin stops *holding* storage
but its record remains until something removes it.

### Pins have a ledger now, so releasing one is retryable

A pin is written per shard, so an index-wide pin is N pins under one id, and nothing recorded that they were
one thing. Three consequences of the same gap: release walked the index's *current* shard count rather than
the count the pin was taken across; a release that failed halfway left pins with nothing naming them; and a
coordinator that died mid-fan-out leaked pins nobody could enumerate, holding generations against GC
forever.

Core solved this from the other direction for remote-store shallow copy, and the ledger is deliberately the
same shape: there every lock on a remote segment file is paired with a `shallow-snap-<uuid>` blob in the
repository, and snapshot deletion reads that blob to learn which locks to release -- **releasing first and
deleting the record only afterwards**, so a failed release leaves the record for the next attempt. Their
repository entry is the ledger. A pin taken through `_snapshot_pin` never goes near a repository, which is
exactly what makes it cheap, so ours has to be written on purpose.

`PinLedger` records the pin id, index, uuid and the shard count it covered, written **before** any pin is
taken -- a record of intent, not of completion, because a ledger written afterwards would not exist for the
case it is meant to cover. Release reads it, releases what it names, and deletes it last.

Asserted by interrupting a release halfway and requiring a retry to finish the job; by releasing twice and
requiring both to succeed; and -- the one that stops the others passing vacuously -- by rewriting the ledger
to name fewer shards than were pinned and requiring the release to honour the record rather than the index's
current shard count.

Deliberately not carried: the pinned generations, which the pins themselves already state and release does
not need. Still open: pins have no owner or TTL, so an abandoned ledger is discoverable but not yet swept,
and the ledger lives in shard 0's container because that is the only per-index location the plugin exposes.

### A standard snapshot of a serverless index works: copied, finalized, restored by core

The headline question of round 006, answered by building it. `DeepSnapshotOrchestrationIT` takes a
serverless index's published manifest, opens a `LazyBundleDirectory` over it -- no live shard, no engine, no
local Lucene directory -- wraps it in a `Store`, hands the commit to core's existing
`Repository#snapshotShard`, finalizes, and then **restores the result through the ordinary `_restore` API**
under a new name. All 25 documents come back, built from the repository's own copy of the bytes.

That is an independent copy, in the standard repository format, restorable by a cluster that has never heard
of this plugin -- with no new `BlobContainer` SPI, no repack format, no new restore path and no temporary
disk. The design writeup had declined this as "new infrastructure from scratch" on the grounds that
`BlobContainer` has no `copyBlob` and no plugin uses server-side copy; that objection is about an
optimization, and once the node moves the bytes it does not apply.

Two things the building taught:

- **`Store#getMetadata` asserts the commit's directory is identity-equal to the store's own.** The commit
  has to be listed from `store.directory()`, not from the lazy directory underneath. A good assertion: it is
  what stops a snapshot copying files from one directory while describing another.
- **The copy distributes; the finalization does not.** `finalizeSnapshot` submits a cluster state update, so
  it fails `NotClusterManagerException` anywhere else, while the copy ran happily on a data node. A deep
  snapshot is therefore one cluster-manager operation per snapshot with all the byte movement off it --
  the right shape for a branch whose whole project was removing per-index cluster-manager work.

Preceded by `BundleBackedCommitIsCopyableTests`, which proved the assumption everything rested on: a commit
opened over bundles reads end to end, file by file, checksums verified -- 20 files and 12,953 bytes matching
the local commit exactly, so nothing is copied that the commit does not name and nothing it names is missed.
That is the opposite access pattern to the one the lazy directory was built for, and nothing else exercised
it.

**What is left is a shipped action rather than a question**: pin, copy per shard, finalize on the cluster
manager, release; plus the deep-versus-shallow setting.

### Restore to a time works, and a restore does not survive reopening

Round 006 item 2. `PitrRestoreResolution` answers which generation was current at an instant -- the newest
manifest created at or before it -- and `restore_to` carries an instant through both REST routes and both
request levels. The retention half already kept the data for any instant in the window alive; nothing could
ask for one, so the state at 14:32 was on disk and unreachable. Both refusals are wired and tested: an
instant older than every surviving generation is refused naming how far back the index *can* go, rather than
rounded up to the oldest; and a generation nothing pins is refused, because pointing the head at blobs GC
may reclaim is a corruption with a delay on it. The index-level path resolves per shard, twice, because
shards have their own commit histories -- one instant is one generation per shard, not one across the index.

**Proven**: after a restore to an instant between two commits, the durable head sits at the generation that
answers for that instant, asserted against the manifest list rather than a number written into the test.

**Found**: a restore does not survive reopening the index. The writes past the restore point come back. The
likelier cause is WAL replay, which exists to reapply writes a manifest does not yet carry and cannot tell
"not yet published" from "deliberately rolled back"; the alternative is that opening resolves the newest
manifest rather than the head. The distinguishing measurement is whether the reopened shard's first new
manifest descends from the restored generation or the latest one. `AwaitsFix` rather than explained, because
which of the two it is changes the fix entirely.

The existing restore coverage could not have caught this: `ServerlessStorageIndexSnapshotActionIT` asserts
the head record and never reopens the index, so a restore undone by recovery looks exactly like one that
holds. **This now outranks the rest of the round** -- a restore that recovery undoes is not a restore.

### Round 006: a restore survives reopening now, and that was the gate on the whole time machine

The `AwaitsFix` that outranked everything else in the round is gone. A restore used to be undone by the
next recovery, which meant a restore was not a restore whichever cause it turned out to be.

**The cause was not the one the round guessed.** It guessed WAL replay. What actually carries it is the
local store: reopening on a node that still holds the pre-restore Lucene files recovers from those files and
never consults the object store at all, because `recoverMissingLocalStore` only runs when there is *no*
readable local commit. `EngineFactory#localStoreIsStale` is the new seam — an engine whose authority lives
in the object store can say its local copy is out of date, and `StoreRecovery` cleans and re-materialises.
Core already did exactly this for a revived in-place-merge parent a few lines below the new branch, so this
generalises an existing precedent rather than inventing one.

**A restore also stopped rewinding the head.** It publishes a new generation carrying the target's segments
(`RestoreManifestSynthesis`), so it moves forward like every other publication: head generations stay
monotonic, a restore becomes a point on the timeline that can itself be restored past, and GC keeps the
restored files because the live head manifest names them directly rather than depending on the pin
outliving the restore. Three existing assertions that read "the head generation equals the target
generation" now assert the head names the target's *files*, which is the stronger claim.

**One half is reasoned and not proven, and it is labelled that way.** Taking the newest manifest's WAL
position rather than the target's is correct by construction, but reverting that single field leaves the
reopen test green even with WAL mirroring on — and so does leaving the write unflushed so it lives only in
the WAL. The new mirroring-on test carries a guard that fails if mirroring silently is not on, and its
javadoc says outright that it does not isolate that field. Measured in both directions rather than claimed.

**Closing a gated index is two-thirds fixed and still failing.** `DescriptorChange` carries `CLOSED` and the
tailer asks `releasesShard()` rather than `!live()`. That changed nothing, which surfaced the real second
layer: the `updateGated` path — which is the path a close takes — appended no change of any kind, so no
entry existed for the new kind to travel in. It records one now, which also stops mapping updates through
that path leaving other nodes' caches stale. The third layer is that the harness installs no change log at
all; whether the release then holds or the shard is reopened on demand has **not** been measured, and the
marker says so.

**Also landed**: the wildcard-snapshot omission now warns (all four spellings of "everything", one test
each), and `design/durability-posture.md` says what each of the three tiers insures against, that a pin
protects only against our own GC, and that fleet-wide physical DR is object-store configuration and not code
here. The snapshot page's "considered, not built" section is corrected — the byte-copying variant is proven
end to end, and the objection it recorded was always to the server-side-copy optimization, not to the
operation.

**Still open, and named in the plan**: the deep snapshot's shipped action (proof exists, API does not); the
pin-ledger sweep, deliberately not half-built because it needs a per-index container resolver and a sweep
run from shard 0 alone would delete the only record of pins still held on other shards.

### Round 006 opened: durability. Close was a label, and finding that out was the round's first result

The plan is `docs/rounds/006-durability/plan.md`. Three tiers — pin, pointer snapshot, independent copy —
insuring against three different failures, with the load-bearing judgment that **fleet-wide physical DR
belongs to the object store** (versioning, deny-delete, cross-region replication) and not to a per-index
snapshot API, because a per-index deep copy across a hundred million indices is O(N) cluster work for a
fleet whose whole premise is that per-index work was removed.

**Landed.** The three index-level pin actions resolve through `AbsentIndexDescriptorSuppliers
.metadataOrDescriptor`, so pin and release now work on a gated index — asserted, where before they failed
`IndexNotFoundException` for an index that was serving traffic.

**What that turned up, which matters more.** Restore-in-place refuses while a writer lease is held, and the
ordinary way to release one is to close the index. Closing a *gated* index did nothing at all:

- `IndexDescriptor.toIndexMetadata()` never set the state, so every reader that synthesised metadata saw
  `OPEN` whatever the descriptor said.
- `IndexNameExpressionResolver`'s gated branch returns a concrete index and continues **before**
  `shouldTrackConcreteIndex`, the one place that refuses a closed index.

So `closeGatedIndices` wrote `descriptor.withState(CLOSE)`, answered acknowledged, and the index went on
accepting writes and answering searches while an ordinary one refused both with `IndexClosedException`. The
test that covered gated close asserted the descriptor's own state — the label it had just written — which
is exactly why a label was all it was. **Both fixed**, and `GatedShallowSnapshotIT` now measures a closed
gated index against a closed ordinary one rather than against a written-down expectation.

**One difference survives and is asserted rather than papered over.** An ordinary close is acknowledged once
its cluster state update is applied; a gated close writes the descriptor and acknowledges, and other nodes
learn of it by tailing the change log. A gated close therefore *converges* rather than arriving, and a
client that closes and immediately writes can still be served. Found by the test passing alone and failing
inside a loaded full suite — the window, not flakiness.

**A miss in the namespace migration, found by this round and fixed.** `ComputedPlacementColdStartIT`
creates about a thousand background indices named `cold-start-background-%06d` carrying the storage setting.
The migration probe never recorded them, so they were left outside the namespace and quietly became a
thousand *ordinary* indices — which is both the wrong thing to measure cold start against and enough load to
time the cluster manager out. It surfaced as `Too many open files` inside the full suite and as
`ClusterManagerNotDiscoveredException` alone, neither of which points at a renaming. Five more were found by
a static sweep of every test file that enables the setting, checked one at a time against whether being
ordinary changes what the test measures: `cold-start-probe`, `drain-`, `gated-mixed`, `chaos-failover-target`
and the two `prewarm-target`s. The deliberate controls — `gated-refused`, `gated-aliased`, `mixed-ordinary`,
and the resize sources, which a namespaced index may not be — were left alone.

The lesson for the probe method, which was otherwise good: a probe records what ran, and a test that fails
early records nothing. It needs a static sweep beside it, not instead of it.

**Still open, and it blocks the restore round trip.** The shard is not released on close, so the writer
lease survives. `DescriptorChange` carries `(name, uuid, kind, atMillis)` and no state, so the tailer cannot
tell a close from a mapping update and releases shards only for deletions. The fix is either a persisted
change-log format carrying state or a descriptor read per update change — a decision about a format that
lives on the object store. The round-trip test is `AwaitsFix` against it rather than weakened, on the same
grounds the name-collision test was.

### Shallow snapshot exists and works; it just cannot name a gated index

Asked whether a shallow snapshot or an Aurora-style time machine is possible, the answer turned out to be
mostly built already, which is worth writing down before anyone designs it again.

**What is there.** `_snapshot_pin` writes a durable pin naming a manifest generation and copies nothing --
the bundles that generation refers to are already in the object store, which is exactly what "shallow"
means. `_snapshot_restore` compare-and-swaps the shard head back to a pinned generation and refuses while a
writer lease is active. `_snapshot_release` drops the pin so GC can reclaim. Index-level wrappers fan all
three across every shard, all-or-nothing, and roll back partial pins. `PitrRetentionPolicy` decides *when* a
pin is required: every manifest created inside the window, plus the last one before it, so any instant in
the window has something to resolve to. Five integration tests cover it.

**Gap 1, measured.** None of it reaches a gated index. `TransportIndexSnapshotPinAction` and its restore and
release siblings resolve the index name through `clusterService.state().metadata().index(name)`, which is
null for a gated index, so the call fails `IndexNotFoundException[no such index [serverless_pin-me]]` for an
index that exists, is serving traffic, and has manifests to pin. The shard-level actions underneath take a
uuid and a shard id and never consult cluster state, so the mechanism is indifferent to gating -- only the
name-to-uuid step is not. `GatedShallowSnapshotIT` pins both halves: the gated failure and the identical
call succeeding against an ungated serverless index.

**Gap 2.** `serverless_storage.pitr_window` defaults to `-1`, which disables PITR entirely. The machinery
runs only when someone turns it on.

**Gap 3, the one that is actually missing rather than misrouted.** Nothing resolves a *timestamp* to a
manifest. `CommitManifest.createdAtMillis` exists and is read only by retention policies deciding what to
keep; the restore request carries a pin id and no time, and picks the highest generation carrying that id.
So the data for any instant in the window survives, and there is no way to ask for an instant. Sub-commit
precision is a further step: `WalRecord` carries `(indexUuid, shardId, primaryTerm, seqNo)` and no
timestamp, so replay can stop at a sequence number but not at a wall-clock second without a time-to-seqNo
index.

**Shape of the work, smallest first**: teach the three index-level actions to resolve through
`AbsentIndexDescriptorSuppliers` (afternoon); add `restore_to` resolving a timestamp to the newest manifest
at or before it (small, the inputs are all present); restore into a *new* index rather than in place, which
is the Aurora clone shape and can compose the existing `ShardCloner` (medium); per-record WAL timestamps or
a periodic time-to-seqNo index for second-granularity (medium). **A question for a human: which of these,
and in what order.**

### Snapshot does not cover the gated fleet, and the way it does not is silent

Asked whether snapshot works, measured rather than read. Two behaviours, and the second is the one that
matters:

- **By name**, a snapshot of a gated index is **refused** with an honest error ("metadata is not stored in
  cluster state for these indices"). That refusal was deliberate, and it replaced a false message
  ("Indices don't have primary shards") -- the index does have primary shards serving live traffic.
- **A snapshot of everything succeeds and silently omits them.** Measured across all four spellings an
  operator would use -- no indices set, `*`, `_all`, and a matching prefix wildcard -- every one reported
  `SUCCESS` with only the ordinary index in `SnapshotInfo.indices()`. The by-name refusal is never reached,
  because the expansion snapshot performs resolves against cluster state, where a gated index is not.

**The consequence, stated at the mission's scale.** One ordinary index and a hundred million gated ones give
a green nightly backup covering one index. Nothing in the response reports how many were skipped; the
omission is visible only to someone who counts `SnapshotInfo.indices()` against what they believe they have.
A backup that reports success while covering nothing is worse than one that fails, because only the second
gets investigated.

`ServerlessStorageGatedIndexSnapshotIT` pins all of it, including the four spellings together, so a fix that
closed one of them would not look like a fix for the rest.

**Not fixed, and the two options are different sizes.** Actually capturing a gated index means teaching the
snapshot and repository pipeline to read shard state through `AbsentIndexDescriptorSuppliers` and
`AbsentIndexRoutingSuppliers` instead of `Metadata` and `RoutingTable` -- the materially larger change that
file already declines. Making the silence audible -- reporting the count of skipped gated indices on the
response -- is cheap and is a product decision, not a test's to make. **A question for a human, and the first
one on this list that is about data protection rather than throughput.**

### Two of the four document operations had never been run against a gated index

Asked whether a gated index can be indexed, updated, deleted and searched like a normal one, the suite could
not answer. A survey of every integration test found **no test had ever issued a document update or a
document delete against a gated index**: every `prepareUpdate` in the suite was `prepareUpdateSettings`, and
every `prepareDelete` was `admin().indices().prepareDelete`, which deletes the index. Both looked covered
from a file listing, which is why the survey had to read the call and not the name.

`GatedDocumentLifecycleIT` runs the whole lifecycle -- index, realtime get, partial-document update, upsert,
optimistic-concurrency conflict, a bulk mixing index/update/delete, document delete, delete-missing, refresh,
search, and a phrase query proving the updated value was reindexed rather than merely stored -- against a
gated index **and an ordinary index in the same cluster**, and asserts the two observation lists are equal
step for step. "Like a normal index" is a comparison, so the test makes the comparison rather than encoding
what normal is believed to be.

All sixteen observations match. Nothing was broken; what was missing was the ability to say so. The one
assertion that had to be tightened was in the test itself: a `match` query on an analysed field answered 2
on both planes, passing while asserting nothing, because `second-updated` and `third-updated` share a token.

### The name decides: `serverless_`

An index whose name begins with `serverless_` is a serverless index. The prefix is the declaration, and
`DescriptorOnlyCreation.namesAServerlessIndex` is the whole of the rule.

**Why a name and not a path.** The alternative on the table was a distinct REST path (`/serverless/{index}/...`)
for the index and search roads. A path is a bigger change to a bigger surface -- every client, every SDK, every
`_bulk` body line addressing an index by name, and the internal callers that build requests without going
through REST at all -- and it moves the declaration to the *request* when the property being declared belongs
to the *index*. A name travels with the index through every API that already exists, costs nothing at runtime,
and is visible in a log line.

**What it buys, in order of how much it matters:**

- *Admission stops depending on template resolution.* `certainlyGated` previously had to resolve templates on
  the request path to find out whether the gated setting would be contributed, and then resolve them again to
  find out whether a template's alias would make the finished index non-representable. The name answers the
  first question before anything is read.
- *The refusals become knowable to the caller.* An index in the namespace may not carry an alias, a context, a
  data-stream name, or a resize source. `validateServerlessNamespace` refuses those at `validate`, with a
  message naming the conflict, instead of the creation being quietly admitted and then declined onto a road
  that only works on the cluster manager.
- **B2 is answered by implication.** The blocked question was whether a gated index could carry a
  post-creation-mutable alias or data-stream membership. Under the namespace it cannot, by construction and
  visibly: the creation is refused rather than half-supported. `GatedAutoCreateAndRolloverIT` lost its rollover
  arm for exactly this reason -- a rollover target needs an alias -- and kept the auto-creation arm, which
  needs neither. That is the "gating stays permanently scoped" branch of B2, chosen and now enforced at the
  door rather than discovered at the wall.

**Finished on 2026-08-15: the name is now the only way in.** `DescriptorGate::gatable` requires the namespace,
`clusterStateCreateIndex` refuses a namespaced name a cluster state entry, and the settings-based admission
road is gone -- both predicates, the template-merged settings computation, the admission template cache, the
fallback onto the ordinary path, and the two test classes that covered them. What replaced roughly two hundred
lines of inference is a string comparison.

**The setting keeps its other meaning and only that one.** It still selects serverless storage, the lazy
directory and computed placement, which is what a data stream backing index or an alias-bearing index needs
and can have without being gated. What it no longer does is decide whether an index has a cluster state entry.

**What the migration cost, measured rather than estimated.** A temporary probe in the gate recorded every
index that would have been gated under the old rule across a full integration run: **128 names in 40 files**,
against an estimate of 74 across 63. Guessing the list from naming conventions would have missed the
generated families (`gated-batch-NNN`, `e2e-tenant-NNN`) and over-renamed the deliberate controls
(`gated-refused`, `gated-aliased`), which are indices that carry the setting and must *not* be gated.

**The finding: gating and placement had drifted apart, and the shape they made was a hang.** An index is
gated exactly when its placement is computed — a gated index has no cluster state entry and so can have no
published routing table, and an index whose routing is unpublished has nothing else to place it.
`DescriptorGate`'s header has said so since C5. Moving gating to the name and leaving computed placement on
the setting broke that agreement: an index carrying `index.serverless_storage.enabled` outside the namespace
got a cluster state entry **and** unpublished routing. The ordinary allocator skipped it, having no routing
entry to allocate; the on-demand opening path skipped it, not being gated. Its shards were placed by nobody.

A write to such an index does not fail. It retries — correctly, because a shard that has not opened yet and a
shard that never will look identical to a write — so this surfaced as one unrelated test taking twenty
minutes and reporting `no such index`, which reads as a test being flaky. `ComputedPlacementGate.
placementIsComputed` now reads the name exactly as `gatable` does, so the two cannot drift again, and
`ServerlessStorageWithoutGatingIT` asserts the pair: an ungated serverless index is placed and servable
within a bounded time, a gated one still has neither entry nor published routing. Verified by reverting the
one line and watching the test fail on the routing assertion.

Worth noting the direction: this was **latent before the namespace**, not created by it. Any index that
carried the setting and was refused by `DescriptorRepresentable` — an alias-bearing index, a data stream
backing index — already landed in that shape. No test wrote to one, so nothing saw it.

**A second defect fell out of the deletion.** `TransportRolloverAction` had a branch that computed a rollover off
the cluster state thread when the create request's settings said gated, and discarded the cluster state it
computed -- correct for a gated target, which publishes nothing. But the condition read the *setting*, which
an index can carry without being gated: a data stream backing index, or any alias-bearing index on serverless
storage. For those, the rollover ran, threw away its result, and answered acknowledged. A rollover that
silently did nothing. Nothing measured it because every test exercising that path used indices that really
were gated. The branch is now deleted rather than fixed: a rollover target needs the alias it is rolled over
by, an index in the namespace may not carry one, so no rollover target can be gated. B2, applied.

## Position

**Correction found by grounding this file against the real tree (2026-08-12): everything below
this notice was accurate as of 2026-08-08 but the tree has moved on without this file being kept
in sync.** `git grep -hoE '\bT[0-9]{1,3}\b' -- server/src plugins/serverless-storage/src | sort -u
-V | tail -3` now returns **T104**, not T59 — the commit log shows task-numbered commits through
**T106** (`git log --oneline --all | grep -E '^[a-f0-9]+ T[0-9]+:'`), most landed in a burst on
2026-08-08/09 after this file's own "Updated" timestamp. T50 through T59 (this round's own plan)
are now: T50 done (third firing, commit 66f116a2c72), T51 blocked as B2 (unchanged, see below),
**T52 done too** (commit `4d53befd13e`, "Align admission template resolution for gated indices" --
landed without a "T52:" commit-message prefix, which is why a first pass grepping for that literal
string called it still open; the code comment above `settingsForAdmission` and the real
`AdmissionTemplateResolutionTests` -- 4/4 passing on a fresh run -- both cite T52 by name and cover
exactly this round's stated acceptance criteria: resize-target and system-index non-admission, and
data-stream-name resolution), T53–T58 done (commits 37bba504936,
6e848375d94, 803c6086100, 71f9c2da732, b6c2787d055, 3109191f655 and neighbors), T59 done as
recorded below. **T60 through T106 have no round directory under `docs/rounds/` at all** — only
`004-mapping-write-path/` and `005-store-failure-modes/` exist on disk, so whatever plan drove
T60–T106 (topically: `DescriptorCache`, `MappingGenerationStore`/`IndexBackedMappingStore`,
`TombstoneScrubber`, `DescriptorCheckTool`, `DescriptorEnumerator`, `IndexDescriptor` — all Area-H
descriptor/mapping-store work, consistent with round 005's own theme) was never committed as a
round plan/log/retro the way rounds 004–005 were. Before trusting the "Position" table below,
re-run the `git grep` above and read `git log --oneline -80` yourself — do not propagate this
table's numbers forward again without checking, which is the exact failure this correction is
fixing.

| | |
|---|---|
| Active round | 005, the doors into the mapping store ([plan](005-store-failure-modes/plan.md)) — as of 2026-08-08; stale per the correction above, T60+ is unaccounted for by this round's own plan |
| Next action | *(stale, see correction above)* was: `/round-next`, T51 through T58 open and unblocked; T52–T58 are all now done, T51 is blocked (B2) |
| Last task | *(stale, see correction above)* was T59 (commit 66a38381239); the tree's actual last task-numbered commit is **T106** (`2a623d36cf4`, "Add safePrefix null check in DescriptorEnumerator.expandPrefix") — but see the note above that T60–T106 have no corresponding round plan on disk to check off against |
| T51 attempt | Two firings, neither committed. First: refuted (disabled T49's tripwire instead of fixing it). Second: investigated, stopped deliberately without writing code — the acceptance criteria as expanded require a *gated* rollover/data-stream target's alias or backing-index membership to be recorded correctly, which traces to genuinely new machinery (alias mutation and data-stream resolution for indices with no cluster-state entry), not a bounded fix. Blocked as B2. Full write-up in [log.md](005-store-failure-modes/log.md#t51--second-firing-investigated-stopped-without-committing). |
| Branch | `feature/serverless` |

Rounds 001 to 003 predate this file and have no round directories. Their work is in the git
history and in [rfc-100m-index-architecture.md](../../rfc-100m-index-architecture.md).

**T-numbers are cited in permanent code comments, so check the tree before allocating them.**
This file used to say they ran to T22, which was wrong: the tree cites up to T39, and round 004
was planned as T23 to T32 on the strength of that sentence. Eight of those ten numbers already
belonged to earlier work, so the round was renumbered to T40 to T49 after T45 landed. To find the
next free number, do not trust this paragraph either:

```
git grep -hoE '\bT[0-9]{1,3}\b' -- server/src plugins/serverless-storage/src | sort -u -V | tail -3
```

## Progress

The 100M argument has no structural gap left: records live outside cluster state, creation
and deletion are off the serialized thread, resolution answers instead of silently
emptying, residency is bounded by construction, and mappings are carried.

Measured, with controls:

| | |
|---|---|
| heap per open gated index | 150,888 B |
| file descriptors per open gated index | 3.0 |
| open gated indices per node, 31 GiB heap | ~110,000 |
| creates per second, no declared mapping | 10,505 |
| creates per second, with a declared mapping | 2x to 10x slower, by warm-up; at least ~80% of it the mapping store. T41 removed one of its two round trips; T42 could not size that cleanly |
| deletes per second | 646 |
| eviction under CPU pressure (2026-08-12/13, see note) | quiet 17.2-17.5/s, loaded (20 burner threads) 18.5-19.2/s |

Two cycles running, most of the value came from defects found while doing something else rather
than from work that was planned. Round 004's planned work was one measurement and one wasted round
trip; what it found was a mapping store that reported every failure as "no fields", a shared index
nothing ever deleted from, a template-gated creation writing from the cluster state thread, and a
measurement class that had been leaking its whole population on every run. The cycle before it
(T13 to T21) went the same way: field parameters silently dropped from gated mappings, two blocking
round trips on the cluster manager update thread, and a headline throughput figure that only
held for a population nobody would create.

## Carried out of round 004

All of it is planned as round 005, T50 to T58. The two that matter most: a document indexed into a
name matching a gated template still reaches the mapping write on the cluster state thread (T51), and
a lost mapping index is refused by reads and quietly rebuilt at cluster defaults by writes (T54).

**Eviction under load: now measured for real (2026-08-12/13), not deferred a third time.**
`GatedEvictionUnderPressureIT` existed but had never actually been run -- both its tests are gated
behind `-Dtests.pressure=true`, which nothing had ever set. Ran it twice with a real JDK:

```
./gradlew :plugins:serverless-storage:internalClusterTest \
    --tests '*GatedEvictionUnderPressureIT*' -Dtests.pressure=true
```

| run | quiet drain (100 indices) | loaded drain (20 burner threads) |
|---|---|---|
| 1 | 5,809 ms, 17.2/s | 5,206 ms, 19.2/s |
| 2 | 5,712 ms, 17.5/s | 5,413 ms, 18.5/s |

**The finding: at this population (100) the eviction sweep itself shows no measurable slowdown
under synthetic CPU pressure** -- loaded and quiet drain rates are within noise of each other
across both runs, not the order-of-magnitude collapse `GatedIdleEvictionIT`'s own residency-peak
finding (20/200 quiet vs 148/200 "under an unrelated build") made plausible. This directly answers
the RFC's first order-of-work item for the first time with real numbers rather than carrying
"unmeasured" forward a third time.

**Two things this does not settle, so the item is measured, not closed:** (1) `GatedIdleEvictionIT`'s
own finding was about resident *peak* under real incidental contention (an unrelated build sharing
the box), a different signal from this test's synthetic burner-thread *drain rate* -- the two
should not be read as confirming or contradicting each other. (2) Run 2's `testHowFastAQuietNodeDrains`
took 2,768 s of total JUnit wall time for a 5.7 s measured drain -- the fill phase (creating and
populating 100 indices before the timed drain starts) is not what this test times, but something
outside the measured window varied by roughly 40x between runs on this shared, documented-as-noisy
box (see the Environment section below). Almost certainly host contention rather than a code
regression, given the actual timed metric stayed consistent across both runs, but not confirmed --
worth a controlled re-run on a quiet box before ruling out a real fill-phase issue. Also note the
table above's now-superseded "eviction under CPU pressure | 19.6 to 1.5 per second" row predates
this section calling the item "still unmeasured," which was already an internal inconsistency in
this file before this correction -- left visible in the table's history rather than silently
erased, but do not trust the 1.5/s figure's provenance without finding where it actually came from.

## Open, not blocked

Work that can start without asking anyone. Round 004 took the mapping cost item; T22 was
refiled as T40 in its plan. The rest are candidates for round 005:

- **Eviction under load**, the RFC's first order-of-work item. Since it was written the
  ceiling gained `indices.gated.max_open` and an eviction on the open path, which may have
  answered it. Whether it did is unmeasured.
- **T14: now measured at small scale (2026-08-13), not fully closed.** New
  `server/src/internalClusterTest/java/org/opensearch/cluster/service/PublicationLatencyVsClusterSizeIT`
  grows one real cluster (1, 3, 6 data nodes plus a dedicated cluster-manager) and times a single
  unbatched publication at each size, the same "guard against measuring nothing" discipline G1's own
  `PublicationLatencyMeasurementIT` established (asserts the task executed, the version advanced by
  exactly one, elapsed time is non-zero). Two runs: [21.79, 17.32, 25.00] ms and
  [20.19, 17.36, 16.42] ms for 1/3/6 data nodes respectively -- **flat within noise across this
  range, well under the 50-200 ms estimate, no scaling trend visible.** This narrows but does not
  close the item: 1-6 nodes is far short of the fleet sizes where fan-out cost would plausibly
  dominate over fixed per-publication overhead (serialization, local apply), so "flat at small N"
  is a real data point, not proof the same holds at hundreds of nodes. The original question --
  does wake/sleep need the same batching G1 already proved matters for publication *count* -- is
  still open; this only answers the latency-per-publication half, and only at small scale.
- **T16**: computed placement under hot-tenant skew. A hash cannot know one tenant takes a
  thousand times the traffic, and K=3 gives room to choose rather than solving it.
- Pre-warm before rotation. 12.4% of shards lose all warm candidates at fleet-doubling.
- Manifest sharding aligned with the routing hash, so a coordinator warms its whole
  partition in one read.
- **Leading wildcards (`*-logs`)**: Unsupported and explicitly restricted in the serverless API contract (removed from planned work; object storage prefix listings serve trailing `prefix*` patterns only).

## Blocked

Items needing a human decision. The loop skips these and continues with other work.

### B1. Fork or upstream contribution

Computed placement replaces OpenSearch's placement model for serverless indices rather than
optimising it. That is a hard sell upstream while being the obviously right answer for a
serverless-only system. The codec namespace question, reserving a high integer range versus
a distinct blob codec name, is downstream of this and straightforward either way.

Nothing in the current task list depends on the answer, so the loop can run without it.

### B2. Can a gated index carry a post-creation-mutable alias or data-stream membership at all — ANSWERED

**Answered 2026-08-15 by the `serverless_` namespace: no, and now visibly rather than by wall.** The second
branch below was taken. An index in the namespace may not carry an alias, a context, a data-stream name or a
resize source, and `validateServerlessNamespace` refuses such a creation at `validate` with a message naming
the conflict. `GatedAutoCreateAndRolloverIT` lost its rollover arm and kept its auto-creation arm, which is
exactly the shape the second branch predicted. The analysis below stands as the reason the answer is no; it is
kept because it is the argument, not because the question is still open.

T51 needs a gated rollover target's alias, and a gated data-stream target's backing-index
membership, recorded correctly, not just its mapping write made safe. Both traced to the same wall:
`DescriptorRepresentable` already refuses to gate any index with an alias, deliberately (T29) —
alias mutation is a cluster-state update that looks the index up in `Metadata`, a gated index is not
in `Metadata`, and a set-once alias that can never be repointed was rejected as a partial feature.
Baking a rollover alias in at the target's own creation only survives the *first* rollover of that
alias; the second rollover mutates the alias on what is by then a previous gated target, hitting the
same wall from the other side, and `MetadataIndexAliasesService` has no gated-index handling to
extend. Data-stream backing-index membership is worse: it lives in `Metadata.custom` as a list the
`metadataTransformer` appends to, that transformer never runs for a gated creation, and every other
reader of `DataStream.getIndices()` assumes `metadata.index(name)` resolves — teaching that
assumption to tolerate absence is the same generalization `AbsentIndexRoutingSuppliers` and
`AbsentIndexDescriptorSuppliers` already had to build for ordinary get/search/bulk, applied fresh to
a second, structurally different subsystem.

The question for a human: does gating extend to cover post-creation-mutable aliases and data-stream
membership (a genuine design/build effort, roughly the size of the routing work gating already
needed), or does gating stay permanently scoped to indices that will never need either — in which
case T51's auto-creation door (no alias, no data stream) can still be fixed on its own, and the
rollover/data-stream doors are declined from gating rather than fixed, closing the tripwire without
meeting the letter of "a gated rollover target's alias ... recorded correctly." Full trace in
[log.md](005-store-failure-modes/log.md#t51--second-firing-investigated-stopped-without-committing).

## Environment

- `-Dbuild.docker=false` on every Gradle invocation. Docker is not running, and a dead
  daemon fails every task including `compileJava`.
- The box is shared and often at load average 20 or higher. Timing-sensitive tests fail
  under contention and pass alone; the rule for telling that apart is in `/round-next`
  step 4.
- Do not kill the unrelated `cargo-mutants` and `cargo test` jobs belonging to
  `lucene-rust`.
