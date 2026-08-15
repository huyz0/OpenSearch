# STATE

Read this first, every time. It is the only source of truth for where work stands, and it
is written to survive context loss: nothing here depends on remembering a previous session.

Updated: 2026-08-15 (second correction below; the 2026-08-12 one and the body past it are unchanged)

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

### One name can be claimed in both planes, and it is not a race

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

**What it does not yet do, stated plainly.** `DescriptorGate::gatable` still accepts the *setting*, so an index
outside the namespace that carries `index.serverless_storage.enabled` is still gated. The namespace is exact
for admission and for distribution; it is not yet the only way in. Measured rather than assumed: making the
name authoritative for admission moved **3 of 222** integration tests, not the ~66 that would have moved had
the bottom gate required it too. So the name collision documented above stays open -- closing it by
construction means requiring the namespace at `gatable`, migrating 74 index names across roughly 63 test
files, and retiring the settings road. That is a decision, not a follow-up.

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
