# Session handoff

Branch `feature/pluggable-engine-per-shard-role`, 237 commits ahead of its own remote and 946 ahead of
`origin/main`. **Nothing from this work is pushed.**

Measure it rather than incrementing it: `git rev-list --count origin/feature/pluggable-engine-per-shard-role..HEAD`.
The count carried in earlier versions of this file was wrong and was being bumped by hand each session.
Working tree clean.

**Build note that costs an hour if you do not know it.** `:distribution:docker` shells out to the
`docker` binary at *configuration* time, so when Docker is unavailable every Gradle task fails,
including `:server:compileJava`, with a message naming a project you are not building. Pass
`-Dbuild.docker=false`. Nothing in this area needs a container.

## Goal

100M indices, 3 to 100 shards each, with index/search separation and compute/storage separation.

Master plan: `plan-100m-index-implementation.md` (requirements, architecture, seven areas A through G).
Evidence for every number claimed: `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md` (S1 through S14).

## Where things stand

| area | state |
|---|---|
| **A. Name index tier** | **done**, `plan-area-a-name-index.md`. 100 unit tests plus `ServerlessStorageNameIndexIT`. Disabled by default. |
| **C. Computed placement** | **not blocked**, `plan-area-c-computed-placement.md`. C0 to C29 done. A computed index is creatable, writable, searchable, visible to cat and stats, and survives a full restart. What remains is reach rather than function: see the gaps below. |
| **G. Validation** | **G1 done**, S15. Publication latency measured, batching decided. G2 to G5 not started. |
| **F. Cluster state diet** | **F1 done**, S16. Audit says `inSyncAllocationIds` is emptiable and `primaryTerms` is not. F2 to F4 not started. |
| **H. Metadata off cluster state** | **in progress**, `plan-area-h-metadata-off-cluster-state.md`. Ceilings 2 and 3 measured as cleared on the gated path: creation flat at 0.0005 ms against 57.777 ms at 50k indices, and a thousand gated creations leave zero cluster state entries. Dynamic mappings are designed and proven in isolation (H6a to H6c) but **have no production caller yet**, which is the same shape as the two mechanisms C18 shipped correct and unreachable. |
| B, D, E | not started |

## The one finding that matters most: this seam fails by succeeding

Eight times now, a computed index has made an operation return a **confident empty answer rather than an
error**: refresh reaching no shards (C21), field mappings reporting no fields (C22), stats, segments,
recovery and force merge all reporting nothing (C23), cat listing no shard (C26), and the file cache
capacity check undercounting (C29). Not one threw.

Two of them are worse than a wrong answer. Force merge accepted an instruction, reported success, and did
no work at all. The capacity check in C29 fails **towards the damage**: a total that is too small admits a
restore that overflows the cache, and a total that is too small looks entirely normal.

**It caught the fix as well as the bug.** C26's first attempt walked metadata that the cluster state
response did not contain, so it returned a shorter list with nothing to indicate anything was missing.
When writing a fix here, ask what it does when its own input is empty.

This is why C21 survived six passes over the same seam while every other instance was found quickly: the
others failed loudly with a hang, an exception or a red cluster.

**The consequences for how to work here.** Never fire a request and assert only that it did not throw.
For every seam, find the number that is zero when it is broken and assert on that:
`getSuccessfulShards`, a document count, a token count, a field count. Six of the eight bugs in this area
would have been invisible to a test that only checked for exceptions.

**And what actually finds them.** Every hypothesis argued from reading the code was wrong; every probe
was right. C22 is the cleanest example: `TransportUpdateAction` was predicted to be the serious one and
turned out to be nearly unreachable, while the two call sites with no special reasoning attached were the
broken ones.

## What is done, and what each of them cost

- **C17** counting active shards resolves through the supplier, and health counts computed entries
  instead of reporting a false GREEN for a cluster with nothing placed.
- **C18** a computed index is created, its shard opens, enters primary mode in about three seconds, and
  takes a write. Five pieces of cluster-manager state had to become functions of the placement:
  allocation identity, the started transition, the in-sync set, the primary term, and the refusal to act
  on an unresolvable placement.
- **C19** the recovery source comes from the node rather than the placement function, because only the
  node knows whether it holds data.
- **C14** resharding rejects a computed index with a sentence saying why, instead of throwing
  `IndexNotFoundException`, dereferencing null, or hanging pending forever.
- **C20** state recovery no longer republishes routing for a computed index. It had been silently
  turning every computed index back into an ordinary one on recovery, which also turned out to be what
  stopped the cluster coming back at all after a restart.
- **C15** adaptive replica selection already ranks the computed candidates, verified rather than
  assumed, because a computed entry is an ordinary `IndexShardRoutingTable` and the ranking does not
  care where it came from.
- **C2** the node set is published, versioned and never shrinks, so a node that is merely restarting no
  longer takes its shards with it. Adding is automatic, removing is deliberate and unimplemented.
- **C13** done. A computed index survives a full restart with every document, on the same node, from its
  existing store. Verified over seven consecutive runs with fresh seeds. It needed no recovery work in the
  end: the documents had always been in the engine, and C21's refresh was what could not see them.
- **C21** refresh and flush resolve their shards, so a computed index can be made visible. This is the
  fix that unblocked C13, and every earlier theory about C13 was downstream of it.
- **C22** analyze, get field mappings, and the retried-update branch resolve. Bulk update was found to be
  already working and was left alone rather than converted for symmetry.
- **C23** the sweep operations resolve through a bulk counterpart to `resolve`, across nine callers.
  Stats, segments, recovery and force merge are proven; clear cache, upgrade, upgrade status, remote store
  stats and segment replication stats are converted for consistency and not individually covered.

- **C26** cat shards, paginated cat shards and cat allocation all show a computed shard now. Metadata is
  the index list, since routing structurally cannot name a computed index. Both cat callers request
  metadata only when a supplier is installed, so an ordinary cluster's response is unchanged.

- **C24** a broadcast that resolves no shards for an open index now asserts in CI and warns in
  production, from `TransportBroadcastByNodeAction` rather than from the resolver. The resolver was the
  proposed home and would have been useless: a check there only runs for callers that already resolve
  correctly, and the bug is a caller that does not.

- **C28** search-only scaling refuses a computed index with a reason, replacing a NullPointerException
  thrown on the cluster state applier thread and a scale-up that silently built a routing table with no
  trace of the index. Tiering already refused it; the refusal now names computed placement instead of
  claiming the index is red, and runs first because it is the only condition there an operator cannot act
  on.
- **C29** the file cache capacity check resolves, so a computed remote snapshot shard counts. This one
  failed towards the damage rather than away from it: an undercount admits a restore that overflows the
  cache.
- **C25** the membership maintainer has unit tests, including the self-removal that no integration test
  looks for.

## G1's answer, since it changes what to build next

Publication costs 6 to 13 ms on an idle small cluster and is paid **once per task** without an executor.
Batched, a hundred tasks collapse into two publications. Measured ratio at batch=100 is 19x to 54x.

**Done, S17.** `TransportReactivateShardsAction` and both `ShardSuspensionCoordinator` submissions now
use a `ClusterStateTaskExecutor`.

The starvation went with it: measured before and after in the same run, a NORMAL task behind 300 URGENT
tasks waited 2.5 to 2.7 seconds unbatched and 23 to 82 ms batched, across four runs. **No priority change
was needed**, and none was made. Suspension was not starving because URGENT outranks NORMAL but because
each wake held the queue for a whole publication.

The trap for anyone extending this: a batch reports one `clusterStateProcessed` to every task in it, so
"did I change anything" has to be recorded per task inside the executor. Without it, one real suspension
in a batch makes every task in that batch evict.

## F1's answer, which splits the plan's premise

**`primaryTerms` cannot be emptied.** `TransportReplicationAction` sends the metadata term with every
write and `IndicesClusterStateService` reads it to open a shard. C18 already hit `primary term must be
positive but was [0]` from that path. The plan assumed the shard head made it redundant; core does not
consult the shard head.

**`inSyncAllocationIds` can**, and is worth more than the plan assumed: 26% of index metadata at 3 shards
and **63% at 30**, because the cost is per shard rather than per index. Roughly a terabyte at 100M
indices at 30 shards.

Two readers needed probing rather than assuming, and both are safe:
`SegmentReplicationSourceService` unions the metadata set with the runtime tracker's, and
`ShardStateAction`'s stale-marking is a failure path whose promotion decision C18 already moved.

**C11, finally re-measured:** deferred metadata is 698 B/index at both 3 and 30 shards. Flat in shard
count, which is the property the architecture rests on and which had never been checked. Note the
consequence for F: the deferred path never materializes in-sync ids anyway, so F's saving applies to the
working set rather than to the 100M at rest.

## Area H, what is proven and what is only designed

Proven by measurement, both arms in the same run:

| claim | measured |
|---|---|
| creation independent of population | 0.0005 ms flat against 57.777 ms at 50k, 101,542x |
| gated indices leave nothing resident | a thousand creations, zero cluster state entries |
| descriptor point lookup flat | 0.78 ms at fifty thousand descriptors |
| wildcards viable | about 22 ms, population-independent |
| mapping CAS converges | eight concurrent inferences, eight fields survive |
| lazy refresh cheaper than broadcast | one read per hundred requests at the same generation |

All three caveats from the previous review have been closed, and one of them changed a number materially:

1. **The mapping mechanism now has a caller** (H7a). A gated index's mapping update records through
   `MappingGenerationStore` and makes no cluster state change. Proven by mutation: the first version of
   that test called the store directly and passed with the wiring disabled, which is the
   correct-and-unreachable failure this project has shipped twice.
2. **Point lookups are flat against shard count** (H7b, S23): 0.476 ms at one shard, 0.435 ms at sixty.
   The wildcard fan-out cost is not measurable up to sixty shards, which is weaker than saying it is
   absent at a hundred, and is recorded that way.
3. **Wildcards returning names cost 55x counting them** (H7c, S24): 5.9 ms against 327.5 ms at ten
   thousand matches. The previously recorded 22 ms understated resolution by roughly fifteen times.
   Wildcard resolution needs pagination in its contract or a bounded match set, at about 33 ms per
   thousand names.

**What is built but has no production caller.** Three mechanisms are proven and not yet load-bearing, and
the distinction matters because a test that exercises them directly passes whether or not they are wired.
That failure has shipped twice here (H7a, H8a).

- `MappingRefreshOnDemand` has no construction site outside tests. Lazy refresh is proven, not in use.
- `IndexDescriptor.suspendedShards` is written by no production path (H13), so suspension is node-local and
  volatile today: a full-cluster restart wakes every sleeping shard, which then sleep again on the next
  idle tick.
- The gated suspension registry has to be constructed and installed by the plugin's node setup, which is
  the last hop between H9d being correct and H9d being on.

## What is left

Nothing in Area C is known broken. What remains is unclaimed rather than pending:

- **Extend C24's guard beyond broadcast-by-node.** It protects that family only. The single-shard family
  that analyze and field mappings use, the cat callers, and `RestoreService` have no equivalent, so an
  unconverted caller in any of those is still silent.
- **A latent scaling bug, unrelated to this area.** `ScaleDownClusterStateUpdateTask.execute` catches
  `Exception` and returns `currentState`, so `clusterStateProcessed` sees an unchanged state and answers
  `AcknowledgedResponse(true)`. A failed scale-down reports success. Found during C28 and deliberately
  left alone, since fixing it is scope this area was not asked for.
- **C16** hot-tenant override, which needs a product answer first.

**What C24's guard does not cover.** It protects the broadcast-by-node family only. The single-shard
family that analyze and get field mappings use, the cat callers, and `RestoreService` have no equivalent,
so an unconverted caller in any of those is still silent. Extending the check to them is unclaimed work
rather than a decision against it.

## The defect class this pass kept finding: per-index state that moved rather than left

Two instances in one review sweep, neither found by a failing test, both the same shape. Area H removes
per-index state from cluster state, which creates pressure to cache it somewhere else, and the replacement
does not get audited the way the original did.

- **H11**, the mapping cache. `MappingRefreshOnDemand` held an entry with a full field map for every index
  a node had ever served, and never evicted. Per-node memory grew with indices *touched*, not indices
  active, and each entry is larger than the 698 B of deferred `IndexMetadata` it replaced.
- **H12**, the suspension registry. H9d argued it was bounded because it held only sleeping shards. That is
  true and the conclusion is backwards: under scale-to-zero most indices are asleep, so the map approaches
  one entry per index. The feature's defining property is what invalidated the bound, which is why the
  argument read as convincing when it was written.

Both are now fixed-capacity access-ordered caches. Access order is load-bearing rather than a detail:
scale-to-zero means most indices are cold, so an insertion-ordered cache lets any sweep over them flush the
working set on every pass. Mutation testing confirms it, since switching to insertion order kills exactly
the hot-versus-cold test in each suite and nothing else.

**Their eviction arguments are not the same, and conflating them would be a mistake.** Evicting a mapping
costs a refetch and cannot be wrong. Evicting a suspension wakes a shard, which is a real behaviour change,
acceptable only because of its direction: a shard wrongly awake serves requests, a shard wrongly asleep is
an outage, and the next tick re-suspends it.

**What to check next in this class**: any cache added when per-index state is removed from somewhere. The
sweep across Area H's own classes found no third instance, but the pattern is structural rather than
accidental.

## Three times a green suite measured nothing, and what stops the fourth

This is the failure mode of this area, and it has now happened three times.

1. C1 through C11 were thirty green unit tests against a mechanism that never ran. C12 found it.
2. `computedAwareInSyncIds` and `startComputedShardLocally` were committed correct and unreachable. A
   probe that printed nothing found it.
3. An edit stripped the registrations that make an index computed while failing to add the `@Before`
   meant to replace them. Three tests then passed while exercising an ordinary index, and C13 was
   declared done on that basis. It also manufactured a fake mystery: a recovery source moving from
   `EMPTY_STORE` to `EXISTING_STORE` that nothing could explain, because an ordinary index simply does
   that.

**The guard against the fourth:** `createComputedIndex` in both IT classes asserts that the index it
just created has no published routing entry. The premise cannot be silently lost. Keep that assertion in
anything new, and treat an unexplained result as a reason to distrust the test rather than as a
curiosity to record.

## Method that has actually worked

**Probe, do not reason.** Every layer of C18 was named by one log line, and four confident arguments
were wrong: the version gate, an empty in-sync set, "the shard never enters primary mode", and the
recovery-source mystery. **Ask the component that owns the property** rather than inferring from a
request that travels through it: a write test measures everything between client and engine, so when it
fails it names nothing, while `isPrimaryMode()` answers in three seconds.

**Watch for tests that pass by timing out into success.** C18's write assertion passed against its own
mutation until it asserted `shardsAcknowledged`; the write test passed two runs in three at exactly the
sixty second retry timeout. Both looked green.

## Key files

Core:
- `server/src/main/java/org/opensearch/cluster/routing/AbsentIndexRoutingSuppliers.java` — the seam,
  four registrations and two resolvers
- `server/src/main/java/org/opensearch/cluster/routing/ComputedShardRouting.java` — the two views of a
  computed shard, and the deterministic allocation id that lets them agree
- `server/src/main/java/org/opensearch/cluster/routing/OperationRouting.java` — two hooked call sites
- `server/src/main/java/org/opensearch/cluster/metadata/MetadataCreateIndexService.java` — publication skip
- `server/src/main/java/org/opensearch/indices/cluster/IndicesClusterStateService.java` — shard
  lifecycle for computed indices, and where the remaining C18 work is
- `server/src/main/java/org/opensearch/cluster/routing/RoutingNodes.java` — `localRoutingNode` hook

Tests to run first, both fast, and nothing is `@AwaitsFix` any more:
- `:server:internalClusterTest --tests "*ComputedPlacement*IT" -Dbuild.docker=false`, 5 suites
- `:server:test --tests "org.opensearch.cluster.routing.Computed*" -Dbuild.docker=false`

Plugin, `plugins/serverless-storage/src/main/java/org/opensearch/serverless/storage/`:
- `placement/` — `RendezvousShardPlacement`, `ComputedRoutingTable`, `ComputedPlacementGate`
- `nameindex/` — Area A, complete

Settings, both **off by default**:
- `serverless_storage.name_index.enabled`
- `serverless_storage.computed_placement.enabled`

## Standing constraints

- **Never `git add -A`.** Stage explicit paths. `spotlessApply` reformats unrelated files every time;
  stage yours, then `git restore` the rest.
- **Never amend.** New commits only.
- **Never push** without being asked.
- Never run two Gradle builds at once; it corrupts the shared build directory.
- Measure with `-da -dsa`. Gradle enables assertions and they cost about 2x.
- Detailed commit messages explaining *why*, not just what.
- Prose: no em-dashes, no rule-of-three padding, no ceremonial closers.

## How the work has been going, and it matters

**Every test must be shown to fail without its production change.** Mutation testing found the tombstone
bug, the chunking bug, the alias-rebuild bug, the forward/reversed divergence, and the ownership-gate
bug. All of them passed a green suite first.

**Prefer measuring to reasoning.** Reasoned figures were wrong three times: rebuild memory (16.8 GiB
reasoned, 108 GiB measured, and then the metric itself was wrong), the descriptor cost (18 GiB isolated,
65 GiB in a real `Metadata`), and the manifest shard count (64 by reasoning, 256 by measurement).

**Unit tests construct the state they assert against.** C1 through C11 were 30-odd green tests and none
could notice the mechanism never ran. One integration test invalidated conclusions from three tasks.
Prefer an integration test early over more unit tests.

## Traps that have already bitten

- `addAsRecovery` picks its recovery source from `inSyncAllocationIds`, so with those absent it silently
  chooses `EmptyStoreRecoverySource` and an index recovers **blank**. Build entries explicitly.
- `recoverySource` is cleared once a shard is STARTED. Assert it on unassigned shards or the assertion
  passes for the wrong reason.
- A structure derived from another is lossy until proven otherwise. Alias targets were dropped twice.
- Static registries leak across test suites. Clear them in `@After`.
- `:server:internalClusterTest` takes over 20 minutes. Run it in the background.

## Two mistakes from the C21 and C23 passes, both worth not repeating

**A retry that compiles is not a retry that runs.** `assertBusy` retries on `AssertionError` and lets
every other exception through. Wrapping a block that throws `SearchPhaseExecutionException` changes
nothing, and the run after that "fix" failed identically. Convert the exception inside the block.

**Re-running the seed that failed proves almost nothing.** It passed while the bug was fully present,
because the failure was a timing race rather than seed-determined. Fresh seeds found it. Three clean runs
minimum for anything involving a restart or a settling cluster.

**A no-op path must be identical, not equivalent.** C23's fast path called
`allShardsSatisfyingPredicate(indices, alwaysTrue)` where the caller had called `allShards(indices)`.
Same results, since the former implements the latter, and still wrong:
`TransportRemoteStoreStatsActionTests` stubs `allShards(String[])` on a spy, and the equivalent call
walked past the stub. Callers and tests bind to methods, not to behaviour.

**A mechanical sweep needs hand-reading.** Of nine call sites converted by regex in C23, two would have
been wrong: force merge would have silently lost its `ShardRouting::primary` filter and merged replicas.

## Still unscheduled

C16 hot-tenant override, which needs a product answer first.

## What this pass adds to "how the work has been going"

**Write the integration test before the code, not after.** C18's test failed five times and each
failure named the next requirement. Four of the five were invisible from reading the code, and one of
them produced no error message at all.

**A test that passes against the mutation is not a test.** C17's first create assertion used
`assertAcked` and survived the fix being reverted, because creation stopped hanging and started timing
out into an acknowledged response with `shardsAcknowledged=false`. Only the mutation run caught it.

**Some findings are events, not lines.** The START transition has to be driven by recovery completion
because on an idle cluster there is no next cluster state to act on. No amount of reading finds a
missing event.
