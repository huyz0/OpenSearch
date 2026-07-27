# Session handoff

Branch `feature/pluggable-engine-per-shard-role`, 185 commits ahead of its own remote and 894 ahead of
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
| **C. Computed placement** | **not blocked**, `plan-area-c-computed-placement.md`. C0 to C23 done. A computed index is creatable, writable, searchable, and survives a full restart. What remains is reach rather than function: see the gaps below. |
| B, D, E, F, G | not started |

## The one finding that matters most: this seam fails by succeeding

Seven times now, a computed index has made an operation return a **confident empty answer rather than an
error**: refresh reaching no shards (C21), field mappings reporting no fields (C22), stats, segments,
recovery and force merge all reporting nothing (C23), and cat listing no shard (C26). Not one threw.
Force merge is the worst of them, because it accepted an instruction, reported success, and did no work
at all.

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

## What is left, in priority order
- **C27** three more bulk callers that pass an explicit index list, so directly convertible.
- **C28** search-only scaling and tiering read routing directly and one of them dereferences without a
  null check, so a computed index is an NPE rather than a refusal. Probably the C14 answer: reject with a
  reason rather than half-support.
- **C25** `ComputedPlacementMembershipService` has no unit tests at all.

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
