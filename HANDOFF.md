# Session handoff

Branch `feature/pluggable-engine-per-shard-role`, 31 commits ahead of origin, **nothing pushed**.
Working tree clean apart from `docs-site/` which is already committed.

## Goal

100M indices, 3 to 100 shards each, with index/search separation and compute/storage separation.

Master plan: `plan-100m-index-implementation.md` (requirements, architecture, seven areas A through G).
Evidence for every number claimed: `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md` (S1 through S14).

## Where things stand

| area | state |
|---|---|
| **A. Name index tier** | **done**, `plan-area-a-name-index.md`. 100 unit tests plus `ServerlessStorageNameIndexIT`. Disabled by default. |
| **C. Computed placement** | **blocked on C18**, `plan-area-c-computed-placement.md`. C0 to C12 and C17 done. A computed index is creatable and its shard opens; it will not take a write. |
| B, D, E, F, G | not started |

## The blocker: C13, a restarted computed shard comes back empty

The cluster recovers now. What fails is the data: twenty documents indexed, zero found after a full
restart. `ComputedPlacementRestartIT` is `@AwaitsFix` and carries the account.

**Ruled out by probe, so do not spend time here.** The recovery source is correct.
`withNodeLocalRecoverySource` runs twice and is right both times: `hasData=false` at creation so the
shard stays `EMPTY_STORE`, `hasData=true` after restart so it becomes `EXISTING_STORE`. C19 works.

**Where to look.** The store opens and comes back empty, which points at history and translog rather
than placement. The suspects are the three things computed placement changes about a shard's identity:
the primary term synthesised at creation, the in-sync set derived from the placement, and the shard
starting itself locally without the cluster manager. Any of those can make recovery bootstrap a new
history instead of replaying the existing translog, and a new history on an existing store is a blank
index that looks perfectly healthy.

**Next step is a probe, not an argument.** `StoreRecovery.internalRecoverFromStore`: is the translog
replayed or a new history bootstrapped, and what are the global checkpoint and translog UUID on the way
in. Five confident arguments in this area have been wrong and five probes have each been decisive in a
single run.

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
- **C13** still open, and it is the blocker above.

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

Tests to run first, both fast:
- `:server:internalClusterTest --tests "*ComputedPlacement*IT"` — 4 pass, 1 passes, 1 `@AwaitsFix`
- `:server:test --tests "org.opensearch.cluster.routing.Computed*"`

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

## After C18

C13 node restart with a computed index. It is blocked on C18 rather than merely scheduled after it:
asking whether a computed index recovers its data is not a question you can ask while the index cannot
accept a document. When it unblocks, the recovery source is the thing to watch, because A5 was exactly
this and `ComputedShardRouting` makes the caller state it rather than derive it.

C14 resharding. C15 wire adaptive replica selection to rank the K candidates. C16 hot-tenant override,
which needs a product answer first.

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
