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

## The blocker: C2, the node set is not stable, so a restart moves shards off their data

**A computed index is not safe across a restart, and the cause is placement rather than recovery.**
Placement is computed against the data nodes visible in the cluster state at that instant. During a
restart that list grows as nodes rejoin, ownership moves, and the node that gains a shard has none of
its data. It recovers empty, correctly by its own lights. Different timing gives different symptoms:
one run returns zero documents, the next cannot find the shard at all.

C2 named this before any code was written: two coordinators computing against different node lists
produce different placement, so the node set is a correctness input rather than a convenience. A restart
is the same problem in time rather than in space.

**Ruled out by probe, do not spend time here again.** Recovery is not the cause. The recovery source is
correct at both creation and restart, the `cleanLuceneIndex` branch that silently discards an existing
store never fires, and the pre-restart recovery is a textbook new index. C19 works.

**What C2 needs.** A node snapshot that every node and every moment agrees on, rather than
`DiscoveryNodes` as-of-now, and placement that does not move a shard because a node is briefly absent.
`RendezvousShardPlacement` from C1 minimises movement when membership genuinely changes and
`ComputedPlacementRestartIT` should use it instead of modulo, but that only shrinks the blast radius; it
does not make the input stable.

**Until C2 lands, do not turn the setting on anywhere real.** A restart silently blanks indices, and a
blank index is STARTED and green.

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
- **C13** open, blocked on C2 above. Recovery is cleared as its cause.

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
