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

## The blocker: C18, and it is one question

**A computed shard never enters primary mode, so every write is rejected.**

Start here: `ComputedPlacementShardLifecycleIT.testADocumentCanBeIndexedAndRead`, which is `@AwaitsFix`
and whose javadoc has the run-by-run account of how the failure moved.

**Where it is now.** The write fails with `AlreadyClosedException: engine is closed` and the tracker's
invariant `local checkpoints {} not in-sync with routing table`. That is two layers further along than
"not in primary mode", and the shard-open test passes in about four seconds.

**What is already settled, so as not to re-derive it.**

- `updateShard` never runs for a computed shard. Measured: a probe on that path printed nothing across a
  full run with node logging captured. It fires only on a cluster state applied after the one that
  created the shard, and an idle cluster publishes no such state. The start transition therefore lives
  in `handleRecoveryDone`, where an ordinary shard tells the cluster manager it started.
- The primary term is set at creation, in the same branch that skips publication. Zero is not a legal
  term and the allocator is what normally bumps it. Setting it node-side instead trades "primary term
  must be positive" for "term is only increased as part of primary promotion", because the shard is
  constructed from metadata and a node-local term disagrees with its own shard.
- The `ReplicationTracker` version gate is **not** a factor. Passing a higher version changed nothing.

**How to make progress on the current failure.** Put the probe back inside
`startComputedShardAfterRecovery` (the shape is in commit `fe093d9fbba`) and print what
`updateShardState` throws. Every layer so far has been named exactly by one log line, and every attempt
to reason ahead of the probe has been wrong: the version gate, then an empty in-sync set, both
confidently argued and both false. Three log lines have beaten three arguments.

**The framing that makes the rest of C18 predictable, and it has now paid four times.** Computed
placement removes the cluster manager from the loop, so every piece of state it used to maintain as a
side effect has to become a function of the placement instead. So far: allocation identity
(`ComputedShardRouting`), the started transition (`handleRecoveryDone`), the in-sync set (from the
placement), and the primary term (set at creation). Expect the next surprise to be a fifth item on that
list rather than a new category, and look for it in `ReplicationTracker`, which is where the remaining
cluster-manager-shaped assumptions live.

Four seams exist now, and the fourth is the one that keeps being needed:

1. Skip publication: `registerUnpublished` plus the skip in `MetadataCreateIndexService`.
2. Count active shards through the supplier: `ActiveShardCount` and `ClusterStateHealth`. C17.
3. Materialize shards locally: `registerLocalShards` plus the hook in `RoutingNodes.localRoutingNode`,
   deliberately not the `RoutingNodes` constructor, which the allocator uses.
4. **Resolve, never look up.** `AbsentIndexRoutingSuppliers.resolve` and `resolveShard`. Three
   open-coded lookups produced three bugs. When you find a fifth site, use these rather than pairing a
   table read with a supplier call.

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
