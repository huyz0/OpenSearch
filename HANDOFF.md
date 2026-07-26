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

**A write to a computed index retries for sixty seconds and succeeds about two times in three.**

Start here: `ComputedPlacementShardLifecycleIT.testADocumentCanBeIndexedAndRead`, which is `@AwaitsFix`
and whose javadoc has the run-by-run account of how the failure moved.

**The shard is not the problem, and this is now asserted rather than assumed.**
`testTheComputedShardEntersPrimaryMode` passes in 3.2 seconds: a computed shard is created, recovers,
starts itself locally without the cluster manager, and enters primary mode. C18's local half is done.

**The write is a request-path problem.** It takes sixty seconds, the replication retry timeout, and
passes about two runs in three: 60.16s pass, 60.38s fail, 60.29s pass. Something between the coordinator
and the primary keeps retrying against a shard that was ready the whole time. Do not read the passing
runs as success.

Where to look, in order:

1. `TransportReplicationAction.AsyncPrimaryAction`, comparing the allocation id the request carries with
   the one the shard holds. `ComputedShardRouting` makes both deterministic, so a mismatch means two
   different derivations exist somewhere.
2. Whether the coordinator resolves the primary to the node that actually holds it. `compute` and
   `localShards` in the lifecycle IT both sort data node ids, so a divergence there is a test bug rather
   than a production one.
3. `AlreadyClosedException: engine is closed` in the failing run, which may be a consequence of the
   retrying rather than its cause.

**What already paid for itself, twice.** Ask the component that owns the property, not a request that
travels through it. A write test measures everything between client and engine, so when it fails it
names nothing; three log lines and one direct assertion each cracked a layer that arguments had not.

**Three things ruled out by measurement, so as not to re-derive them.**

- `updateShard` never runs for a computed shard. A probe on that path printed nothing across a full run
  with node logging captured. It fires only on a state applied after the one that created the shard, and
  an idle cluster publishes no such state, which is why the start transition lives in
  `handleRecoveryDone`.
- The primary term belongs at creation, in the branch that skips publication. Setting it node-side
  instead trades "primary term must be positive" for "term is only increased as part of primary
  promotion", because the shard is constructed from metadata.
- The `ReplicationTracker` version gate is not a factor. Passing a higher version changed nothing.

**The framing that got C18's local half finished.** Computed placement removes the cluster manager from
the loop, so every piece of state it used to maintain as a side effect had to become a function of the
placement: allocation identity (`ComputedShardRouting`), the started transition (`handleRecoveryDone`),
the in-sync set (from the placement), and the primary term (set at creation). All four are done, and
together they are why the shard now works. The remaining question is a different shape, since it is
about a request finding a shard rather than a shard existing.

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
