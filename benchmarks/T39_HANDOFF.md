# T39 handoff: opening a gated index's shard on demand

Written to be read cold, in a session with no memory of the work that produced it. It carries the project
context, the state of the branch, the one open problem, and the way this area has learned to work.

Branch: `feature/pluggable-engine-per-shard-role`. Working tree clean and green at `f5eaf8ada60`.

---

## 1. What the project is

The goal is an OpenSearch cluster that holds a very large index population without holding it in cluster
state. The original target was 100 million indices; it was re-scoped to **10 million**, because the local
machine cannot run the larger one and the scaling curve is already established at that size.

The mechanism is **descriptor-based metadata**. A gated index has no entry in cluster state metadata at all.
Everything cluster state would have held for it lives in an `IndexDescriptor` record, persisted in a system
index `.opensearch-index-descriptors` behind `DescriptorStore`. An index is gated by creating it with
`index.serverless_storage.enabled: true`.

Core carries **seams**, not serverless logic. Each seam is a static registry, unset by default, so an
ordinary cluster behaves exactly as it always has. The plugin installs them through
`DescriptorGate.install(...)` and removes them in `uninstall()`. The ones that matter here:

| seam | file | answers |
|---|---|---|
| `AbsentIndexDescriptorSuppliers` | `server/.../cluster/metadata/` | supply, exists, page, expandPrefix |
| `AbsentIndexRoutingSuppliers` | `server/.../cluster/routing/` | routing table, `resolveShard`, suspended shards, local shards |
| `DescriptorOnlyCreation` | `server/.../cluster/metadata/` | which indices skip cluster state entirely |
| `MappingGenerationStore`, `UnknownFieldRefresh` | `server/.../index/mapper/` | mappings off cluster state |
| `GatedMappingStatsAggregator` | `server/.../` | stats without enumerating |

Placement is computed rather than published: `RendezvousShardPlacement` hashes an index onto nodes and
`ComputedRoutingTable` builds a routing table from it, behind `ComputedPlacementGate`. It is controlled by
`serverless_storage.computed_placement.enabled` (`ServerlessStoragePlugin:1270`) and **defaults to false**.
That default has already caused one wrong diagnosis; a test that exercises the gated write path must set it.

Two documents hold everything measured so far, and are the record of record:

- `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md`, spikes S1 to S54. Read S50 to S54 before starting.
- `benchmarks/GATED_WILDCARD_DESIGN.md`, the wildcard contract and, at the end, the T39 design note this
  file expands.

## 2. What works today, and what does not

A gated index **can** be created, resolved by name, listed, paginated, wildcard-matched by prefix, deleted
with a durable tombstone, suspended and woken, and searched by name. Per-shard residency is 118 KB and 3.06
file descriptors; wake takes 41.5 ms (T20, T21, T22).

A gated index **cannot take a write**. Nothing in the spike results has ever measured one serving a
document. S45 claimed it did and was corrected in S49: those documents were served by ordinary indices that
auto-creation had quietly put back into cluster state.

The reason is now known precisely, after four wrong diagnoses (S48, S52, S53, S54 each corrected the one
before):

| | |
|---|---|
| residency assumptions on the write path | eleven sites, all identified, all fixable with seams that already exist |
| computed placement | works, assigns and starts a gated primary, off by default, one setting |
| **shard materialisation** | **real, unimplemented, and the actual wall** |

`IndicesClusterStateService` builds an `IndexService` and opens shards when an index appears in an applied
cluster state. A gated index never appears in one. So no node ever constructs the shard, and a write that
survives all eleven residency sites arrives at a data node and dies:

```
IndexNotFoundException
  at IndicesService.indexServiceSafe(IndicesService.java:1029)
  at TransportReplicationAction.getIndexShard(TransportReplicationAction.java:947)
  at TransportReplicationAction$AsyncPrimaryAction.doRun
```

That is T39.

## 3. The eleven residency sites

Written and verified together in T38, then reverted. They are mechanical. Sites 1 to 10 are from S52, site
11 from S53.

| # | site | needs |
|---|---|---|
| 1 | `AutoCreateIndex.shouldAutoCreate` via `hasIndexAbstraction` | existence |
| 2 | `TransportBulkAction.addFailureIfIndexIsUnavailable` | `getState()` |
| 3 | `TransportBulkAction.doRun`, index/create branch | `mapping()`, `getCreationVersion()` |
| 4 | `TransportBulkAction.doRun`, append-only branch | `isAppendOnlyIndex()` |
| 5 | `TransportBulkAction.addFailureIfAppendOnlyIndexAndOpsDeleteOrUpdate` | `isAppendOnlyIndex()` |
| 6 | `TransportBulkAction.doRun`, data stream guard | `IndexAbstraction` |
| 7 | `OperationRouting.indexMetadata` | shard count to hash against |
| 8 | `OperationRouting.shards` routing table | computed placement, once enabled |
| 9 | `TransportBulkAction.executeBulk`, adaptive shard selection | `isAppendOnlyIndex()` |
| 10 | `TransportReplicationAction.ReroutePhase.doRun` | `IndexMetadata` for the shard |
| 11 | `TransportReplicationAction.ReroutePhase`, `resolveShard` overload | the three-argument `supply(state, indexName, indexMetadata)` that synthesises metadata from the descriptor, not the two-argument one |

Each of them reads cluster state for something the descriptor holds. Every one has an existing seam to ask
instead. None needs a new mechanism.

**They land with T39, not before it.** Site 1 removes auto-creation, which is the crutch that has been
masking the whole problem. Removing it before a shard can be opened turns a silent design failure into a
loud one, which is why the T38 build was reverted rather than merged.

A `metadataOrDescriptor(Metadata, Index)` helper was written three separate times for these sites and
reverted each time. It was deliberately **not** folded into `Metadata.getIndexSafe`, because W4 established
that widening a hot core accessor to do a remote lookup deadlocks.

## 4. What T39 has to build

### The `IndexService` half is cheap

`IndicesService.createIndex(indexMetadata, listeners, writeDanglingIndices)` is public and takes metadata
that can be synthesised: `IndexDescriptor.toIndexMetadata` already produces one, and computed placement has
used it since P6. It rejects `INDEX_UUID_NA_VALUE`, and a descriptor carries the real uuid, so that check
passes.

### The shard half is not

`IndicesService.createShard` takes fifteen collaborators: the segment replication checkpoint publisher, peer
recovery target service, recovery listener, repositories service, shard failure and global checkpoint
consumers, retention lease syncer, target and source nodes, remote store stats tracker factory, discovery
nodes, merged segment warmer factory and publisher, among others. Every one is held by
`IndicesClusterStateService`.

### The shape

**Give `IndicesClusterStateService` a second trigger rather than building a parallel path.** It already owns
every collaborator, the recovery wiring and the failure handling. Two shard lifecycles that must agree would
be a worse problem than the one being solved.

The trigger is not a cluster state diff but a request: *a shard arrived for an index that computed placement
says belongs on this node, and no `IndexService` exists for it.*

That class already consults the routing seam in two places, so the pattern is established rather than novel:
`computedAwareInSyncIds` at `IndicesClusterStateService:874` and `computedAwareShardRoutingTable` at
`:896`, both calling `AbsentIndexRoutingSuppliers.resolveShard(state, shardId)`.

### The one thing that is easy to get silently wrong

The recovery source. This is the same shape as scale-to-zero wake, which is the strongest reason to think
T39 is tractable: T21 measured wake at 41.5 ms, and wake already reopens a shard on demand outside the
ordinary allocation path. But wake resumes a shard that was built and then suspended, so its data and its
`ShardHead` exist. A never-built shard has neither and starts from an empty store.

`ComputedRoutingTable` states `ExistingStoreRecoverySource` deliberately, and its own comment warns that
*inferring* the recovery source picks "empty store" whenever `inSyncAllocationIds` is absent, which under
computed placement is always. A first write is exactly the case that comment is about. **Choose the recovery
source explicitly; do not let it be inferred.** The comment beside `computedAwareInSyncIds` calls this the
A5 trap's neighbourhood and is worth reading before touching it.

Getting this wrong means a live index recovering as blank, silently, which is the failure mode this whole
area exists to be suspicious of.

## 5. How to verify it

`plugins/serverless-storage/src/internalClusterTest/java/org/opensearch/serverless/storage/descriptor/GatedEndToEndIT.java`
is the harness. `testAGatedIndexCanBeWrittenToAndSearched` is the test that must pass, and its class already
enables computed placement in `nodeSettings`. Note the header comment on that class: it explains why its
earlier headline result was wrong, and it should stay honest.

The premise guard matters. Before believing any pass, assert the index is actually gated, because "the test
passed because an ordinary index served it" is the exact failure this class already shipped once. There is a
`testWhetherTheDisagreeingIndexIsEvenGated` alongside it that shows how to check.

The descriptor index is refresh-bound. A read after a write needs a refresh, or the test reads zero and the
zero looks like a defect.

Before landing: run the `serverless-storage` internal cluster tests as a suite, plus `server` unit tests
around `IndicesClusterStateService`, `TransportReplicationAction` and `OperationRouting`. Add
`-Dbuild.docker=false`, or a dead Docker daemon fails every Gradle task including `compileJava`.

Known unrelated failures, so they do not read as regressions:

- `ServerlessStorageShardRetentionStatsActionIT`, pre-existing.
- `GatedEndToEndIT.testHowManyShardsAGatedIndexActuallyGets` is muted; it reproduces only when the whole
  class runs, and asking for 1/2/4/7 shards in isolation is honoured every time.

## 6. How this area has learned to work

These are not general principles. Each one was paid for.

**An unexplained failure on the gated path is a residency assumption until proven otherwise**, because that
is what gating changed. Nothing else has been true yet. Four diagnoses in one session blamed the nearest
unexamined thing (a random template, the routing seam, a missing subsystem) and all four were wrong. Every
correct answer came from asking the system a direct question instead.

**Probe, do not reason.** Use controls, alternate arms, medians over single runs, and state the decision rule
before seeing the numbers. Change the variable you attribute the effect to, more than once. A single-run A/B
once had me blame a test timeout on an index sort; a per-operation measurement said 0.87x and 0.95x, and the
real cause was suite contention.

**Do not land what you cannot verify.** Three builds today went past what could be checked and all three
were reverted, which is why the branch still works. If T39 cannot be driven to a passing `GatedEndToEndIT`
in one session, revert it and write down the map, the way S51 to S54 did.

**Beware the two recurring failure shapes.** *Correct and unreachable*: a mechanism that is right and that
nothing calls, which this area has produced seven times. *Fails by succeeding*: a confident wrong answer,
like a wildcard that matched five thousand tenants and returned a hundred, or a page that reports itself as
the whole population.

**Corrections go in the record.** The spike document contains withdrawn claims with the reason they were
wrong, kept rather than deleted. Keep doing that.

## 7. Open items after T39

Not part of T39, listed so nothing is lost:

- The cluster manager thread is 43.5% of OpenSearch CPU on one serialised thread. Ceiling is 23,843 index
  creations/second against a few hundred today.
- The full 10M creation run, roughly 3.3 hours.
- `GatedShardSuspensionRegistry` is entry-bounded with variable-size entries.
- `DescriptorStore.findByPrefix` has no tombstone filter. It has no production caller, which is the only
  reason it is not a defect yet.
- Counting has no bounded answer. `_cat/indices` without pagination and `_cluster/health` over `*` still
  cannot say how many indices exist. The wildcard cap makes that explicit rather than solving it.
