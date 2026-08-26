# S0 — result

- Spike: `server/src/test/java/org/opensearch/s0/` (`S0ShellSpikeTests`, `S0Wiring`)
- Gate for: [`rfc-serverless-shell.md`](rfc-serverless-shell.md) §11, decision D1
- Run: `./gradlew :server:test --tests "org.opensearch.s0.S0ShellSpikeTests" -Dbuild.docker=false`
- Result: **2 tests, 0 failures.** `tests="2" skipped="0" failures="0" errors="0"`

## Verdict

**§5 is not falsified. Proceed to phase 1.**

A shard was opened, recovered, started, written to three times and searched three times in a process
containing no `Coordinator`, no `AllocationService`, no `GatewayMetaState` and no `Node`. The
projected-`ClusterState` thesis survived contact with execution.

This is a *pass*, not a proof. §5.2's real risk — a partial projected view read as shard removal —
was never exercised, because S0 drives the shard directly rather than through
`IndicesClusterStateService`. What S0 establishes is that the direct path works and is small; see Q2.

## Pass criteria, as measured

| # | Criterion | Measured |
|---|---|---|
| 1 | Search returns hits, asserted on a non-zero number | `2` hits after two writes |
| 2 | Hit count equals indexed document count | `2` then `3` |
| 3 | Documents retrievable by query | `matchQuery("msg","hello")` matched all |
| 4 | Post-search write visible after refresh | `2` before refresh, `3` after — **the pre-refresh assertion is the load-bearing one**; without it a permanently-stale searcher would pass |
| 5 | No forbidden class in the object graph | **12,728 objects walked, none forbidden** |

Criterion 5 has two guards against passing vacuously, both added after the first green run:

- A floor assertion (`visited > 1000`), because a walker that silently stops working visits ~0 objects
  and reports success — the exact `HANDOFF.md` failure shape.
- A **negative control** (`testObjectGraphWalkerDetectsAPlantedViolation`) that plants a decoy class
  three hops deep and asserts the walker finds it. Criterion 5 means nothing without it.

## Q1 — the minimum `ClusterState` a shard will accept

**The shard accepts none.** `IndexShard` never reads a `ClusterState`; it is driven entirely by the
value objects of `updateShardState` (RFC §2.2), which S0 confirms empirically.

What *does* need a well-formed state is `ClusterApplierService`, which refuses to start without an
initial one. The minimum that worked:

```java
ClusterState.builder(new ClusterName("s0-cluster"))
    .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build())
    .metadata(Metadata.builder().put(indexMetadata, false).build())
    .routingTable(RoutingTable.builder().addAsRecovery(indexMetadata).build())
    .blocks(ClusterBlocks.EMPTY_CLUSTER_BLOCK)
    .build()
```

**`clusterManagerNodeId` was never set, and nothing needed it.** This confirms §10.5's choice of `null`
over naming the local node — and it differs from `ClusterServiceUtils`, which sets the local node as
manager. The test framework's choice is not evidence about what the data plane requires; it is a
convenience for tests that exercise manager paths.

## Q2 — how large is the shell-owned reconciler?

**Four calls for the create-and-start path.** Against `IndicesClusterStateService`'s 1,705 lines:

```java
IndexService svc   = indicesService.createIndex(metadata, List.of(), false);
                     svc.updateMapping(null, metadata);            // step 2 — see F5
IndexShard shard   = svc.createShard(routing, ..., localNode, null, nodes, warmer, null, null);
                     shard.markAsRecovering("s0-store", new RecoveryState(routing, localNode, null));
                     shard.recoverFromStore(future);
                     shard.updateShardState(started, term, null, version, inSync, routingTable, nodes);
```

**Honest scope:** this is create-and-start only. Removal, failure handling, relocation, replica
promotion and mapping *updates* are not covered, and `IndicesClusterStateService` spends most of its
length on exactly those. The defensible claim is that the happy path is four calls and the class is
replaceable — not that the replacement is 20 lines.

## Q3 — the `IndicesService` / `SearchService` constructor closure

S0 used the 28-argument `IndicesService` overload. **Six of 28 have no serverless value** and were
passed `null` (or a null-returning supplier):

| Position | Parameter | Why null is right |
|---|---|---|
| 15 | `Client` | no node client in the spike |
| 19 | `ValuesSourceRegistry` | aggregations not exercised |
| 21 | `remoteDirectoryFactory` | serverless supplies its own directory |
| 22 | `Supplier<RepositoriesService>` | `() -> null` |
| 23 | `SearchRequestStats` | stats surface not built |
| 24 | `RemoteStoreStatsTrackerFactory` | ditto |

Four more took empty collections (`engineFactoryProviders`, `directoryFactories`,
`recoveryStateFactories`, `ingestionConsumerFactories`) — those are "no plugins installed," not "no
sensible value." The remaining 18 are all genuinely required.

`SearchService` took 14 arguments with **2 nulls** (`indexSearcherExecutor`, and the trailing
parameter) — no concurrent-search executor, no stream search.

**Assessment: the closure is not a blocker.** It is verbose, not deep. Nothing in it demanded a
control plane.

## Q4 — cross-node version comparisons beyond `ReplicationTracker`

**Not answered. S0 could not answer it.** The spike is single-node, so primary relocation — the path
where `PrimaryContext` carries `appliedClusterStateVersion` between nodes (§5.3) — never executes.
Enumerating this needs a two-node probe, which is phase 2 work, not S0.

Recording it as unanswered rather than inferring from a green run: a single-node pass is not evidence
about a cross-node comparison.

## Findings not anticipated by the RFC

| # | Finding | Consequence |
|---|---|---|
| **F1** | `ClusterApplierService.doStart()` requires a `NodeConnectionsService`. | §2.3 recorded **two** required collaborators (publisher, supplier). There are **three**. §5.1's wiring snippet is incomplete as written. In the shell this is a real service driven by the membership view (§10), not a no-op. |
| **F2** | `PluginsService` is mandatory — `DataFormatRegistry` calls `filterPlugins()` during `IndicesService` construction. | Cannot be null. The shell holds a real one with no plugins. |
| **F3** | `IndicesService` and `SearchService` are `AbstractLifecycleComponent`s and reject work until `start()`. | `Node` does this via its own lifecycle; the shell owns it explicitly. |
| **F4** | **A primary refuses to activate at term 0** — `ReplicationTracker.activatePrimaryMode` → `RetentionLeases`: *"primary term must be positive"*. | §5.3's "feed the shard-head's CAS term" is **load-bearing, not an optimisation**. The data plane will not start a primary without a term from somewhere, and in this design the only somewhere is the shard-head register. |
| **F5** | `createIndex` does **not** apply the mapping. Without a following `updateMapping(null, metadata)`, the first write returns `MAPPING_UPDATE_REQUIRED` — **a result value, not an exception**. | Textbook fails-by-succeeding: a reconciler that ignores the result indexes nothing and reports no error. S0 asserts on the result type for exactly this reason. |
| **F6** | `SearchRequest.allowPartialSearchResults()` defaults to `null` and NPEs in `ShardSearchRequest`; `TransportSearchAction` normally fills it in. | Any shell path calling `SearchService` directly must set it. |
| **F7** | `clusterManagerNodeId == null` works end to end. | §10.5 confirmed by execution. |

F1 and F4 are corrections to the RFC and are applied there. F5 is the most dangerous of the set,
because it is the failure mode this project already knows it is bad at seeing.

## What this does not establish

- That a **partial** projected view is safe (§5.2, R1). S0 projected a complete single-index state.
- That `IndicesClusterStateService` can be replaced for removal, failure and relocation — only that
  the create-and-start path does not need it.
- Anything about CAS, object stores, membership or gossip. R9 and R11 remain fully open.
- Anything about scale. One shard, three documents.

## Recommendation

Proceed to phase 1 with R1 still open and now sharply defined: the next probe is a **two-node**
spike that (a) exercises a projected view that omits an index the node still hosts, and (b) performs a
primary relocation so Q4 can be answered by measurement.
