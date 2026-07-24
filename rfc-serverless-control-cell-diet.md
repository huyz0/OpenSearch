# RFC Amendment: Control-Cell Diet (Class C — Per-Index Metadata)

- Status: DRAFT — design-review artifact, no `server/` implementation attached
- Amends: `rfc-serverless-metadata-plane.md` §12 Phase 5.5 ("Control-cell diet"), informed by
  its §4.2 addendum
- Question answered here: given Phase 5.5 is "deliberately not attempted" pending its own
  design review, what would the review actually approve — a concrete SPI shape, not just the
  decision to build one

This document does not ask "should Class C stop being universally resident." §4.2 already
answered that with measured numbers: a compact-but-diff-compatible `IndexMetadata` buys
~3.3x, not enough on its own, and the real fix is the same shape already applied to Class D —
stop holding it everywhere, hold it only where and while it's needed. This document proposes
*how*, so that "how" is what gets reviewed and approved or rejected, not left as an open
question the next engineer re-derives from scratch.

---

## 1. Constraints the design must satisfy

Four, in priority order — violating any of the first three should be a reason to reject a
proposed design outright, not a tradeoff to accept:

1. **Zero behavior or cost change for indices that don't opt in.** This is not a general
   ClusterState redesign; it is a per-index opt-in, exactly like `EnginePlugin`/
   `IndexStorePlugin` already are in this codebase. A cluster running zero opted-in indices
   must be indistinguishable from today's OpenSearch in every measurable way.
2. **Engine-agnostic.** The opt-in must be selectable independent of `index.engine`. A
   classic, local-disk `InternalEngine` index must be able to opt into compact Class-C
   residency without adopting serverless-storage's object-store engine, and vice versa. This
   was an explicit requirement raised during this investigation and is a hard constraint, not
   an aspiration — a design that only works for one engine is a narrower, less valuable
   proposal and should say so plainly rather than imply generality.
3. **Does not change the diff/publish protocol's correctness contract.** `DiffableUtils`'s
   map diffing ships deletes/upserts and silently carries forward everything else *by
   reference* from whatever the node already has (confirmed by reading
   `DiffableUtils.JdkMapDiff`, `Metadata.MetadataDiff`, `RoutingTable.RoutingTableDiff`,
   `PublicationTransportHandler.lastSeenClusterState`). Any design that has a node discard its
   diff-basis for an index breaks that node's ability to apply the *next* diff for that index —
   this is the specific defect an earlier, more naive version of this proposal had, caught by
   adversarial review before it reached a design document. **Retain a compact diff-basis
   object; never discard the diff-basis itself.**
4. **`IndexAbstraction`/wildcard-alias resolution stays correct.** `Metadata.getIndicesLookup()`
   is read by `IndexNameExpressionResolver` for essentially every request. A stub that can't
   answer alias/wildcard queries correctly is not a compact representation, it's a correctness
   bug with a delay timer on it.

## 2. The SPI

Two new interfaces, one new `Plugin` registration hook, mirroring the exact pattern
`EnginePlugin#getEngineFactory`/`IndexStorePlugin#getDirectoryFactories` already establish in
this codebase (`ServerlessStoragePlugin implements EnginePlugin, ClusterPlugin, IndexStorePlugin`
is the existing precedent for "per-index opt-in core SPI implemented by a plugin").

```java
// server/src/main/java/org/opensearch/plugins/ShardPlacementSourcePlugin.java (new)
public interface ShardPlacementSourcePlugin {
    /**
     * Return a source for indices this plugin owns, or null to decline. Called once per index
     * at index-service construction, mirroring EnginePlugin#getEngineFactory's timing.
     */
    @Nullable
    IndexMetadataSource getIndexMetadataSource(IndexSettings indexSettings);
}

// server/src/main/java/org/opensearch/cluster/metadata/IndexMetadataSource.java (new)
public interface IndexMetadataSource {
    /** Compact diff-basis view for an index this node does not currently host a shard of. */
    CompactIndexMetadata compactView(ClusterState state, Index index);

    /** Full materialization, called once a shard is actually assigned locally. */
    IndexMetadata materialize(CompactIndexMetadata compact);
}
```

`CompactIndexMetadata` is the retained diff-basis object from constraint 3 — not a new core
type replacing `IndexMetadata`, but a companion held *alongside* it for opted-in indices this
node isn't currently hosting. Its field list is exactly what §4.2's `RealisticCompactTenantIndex`
measured: the full `Settings` object, full alias map, four per-field version longs, `state` —
everything `IndexMetadataDiff` keeps whole — with `DiscoveryNodeFilters` and empty
mappings/rollover/custom-data deferred to `materialize()`, called lazily when a shard actually
lands locally.

## 3. Where the seam goes

Read `IndicesClusterStateService.java` in full for this (not assumed from its public surface):
it has no single chokepoint. `applyClusterState()`'s eight private sub-methods each
independently re-derive "my local shards" via `state.getRoutingNodes().node(localNodeId)` and
"this index's metadata" via `state.metadata().index(index)`, at roughly ten call sites. The
seam is additive at each: `pluginViews.isEmpty() ? <original expression> : <consult
CompactIndexMetadata, materialize on demand>`. For a cluster with no opted-in indices,
`pluginViews` is a static empty map and every guard is one predictable, zero-allocation branch
— constraint 1 satisfied, and cheap enough (a `Map.isEmpty()` check) that it shouldn't need
benchmarking to justify.

Two consequences of this being spread across eight methods rather than one, worth surfacing
explicitly rather than glossing over in an interface diagram:

- **`ClusterChangedEvent`'s diff detection is reference-equality based**
  (`indicesDeleted()`/`metadataChanged()`/`indexMetadataChanged()` compare the two full
  `Metadata` objects). An opted-in index whose full `IndexMetadata` is deliberately not
  resident can never trigger these by definition. `deleteIndices`/`updateIndices` need a
  parallel, plugin-sourced diff signal — comparing `CompactIndexMetadata` versions — not a
  fallback bolted onto the existing metadata-lookup calls.
- **Replication safety is not a formatting concern.** `primaryTerm`/`inSyncAllocationIds` feed
  `ReplicationTracker.updateFromClusterManager` via `shard.updateShardState`
  (`IndicesClusterStateService.java:729-743`) and guard split-brain/data-loss on primary
  promotion. Whatever supplies these for an opted-in index must reproduce the cluster-manager's
  authoritative sequencing exactly — a divergence here is a correctness bug, not a missed
  optimization, and needs its own dedicated test suite before this ships, not just "the existing
  tests still pass."

## 4. Reconciling with what's already built (Class D)

This SPI is deliberately narrow: it is about Class C (index existence, settings, mappings,
aliases), not Class D (shard truth/placement), which `rfc-serverless-metadata-plane.md` already
solved and partially shipped (`ShardHead`, `BlobContainerShardStateStore`, `ShardDirectory`,
`PartitionedShardDirectory`). The two compose, they don't overlap:

- A `ShardPlacementSourcePlugin` implementation for serverless-storage indices would source
  `CompactIndexMetadata` from the plugin's own object-store metadata, and would *not* need to
  re-solve shard placement — it defers to the existing `ShardHead`/directory-tier machinery for
  "which node serves shard S," exactly as today.
- For a **classic, local-disk `InternalEngine` index** opting into compact Class-C residency
  (constraint 2), there is no equivalent Class-D solution today — `RoutingTable`/`ShardRouting`
  still fully own placement for classic indices, and that is out of scope for this amendment.
  A classic index can shrink its *metadata* footprint through this SPI without changing how its
  *shards* are placed at all; those are independent axes. This is a real, useful, smaller win
  for classic indices on its own, and should not be oversold as "classic indices get the full
  serverless treatment" — they don't, not from this document.

## 5. What this does not solve

Two limits, carried forward from validated research earlier in this investigation, stated
plainly rather than left implicit:

1. **The cluster-manager's own view stays global.** `AllocationService.getMutableRoutingNodes()`
   rebuilds `RoutingNodes` from the entire routing table on every reroute, with no per-tenant
   scoping, regardless of how any individual data node retains state. This SPI does not touch
   it. §4.2's ~102 GiB-at-100M-tenants number is the realistic floor for what the
   cluster-manager itself needs resident, not what any one data node needs — and per
   `rfc-serverless-metadata-plane.md`'s own math, that still requires horizontal cell
   partitioning to reach 100M tenants, this SPI or not.
2. **Classic-engine indices don't get dormant shards from this alone.** `InternalEngine` has no
   dormant state — an assigned shard means a live `IndexWriter`, open translog, and running
   merge/refresh threads for as long as it's assigned. Reaching millions of *shards* (as
   opposed to compact *metadata*) for classic indices needs a lazy/dormant-shard engine mode or
   remote-backed storage, independent of and not delivered by this SPI.

## 6. Which of the two Phase 5.5 options this is

`rfc-serverless-metadata-plane.md` §12 posed two options for Phase 5.5: (a) teach the existing
coordination path to skip publishing routing entries for opted-in shards, or (b) a standalone
parallel control-cell process. This document is a fully-worked-out version of **(a)** — additive
branches inside `IndicesClusterStateService` and friends, not a second subsystem. That is a
deliberate scoping choice: (b) is very likely the eventual shape once cell-based federation
(the longer-term roadmap item both this investigation and the original RFC converge on) is
real, but building a parallel control-cell process before there's a federation story to plug it
into is speculative infrastructure with no consumer. (a) is reviewable and shippable on its own,
narrower, and doesn't foreclose (b) later — the `IndexMetadataSource` interface in §2 would
become one implementation detail of a future control-cell client, not something to rebuild.

## 7. Smallest reviewable slice

In order, each independently shippable and testable:

1. `IndexMetadataSource`/`ShardPlacementSourcePlugin` interfaces + the `pluginViews.isEmpty()`
   guards at all ten `IndicesClusterStateService` call sites, with `pluginViews` always empty
   (no registered implementation) — this slice should be indistinguishable from today's
   OpenSearch in every test, and is where constraint 1 gets proven, not asserted.
2. The parallel diff-signal for `deleteIndices`/`updateIndices` (§3's first bullet) — its own
   dedicated correctness tests, since `ClusterChangedEvent`'s existing reference-equality
   detection structurally cannot cover it.
3. A reference `IndexMetadataSource` implementation for classic, local-disk indices (the
   narrower, Class-C-only win from §4) — smaller blast radius than a serverless-storage
   implementation, since it doesn't also depend on Class-D's object-store machinery, and
   validates constraint 2 (engine-agnostic) directly rather than by argument.
4. A `ShardPlacementSourcePlugin` implementation for serverless-storage indices, composing with
   the existing `ShardHead`/directory tier per §4.
5. Only after 1-4 are reviewed and merged: revisit whether §6's option (b) is warranted, in the
   context of whatever the actual federation/cell design looks like by then.

## 8. Risks

1. **Replication-safety regression** (§3) is the highest-severity risk in this document, not a
   footnote — any implementation must ship with a dedicated primary-term/in-sync-allocation-id
   test suite proving parity with the unopted-in path before slice 1 merges, not after.
2. **Reference-equality diff detection** (§3) is a structural gap, not a bug to patch later —
   slice 2 is not optional if slice 1 ships.
3. **Two-mode complexity**, same as `rfc-serverless-metadata-plane.md` §13's risk 4: classic and
   compact-Class-C control paths coexist indefinitely inside `IndicesClusterStateService`. This
   argues for slices 1-2 being reviewed with unusual care, since bugs there are shared-blast-radius
   by construction (they touch code every non-opted-in index also runs through).
4. **Scope creep into "this solves 100M tenants."** It doesn't (§5). Every document referencing
   this SPI — including a future PR description — should carry §5's caveats forward, not just
   this one.

## 9. Explicit ask

This is a design-review artifact. It proposes an interface shape, a call-site inventory, a
phasing plan, and an explicit list of what it does and doesn't solve — everything needed for a
maintainer to approve, reject, or redirect the approach before engineering time is spent.
Recommendation: do not begin `server/` implementation of §2's interfaces or §3's call-site
changes until this document (or a revision of it) has that sign-off. Slice 1 (§7) is the
smallest thing worth prototyping once approved, since it's the one piece provable in isolation
against every existing test in the suite.
