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
stop holding it everywhere, hold it only where and while it's needed. This document tries to
propose *how*.

**Revision note:** the first draft of §2's mechanism did not survive its own adversarial
review, the same discipline applied throughout this investigation before anything gets treated
as settled. It proposed a `CompactIndexMetadata` "companion held alongside" the full
`IndexMetadata` — but `Metadata`'s own storage (`Map<String, IndexMetadata>`) and
`MetadataDiff.apply()` (`Metadata.java:1123-1137`, always ending in `builder.build()` via the
full private constructor) never stop holding a real, fully materialized `IndexMetadata` under
that framing, on every node, for every index, whether or not it's locally hosted. A companion
object next to a full object saved nothing — arguably cost more. The review also found a call-site
class this document had missed entirely: `OperationRouting`/`TransportBulkAction` need full
`IndexMetadata` synchronously, on *any* coordinating node, for *any* request against an
opted-in index, not just on nodes that host a shard of it. Both are reflected below, not
smoothed over. The honest state of this document post-review: the problem is precisely
characterized and constrained (§1), the request-time and node-local call-site surfaces are
now both inventoried (§3), and §2 presents what a mechanism satisfying all of it would actually
require — which is larger and less settled than the pre-review draft claimed. That gap is the
main thing a design review needs to resolve, not a detail to wave through.

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

## 2. What a mechanism actually has to do (revised after review)

Achieving real non-residency — not just adding a smaller object next to the existing one —
means `Metadata`'s own storage stops guaranteeing every entry is a fully-materialized
`IndexMetadata`. There is no way to get the §4.2 heap win without that: as long as
`Metadata.indices()` is `Map<String, IndexMetadata>` and `MetadataDiff.apply()` always
reconstructs a full object for every upsert, the memory is spent unconditionally, on every
node, before any SPI gets a chance to intervene.

Concretely, that requires `Metadata`'s per-index storage to become a **union of two shapes** —
call it `IndexMetadataOrStub`, wrapping either the real `IndexMetadata` or a compact,
diff-capable stand-in (the same object referred to as `CompactIndexMetadata` below — the stub
case of this union, not a competing proposal) — with `Metadata.index(name)` returning that
union instead of a bare `IndexMetadata`. This is a
materially bigger change than the pre-review draft's "companion object": it touches the type
every caller of `metadata.index(name)`/`metadata.indices()` across all of `server/` sees, not
just the ~10 call sites inside one class. Two consequences follow directly, and a design review
should treat both as open questions this document raises rather than closes:

1. **Callers need to handle "not yet materialized."** For opted-out indices, resolution must
   still be synchronous and free (constraint 1) — a caller touching only classic, never-opted-in
   indices should never see anything but an already-real `IndexMetadata`, ideally without even a
   type-level change to their code. That likely means `IndexMetadataOrStub` needs to behave like
   `IndexMetadata` for every already-resident case and only differ in the stub case, which is
   closer to a lazily-resolving proxy than a plain sum type — a harder interface to get right
   than either shape alone, and not fully designed here.
2. **The request-time path is now in scope, not just shard lifecycle.** Reading
   `OperationRouting.java` and `TransportBulkAction.java` directly (not assumed): both call
   `clusterState.metadata().index(index)` synchronously on whichever node is *coordinating* a
   request — a node that may never host a shard of that index at all. `OperationRouting.
   indexMetadata()`/`shardId()` (`OperationRouting.java:489-505`) and `TransportBulkAction`
   (multiple sites, e.g. lines 458, 501, 643, 682, 743, plus system-index lookup checks around
   251/294) both need this to resolve every bulk/index/get/search request. A stub-aware
   `Metadata` therefore needs a resolution path on the *hot request path*, for *any* coordinating
   node, not only inside `IndicesClusterStateService`'s node-local shard lifecycle — a
   materially larger surface than this document originally scoped.

**The reasonable answer to (2), not yet the reasonable answer to (1):** this is precisely the
shape `rfc-serverless-metadata-plane.md` §9 already describes for Class D — a cold shard blocks
briefly on first touch (read head, materialize, proceed) rather than requiring everything to be
resident in advance. Extending that same activation-style pattern to Class C resolution — a
coordinating node hitting a stub blocks briefly, triggers `materialize()`, proceeds — is
consistent with a pattern this system already accepts elsewhere, not a new kind of risk. What
*isn't* resolved is (1): making that blocking path invisible to every caller that shouldn't pay
for it, at the type level, for a class this pervasively read. That is the actual open design
question this amendment surfaces rather than answers, and it should be treated as the thing a
design review needs to make a call on — not something implied to already have a clean answer.

## 3. Where the node-local seam goes (shard lifecycle only — see §2 for the request-path surface)

Read `IndicesClusterStateService.java` in full for this (not assumed from its public surface):
it has no single chokepoint, and its eight private `applyClusterState()` sub-methods are not
equally in scope. Five of them — `removeIndices`, `updateIndices`,
`createOrUpdateShards`/`createShard`/`updateShard` — iterate either `indicesService` (already
locally-allocated indices) or `localRoutingNode` (locally-assigned shards); by construction, a
locally-assigned shard implies materialization already happened, so these five only ever see
fully materialized metadata regardless of this design. **Only `deleteIndices` (for
previously-known-but-now-unloaded indices) and `createIndices` (the materialization trigger)
sit on the actual compact/full boundary.** The pre-review draft's "additive at each of eight
methods" overstated this; the real node-local seam is two methods, not eight — smaller than
originally claimed, though it does not change §2's larger request-path finding.

At those two methods, the seam is additive: `pluginViews.isEmpty() ? <original expression> :
<consult stub, materialize on demand>`. For a cluster with no opted-in indices, `pluginViews` is
a static empty map and every guard is one predictable, zero-allocation branch — constraint 1
satisfied for this seam specifically. Whether the *type* change to `Metadata`'s own storage
that §2 identifies as necessary can be made equally free for opted-out indices is the open
question §2 raises, not something this section's node-local analysis resolves on its own.

Two further consequences worth surfacing explicitly rather than glossing over in an interface
diagram:

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
  Placement and metadata residency are independent axes in principle. **But §2's residency gap
  and request-path gap are not Class-D-shaped limitations — they apply identically to classic
  and object-store-engine indices**, since `Metadata`'s storage type and
  `OperationRouting`/`TransportBulkAction`'s resolution path are shared by both. The earlier
  draft framed classic indices' remaining limitation as "no placement story," implying Class-C
  compaction itself was otherwise settled for them; it isn't, for either engine, until §2's open
  question has an answer. What genuinely is engine-specific is only what §5's second point already says:
  reaching millions of *shards* (not just compact metadata) for classic indices additionally
  needs a dormant-engine mode Class D doesn't provide — that is a real, separate, additional gap
  on top of §2's, not instead of it.

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

## 6. Which of the two Phase 5.5 options this leans toward

`rfc-serverless-metadata-plane.md` §12 posed two options for Phase 5.5: (a) teach the existing
coordination path to skip publishing routing entries for opted-in shards, or (b) a standalone
parallel control-cell process. This document set out to be a worked-out version of (a); §2's
review-driven finding complicates that. The node-local shard-lifecycle seam (§3) genuinely is
additive, narrow, and (a)-shaped. The request-path resolution problem (§2) — a type change to
`Metadata`'s own storage, felt by every synchronous caller of `metadata.index(name)` across
`server/` — is no longer obviously smaller or safer than (b); it may simply be a different
*shape* of large change, touching breadth of callers instead of a second process. This document
does not resolve which is actually smaller in practice, and says so rather than asserting an
answer it doesn't have. A design review should treat "(a) done properly, or (b) after all" as a
genuinely open question for §2's problem specifically, informed by whoever attempts the §7
slice-1 spike below, not settled by this document's original framing.

## 7. Smallest reviewable slice

Revised after review: slice 1 can no longer be "harmless guards, `pluginViews` always empty" —
that framing assumed the residency win was already achieved by construction, which §2 shows
isn't true. The actual first slice is answering §2's open question, before any of the
originally-planned interface work:

1. **A `Metadata`-storage-representation spike, not an implementation.** Prototype (in
   isolation — a standalone core-local experiment, not wired into `IndicesClusterStateService`
   or shipped) whether a lazily-resolving `IndexMetadataOrStub`-shaped type can sit behind
   `Metadata.index(name)` such that every opted-out caller sees zero type-level or behavioral
   change, and every opted-in caller synchronous-resolves through an activation-style block
   (§2's `rfc-serverless-metadata-plane.md` §9 analogy) only when it actually hits a stub. This
   is genuinely uncertain, not a formality — resolve it with a working prototype and real
   answers to "does constraint 1 actually hold" and "does the request-path change stay
   correctness-preserving under diffing," before proposing it as settled. This is the slice
   most likely to change §2's proposed shape, possibly substantially.
2. Only once (1) has a working answer: the node-local `IndicesClusterStateService` seam (§3) —
   now genuinely narrow (two methods, not eight) and additive, assuming (1)'s type is available
   to build against.
3. The parallel diff-signal for `deleteIndices`/`updateIndices` (§3's first bullet) — its own
   dedicated correctness tests, since `ClusterChangedEvent`'s existing reference-equality
   detection structurally cannot cover it.
4. A reference `IndexMetadataSource` implementation for classic, local-disk indices — validates
   constraint 2 (engine-agnostic) directly, and per §4, does not on its own reach millions of
   shards for classic indices (a separate, larger gap), only compact metadata.
5. A `ShardPlacementSourcePlugin` implementation for serverless-storage indices, composing with
   the existing `ShardHead`/directory tier per §4.
6. Only after 1-5: revisit §6's now-open (a)-vs-(b) question in the context of whatever (1) and
   the actual federation/cell design look like by then.

## 8. Risks

1. **§2's residency mechanism might not have a clean answer.** This is the central open risk,
   promoted here rather than left implicit: it is possible that no `IndexMetadataOrStub` design
   satisfies constraint 1 (zero cost/behavior change for opted-out indices) at the type level
   for a class this pervasively read, in which case option (a) collapses into something
   materially closer to option (b), or into accepting a smaller win than §4.2's ~3.3x implied
   was reachable. Slice 1 (§7) exists specifically to surface this before any other slice is
   built on top of an assumption that hasn't been tested.
2. **`materialize()`'s synchronicity and failure contract is undefined** in §2's sketch. What
   blocks, for how long, and what happens on a materialize failure (object-store timeout,
   corrupt compact record) mid-request is unspecified. This needs a real answer — almost
   certainly modeled on the bounded activation budget `rfc-serverless-metadata-plane.md` §5.1
   already uses for cold-shard query planning — before slice 2 or later can be built, not
   discovered during implementation.
3. **Replication-safety regression** (§3) — any implementation must ship with a dedicated
   primary-term/in-sync-allocation-id test suite proving parity with the unopted-in path before
   slice 2 merges, not after.
4. **Reference-equality diff detection** (§3) is a structural gap, not a bug to patch later —
   slice 3 is not optional if slice 2 ships.
5. **Two-mode complexity**, same as `rfc-serverless-metadata-plane.md` §13's risk 4: classic and
   compact-Class-C control paths coexist indefinitely inside `IndicesClusterStateService` and,
   per §2, potentially inside `Metadata`/`OperationRouting` as well. This argues for slices 2-3
   being reviewed with unusual care, since bugs there are shared-blast-radius by construction
   (they touch code every non-opted-in index also runs through).
6. **Scope creep into "this solves 100M tenants."** It doesn't (§5). Every document referencing
   this SPI — including a future PR description — should carry §5's caveats forward, not just
   this one.

## 9. Explicit ask

This is a design-review artifact, and its own adversarial review changed what it's actually
asking for. It is not a finished interface proposal seeking rubber-stamp approval — §2's core
mechanism is an open question, not a settled answer, and this document says so rather than
presenting §2's sketch with more confidence than the evidence supports. What this document does
provide: a precise characterization of the problem (§1, §4.2 of the companion RFC), a
call-site inventory covering both the node-local shard-lifecycle surface and the previously-missed
request-time surface (§2, §3), an honest statement of what any solution here does and doesn't
reach (§5), and a phasing plan whose first step is resolving the open question, not building on
top of an unproven assumption (§7).

Recommendation: a maintainer reviewing this should be deciding whether §1's constraints and §2's
problem framing are right, and whether slice 1 (§7) — a standalone, non-production
`Metadata`-storage-representation spike — is worth someone's time to attempt. That decision does
not require pre-approving §2's `IndexMetadataOrStub` sketch or any interface shape; those are
explicitly unresolved pending slice 1's outcome. Do not begin any `server/` implementation
beyond that spike until slice 1 has a working answer and this document (or a revision of it) has
been reviewed again in light of it.
