# extract/generic-fixes-from-serverless

Phase I of `core-pluggability-refactor-plan.md` (on `refactor/core-pluggability`): independent bug
fixes and generic improvements found scattered through `feature/serverless`'s 1,264 commits, extracted
onto their own branch off `main` so they can be reviewed and merged without waiting on the serverless
architecture question to resolve. Each commit is self-contained, has its own test coverage, and was
verified against a clean `main` checkout (not the fork) with the full relevant test suite passing before
and after.

## Landed (11 commits, ~13 of the review's 15 identified items)

1. **CWE-22 path traversal in `FsRepository`'s `base_path`** — security fix, also worth a backport.
2. **`NestedQueryBuilder#visit` not recursing** into the nested query's own sub-clauses (and NPE'ing on
   a legitimately-null child visitor).
3. **Path traversal in `Analysis#resolveAnalyzerPath`** — same class of bug as #1, different subsystem.
4. **NPE in `ShardSplittingQuery#findSplitDocs`** for a segment with no terms for the field.
5. **Two NPEs when a resize source index has no routing table entry** (`DiskThresholdDecider` and
   `MetadataCreateIndexService#validateShrinkIndex`).
6. **Vacuous-truth bug**: an empty node set was wrongly treated as "remote store enabled" in
   `MetadataUpdateSettingsService`.
7. **Stale-iterator and eager-allocation bugs** in `Bitmap64IndexQuery`/`BitmapIndexQuery`'s scorers.
8. **Index templates rejecting `total_primary_shards_per_node`** on remote-store clusters (checked the
   wrong "is remote store enabled" signal for a template context).
9. **`DEFAULT_REPLICA_COUNT_SETTING` ignoring the node-local default** when no persistent/transient
   override was also set.
10. **Cluster-wide default for delayed allocation** (`cluster.routing.allocation.unassigned.node_left.delayed_timeout`)
    plus a fast path in `AllocationService#adaptAutoExpandReplicas` for the common case of no index using
    auto-expand-replicas.
11. **Crash-durability gap in `SubdirectoryAwareDirectory`** — subdirectory files weren't actually
    fsync'd, and directory entries for new subdirectory files were never synced at all.

## Deliberately not attempted here

- **`SplitShardsMetadata`'s in-place-merge (undo-split) primitive** — a real feature, not a bug fix; a
  much larger port than the rest of this branch, and out of scope for "extract independent fixes."
- **`BalancedShardsAllocator`'s balance-factor-sum validators and `weightSpreadAcrossAllNodes` decider-scan
  skip** — the review itself flagged the latter as needing independent re-benchmarking before landing;
  deferred rather than landing perf-sensitive allocator changes without that verification.

See `core-pluggability-refactor-plan.md` on `refactor/core-pluggability` for the full context this
branch was extracted from.
