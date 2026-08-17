# extract/generic-fixes-from-serverless

Phase I of `core-pluggability-refactor-plan.md` (on `refactor/core-pluggability`): independent bug
fixes and generic improvements found scattered through `feature/serverless`'s 1,264 commits, extracted
onto their own branch off `main` so they can be reviewed and merged without waiting on the serverless
architecture question to resolve. Each commit is self-contained, has its own test coverage, and was
verified against a clean `main` checkout (not the fork) with the full relevant test suite passing before
and after.

## Landed (13 commits, 14 of the review's 15 identified items)

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
12. **`BalancedShardsAllocator` cross-setting validator for the balance-factor sum constraint** —
    `cherry-pick -x b99229e3f36`. A pure correctness guard (rejects a config where
    `cluster.routing.allocation.balance.index` + `.balance.shard` sums to `<= 0`, which would otherwise
    publish as a poison-pill setting), not perf-sensitive, no benchmarking needed. `:server:test --tests
    "org.opensearch.cluster.routing.allocation.allocator.*"` green.
13. **`BalancedShardsAllocator#weightSpreadAcrossAllNodes` decider-scan skip** — `cherry-pick -x 50ef318afe4`.
    Skips the full per-node `AllocationDeciders` scan for an index whose all-node weight spread is already
    under the rebalance threshold (provably cannot yield a relocation), cutting ~800k decider invocations
    per reroute at 40k-index/20-node scale in the original author's own measurement. **The performance
    claim in this commit (10k shards 75/71ms→43ms, 40k shards 445/355ms→287ms) is carried over from its
    original authorship, not independently re-verified in this session** — the `AllocationCeilingSpikeTests`
    harness that commit's own message references doesn't exist in this repo (was an ad-hoc, uncommitted
    benchmark harness), and no equivalent load-testing infrastructure was available here to re-run it.
    Correctness is verified (its own dedicated `WeightSpreadBoundTests`, which specifically guards the
    over-skip failure mode by injecting an unconditional skip and confirming it fails, plus the full
    allocator test package, all green) — the *performance* number is not independently confirmed and should
    be re-measured before this is treated as a settled win on `main`'s own workload characteristics.

## Deliberately not attempted here

- **`SplitShardsMetadata`'s in-place-merge (undo-split) primitive.** Attempted this session and reverted
  after a real scoping discovery: what looked like "a feature port" turned out, on tracing the full
  dependency closure (not just the files matching an initial narrow grep), to be **90 commits**, not the
  low-20s originally estimated — an entire "resharding-by-copy" subsystem spanning shard split, shard
  merge, physical partition rewriting, data-stream autosharding, and write-partition routing/cutover, built
  over what its own commit history documents as multiple dedicated review rounds ("Fix 8 issues from
  gc/clone/retention review," "Round 9: broadened-scope review finds and fixes 5 real issues," "Fix 6 issues
  from broadened whole-resharding-package review," and more). Three commits were cherry-picked cleanly
  before this became clear (`93a945441f9`, `747d3695797`, `7e9327a0f93`) and then reverted rather than left
  as a partial, unverifiable 3/90 port of shard-routing/data-integrity-critical code — a half-integrated
  fragment of a feature this size is worse than not starting it, since it can't be verified as behaviorally
  complete or correct on its own. This needs its own dedicated effort with real time budgeted, not a
  same-session extension of "extract independent fixes."

See `core-pluggability-refactor-plan.md` on `refactor/core-pluggability` for the full context this
branch was extracted from.
