# Round 004: The mapping write path

Goal: the ~10x a declared mapping costs a gated creation is attributed to a measured cause
rather than a named suspect, the part of it that is provably wasted work is removed, and the
store that path runs through is tested instead of assumed.

Exit: the RFC's "leading candidate ... unmeasured, and not to be assumed" wording is replaced
by a measured split with a control; the creation path no longer issues a read that cannot
return anything; and the store has tests for the three behaviours nothing currently checks
(read failure, deletion, geometry).

Why this round and not eviction under load, which the RFC's order of work puts first: the
residency ceiling has since gained `indices.gated.max_open` and `evictColdestOfASample`, which
moves the bound onto the request path and off the sweep's ability to get CPU. Whether that
closed the question is worth its own round, and it needs a soak run per task. This round is
the one with tasks that fit a single invocation each.

## Standing notes for this round

- Every measurement here goes behind an opt-in system property, as `GatedMappedCreationCostIT`
  and `GatedResidencySoakIT` already do. A measurement class that creates hundreds of indices
  in the shared test cluster breaks three unrelated tests and none of them where the problem
  is.
- `MappingGenerationStore.register` is a static registry. A test that swaps the store must put
  the previous one back in `@After`, or the class after it inherits the swap.

## Tasks

### T23 — Split the mapped creation cost into store traffic and mapping work

- Depends on: none
- Goal: say how much of the surviving ~10x is the mapping store's two blocking round trips and
  how much is everything else, instead of naming the store as a candidate.
- Acceptance:
  - a three-arm run in one cluster, in one class, under one opt-in property: mapped with the
    index-backed store registered, mapped with an in-memory `MappingGenerationStore.Store`
    registered in its place, and unmapped as the control
  - the in-memory arm keeps the code path identical up to the round trips, so the difference
    between arms one and two is store traffic and the difference between arms two and three is
    everything the mapping costs elsewhere
  - the class restores the index-backed store in `@After` and deletes every index it created
  - all three rates and both ratios are logged; the assertion is that each arm made progress,
    not that any duration held
  - the RFC's "Unknown, and honestly so" bullet on the declared mapping loses the phrase
    "leading candidate ... unmeasured" and gains the split; the stale "8.5x" in Order of work
    item 2 is corrected in the same commit
- Risk: medium
- Kind: measurement

### T24 — Stop reading a mapping that cannot exist yet

- Depends on: none
- Goal: a creation's mapping write costs one round trip rather than two.
- Acceptance:
  - `MappingGenerationStore` gains a create entry point that attempts the swap at generation 1
    and falls back to the existing read-and-merge loop only when the swap is refused
  - `MetadataCreateIndexService.clusterStateCreateIndex` calls it for the gated branch instead
    of `updateMapping`
  - a `MappingGenerationStoreTests` case with a counting `Store` double asserts zero reads and
    one `compareAndSwap` for the uncontended create
  - a second case, whose double refuses the first swap and holds a conflicting field, asserts
    the fallback runs and the resulting fields are the merge, not the caller's map alone
  - `GatedCreateTimeMappingIT` and `GatedMappingFidelityIT` pass unchanged
  - mutation: delete the fallback branch and the conflict case fails
- Risk: low
- Kind: fix

### T25 — Re-measure the mapped ratio with the create-path read gone

- Depends on: T23, T24
- Goal: a number for what T24 bought, or evidence it bought nothing.
- Acceptance:
  - the T23 harness, unchanged, run twice on each side of T24 in a worktree at the parent
    commit and at HEAD
  - the unmapped control arm is within 10% across the four runs, or the runs are discarded and
    repeated; the load average at each run is recorded in `log.md`
  - the RFC table's "with a declared mapping" row carries the new ratio, and the commit says
    plainly if the ratio did not move
- Risk: medium
- Kind: measurement

### T26 — A store read that fails must not read as an absent mapping

- Depends on: none
- Goal: a transient failure to read the mapping index currently returns null, which every
  caller treats as "this index has no fields". On the update path that starts the merge from
  empty and drops fields; on the refresher it reports a shard as current when it is stale.
- Acceptance:
  - a new `IndexBackedMappingStoreTests` with a client double: a get that throws propagates,
    and a get whose response says `isExists() == false` still returns null
  - `MappingGenerationStore.updateMapping` propagates rather than merging onto empty when the
    read throws, asserted with a throwing double
  - mutation: restore the blanket catch and the propagation cases fail
- Risk: low
- Kind: fix

### T27 — Prove the mapping store is never touched from a cluster state thread

- Depends on: none
- Goal: T19 moved the put-mapping work to `MetadataMappingService#putMapping` so both callers
  dispatch to GENERIC first. Nothing asserts it, and the reason the original defect survived
  was that the only test of the path drove the executor with an in-memory double.
- Acceptance:
  - a store wrapper installed for the test records the calling thread name of every `read` and
    `compareAndSwap`
  - a real gated creation with a declared mapping, and a real put-mapping against a gated
    index, both drive at least one recorded call, and no recorded call came from a cluster
    state update thread
  - the test fails if it recorded no calls at all, so a path that silently stopped reaching the
    store cannot pass it
  - mutation: call `updateMapping` from inside `PutMappingExecutor` and the test fails
- Risk: medium
- Kind: proof

### T28 — The mapping index's geometry is a hardcoded 5

- Depends on: none
- Goal: `IndexBackedMappingStore.ensureIndexExists` fixes the shard count at 5 with no
  reasoning recorded and no way to change it, on a shared index that every gated creation
  writes through. Make it a node setting so T29 can vary it and an operator can size it.
- Acceptance:
  - a node setting with 5 as its default, registered by the plugin and documented in the
    javadoc with what the number means
  - a test asserts a configured value reaches the `CreateIndexRequest`, using a client double
    that captures the request
  - no behaviour change at the default: `GatedMappingStatsIT` and `GatedMappingFidelityIT` pass
    unchanged
- Risk: low
- Kind: fix

### T29 — Measure the mapping index's geometry against creation throughput

- Depends on: T23, T28
- Goal: decide whether the shared fixed-geometry index is a funnel on the creation path or
  merely a shared index.
- Acceptance:
  - the T23 harness run at three shard counts for the mapping index, with the unmapped arm as
    the control in each run
  - a stated threshold before the run: a difference smaller than the control's own spread
    across runs is reported as no measurable effect, which is a completed task and not a
    failure
  - the outcome, either way, replaces the RFC's `.opensearch-index-mappings` bullet
- Risk: high
- Kind: measurement

### T30 — A deleted gated index leaves its mapping behind forever

- Depends on: none
- Goal: nothing ever removes a document from `.opensearch-index-mappings`. The interface has no
  delete at all, so the shared mapping index grows with every index ever created rather than
  with the live population. That is the residency problem this area exists to remove, one level
  down, and churn is what makes it bite.
- Acceptance:
  - `MappingGenerationStore.Store` gains a delete, implemented against the mapping index, and
    the gated deletion path calls it
  - an IT creates a gated index with a declared mapping, deletes it, and asserts no document
    with that UUID remains
  - deleting an index that never had a mapping does not fail the deletion, asserted
  - a delete that throws does not fail the index deletion: the index is gone either way and a
    stranded document is the lesser outcome, asserted with a throwing double
  - mutation: remove the delete call and the IT fails
- Risk: medium
- Kind: fix
