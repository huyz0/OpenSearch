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

### T40 — Split the mapped creation cost into store traffic and mapping work

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

### T41 — Stop reading a mapping that cannot exist yet

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

### T42 — Re-measure the mapped ratio with the create-path read gone

- Depends on: T40, T41
- Goal: a number for what T41 bought, or evidence it bought nothing.
- Acceptance:
  - the T40 harness, unchanged, run twice on each side of T41 in a worktree at the parent
    commit and at HEAD
  - the unmapped control arm is within 10% across the four runs, or the runs are discarded and
    repeated; the load average at each run is recorded in `log.md`
  - the RFC table's "with a declared mapping" row carries the new ratio, and the commit says
    plainly if the ratio did not move
- Risk: medium
- Kind: measurement

### T43 — A store read that fails must not read as an absent mapping

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

### T44 — Prove the mapping store is never touched from a cluster state thread

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

### T45 — The mapping index's geometry is a hardcoded 5

- Depends on: none
- Goal: `IndexBackedMappingStore.ensureIndexExists` fixes the shard count at 5 with no
  reasoning recorded and no way to change it, on a shared index that every gated creation
  writes through. Make it a node setting so T46 can vary it and an operator can size it.
- Acceptance:
  - a node setting with 5 as its default, registered by the plugin and documented in the
    javadoc with what the number means
  - a test asserts a configured value reaches the `CreateIndexRequest`, using a client double
    that captures the request
  - no behaviour change at the default: `GatedMappingStatsIT` and `GatedMappingFidelityIT` pass
    unchanged
- Risk: low
- Kind: fix

### T46 — Measure the mapping index's geometry against creation throughput

- Depends on: T40, T45
- Goal: decide whether the shared fixed-geometry index is a funnel on the creation path or
  merely a shared index.
- Constraint found while doing T45, and it invalidates the obvious approach: three shard counts
  cannot be had in one cluster. The setting is node-scope and read once at startup; the mapping
  index is created lazily by the first write and never deleted, so a second store with a different
  count gets `ResourceAlreadyExistsException` and runs against the first geometry while reporting
  the second. Deleting the index between arms does not help either, because the retained store
  still believes it exists and the next write auto-creates it at the cluster default of one shard.
  Three counts means three clusters, and every arm must read the live `index.number_of_shards` of
  `.opensearch-index-mappings` before timing anything.
- Acceptance:
  - the T40 harness run at three shard counts for the mapping index, one cluster per count, with
    the unmapped arm as the control in each run
  - each arm asserts the mapping index's actual shard count before it times anything, so an arm
    that silently ran against another geometry fails instead of being reported
  - a stated threshold before the run: a difference smaller than the control's own spread
    across runs is reported as no measurable effect, which is a completed task and not a
    failure
  - the outcome, either way, replaces the RFC's `.opensearch-index-mappings` bullet
- Risk: high
- Kind: measurement

### T47 — A deleted gated index leaves its mapping behind forever

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

### T48 — A deleted mapping index still reads as "no index has a mapping"

- Depends on: T43, T47
- Goal: T43 made an unreadable store say so, with one deliberate exception: a missing
  `.opensearch-index-mappings` still reads as absence, because nothing has ever been written to it
  before the first mapped gated creation. If the index is instead deleted out from under a live
  cluster, that same absence means the opposite, and the merge that follows writes a mapping holding
  one field where there had been many, successfully. Close it or record why it cannot be closed.
- Acceptance:
  - a test that deletes the mapping index while a gated index has a stored mapping, then drives a
    field inference, and asserts the previously declared fields are not silently replaced
  - whatever distinguishes the two cases is named in the code rather than inferred from the
    exception type: a marker document written at creation, a cluster-state check, or an explicit
    "the index existed and now does not" signal
  - the guard is mutation-tested: making the two cases indistinguishable again fails the test
- Risk: medium
- Kind: fix

### T49 — A template-gated creation writes its mapping from the cluster state thread

- Depends on: T44
- Goal: `DescriptorGate.worthAdmittingOffThread` reads only the request's own settings, so an index made
  gated by a matching template is not admitted onto GENERIC. It takes the ordinary path, and at the
  bottom `clusterStateCreateIndex` reads the finished settings, finds it gated, and calls
  `MappingGenerationStore.createMapping` from the cluster manager's update thread. That is a blocking
  store call, and `compareAndSwap` can submit a cluster state update of its own through
  `ensureIndexExists`, which is the W4 deadlock T19 removed from put-mapping arriving by another door.
- Acceptance:
  - `GatedMappingOffClusterStateThreadIT` gains a template-gated creation phase using the same recording
    store and bounded wait, and it fails before the fix
  - after the fix no recorded call comes from a cluster state thread for either shape
  - the comments falsified by this are corrected in the same commit: `MetadataCreateIndexService`'s
    "Blocking is safe here. Every path reaching this branch came through createGatedIndex on GENERIC",
    `IndexBackedMappingStore`'s "both callers running off the cluster state thread by construction", and
    `DescriptorGate`'s "takes the ordinary path and is gated at the bottom exactly as it was before --
    correct, and no faster"
  - mutation: reverting the fix fails the new phase with the offending thread named
- Risk: medium
- Kind: fix
- **Not started. Handoff, written after T48 closed:** the fix is one of two, and choosing between them
  is the first thing to do rather than a detail.
  - *Widen admission.* `DescriptorOnlyCreation.mayBypassClusterState` takes only
    `request.settings()`, which is why a template-gated index is missed. Giving it the index name and
    cluster state so it can resolve templates would admit those creations onto GENERIC like any other,
    and the cluster-state branch would become unreachable for gated indices. The existing comment
    rejects this as "doing the expensive work in order to decide whether to avoid it", which is worth
    re-examining: template resolution is a local metadata lookup, not the index-service construction
    that admission exists to avoid.
  - *Do not write the mapping there.* Leave admission alone and stop `clusterStateCreateIndex` making
    a blocking store call on the cluster manager's thread. The constraint to respect is the ordering
    already recorded in that method: the mapping must land before the descriptor, because the
    descriptor is the acknowledgement, so this cannot simply be made asynchronous without moving the
    acknowledgement too.
  - Either way the three falsified comments listed above must be corrected in the same commit, and
    `GatedMappingOffClusterStateThreadIT` is where the phase belongs.

## Numbering

This round was planned as T23 to T32, on the strength of STATE.md saying "T-numbers run to T22
and the next round starts at T23". That was wrong. The tree cites T-numbers up to T39, and eight
of the ten numbers this round claimed already belonged to earlier work: T23 to concurrent
creation uniqueness, T25 and T27 to wildcards, T28 to the wildcard expansion cap, T29 to alias
representability, T30 to gated serving, T32 to creation batching.

Renumbered to T40 to T49 after the geometry task landed. The mapping is T23→T40, T24→T41,
T25→T42, T26→T43, T27→T44, T28→T45, T29→T46, T30→T47, T31→T48, T32→T49. Code comments, this
plan, the log, the RFC and STATE all use the new numbers. **Commit messages from this round still
use the old ones**, since rewriting them would rewrite history that has already been pushed
through review; a reader following T23 out of a commit message from 2026-08-06 should read T40.

The convention's whole point is that a number cites one decision. Two rounds sharing T28 is the
failure it exists to prevent, and the check that would have caught it costs one grep.

### T50 — A mapping written after its index was deleted is stranded forever

- Depends on: T47
- Goal: T47 prunes a gated index's mapping when the index is deleted. A write that arrives after the
  prune re-creates the document, and nothing will ever remove it again. The window is real: the
  tombstone invalidates only the writing node's descriptor cache, so another node keeps resolving the
  name until its freshness window expires, and an in-flight put-mapping or dynamic-field inference on
  that node reaches `recordGatedMapping` and writes generation 1 under the deleted UUID.
- Acceptance:
  - a test that writes a mapping for a UUID whose tombstone is already durable, and asserts the write
    is refused or the document does not survive
  - whichever way it is closed is stated in the code: refusing the write needs the tombstone consulted
    on the mapping path; letting it land needs something that prunes it later, and a scrubber is a
    second sweep to justify
  - the same test covers the read-side symptom: a query against the still-resolving name must not
    silently return zero hits on a declared field
- Risk: medium
- Kind: fix
