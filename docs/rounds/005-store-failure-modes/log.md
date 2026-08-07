# Round 005 log

Append-only. One entry per task, written when the task's commit lands.

## T50 — attempted, refuted, reverted

- Status: not complete. The tree is back at T47's behaviour; nothing was committed.
- What was tried: moving the prune out of the deletion path into `TombstoneScrubber`, so a mapping
  would live exactly as long as its tombstone. That closes both halves of T50 on paper, because the
  tombstone's window is exactly the period in which a stale-cache node can still write or read.
- Why it is wrong, both verified in the code after review raised them:
  - the scrubber is off by default (`tombstone_scrub_interval` defaults to zero) and the README
    prefers an object-store lifecycle rule, which never runs it. Default and recommended
    configurations would both stop pruning entirely, which is worse than the defect being fixed.
  - tombstones are keyed by name, so deleting a recycled name overwrites the record of the previous
    uuid. The scrubber's worklist has one uuid per name; mappings have one per uuid.
- Also not met, and worth knowing before the next attempt: neither of T50's first two acceptance
  criteria was actually satisfied by what was built. No test wrote a mapping for a uuid whose
  tombstone was durable, and no test issued a query against a still-resolving name. Both were argued
  in javadoc instead. A design that cannot be tested against those two criteria is not the design.
- Next attempt starts from the plan's refutation note.

## T50 — second firing, design recorded, not implemented

- Status: still not complete. Nothing in the tree changed this firing.
- What the firing established: the write path cannot tell a live gated index from a deleted one
  because `isGated` decides gating from absence in cluster state, which both cases share, and
  `recordGatedMapping` writes against a uuid resolved by the coordinating node's cache. Refusing the
  write means resolving the descriptor for that uuid before writing. That is a cache hit for a live
  index, so it is affordable on the path.
- And that the read-side half is a separate mechanism: `IndexDescriptor` already carries a
  `mappingGeneration`, so a descriptor claiming generation N against a store with no document is a
  missing mapping rather than an empty one. Filed as T59 rather than folded in, because a task with
  two mechanisms is how the last attempt produced something that passed its tests and was wrong.
- Stopped here deliberately. The loop's own rule is to stop when a task has not completed on two
  consecutive firings, and the previous firing's failure was a design pushed through a context that
  had no room left to check it. The design is now mechanical; the next firing should implement it.

## T50 — third firing, implemented

- Status: complete
- Commit: d7b28f13909ee5fa8ccf6a217de17c2a20414f33
- Result: `PutMappingExecutor#recordGatedMapping` now resolves the descriptor for each index's uuid
  through `AbsentIndexDescriptorSuppliers` before writing, and refuses with `IndexNotFoundException`
  when the current descriptor is tombstoned or names a different uuid than the request carries. This
  is the branch the plan calls "the tombstone consulted on the mapping path". The contract is named in
  `refuseIfDescriptorShowsTheIndexIsGone`'s javadoc.
- Notes:
  - A `task-reviewer` pass on the first version of this diff found a real defect before commit: refusing
    on a null resolver answer as well as on a confirmed tombstone would have turned an ordinary resolver
    bug into a refused write for a live index, contradicting `AbsentIndexDescriptorSuppliers#supply`'s
    own documented "null means no answer, never fail the request" contract. Fixed to refuse only on a
    non-null descriptor that actually says the index is gone; a regression test for the null case is
    included (`testANullResolverAnswerDoesNotRefuseTheWrite`).
  - The third acceptance criterion (the same test covering the read-side symptom of a query not
    silently returning zero hits) is deliberately not fully met by this task's test. The plan's "second
    look" for T50 splits the read-side mechanism into T59, which depends on T50 and reads
    `IndexDescriptor.mappingGeneration` to tell a missing mapping from an empty one. This task's test
    covers the write-side half instead: after a refusal, a read for the stale uuid finds nothing, so
    there is no resurrected document for T59's read path to ever have to surface.
  - Mutation performed by hand: removed the guard call, confirmed the failing test failed (write landed,
    no exception), restored the guard, confirmed the suite passed again. Not left in the tree at any
    point between commits.
  - One eviction IT, `GatedIdleEvictionIT.testResidencyIsBoundedByArrivalRateRatherThanByPopulation`,
    failed intermittently during gating -- alone and in the suite, both with this change applied and on
    a clean pre-T50 tree on the same box. The failure is a node-disconnect/leader-failover assertion
    inside `IndicesClusterStateService`, unrelated to the mapping write path this task touches. Recorded
    as interference per round-next step 4 rather than chased further.
- Deferred: none beyond T59, which was already filed and unblocks now that this landed.
