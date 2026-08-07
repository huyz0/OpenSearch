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
