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
