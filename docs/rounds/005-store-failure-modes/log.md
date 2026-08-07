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

## T59 — a missing mapping reads as an index with no fields

- Status: complete
- Commit: 66a38381239 (feature/serverless)
- Result: `MappingGenerationStore.currentMapping` gained a second-arg overload taking the caller's expected
  mapping generation and throwing a new `MissingMappingException` when the store answers absent but the
  generation says otherwise; the single-arg form is unchanged. `StoreBackedFieldRefresher.refresh` --
  the one production caller with an index name in reach -- resolves the expected generation from
  `AbsentIndexDescriptorSuppliers` before reading, with the same null-safety contract T50 established: no
  registration, no answer, or a mismatched uuid all mean "no evidence", never a forced failure.
- Notes:
  - A second, unplanned fix was needed to make the first one reachable. `UnknownFieldRefresh.refreshed`
    catches every exception a refresher throws and reports the field absent, deliberately, so a store
    hiccup degrades a document instead of failing it (T43's contract). That catch also swallowed
    `MissingMappingException`, so the new failure never surfaced anywhere -- an indexing request would
    "succeed" by silently treating a declared-but-missing field as brand new, which is the exact defect
    this task exists to close, one layer above where `StoreBackedFieldRefresher` raises it. Fixed by
    re-throwing `MissingMappingException` specifically, mirroring the "must not degrade" carve-out
    `AbsentIndexDescriptorSuppliers#supply` already makes for `DescriptorUnavailableException`. Found only
    by driving the integration test through the real registered refresher and watching the write succeed
    when it should have failed -- a unit test with a hand-rolled double would not have caught this, because
    nothing stands between a double and its caller.
  - The integration test does not drive the fix through a real `client().prepareIndex` write.
    `DocumentParser`'s only call into `UnknownFieldRefresh` is inside its `disable_objects` flattening
    branch, an unrelated mapping feature a document has to opt into; reaching it also collides with
    `StoreBackedFieldRefresher`'s own one-second recheck window, keyed per index rather than per field, so
    a same-index probe field silently absorbed the real assertion in an earlier version of this test.
    `GatedMappingMissingWindowIT` installs the real `ServerlessStoragePlugin` -- so the store and the
    descriptor resolver are the real seams `DescriptorGate.install` wires -- and drives the read directly
    against a live shard's real `MapperService`, the same trade T50's `GatedMappingStrandedIndexTests`
    already made on the write side rather than standing up two disagreeing nodes.
  - A `task-reviewer` pass on the first version of this diff found both of the above: the IT calling the
    refresher directly while claiming to exercise `DocumentParser` end to end, and the goal's "a reader
    sees" / "a query" language implying a search-time symptom when the only production caller is a write
    path (document parsing). Both fixed before commit -- the IT rewritten to install the real plugin and
    say plainly what is real versus directly driven, and every doc comment corrected to say "read", not
    "query", for what T59 actually closes.
  - Mutation performed by hand: removed the generation check, confirmed both
    `testAnAbsentMappingWithANonZeroExpectedGenerationFails` (unit) and `GatedMappingMissingWindowIT`'s
    first assertion failed, restored it, confirmed the suite passed again.
  - Considered and rejected: adding the generation parameter to `MappingGenerationStore.Store#read`
    directly. That would force every one of the interface's seven-odd implementations (T56's still-open
    complaint) to handle a parameter only one caller needs. Layering the check in a new overload on top of
    the existing `read` needed no interface change.
- Deferred: none. T51 through T58 remain open and unblocked.

## T51 — attempted, refuted, reverted

- Status: not complete. Nothing was committed; the tree is unchanged from T59.
- What was tried: auto-creation, rollover, and data stream creation all reach
  `applyCreateIndexRequest` from inside a cluster state update task, so T49's tripwire
  (`AbsentIndexDescriptorSuppliers.blockingIsUnsafeHere()` in `MetadataCreateIndexService`) fires and
  refuses the write. The attempt short-circuited that check (`if (false && ...)`), which does make
  both a gated auto-create and a gated rollover succeed.
- Why it is wrong: the tripwire's refusal is the safe behavior T49 built on purpose. Disabling it
  does not route those callers off the cluster-state thread — it just removes the guard, so the
  mapping write goes back to happening on the cluster manager's update thread with nothing checking
  it, which is the actual defect T51 exists to close. A silently-succeeding unsafe write is worse
  than a refused one; refusal is at least visible to the caller as a bulk failure.
- No test survived this attempt. The one artifact produced, an `ExperimentRolloverGatedIT`, was
  exploratory and discarded along with the tripwire change; neither was committed.
- What the next attempt should investigate instead: a real dispatch mechanism, not a suppression.
  The three callers need to hand the create-index (or mapping-write) work off the cluster-state
  update thread before it reaches the gated branch, the same way T49's own fix presumably intended
  the *admission* check to run off-thread rather than being skipped entirely. Read how
  `MetadataCreateIndexService.applyCreateIndexRequest` is invoked by auto-creation, rollover, and
  data stream creation specifically — whether they can be converted to submit a follow-up task (e.g.
  via the cluster state task executor's ability to chain work, or a listener-driven retry off the
  applier thread) rather than doing the mapping write synchronously inside the state update itself.
  One shared mechanism, named in code, is still the requirement; do not solve auto-creation,
  rollover, and data-stream creation as three separate call-site patches.
