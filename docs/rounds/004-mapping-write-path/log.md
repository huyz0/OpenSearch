# Round 004 log

Append-only. One entry per task, written when the task's commit lands.

## T40 — Split the mapped creation cost into store traffic and mapping work

- Status: complete
- Commit: 5eb5ca0976b
- Result: the index-backed mapping store is at least about 80% of what a declared mapping
  costs a gated creation. Five runs of the final harness, round 2, share taken from the front
  arm and from the repeat: 99/99, 115/109, 83/83, 95/94, 101/101 percent. The in-memory arm
  ran at 0.84x to 1.18x the unmapped control (control over arm), so the residual is not
  resolvable at this precision and came out negative twice. Mapping parsing and merging do
  not show up.
- Control: the unmapped arm, plus a repeat of the index-backed arm at the end of each round.
  The repeat is what the finding rests on: it measured the same arm three times apart
  depending only on position, which is larger than the effect, and it also showed the share
  moving at most 6 points when taken from either end while throughput moved up to 1.9x.
- Notes:
  - Do not quote a single mapped-to-unmapped multiple. It ran 2.0x to 2.9x in a settled round
    and 4.3x to 10.0x in an unwarmed one, same code, same run.
  - The measurement cannot separate the store's round trips from the local work in the same
    implementation (request building, field type counts, the exists check). T41 removing one
    round trip is also the experiment that separates them.
  - Only the index-backed arm has a positional control. The in-memory and unmapped arms
    always run second and third and are never transposed, so the residual carries whatever
    the drift between those two positions is worth. Recorded in the RFC as a limit.
  - Three harness defects found and fixed here: wildcard cleanup that could never match (the
    class had been leaking its whole population every run while logging "could not clean
    up"); a partly failed arm reported as a slow arm; worker threads never joined, so a timed
    out arm kept creating indices past `DescriptorGate.uninstall`.
  - Reviewed twice. The first pass found that the drift control had been dropped from the
    two-round design, which is what led to the repeat arm; the second found the repeat arm
    itself unguarded and the RFC claiming more than the runs supported. Both acted on.
- Gates: one full suite run green. Three other runs each failed one timing-sensitive test,
  a different one each time (`GatedIdleEvictionIT.testResidencyIsBoundedByArrivalRate...`,
  `InternalEngineTests.testForceMergeWithSoftDeletesRetentionAndRecoverySource`,
  `FsHealthServiceTests.testFailsHealthOnHungIOBeyondHealthyTimeout`), at load average 12 to
  25. Each passed alone three times at load 8 to 16. This task adds nothing to the default
  suite: the measurement stays behind `-Dtests.mappingcost`.
- Deferred: none. The positional control on the residual arm is noted in the RFC as a limit
  rather than filed as a task, because T41 changes the arm it would measure.

## T41 — Stop reading a mapping that cannot exist yet

- Status: complete
- Commit: 20079e11096
- Result: a gated creation's mapping write is one round trip instead of two.
  `MappingGenerationStore.createMapping` swaps at generation 1 and keeps read-and-merge as its
  fallback. Whether that made creations measurably faster is not claimed here: one run after
  the change read 2.21x against the unmapped control, inside the 2.0x to 2.9x range measured
  before it, at load average 20. T42 measures it.
- Proof it entered the path: `GatedCreateTimeMappingIT` creates one mapped gated index through
  a counting store and asserts zero reads and one swap. Reverting the call site to
  `updateMapping` fails it, verified. Before that assertion existed the revert left every test
  CI runs green, because the only other check was behind `-Dtests.mappingcost`.
- Mutation: both unit tests fail against a `createMapping` that just delegates to
  `updateMapping`; removing the fallback branch fails the merge test. Verified in that order,
  test first.
- Notes:
  - The fallback's first stated justification was wrong and the review caught it. A retried
    creation task does not reuse its UUID: `aggregateIndexSettings` puts a fresh
    `UUIDs.randomBase64UUID` into every attempt. The reachable case is the store's own write
    being retried after it landed, surfacing as a version conflict. Corrected in the javadoc
    and at the call site.
  - `createMapping` with an empty map now returns without writing, matching `updateMapping`.
    No caller reaches it today, but the two entry points disagreeing on the same input is a
    trap, and this one would leave an empty document in an index nothing deletes from.
  - T40's early return in the measurement class's teardown had also dropped an unconditional
    `DescriptorGate.uninstall`. Restored. See the gates note below.
- Gates: green, 12m01s at load average 16. An earlier run failed
  `GatedCreationSwitchIT.testInstallingWhileDisabledLeavesNothingGated` on "a disabled gate
  must gate nothing" at load average 30, which is the leaked-registry signature this package
  produces. It passed alone three times and twice beside the classes this task touches; the
  dropped `uninstall` was the one plausible link and is restored either way.
- Deferred: none.

## T42 — Re-measure the mapped ratio with the create-path read gone

- Status: complete
- Commit: ed4e984ba46
- Result: T41 is worth about a seventh to a fifth of what a declared mapping costs a creation.
  Final batch, eight pairs on matched storage with alternating order: median ratio 2.82x to
  2.42x, median per-creation penalty down 15%, the same runs read as 22% in ratio units. HEAD
  won 5 of 8 pairs in that batch, which is not significant. Across all four batches, 36 pairs,
  HEAD won 27, sign test p = 0.004, and every batch's median favoured HEAD.
- Control: the unmapped arm within each run, plus T40's repeat arm, which moves with the front
  arm on both sides (median ratio 2.73x parent to 2.32x HEAD).
- Deviation from the acceptance criteria, stated plainly:
  - The criterion was two runs a side with the unmapped control agreeing within 10% across the
    four. Five consecutive sets failed it, best spread 13.4%. This box cannot hold a control
    that still for four consecutive runs, so the design changed: the metric is the within-run
    ratio, which normalises the machine by construction, and the sample is eight pairs.
  - The criterion said "the T40 harness, unchanged". Not strictly true: T41 added a read counter
    and an assertion to it. Neither can move a rate -- the counter never increments on HEAD and
    the assertion runs after the timing -- but the two sides did not run byte-identical files.
- Two confounds found by measuring, both removed, both had been working against the finding:
  - Order. The first design ran parent first in every pair. The second slot is slower, so the
    order effect landed entirely on HEAD; its control arm came out 11.5% slower than parent's,
    p = 0.019. Order now alternates by pair.
  - Storage. The worktree was under /tmp, which is tmpfs here, while HEAD is on ext4. The parent
    side had faster storage on the I/O-heavy arm under comparison. Worktree moved to
    /home/tuong/work/t25-parent, both sides ext4.
- Batches, in order run: tmpfs + parent-first 6/10 (p = 0.75); tmpfs + parent-first 9/10
  (p = 0.021); tmpfs + balanced 7/8 (p = 0.070); ext4 + balanced 5/8 (p = 0.727).
- Load averages, final batch, per run in pair order: 5.1, 6.8, 7.0, 8.4, 13.2, 9.2, 8.2, 9.1,
  9.9, 9.4, 7.5, 9.6, 8.8, 8.0, 6.6, 7.2. Parent-side median 8.9, HEAD-side 7.8.
- Notes:
  - The review of the first write-up killed it, correctly. It had converted an 18.7% ratio drop
    into "the read was 19% of the penalty", which is neither of the two consistent conversions
    (28% in ratio units, 11% in microseconds); it offered the in-memory arm as a control against
    contention when that arm has no power to detect it and is itself treated by T41; and it
    quoted an unpaired rank test on a paired design at 30x the confidence the design supports.
  - Anyone re-running this on eight pairs should expect a null result about as often as not.
- Deferred: none. The swap's own cost, and the shared five-shard geometry behind it, are T45 and
  T46.

## T43 — A store read that fails must not read as an absent mapping

- Status: complete
- Commit: d501a8408e8
- Result: `IndexBackedMappingStore.read` propagates everything except `IndexNotFoundException`.
  `updateMapping` retries a failing read the same 16 times the old accidental loop gave it, then
  raises the read's own failure instead of "did not converge, which means sustained contention".
- Mutation: restoring the blanket catch fails all three propagation tests. Verified twice, because
  the first two versions of the update-path test were green under mutation: one used a throwing
  double that never touched the store holding the catch, the other failed every client action so
  the exception came from the swap rather than the read.
- Notes:
  - The premise in the task and in the first write-up was wrong. A swallowed read cannot silently
    overwrite through this store: a merge from empty proposes generation 1 and external versioning
    refuses it. The real cost was 32 wasted round trips and a misdiagnosis.
  - `StoreBackedFieldRefresher` still degrades rather than fails, now explicitly. It does not
    protect the document, because inferring the field sends an auto put-mapping into the same
    unreadable store; the comment says so.
  - Deliberate hole: a mapping index deleted under a live cluster reads as absence and the merge
    that follows would write one field where there were many. Filed as T48.
- Gates: green, 9m11s.
- Deferred: T48.

## T44 — Prove the mapping store is never touched from a cluster state thread

- Status: complete
- Commit: adc0c1a183d
- Result: the property holds for the shape tested. A gated creation carrying
  `index.serverless_storage.enabled` in its request settings, and a put-mapping against a gated
  index, both reach the store only from GENERIC.
- Mutation: restoring the pre-T19 code (no GENERIC dispatch, `recordGatedMapping` inside
  `PutMappingExecutor#execute`) fails in about a minute with "put-mapping called the mapping store
  from [opensearch[node_s2][clusterManagerService#updateTask][T#1]]". Against the first version of
  the test the same mutation hung instead and was killed at ten minutes, because the request never
  returned and the thread check sat after it. Bounded waits and a finally fixed that.
- Second mutation, for sensitivity: adding "generic" to the forbidden list fails with the thread the
  calls actually come from, which confirms the recording path is wired rather than silently empty.
- Notes: the review found a real hole this test does not cover. A template-gated index is not
  admitted off-thread (the admission check reads request settings only), so it reaches
  `clusterStateCreateIndex` on the cluster manager thread and calls `createMapping` there. Three
  comments in the tree currently assert that cannot happen. Filed as T49; the class javadoc says
  what it does and does not prove.
- Gates: green, 9m43s.
- Deferred: T49.

## T45 (planned as T28) — The mapping index's geometry is a hardcoded 5

- Status: complete
- Commit: dd524288c65
- Result: `serverless_storage.mapping_index.shards`, defaulting to the 5 the code already used,
  bounded at 1024 so a typo fails the node at startup rather than the first mapped gated creation.
- Mutation: making the store ignore its constructor argument fails
  `testTheConfiguredShardCountReachesTheCreatedIndex` with "expected:<17> but was:<5>".
- Wiring: the unit test proves the constructor argument is used and nothing about whether anything
  passes it. Reverting the plugin edit left the entire suite green, because every IT installs the
  gate by hand with a default-constructed store. `BlobBackedDescriptorIT`, the only class that runs
  the plugin's own install path, now sets the count to 3 and reads the created index back.
- Notes:
  - The first javadoc claimed the shard count is "the width of the funnel" the creation cost runs
    through. Nothing shows that: a single-document write goes to one shard whatever the count is,
    and it pre-judges what T46 is meant to decide. Rewritten, and the read-side cost of raising it
    is now stated (the stats aggregator fans out across every shard; 2n with a replica).
  - A node started after the index exists reads a value that can never apply. The store logs it,
    which is the only evidence available.
  - T46 cannot vary the geometry within one cluster. Recorded in its plan entry with the three
    ways it silently fails.
- Gates: one full run failed `GatedIdleEvictionIT.testAnIdleGatedIndexIsClosed...` at load 16-18;
  it passed alone three times at load 6-10. This task adds one gated creation to an existing IT.
- Deferred: none.

## Numbering correction

Committed as 6848ad5471f, after T45 landed. This round was planned as T23 to T32 on the strength
of STATE.md saying numbering ran to T22; the tree cites up to T39 and eight of the ten were taken.
Renumbered to T40 to T49. Commit messages from this round still carry the old numbers.

## T46 — pre-registered before the runs

Written and committed before any measurement, because a threshold chosen after seeing the numbers
is not a threshold.

- Conditions: mapping index at 1, 5 and 20 shards. One cluster per condition, since the geometry
  cannot be varied within a cluster; three runs per condition, each its own cluster.
- Metric: the round 2 mapped-to-unmapped ratio, which normalises the machine within each run.
- Decision rule: compare the median ratio across the three conditions. If the spread between
  condition medians is no larger than the largest within-condition spread (max minus min of the
  three runs at one geometry), the result is "no measurable effect at this precision" — a completed
  task, not a failure. Only a between-condition spread exceeding the within-condition noise counts
  as the geometry mattering.
- Every run asserts the live shard count of `.opensearch-index-mappings` before timing anything.

## T46 — Measure the mapping index's geometry against creation throughput

- Status: complete, negative result
- Commit: c84d3268bec
- Result: no measurable effect at this precision. Condition medians 3.58x (1 shard), 2.15x (5) and
  2.90x (20) against the unmapped control, with a largest within-condition spread of 3.91x. The
  between-condition spread of medians, 1.43, is well inside it, and the ordering is not monotonic in
  shard count. By the rule fixed before the runs, that is "no measurable effect".
- Runs: 1, 5 and 20 shards, three runs each, one cluster per run, interleaved by run so drift spreads
  across conditions. Load medians 13.7, 11.7 and 14.6 respectively.
  Raw: 1 shard 2.03/3.58/3.72, 5 shards 2.05/2.15/3.13, 20 shards 2.44/2.90/6.35.
- Every run asserted the live shard count of `.opensearch-index-mappings` before timing. Without that
  assertion this measurement is not possible to do correctly: the index is created lazily and never
  deleted, so a second geometry in the same cluster silently runs against the first.
- Why the answer makes sense, and why it was still worth measuring: a creation writes one document,
  one document goes to one shard, so the shard count cannot divide a cost that was never spread
  across shards. The RFC had called this index "a fixed-geometry funnel on the mapping write path"
  for several rounds. It is a funnel in the sense that everything goes through it, and not in the
  sense that its width matters.
- Consequence: the remaining cost of the mapping write is the round trip itself, so the levers are
  batching or moving it off the blocking path. The T45 setting stays, because the read side has its
  own reason to care about the shard count.
- Deferred: none.

### T46 gates

Run after the commit rather than before it, which is out of order and worth recording as such. Two
failures, both unrelated to a diff of documentation plus an opt-in test:
`ChaosMultiOperationRegressionTests.testMaterializationDetects...` and
`SegmentReplicationWithNodeToNodeIndexShardTests.testSnapshotWhileFailoverIncomplete`. Each passed
alone three times at load ~14. The first appeared to fail three times alone as well, which was the
command rather than the code: the filter was passed to `:server:test` too, where it matches nothing
and the build fails on that.
