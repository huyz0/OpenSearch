# Round 004 log

Append-only. One entry per task, written when the task's commit lands.

## T23 — Split the mapped creation cost into store traffic and mapping work

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
    implementation (request building, field type counts, the exists check). T24 removing one
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
  rather than filed as a task, because T24 changes the arm it would measure.

## T24 — Stop reading a mapping that cannot exist yet

- Status: complete
- Commit: 20079e11096
- Result: a gated creation's mapping write is one round trip instead of two.
  `MappingGenerationStore.createMapping` swaps at generation 1 and keeps read-and-merge as its
  fallback. Whether that made creations measurably faster is not claimed here: one run after
  the change read 2.21x against the unmapped control, inside the 2.0x to 2.9x range measured
  before it, at load average 20. T25 measures it.
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
  - T23's early return in the measurement class's teardown had also dropped an unconditional
    `DescriptorGate.uninstall`. Restored. See the gates note below.
- Gates: green, 12m01s at load average 16. An earlier run failed
  `GatedCreationSwitchIT.testInstallingWhileDisabledLeavesNothingGated` on "a disabled gate
  must gate nothing" at load average 30, which is the leaked-registry signature this package
  produces. It passed alone three times and twice beside the classes this task touches; the
  dropped `uninstall` was the one plausible link and is restored either way.
- Deferred: none.

## T25 — Re-measure the mapped ratio with the create-path read gone

- Status: complete
- Commit: pending in this entry's commit
- Result: T24 is worth about a seventh to a fifth of what a declared mapping costs a creation.
  Final batch, eight pairs on matched storage with alternating order: median ratio 2.82x to
  2.42x, median per-creation penalty down 15%, the same runs read as 22% in ratio units. HEAD
  won 5 of 8 pairs in that batch, which is not significant. Across all four batches, 36 pairs,
  HEAD won 27, sign test p = 0.004, and every batch's median favoured HEAD.
- Control: the unmapped arm within each run, plus T23's repeat arm, which moves with the front
  arm on both sides (median ratio 2.73x parent to 2.32x HEAD).
- Deviation from the acceptance criteria, stated plainly:
  - The criterion was two runs a side with the unmapped control agreeing within 10% across the
    four. Five consecutive sets failed it, best spread 13.4%. This box cannot hold a control
    that still for four consecutive runs, so the design changed: the metric is the within-run
    ratio, which normalises the machine by construction, and the sample is eight pairs.
  - The criterion said "the T23 harness, unchanged". Not strictly true: T24 added a read counter
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
    contention when that arm has no power to detect it and is itself treated by T24; and it
    quoted an unpaired rank test on a paired design at 30x the confidence the design supports.
  - Anyone re-running this on eight pairs should expect a null result about as often as not.
- Deferred: none. The swap's own cost, and the shared five-shard geometry behind it, are T28 and
  T29.
