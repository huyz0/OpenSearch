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
