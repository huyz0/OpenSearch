# STATE

Read this first, every time. It is the only source of truth for where work stands, and it
is written to survive context loss: nothing here depends on remembering a previous session.

Updated: 2026-08-12 (correction below; body past this point not re-verified line by line)

## Position

**Correction found by grounding this file against the real tree (2026-08-12): everything below
this notice was accurate as of 2026-08-08 but the tree has moved on without this file being kept
in sync.** `git grep -hoE '\bT[0-9]{1,3}\b' -- server/src plugins/serverless-storage/src | sort -u
-V | tail -3` now returns **T104**, not T59 — the commit log shows task-numbered commits through
**T106** (`git log --oneline --all | grep -E '^[a-f0-9]+ T[0-9]+:'`), most landed in a burst on
2026-08-08/09 after this file's own "Updated" timestamp. T50 through T59 (this round's own plan)
are now: T50 done (third firing, commit 66f116a2c72), T51 blocked as B2 (unchanged, see below),
**T52 done too** (commit `4d53befd13e`, "Align admission template resolution for gated indices" --
landed without a "T52:" commit-message prefix, which is why a first pass grepping for that literal
string called it still open; the code comment above `settingsForAdmission` and the real
`AdmissionTemplateResolutionTests` -- 4/4 passing on a fresh run -- both cite T52 by name and cover
exactly this round's stated acceptance criteria: resize-target and system-index non-admission, and
data-stream-name resolution), T53–T58 done (commits 37bba504936,
6e848375d94, 803c6086100, 71f9c2da732, b6c2787d055, 3109191f655 and neighbors), T59 done as
recorded below. **T60 through T106 have no round directory under `docs/rounds/` at all** — only
`004-mapping-write-path/` and `005-store-failure-modes/` exist on disk, so whatever plan drove
T60–T106 (topically: `DescriptorCache`, `MappingGenerationStore`/`IndexBackedMappingStore`,
`TombstoneScrubber`, `DescriptorCheckTool`, `DescriptorEnumerator`, `IndexDescriptor` — all Area-H
descriptor/mapping-store work, consistent with round 005's own theme) was never committed as a
round plan/log/retro the way rounds 004–005 were. Before trusting the "Position" table below,
re-run the `git grep` above and read `git log --oneline -80` yourself — do not propagate this
table's numbers forward again without checking, which is the exact failure this correction is
fixing.

| | |
|---|---|
| Active round | 005, the doors into the mapping store ([plan](005-store-failure-modes/plan.md)) — as of 2026-08-08; stale per the correction above, T60+ is unaccounted for by this round's own plan |
| Next action | *(stale, see correction above)* was: `/round-next`, T51 through T58 open and unblocked; T52–T58 are all now done, T51 is blocked (B2) |
| Last task | *(stale, see correction above)* was T59 (commit 66a38381239); the tree's actual last task-numbered commit is **T106** (`2a623d36cf4`, "Add safePrefix null check in DescriptorEnumerator.expandPrefix") — but see the note above that T60–T106 have no corresponding round plan on disk to check off against |
| T51 attempt | Two firings, neither committed. First: refuted (disabled T49's tripwire instead of fixing it). Second: investigated, stopped deliberately without writing code — the acceptance criteria as expanded require a *gated* rollover/data-stream target's alias or backing-index membership to be recorded correctly, which traces to genuinely new machinery (alias mutation and data-stream resolution for indices with no cluster-state entry), not a bounded fix. Blocked as B2. Full write-up in [log.md](005-store-failure-modes/log.md#t51--second-firing-investigated-stopped-without-committing). |
| Branch | `feature/serverless` |

Rounds 001 to 003 predate this file and have no round directories. Their work is in the git
history and in [rfc-100m-index-architecture.md](../../rfc-100m-index-architecture.md).

**T-numbers are cited in permanent code comments, so check the tree before allocating them.**
This file used to say they ran to T22, which was wrong: the tree cites up to T39, and round 004
was planned as T23 to T32 on the strength of that sentence. Eight of those ten numbers already
belonged to earlier work, so the round was renumbered to T40 to T49 after T45 landed. To find the
next free number, do not trust this paragraph either:

```
git grep -hoE '\bT[0-9]{1,3}\b' -- server/src plugins/serverless-storage/src | sort -u -V | tail -3
```

## Progress

The 100M argument has no structural gap left: records live outside cluster state, creation
and deletion are off the serialized thread, resolution answers instead of silently
emptying, residency is bounded by construction, and mappings are carried.

Measured, with controls:

| | |
|---|---|
| heap per open gated index | 150,888 B |
| file descriptors per open gated index | 3.0 |
| open gated indices per node, 31 GiB heap | ~110,000 |
| creates per second, no declared mapping | 10,505 |
| creates per second, with a declared mapping | 2x to 10x slower, by warm-up; at least ~80% of it the mapping store. T41 removed one of its two round trips; T42 could not size that cleanly |
| deletes per second | 646 |
| eviction under CPU pressure (2026-08-12/13, see note) | quiet 17.2-17.5/s, loaded (20 burner threads) 18.5-19.2/s |

Two cycles running, most of the value came from defects found while doing something else rather
than from work that was planned. Round 004's planned work was one measurement and one wasted round
trip; what it found was a mapping store that reported every failure as "no fields", a shared index
nothing ever deleted from, a template-gated creation writing from the cluster state thread, and a
measurement class that had been leaking its whole population on every run. The cycle before it
(T13 to T21) went the same way: field parameters silently dropped from gated mappings, two blocking
round trips on the cluster manager update thread, and a headline throughput figure that only
held for a population nobody would create.

## Carried out of round 004

All of it is planned as round 005, T50 to T58. The two that matter most: a document indexed into a
name matching a gated template still reaches the mapping write on the cluster state thread (T51), and
a lost mapping index is refused by reads and quietly rebuilt at cluster defaults by writes (T54).

**Eviction under load: now measured for real (2026-08-12/13), not deferred a third time.**
`GatedEvictionUnderPressureIT` existed but had never actually been run -- both its tests are gated
behind `-Dtests.pressure=true`, which nothing had ever set. Ran it twice with a real JDK:

```
./gradlew :plugins:serverless-storage:internalClusterTest \
    --tests '*GatedEvictionUnderPressureIT*' -Dtests.pressure=true
```

| run | quiet drain (100 indices) | loaded drain (20 burner threads) |
|---|---|---|
| 1 | 5,809 ms, 17.2/s | 5,206 ms, 19.2/s |
| 2 | 5,712 ms, 17.5/s | 5,413 ms, 18.5/s |

**The finding: at this population (100) the eviction sweep itself shows no measurable slowdown
under synthetic CPU pressure** -- loaded and quiet drain rates are within noise of each other
across both runs, not the order-of-magnitude collapse `GatedIdleEvictionIT`'s own residency-peak
finding (20/200 quiet vs 148/200 "under an unrelated build") made plausible. This directly answers
the RFC's first order-of-work item for the first time with real numbers rather than carrying
"unmeasured" forward a third time.

**Two things this does not settle, so the item is measured, not closed:** (1) `GatedIdleEvictionIT`'s
own finding was about resident *peak* under real incidental contention (an unrelated build sharing
the box), a different signal from this test's synthetic burner-thread *drain rate* -- the two
should not be read as confirming or contradicting each other. (2) Run 2's `testHowFastAQuietNodeDrains`
took 2,768 s of total JUnit wall time for a 5.7 s measured drain -- the fill phase (creating and
populating 100 indices before the timed drain starts) is not what this test times, but something
outside the measured window varied by roughly 40x between runs on this shared, documented-as-noisy
box (see the Environment section below). Almost certainly host contention rather than a code
regression, given the actual timed metric stayed consistent across both runs, but not confirmed --
worth a controlled re-run on a quiet box before ruling out a real fill-phase issue. Also note the
table above's now-superseded "eviction under CPU pressure | 19.6 to 1.5 per second" row predates
this section calling the item "still unmeasured," which was already an internal inconsistency in
this file before this correction -- left visible in the table's history rather than silently
erased, but do not trust the 1.5/s figure's provenance without finding where it actually came from.

## Open, not blocked

Work that can start without asking anyone. Round 004 took the mapping cost item; T22 was
refiled as T40 in its plan. The rest are candidates for round 005:

- **Eviction under load**, the RFC's first order-of-work item. Since it was written the
  ceiling gained `indices.gated.max_open` and an eviction on the open path, which may have
  answered it. Whether it did is unmeasured.
- **T14**: cluster-state publication latency against cluster size, which decides whether
  wake and sleep need batching. Estimated at 50 to 200 ms and never measured.
- **T16**: computed placement under hot-tenant skew. A hash cannot know one tenant takes a
  thousand times the traffic, and K=3 gives room to choose rather than solving it.
- Pre-warm before rotation. 12.4% of shards lose all warm candidates at fleet-doubling.
- Manifest sharding aligned with the routing hash, so a coordinator warms its whole
  partition in one read.
- **Leading wildcards (`*-logs`)**: Unsupported and explicitly restricted in the serverless API contract (removed from planned work; object storage prefix listings serve trailing `prefix*` patterns only).

## Blocked

Items needing a human decision. The loop skips these and continues with other work.

### B1. Fork or upstream contribution

Computed placement replaces OpenSearch's placement model for serverless indices rather than
optimising it. That is a hard sell upstream while being the obviously right answer for a
serverless-only system. The codec namespace question, reserving a high integer range versus
a distinct blob codec name, is downstream of this and straightforward either way.

Nothing in the current task list depends on the answer, so the loop can run without it.

### B2. Can a gated index carry a post-creation-mutable alias or data-stream membership at all

T51 needs a gated rollover target's alias, and a gated data-stream target's backing-index
membership, recorded correctly, not just its mapping write made safe. Both traced to the same wall:
`DescriptorRepresentable` already refuses to gate any index with an alias, deliberately (T29) —
alias mutation is a cluster-state update that looks the index up in `Metadata`, a gated index is not
in `Metadata`, and a set-once alias that can never be repointed was rejected as a partial feature.
Baking a rollover alias in at the target's own creation only survives the *first* rollover of that
alias; the second rollover mutates the alias on what is by then a previous gated target, hitting the
same wall from the other side, and `MetadataIndexAliasesService` has no gated-index handling to
extend. Data-stream backing-index membership is worse: it lives in `Metadata.custom` as a list the
`metadataTransformer` appends to, that transformer never runs for a gated creation, and every other
reader of `DataStream.getIndices()` assumes `metadata.index(name)` resolves — teaching that
assumption to tolerate absence is the same generalization `AbsentIndexRoutingSuppliers` and
`AbsentIndexDescriptorSuppliers` already had to build for ordinary get/search/bulk, applied fresh to
a second, structurally different subsystem.

The question for a human: does gating extend to cover post-creation-mutable aliases and data-stream
membership (a genuine design/build effort, roughly the size of the routing work gating already
needed), or does gating stay permanently scoped to indices that will never need either — in which
case T51's auto-creation door (no alias, no data stream) can still be fixed on its own, and the
rollover/data-stream doors are declined from gating rather than fixed, closing the tripwire without
meeting the letter of "a gated rollover target's alias ... recorded correctly." Full trace in
[log.md](005-store-failure-modes/log.md#t51--second-firing-investigated-stopped-without-committing).

## Environment

- `-Dbuild.docker=false` on every Gradle invocation. Docker is not running, and a dead
  daemon fails every task including `compileJava`.
- The box is shared and often at load average 20 or higher. Timing-sensitive tests fail
  under contention and pass alone; the rule for telling that apart is in `/round-next`
  step 4.
- Do not kill the unrelated `cargo-mutants` and `cargo test` jobs belonging to
  `lucene-rust`.
