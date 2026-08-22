# Round 004 retrospective

## Completed

All ten tasks, T40 to T49. Renumbered mid-round; commit messages still carry T23 to T32.

| task | what it did | commit |
|---|---|---|
| T40 | attributed the mapped creation cost | `5eb5ca0976b` |
| T41 | stopped the creation path reading a mapping that cannot exist | `20079e11096` |
| T42 | re-measured what T41 bought | `ed4e984ba46` |
| T43 | made an unreadable store say so | `d501a8408e8` |
| T44 | asserted which thread reaches the store | `adc0c1a183d` |
| T45 | made the mapping index's shard count a setting | `dd524288c65` |
| T46 | ruled the geometry out as a cost | `c84d3268bec` |
| T47 | removed a deleted index's mapping | `5024c4edc92` |
| T48 | refused to call a lost mapping an absent one | `2a61064cf90` |
| T49 | admitted a template-gated creation off the state thread | `d39300665c9` |

Measurements, with their controls:

- **T40.** The index-backed store is at least about 80% of what a declared mapping costs a gated
  creation. Five runs, share taken from both the front arm and its repeat: 99/99, 115/109, 83/83,
  95/94, 101/101 percent. Control: the unmapped arm in the same run, plus a repeat of the
  index-backed arm at the end of each round. Over 100% means the residual measured negative, so the
  claim is a floor rather than a figure.
- **T42.** Direction only. The confound-free batch, order balanced and both sides on the same
  filesystem, was 5 of 8 pairs favouring HEAD (p = 0.727). Pooling the three batches gives 27 of 36
  (p = 0.004), but two of those batches carry confounds this task itself identified, so the pooled
  number is an argument rather than a control.
- **T46.** Negative, by a rule fixed before the runs. Condition medians 3.58x, 2.15x, 2.90x at 1, 5
  and 20 shards, against a largest within-condition spread of 3.91x, and not monotonic. The mapping
  index's geometry does not move creation throughput.

## Deferred

- **T50**: a mapping written after its index was deleted is stranded forever. Found during T47.
- **T51**: three doors into gated creation have no admission check (auto-creation, rollover, data
  stream creation). Found during T49's review. Highest severity of the three.
- **T52**: admission and creation still resolve templates differently in three cases.
- **T53**: the threading proof cannot attribute a store call to a phase.

Plus two limits recorded rather than filed, because closing either needs durable evidence that the
mapping store was lost, which means recording it somewhere that is not the store: T48's latch is per
node and the node that matters is the elected cluster manager, and there is a one-read window because
cluster state is visible before listeners run.

## What the plan got wrong

**The task numbers.** The plan took STATE.md's word that numbering ran to T22. The tree cites up to
T39, and eight of the ten numbers this round claimed already belonged to earlier work. Five commits
shipped citations that collide with existing ones before this was noticed, and it was noticed by a
reviewer rather than by the loop. The check that would have caught it is one grep, which is now in
STATE.md in place of the sentence that was wrong.

**T43's premise was false.** The task said a swallowed read failure lets the merge overwrite a stored
mapping. It cannot, through this store: external versioning refuses the merge-from-empty. The real
cost was 32 wasted round trips ending in a "sustained contention" message that named the wrong cause.
The fix was still worth making; the reason written into the plan was not the reason.

**T46's acceptance criterion could not be met as written.** It asked for the T40 harness at three
shard counts, which cannot be done in one cluster: the setting is node-scope and read at startup, the
index is created lazily and never deleted, and a second store with a different count silently runs
against the first geometry. Discovered in T45, recorded before T46 ran.

**T42's acceptance criterion was unachievable on this machine.** It required an unmapped control
agreeing within 10% across four runs. Five consecutive sets failed it, best 13.4%. The task ran to a
different design — larger sample, paired, control-normalised — which was the right answer but was not
what the plan asked for, and the plan had no way to say "if the box cannot hold still, do this
instead".

**Three tasks were mis-sized.** T40 was planned as one measurement and became a measurement plus
three harness defects. T47 was planned as one call site and shipped as two, because the first version
covered only the all-gated deletion path. T49 was planned as a fix and was really a design decision
with a fix attached; it was the one task that needed the user's input, and the plan did not flag it.

## What was found rather than planned

Most of the round's defect value:

- The measurement class had been leaking its whole population on every run for as long as it had
  existed, because its cleanup deleted by a wildcard that could never match, and the refusal was
  caught and logged as "could not clean up". Its own javadoc blames that leak for breaking three
  unrelated tests.
- A partly failed measurement arm was reported as a slow arm.
- Nothing anywhere deleted from `.opensearch-index-mappings` (T47).
- The mapping store reported every failure as "this index has no fields" (T43).
- A template-gated creation wrote its mapping from the cluster manager's update thread, where the
  write can submit a cluster state update and wait for the thread it is standing on (T49).

The planned work was the measurement and the one wasted round trip. The found work was four defects,
three of which are silent-loss or deadlock shapes. That ratio is the same as the previous cycle's,
which the round's own opening section notes. Two cycles running, the plan has been aimed at the
smaller half of the value.

## Round-level review findings

Each becomes a task in round 005.

1. **Reads refuse after the store is lost; writes rebuild it quietly.** T48 wired its signal into
   `read` only. `compareAndSwap` still short-circuits on a cached "the index exists" flag, so the next
   creation after a deletion auto-creates the mapping index at cluster defaults: T45's setting void,
   `fields` dynamically mapped so the index absorbs every gated index's field names, and
   `fieldTypeCounts` no longer nested so cluster stats silently drops the gated half. The round
   documented this three times as a test-arrangement problem and never as a defect.
2. **`updateMapping` raises a stale read failure as the cause of a contended update.** The last read
   failure is never cleared, so one transient block on attempt 0 followed by fifteen genuine swap
   losses reports the block. That is the misdiagnosis T43 set out to remove, reintroduced by T43's own
   code. The retry it added also has no backoff, so the resilience it claims to preserve is not there.
3. **`DescriptorGate`'s registration site still carries the falsified thread claim.** T44 corrected it
   in the store, T49 in the core, and the copy directly above `MappingGenerationStore.register` still
   names two callers, both incompletely, and asserts the property T49 exists because it was false.
4. **`settingsForAdmission` is computed twice per creation, against two different cluster state
   snapshots**, and the comment claims the second reader shares the first's computation. The second
   evaluation runs on the state update thread, which is the round's entire subject.
5. **Seven implementations of a three-method interface, four of them the same decorator.** The next
   interface method has to be added to all seven by hand; T47 already paid that, and T53 exists
   because two independently written counting doubles diverged.
6. **The floor is quoted three different ways** across the RFC, the plugin, the store javadoc and
   STATE. Reconciled during this close.
7. **`IndexBackedMappingStoreTests`'s class javadoc asserts a protection two of its own tests
   disprove**, since external versioning stops protecting once the document is gone.
8. **`Store.read`'s contract now forbids what its only implementation does** on the recovery path T48
   documents: after a cluster manager restart following a deletion, it answers "absent" for a case it
   cannot find out about.

## What to change in planning

- **Check T-numbers against the tree, not against STATE.** The grep is in STATE now. Planning should
  run it rather than read the paragraph.
- **A measurement task needs its threshold and its discard rule written before the runs, and a
  fallback for when the machine cannot meet them.** T46 pre-registered and was clean. T42 did not have
  a fallback and had to invent one mid-task, which is how a design gets chosen after seeing data.
- **Budget a worktree build and four to ten runs per measurement task.** T42 took three batches of
  runs and two design revisions. One run per side was never realistic on this box.
- **A task whose fix has more than one shape is a decision, not a task.** T49 should have been planned
  as "choose between A and B, then implement", with the choice surfaced early rather than at the end
  of a context window.
- **When a task's premise is a causal claim, plan a step that checks the premise first.** T43's
  premise was wrong and the task still produced a good fix, but the commit message had to correct the
  plan rather than cite it.
- **Expect the found-work ratio.** Two cycles running, most of the value came from defects walked into
  rather than from planned work. Round 005 should either plan explicitly for discovery time or accept
  that its estimates describe half the work.
