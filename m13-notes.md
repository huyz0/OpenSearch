# M13 (part): the node runs by itself, and each driver runs for its own reason

## What was actually wrong

Not "unverified". **Absent.** `tick()` and `heartbeat()` existed, worked, and were called only by tests.
A node deployed as it stood would have let its leases lapse, published nothing, and never picked up a
dead node's shards — while looking perfectly healthy right up to the moment its TTL ran out.

Every other gap in §12.1 is something the system does imperfectly. This was something no arrangement of
the system did at all.

## Why not one timer around `tick()`

That was the obvious fix and it was rejected in conversation before any code was written. Wrapping the
whole pass in one `scheduleWithFixedDelay` makes every latency in the system equal to one number:

- a write's durability window becomes the tick interval, even though the node knows the instant a
  document lands;
- a dead owner goes unnoticed for a full interval, even though the very next write to that shard proves
  it;
- and shrinking the interval to fix either one multiplies the object-store bill for the other, because
  ops are `nodes × shards ÷ interval`.

The jobs have different triggers. Only one of them is a clock.

## The three drivers

| Driver | Trigger | Why that trigger |
|---|---|---|
| Lease renewal | timer, `ttl / 3` | A lease is defined in wall-clock terms. Nothing happens to tell a node its lease is about to lapse, so nothing but the clock can. Three renewals per TTL means two may be lost — GC pause, slow store, dropped connection — before a healthy node loses a shard it is still serving. |
| Publication | edge: a write landed | The node knows the moment a document is applied. Waiting for a timer to rediscover that only widens the loss window. **Coalesced, never per-write.** |
| Activation | failure: ownership looks wrong | A publish fenced by a newer term, a forward to an owner with no live lease, a write to a shard nobody holds. Each is evidence a head is stale, and each arrives long before a timer would notice. Also coalesced. |

Plus a **backstop**: the full `tick()` on a slow timer. It is what makes the two edges safe to be lossy.
Edges are accelerators; the timer is the guarantee. A system that converged only on edges would wedge
the first time a signal was dropped, and the symptom would be a shard no node serves and no node is
looking for.

## Coalescing is the point, not an optimisation

Publishing per write would turn a 10k/s ingest into 10k uploads/s. The first write in a quiet period
schedules a publish one debounce window out; every write inside that window joins it. One upload per
window whatever the write rate — `compareAndSet`, so writes two through a million are free.

Measured: **200 writes produced 1 publish.** The canary that removes coalescing (publish inline on every
write) fails that test.

## What is genuinely new versus rearranged

`BackgroundReconciler.tick()` still does all four jobs in the same order — it is now their composition
rather than their implementation, so all 20 existing `tick()` call sites are unchanged and
`testTickStillDoesEveryJobItUsedTo` guards that. Genuinely new:

- `ReconcileSignals` — the two edges, with a `NONE` default so a node that reports nothing still
  converges through the backstop.
- `ReconcileScheduler` — the timers, the coalescing, and `startFor(node, plane, loop)`, the single call a
  bootstrap makes.
- `publishDirty()` / `markDirty()` alongside `publishAll()` — the edge path narrows the scan; **the
  `maxSeqNo` guard stays on both**, so a spurious mark is never an upload.
- Fencing became a signal. Previously a `StaleWriterException` propagated out of the tick; now the shard
  is released immediately and an activation pass is requested, because being fenced is the clearest
  possible evidence that ownership moved.

## Why no write can fall between the coalescing and the publish

The two clearings look like a lost-update waiting to happen, and are not, for one reason that is easy to
break: **`wrote` fires after the document is applied and synced, never before.**

`publishNow` clears `publishPending` *before* `publishDirty` drains the marks. So for any write W:

- if W's `markDirty` lands before the drain's `removeAll`, then W was already applied before that
  `removeAll`, hence before the pass reads `maxSeqNo` — so this pass publishes it;
- if W's `markDirty` lands after, the mark survives, and W's own `compareAndSet` (which finds
  `publishPending` already false) schedules the next publish, which picks it up.

Either way W is published. Reverse the order — clear `publishPending` after the drain — and the first
case becomes a write that is marked, unmarked, and never published until the backstop.

## Guards against passing vacuously

Almost every test **disables the backstop**. With it running, every assertion would pass whether or not
the edge worked — the slow timer would eventually do the job and the test would be measuring patience.

Eleven canaries planted, run, and confirmed to fail the matching test:

| Defect planted | Test that caught it |
|---|---|
| write edge marks but never schedules | `testAWriteSchedulesItsOwnPublication` |
| ownership doubt goes nowhere | `testAWriteToAnUnownedShardIsWhatMakesSomebodyTakeIt`, `testAFencedPublishAsksForActivation` |
| fenced shard keeps being served | `testAFencedPublishReleasesTheShardImmediately` |
| `drainFenced` reports the same fencing forever | `testAFencedPublishReleasesTheShardImmediately` |
| idle guard removed | `testAnIdleShardIsNotPublishedHoweverOftenItIsMarked` |
| publish inline per write (no coalescing) | `testABurstOfWritesCostsOnePublishNotOnePerWrite` |
| renewal timer never scheduled | `testLeasesAreRenewedWithoutAnybodyTicking` |
| backstop timer never scheduled | `testTheBackstopConvergesWithEveryEdgeSuppressed` |
| `startFor` builds but never starts | `testANodeHandedAPlaneRunsWithoutBeingDriven` |

`testAnIdleShardIsNotPublishedHoweverOftenItIsMarked` also carries its own positive control: after
twenty marks produce nothing, one real write produces exactly one publish. Otherwise the twenty empty
results would be equally consistent with the plumbing being disconnected.

## One bug this found in its own tests

`testAFencedPublishReleasesTheShardAndAsksForActivation` asserted synchronously on a count that
`ownershipDoubted` increments asynchronously, and let the async activation re-acquire the shard while an
earlier assertion was checking it had been released — two claims racing inside one test. Split into
`...ReleasesTheShardImmediately` (driven through the reconciler, no asynchrony at all) and
`...AsksForActivation` (`assertBusy`, because handing the work off is the behaviour under test). The
race was real, not a flake: the fix was in the test, and the code was right.

## What this does NOT establish

- **There is still no `main()`.** The shell has no process bootstrap; nodes are constructed by tests.
  `startFor` is the call a bootstrap would make, and nothing calls it outside a test. "A node runs by
  itself once something builds one" is what was delivered; "a node exists as a process" was not.
- **No automatic reactivation beyond the `wanted` set.** A node takes a shard it was told to want. A
  shard nobody wants stays unowned no matter how many writes arrive for it — activation is
  failure-*triggered*, not failure-*driven placement*. Deciding that any node receiving a write should
  take the shard is a placement policy change, not a scheduling one, and was deliberately not smuggled
  in here.
- **No scale-to-zero controller**, and the two liveness modes (opt-in batched vs per-head) still both
  exist. Both remain open in M13.
- **No jitter.** Every node renews on its own fixed delay from its own start time, which spreads by
  accident rather than by design. A thousand nodes started by the same orchestrator at the same instant
  would renew in lockstep. Untested and unaddressed.
- **Intervals are unmeasured.** `RENEWALS_PER_TTL = 3`, a 500 ms debounce and a 30 s backstop are
  reasoned defaults, not measured ones. The reasoning is written down above; no experiment backs the
  numbers.
- **Single-JVM only.** Every test here runs one node in one process against `FsBlobContainer`. D5.
