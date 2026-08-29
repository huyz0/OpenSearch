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

## Jitter

Renewal moved off `scheduleWithFixedDelay` onto a self-rescheduling `schedule()` that re-draws a ±20%
offset **every renewal, not just the first**. A fixed interval never pulls co-started nodes apart: a
thousand nodes launched by the same orchestrator in the same second renew in the same millisecond
forever, which is a thundering herd against the object store every `ttl/3` — worst exactly when the
fleet is largest. Re-jittering each time also means nodes that shared a pause drift apart again instead
of locking back into step.

Measured: **159 distinct delays over 200 draws, spanning 801–1199 ms** on a 1000 ms interval. The
assertion is phrased as the number that is 1 when jitter is broken.

The reschedule sits in a `finally`, and `renewNow` never throws. A renewal loop that stops rescheduling
because one pass failed is a node that looks alive right up until its lease lapses —
`testLeasesAreRenewedWithoutAnybodyTicking` catches that, because it asserts renewal *repeats* rather
than merely happens.

## Demand-driven activation, and why it had to be decided here

`main()` turned out to be blocked on this rather than adjacent to it. A booted node has to know what to
serve, and there is no allocator to tell it — so "what does a fresh node do?" is not a question a
bootstrap can dodge.

Two answers, and they are genuinely different systems:

- **Take only what you were told to want** (the behaviour up to now). Placement is somebody else's
  decision, which means something outside the system assigns shards — the controller this whole design
  deleted, reintroduced at the edge.
- **Take an unowned shard somebody asked for.** Placement becomes "whichever node the client happened to
  ask". No allocator exists at all; capacity appears where load appears.

Built as a switch, **default off**, so the existing behaviour is preserved and turning it on is visible
rather than a drift. `testByDefaultANodeDoesNotTakeAShardNobodyToldItToWant` pins the default: three
writes arrive, three are refused, an activation pass demonstrably runs, and the shard stays unowned.

Safe under races either way, because acquisition is a compare-and-swap: two nodes reaching for the same
unowned shard produce one owner and one node that forwards.

**The cap is what makes it safe to leave on.** Past `maxShardsHeld` a node refuses, the caller still sees
no owner, and the next node asked takes it instead — admission control without an admission controller.
Refusing is logged as a routing outcome, not an error, because a node that is full and a node that is
broken should not page the same person. The default of 1000 is reasoned, not measured: phase 9 measured
index residency and never shard-count scaling, so nothing here knows what a node can actually hold.

## A node as a process

`ServerlessBootstrap` is the last of the headline gap. The scheduler made a node *capable* of running
unattended; until this existed nothing ever built one outside a test method, so the claim was true of an
arrangement that only occurred inside JUnit.

`main()` reads settings from system properties — no configuration format invented before there is
anything to configure — installs a shutdown hook, and blocks. `start(Settings)` is the same path, usable
from a test, which is how the tests below drive it.

Two ordering decisions that are not tidiness:

- **The scheduler starts last**, after the node is serving and the plane is attached. A renewal that ran
  earlier would advertise a lease pointing at an address that refuses connections — and peers resolve
  forwarding targets from exactly that lease.
- **Shutdown stops the clocks first**, then releases the lease. A renewal racing the release would put
  the lease back after it was dropped.

**The daemon's default for on-demand activation is the opposite of the library's, deliberately.** A
library that silently changed placement policy would be a trap; a daemon that started owning nothing and
never acquired anything would be useless. Both defaults are visible and
`serverless.activation.on_demand` controls the process one.

### The vacuous assertion this found

`testACleanShutdownDoesNotLeaveAGhostBehind` first asserted the lease was *not live 60 seconds from now*.
With a 600 ms test TTL that is true whether or not shutdown does anything — it was measuring the clock,
not the code. Checking revealed why it passed: `BlobLeaseMembership.release` existed and **nothing in
production called it**. So a node that shut down cleanly stayed alive to every peer for a full TTL, and
its shards were unowned and unclaimable that whole time, because a live lease is never stolen.

Fixed both ends: the bootstrap now releases on close, and the assertion is *absence*, which is the one
that fails when it does not. Releasing is what makes a restart a handover rather than a failover, and it
costs one delete.

## What this does NOT establish

- **The process is filesystem-backed only (D5).** `serverless.store.path` is a local directory standing
  in for an object store. `ServerlessBootstrap` is a real process but not a deployment story, and R11 is
  what would lift that. Accepting an S3 bucket it has never been run against would be worse than saying
  what it is.
- **Nothing has run more than one process at a time.** Every bootstrap test starts one node. Two nodes
  contending for a shard, a real kill -9 rather than `close()`, and a restart taking its own shards back
  are all untested at the process level.
- **Demand-driven activation is off by default and unmeasured.** The mechanism exists and is tested;
  no deployment has run with it on, its cap is a guess, and nothing balances shards across nodes once
  they are taken — a node that receives a burst of first-writes takes all of them up to its cap while
  its neighbours stay empty.
- **No scale-to-zero controller**, and the two liveness modes (opt-in batched vs per-head) still both
  exist. Both remain open in M13.
- **Jitter is unvalidated at fleet scale.** ±20% spreads 200 draws from one node; nothing has observed
  a thousand nodes actually failing to collide, and the fraction is a guess.
- **Intervals are unmeasured.** `RENEWALS_PER_TTL = 3`, a 500 ms debounce and a 30 s backstop are
  reasoned defaults, not measured ones. The reasoning is written down above; no experiment backs the
  numbers.
- **Single-JVM only.** Every test here runs one node in one process against `FsBlobContainer`. D5.
