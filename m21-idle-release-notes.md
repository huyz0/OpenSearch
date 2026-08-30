# M21: a node that lets go

Nothing in the shell ever released a shard voluntarily. A shard was given up only when ownership was taken
away — fenced, reassigned, or the lease lost. So a node that answered one query for an index held that
shard for the life of the process, `maxShardsHeld` was a refusal rather than an eviction, and "scale to
zero" meant an operator killing the process. Phase 5 verified it exactly that way.

## The policy is derived, not picked

Both sides were measured last milestone (`m20-fanout-notes.md`):

- **holding** an idle shard costs reads on every reconcile tick, in proportion to how many are held;
- **re-opening** one costs about 41 object-store requests, 12 of them data.

So releasing pays as soon as a shard stays cold for longer than re-open cost over per-tick holding cost —
on the order of a minute at the default backstop interval. The default is **five minutes**, an order of
magnitude above that, because the asymmetry is not symmetric: holding too long wastes a little money, and
releasing a shard that was about to be used costs a cold open and the latency a user sees.

Off in the library, on in the daemon — the same split `ON_DEMAND` already had, and for the same reason: a
test that drives ticks by hand should not have shards vanishing underneath it.

## The order is the correctness argument, and it is not the obvious one

A writer **publishes → releases its head → closes**.

Publishing first is *not* what makes the data safe; the log already does that and a successor would replay
it. It is what stops the release handing the next owner a log to replay for no reason.

**Releasing the head before closing is the part that matters.** A node that closed a shard while the head
still named it would answer 503 for that shard until its lease lapsed — volunteering to become the thing
failover exists to route around. If the head cannot be released, the shard is kept.

## Only requests count as use

`markUsed` is called from the write, get and search paths and nowhere else. The tempting place is
`ShardReconciler#shard`, and it would be wrong: publication, heartbeats and the garbage collector all reach
for shards, so every shard would count as busy because the node was maintaining it.

A shard that has never been asked for anything is treated as used when it opened, not as infinitely idle —
otherwise a demand-driven node releases each shard immediately after taking it, forever.

## Three canaries, and two tests that had to be fixed to catch them

| Planted defect | Caught by |
|---|---|
| Release on time since opening, ignoring use | busy-shard and idle-reader |
| Close the shard without releasing its head | idle-shard |
| Release without publishing first | writer-publishes — **only after the test stopped going through `tick`** |

The third is the instructive one. `tick` publishes everything dirty *before* it releases anything, so
inside a tick the release's own publish is a no-op and deleting it changes nothing. The test went through
`tick` and passed with the publish gone. It now calls `releaseIdle` directly, which is what makes the
method answerable for its own behaviour rather than for its caller's ordering.

## A bug in the wiring, found by the tests failing for the right reason

The first run released nothing at all. `markUsed` stamped `System.currentTimeMillis()` while the reconciler
compares against the clock a tick is given — which a test controls. Every shard looked as though it had
been used far in the future, so nothing was ever idle. The stamp now comes from the plane's clock, chosen
in one place on `ServerlessNode` rather than at each call site.

## What this does NOT establish

- **Nothing is measured about the controller itself.** The break-even above is arithmetic on two measured
  numbers, not an observation of a node that releases and re-acquires under load.
- **No hysteresis and no rate limit.** A shard right at the threshold, used just often enough, will be
  released and re-acquired repeatedly; nothing damps that.
- **The threshold is global.** A rarely-queried index and a hot one are treated identically, and there is
  no per-index policy.
- **Not tested on a bucket or across processes.** Same D5 caveat the rest of the failover suite carries;
  the release path does object-store work and has only been exercised against a directory.
- **`maxShardsHeld` is still a refusal.** Nothing releases the *least recently used* shard to make room for
  a new one when at the cap — only idleness releases, and a node at its cap full of busy shards still
  refuses.
