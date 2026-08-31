# M38 — a reader that goes stale, and a collector nothing ran

Two gaps that had one thing in common: both were *documented* rather than fixed, and neither would ever
have paged anybody.

## A reader served one commit forever

A reader is a cache of a published commit. Nothing invalidated it. A search node that opened shard 0 went
on answering from the commit it opened for as long as it held the shard — however much the writer published
afterwards, with no error, no flag, and no gap in the coverage the answer reported.

`testAReaderPicksUpALaterCommitWhenReopened` had recorded this since phase 5, with a comment saying "Phase
8's notification is what makes this automatic". Phase 8 came and went. At the shard level the behaviour is
defensible — an open reader must not change underneath a running query. At the deployment level it means a
search returns last hour's data and says nothing, which is the confident wrong answer this design refuses
everywhere else.

A reconcile pass now compares the commit each reader was opened at against the one that is published, and
lets go of the ones that have moved. The next search reopens — and the next search is the only thing that
proves the shard is still wanted at all, so a reader nobody comes back for costs one release and then
nothing.

**Release, not reopen.** Moving an open reader onto a newer commit would mean handing a live
`ReadOnlyEngine` a different commit. Core does not offer that and should not.

**The comparison is term *and* files.** Either alone is wrong: a writer publishes many commits at one term,
which is the ordinary case; and a failover can produce the same file names in a different term container,
which are different bytes.

**Frozen views are exempt**, and the exemption is now load-bearing rather than accidental. It first worked
only because a view had no recorded commit to compare — so the canary for the exemption could not fail. A
view records the commit it serves like any other reader now, and is skipped because it is a view.

## The collector had never run

`GarbageCollector` was reachable from a test and from an operator, and from nothing else. A failover leaves
a zombie's unpublishable files behind; merges leave superseded segments; both accumulated for the life of
the deployment. Storage that only grows is not a bug anybody is paged for, which is why it lasted.

**Driven by publishing, not by holding.** A shard nobody writes to produces no garbage, so sweeping every
held shard on a schedule would pay a listing per shard per pass to be told nothing had changed. The pass
sweeps what it has just published, plus what a previous sweep is still watching. An idle deployment pays
nothing.

## The grace, which is the part that needed designing

Running the collector on a schedule is not the same as running it by hand, and the difference is a reader.

A reader holds commit C. The writer publishes C', which no longer names file `f` in a dead term container.
`f` is now exactly what the collector's rule marks as collectable — and exactly what the reader is still
reading. Sweeping immediately turns that into a reader failing mid-query, which looks like corruption and
is not.

So a blob must be seen unreferenced by **two consecutive sweeps** of the same shard before it is deleted.
The interval between them is the grace, and it must be at least as long as the interval on which readers
refresh — which is why the two halves of this milestone are one milestone.

**The watch set is in memory and per-owner, deliberately.** A node that has just taken a shard over starts
with nothing, so its first sweep deletes nothing: a new owner cannot know how long a blob has been
unreferenced, and assuming "long enough" is the one assumption that loses data. Forgetting always delays a
deletion and never causes one.

A grace of zero means "delete on the first sweep". The first version built the eligible set from what a
previous sweep had seen, which made a grace of zero delete nothing on the first pass and everything on the
second — a knob that looks off and is really set to one. `testAGraceOfZeroSweepsImmediately` found that
within a minute of being written.

## Expiry that only stopped answering

A point in time past its keep-alive was refused, which is the visible half. The record stayed in the object
store pinning its blobs against the sweep, and the shards a node had opened for it stayed open — counted
against the node's bound, never a candidate for eviction, serving a commit nobody could still ask for. A
keep-alive that only stops answering is not a keep-alive. The pass reaps the records and closes what this
node was holding for views that are gone.

Local shards are closed by asking what this node holds rather than by acting on what this pass deleted:
another node may have reaped the record first, and this node's shards would then be held against a view
that no longer exists anywhere.

## A canary that found the wiring, not the code

Canary 70 removed the `sweepPublished` call from `tick`, and **nothing failed** — every test drove the
sweep directly. A correct method nothing runs is the same as no method. `testTheBackstopPassSweeps` exists
because of that canary, and the canary is caught now.

## Canaries

- **64 — nothing lets go of a stale reader.** Caught.
- **65 — a pass releases readers whose commit has not moved.** Caught: a warm cache turned cold on a
  schedule.
- **66 — a pass refreshes frozen views too.** Caught, but only after the exemption was made real; the
  first version of this canary passed, which is what exposed that the exemption was an accident.
- **67 — a pass does not close the shards of a view that is gone.** Caught.
- **68 — a pass closes views that have not expired.** Caught.
- **69 — the sweep deletes on first sighting.** Caught: the grace is real.
- **70 — a pass never sweeps what it published.** Passed at first, which found the missing wiring test.
  Caught now.
- **71 — the sweep does not forget a shard it no longer owns.** *Not caught, and not a defect.* Dropping
  the entry bounds a map; no observable behaviour depends on it, because the same check skips the sweep
  either way. Recorded here rather than defended by a test that would only assert an internal field.

## What this does not fix

The grace is two passes, and a pass is the backstop interval. A reader whose node cannot reach the object
store for longer than that keeps a commit that may be swept — it then fails with an error rather than
answering wrongly, which is the right direction to fail in, but it is a real limit and not a theoretical
one.
