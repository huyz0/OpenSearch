# M13 (finished) — hysteresis and per-index pinning

M13's row had carried "no hysteresis and no per-index policy" as its one remaining item across several
milestones' worth of unrelated work landing around it. This closes both, in `BackgroundReconciler`
(`makeRoom()` for cap pressure, `releaseIdle()` for idle release — see [`m13-notes.md`](m13-notes.md) and
[`m21-idle-release-notes.md`](m21-idle-release-notes.md) for where those two came from).

## Hysteresis: what it buys and what it does not

`makeRoom()` used to evict exactly one shard per arrival at a full node — the minimum that request needed,
and no more. A burst of arrivals at a full node paid `makeRoom()`'s victim search, which is O(open shards),
once per arrival, each pass freeing only enough room for the shard that triggered it.

`makeRoom()` now clears down to a floor below the cap — `maxShardsHeld` minus a headroom, defaulting to
`DEFAULT_EVICT_HEADROOM_FRACTION = 0.05` (5%, floored at one shard) — on the first pass a full node needs
to evict at all. The next several arrivals, up to that headroom, find room already there and trigger no
further eviction.

**What this does not do: reduce how many shards get evicted under sustained demand.** A node that needs to
hold K new shards still gives up K old ones, headroom or not — hysteresis is not a discount on eviction
count, it is a discount on how often the victim search itself runs. Under a steady trickle of one arrival
at a time with room to spare between them, hysteresis changes nothing; its entire benefit is bursts.

**Why 0.05, and why that number is reasoned rather than measured.** The javadoc on the constant states this
directly rather than implying more confidence than exists: five percent is a fifth of
`DEFAULT_EVICT_AFTER_MILLIS`'s grace period, chosen the same way that constant was — an order of magnitude
inside what would visibly cost something, rather than measured, because nothing has yet run this shell at
a scale where the difference between 5% and, say, 10% headroom is observable. Evicting a shard nobody asked
to be evicted costs one publish-and-release round trip to the object store; the constant trades a few of
those against fewer O(open shards) scans. `setEvictHeadroomFraction` is exposed so a deployment that does
have a measured answer isn't stuck with the guess.

`testMakeRoomClearsHeadroomNotJustOneSlot` asserts the shape directly: 10 shards resident at a cap of 10,
headroom 0.3 (floor 7), all gone idle. The triggering arrival drops the node from 10 to 8 (10 − 3 headroom
+ 1 new), not to 9 — and the two arrivals immediately after find room with no further eviction.
`testZeroHeadroomEvictsExactlyOneSlot` confirms `evictHeadroomFraction = 0.0` reproduces the original
one-slot-at-a-time behavior exactly, so the default before this change is still reachable, not just
superseded.

## Per-index pinning: a node-local promise, deliberately not a durable one

`pin(String indexName)` / `unpin(String indexName)` / `isPinned(String indexName)` exempt an index's shards
from both eviction paths — `makeRoom()`'s cap-pressure eviction and `releaseIdle()`'s time-based release —
on the node they're called on.

**The scope decision, stated in the javadoc rather than left implicit:** this is a node-local runtime
setting, the same shape as `setMaxShardsHeld` or `setEvictAfterMillis` — set once per node, not a durable
per-index record every node in a deployment discovers on its own. A durable version would need a new
register namespace, a REST surface to manage it, and a place in the reconcile loop to read it every tick.
Nothing has asked for an index to be pinned deployment-wide yet — only for a way to say "this stays
resident" on the node currently serving it, which is what every other capacity knob here already offers.
Building the durable version on spec, with no caller, would be exactly the kind of unmeasured machinery
this project has consistently avoided elsewhere (R11's register-linearizability discipline, `maxShardsHeld`
itself before M20 measured it).

A pin exempts a shard from **both** eviction paths, not one: `testAPinnedIndexIsNeverEvictedForRoom`
confirms it survives cap pressure as the only otherwise-evictable candidate, and
`testAPinnedIndexIsNeverReleasedForIdleness` confirms the same exemption applies to time-based idle
release — a partial promise ("stays resident unless the cap is hit" or "stays resident except when idle")
would be a materially weaker guarantee than "stays resident," and callers asking for one should get it, not
a shape that happens to work in the common case.

`unpin` is not itself a trigger — the javadoc says so directly, and `testUnpinningMakesAShardEvictableAgain`
proves it: a shard past its threshold at the moment it's unpinned becomes a candidate on the *next* pass,
not immediately, which is the same "eviction only ever happens inside a reconcile pass" invariant every
other eviction path here already holds.

## Canaries

- **126 — headroom silently ignored, `makeRoom()` still clears one slot at a time.** `headroomShards()`
  forced to return `0` unconditionally, defeating the floor computation. Caught:
  `testMakeRoomClearsHeadroomNotJustOneSlot` fails on the first assertion (held drops to 9, not 8).
- **127 — the floor computation lets eviction run past the cap instead of down to it.** `floor` in
  `makeRoom()` forced to `0` regardless of headroom. Caught: the same test's headroom-clearing assertion
  fails with a held count far below 8, and `testZeroHeadroomEvictsExactlyOneSlot` fails too (evicts more
  than one slot at zero headroom).
- **128 — `pickVictim` stops honoring `pinnedIndices` under cap pressure.** The `pinnedIndices.contains`
  check in `pickVictim` removed. Caught: `testAPinnedIndexIsNeverEvictedForRoom` fails — the pinned index
  is evicted instead of the ordinary one.
- **129 — `releaseIdle` stops honoring `pinnedIndices`.** The equivalent check in `releaseIdle`'s loop
  removed. Caught: `testAPinnedIndexIsNeverReleasedForIdleness` fails — the pinned index's shard is released
  alongside the ordinary one.

All four planted, watched fail, reverted.

## What this does NOT establish

- **Pinning is not durable.** A pinned index on a node that restarts, or on any other node in the
  deployment, is not pinned — see the scope decision above. This is a stated design choice, not an
  oversight, but it means pinning cannot yet express "this index must never scale to zero anywhere,"
  only "not evicted by the node currently holding it, for as long as that node keeps running."
- **The headroom fraction is reasoned, not measured**, same as `DEFAULT_EVICT_AFTER_MILLIS` and
  `DEFAULT_IDLE_AFTER_MILLIS` before it. Nothing here changes that standing limitation for the milestone's
  other constants.
- **Hysteresis does not change steady-state eviction volume**, only how it's batched — stated above, and
  worth repeating here since it is the most likely thing a reader skimming past the numbers would assume
  the feature buys.

M13 is done.
