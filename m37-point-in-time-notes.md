# M37 — point in time, and the name two indices came to share

A search that pages through a result set is reading a moving target. Between page one and page two the
writer publishes, segments merge, and the ranking the caller is walking is no longer the one they started
in — so page two repeats a document, or skips one, and nothing anywhere reports an error. `search_after`
(M36) fixed the *cost* of deep paging and could not fix this, because the cursor is a position in an order
that is itself changing.

A point in time freezes the order. `POST /{index}/_pit?keep_alive=10m` returns an id; a search quoting it
reads the commit as it was when the id was made, and goes on doing so until the caller releases it or the
keep-alive runs out.

## The record is the commit, not a pointer to one

A manifest register holds one value: the commit that is current. Publishing overwrites it. So "the commit
as it was at 10:03" is not something that can be pointed at afterwards — by 10:04 the register says
something else. The point-in-time record therefore *copies* each shard's manifest into itself. A few
kilobytes for a shard with a few hundred segments, and it is the only version of that commit which survives
the writer continuing to work.

## The half that makes it real is the garbage collector

Freezing a view is easy. The hard part is that the files the view names go on existing — and the sweep
deletes exactly those: blobs unreferenced by the live commit, in a term that is over. Left alone it would
collect the view's segments out from under a caller halfway through their paging, which is worse than not
offering the feature.

So `GarbageCollector.collectShard` adds every live view's pinned blobs to the set it treats as referenced,
in the same `term/file` shape it builds from the live commit — the same shape on purpose, so the two sets
are added rather than compared through a translation nobody would notice getting wrong.

`testASweepDoesNotCollectWhatAViewIsHolding` is the assertion this whole feature stands on, and canary 58
(the collector ignoring pins) is the one that matters most: it is caught.

## A view is an index of its own

A node may already be serving the live shard, and it cannot hold two shards with the same identity. So a
view is opened as an index whose **uuid is the view id**, carrying `index.serverless.storage_uuid` to name
the index whose bytes it actually reads.

Three things broke on the way, and each was a real defect rather than a wiring detail:

- **Core validates index settings and had never heard of ours.** Registering the setting — private, index
  scoped — against a copy of `BUILT_IN_INDEX_SETTINGS` was the shell-side answer. Turning validation off
  would have been the other one, and a silently-ignored setting name is exactly what that validation
  exists to catch.
- **The directory factory has two entry points and only one had the logic.** `newFSDirectory` knew about
  storage uuids; `newDirectory` still derived the path from the shard's own uuid. Live shards cannot tell
  the difference, because for them the two are equal. The view opened onto an empty directory and reported
  an index with no segments — which reads like a corrupted shard, not a shell bug. Both now go through one
  method, because whatever decides which one core calls, it cannot decide correctness.
- **A view asked the publisher which commit to open**, and the publisher answered for the view's own uuid,
  under which nothing was ever written. "Nothing published" starts an *empty* shard: a wrong answer shaped
  exactly like a right one. The commit is now passed in, which is also the only version that means
  anything — reading it would read the commit this feature exists not to see.

## The name two indices came to share

A view carries the name of the index it is a view of. Until one existed, a node could not hold two shards
of one index name, and seven places relied on that without saying so — every one of them finding a shard
by `getIndexName().equals(index) && s.id() == shard`.

With a view open, a document write looking for "shard 0 of paged" could match the view instead. The view
is a reader, so the write path concluded the node was not serving a shard it had been writing to a moment
earlier, and answered `503 activation_in_progress` — for a shard that was open, owned and healthy. It
happened *sometimes*, depending on which of the two a `HashSet` iterated first.

Fixing seven lookups would have left the eighth to write. `openShards()` changed instead: it means the
shards this node serves *as itself*, and a view is not one — it is something a caller is holding, counted
in `frozenShards()`. The bound on how much a node holds still counts views (`heldShards()`), and eviction
still cannot take one, which is the right pair: a node can refuse an activation it has no room for without
ever taking a view away from the caller using it.

The paging test asserts the separation directly rather than leaving it to the writes, which caught it only
when a set iterated the wrong way.

## Expiry is by the clock, and a search does not renew it

The keep-alive is absolute, not sliding. A sliding one would let a caller paging slowly hold a commit
indefinitely without ever having said they meant to. A released or expired view answers `404` rather than
an empty result — an empty answer to a search over a commit that no longer exists is the confident wrong
answer this surface exists to avoid.

## What is not here

- **No cursor is carried in the id.** The id names the view; the caller still pages it with
  `search_after`. That is the composition OpenSearch itself uses.
- **A view is node-local once opened.** The record is in the object store, so any node can serve it, but
  the shards a node opens for a view live only on that node. Two nodes serving the same view open it
  twice.
- **No slicing.** Parallel consumption of one view across workers is a further feature and nobody has
  asked for it.

## Canaries

- **58 — the sweep ignores what a view pins.** `referencedBlobs` replaced with an empty list. Caught: the
  view's files are collected and the view stops being readable.
- **59 — a search ignores `?pit=`.** Caught: the view sees writes made after it was frozen.
- **60 — views counted among the shards a node serves.** `openShards()` back to the whole map. Caught by
  the direct assertion in the paging test.
- **61 — a view opens the current commit rather than the one it froze.** `knownCommit` passed as null.
  Caught.
- **62 — expiry is not enforced.** `expiredAt` returns false. Caught.
- **63 — a view reads its own uuid's path rather than the index's.** Caught: no segments found.

Six planted, six caught, none by a compile failure.
