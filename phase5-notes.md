# Phase 5 — the search path

- Code: reader path in `ShardReconciler`, role-selected engine in `ServerlessNode`
- Tests: `ServerlessSearchPathTests` (6)
- Result: **51 tests, 0 failures**; `check` green on both projects.

## Reader activation needs no coordination, and that is the point

A search node serves a shard by reading its manifest and opening the segments. There is **no
compare-and-swap, no term bump, and no entry in the shard-head** — asserted directly: the head's owner
and term are identical before and after a reader starts serving.

Two search nodes serving the same shard at once is therefore not a race to be arbitrated. It is the
normal case, and it is tested.

## The role picks the engine, which is what makes readers safe

A node that does not accept writer activation constructs `IndicesService` with an
`engineFactoryProvider` returning `ReadOnlyEngine`. A reader is therefore **structurally** unable to
write, not merely expected not to — which matters precisely because reader activation has no CAS
behind it. Safety comes from the engine being incapable.

This is the per-shard-role engine dispatch the parent RFC describes, reached through
`IndicesService`'s existing `engineFactoryProviders` seam with no change to `server/`.

`ReadOnlyEngine.index()` does `assert false` and *then* throws `UnsupportedOperationException`, so which
one surfaces depends on whether assertions are enabled. The test accepts either and rejects anything
else — catching only `Exception` would have passed under `-ea` for the wrong reason and failed without
it.

## Scale to zero, verified by actually doing it

Every search node is closed. Nothing is drained, handed over, or migrated. A **different** node with its
own empty disk then serves the same five documents. The data exists only in the object store between
those two moments.

## Canaries — both load-bearing claims made to fail

| Canary | Failure produced |
|---|---|
| Role no longer selects the engine | `Expected exception Throwable but no exception was thrown` — the reader accepted a write |
| Restore downloads nothing | Every reader test: `IndexNotFoundException ... files: []` |

The second is the one that matters most: it proves the reader's data comes from the object store rather
than from something left on local disk. Tested separately, because the download canary masks the engine
canary — a run where both were planted would have looked like one failure.

## A bug the reopen test caught

A reader re-opening onto a newer commit hit `FileAlreadyExistsException`: the previous commit's files
were still in the local directory. Lucene segment names restart at `_0` after a history bootstrap, so a
same-named file is **not** necessarily the same bytes, and skipping it would have served a mix of two
commits. Restore now deletes before it writes.

## What phase 5 does NOT establish

- **Readers do not follow.** An open reader keeps serving the commit it opened; it picks up a newer one
  only when re-opened. That is correct for a cache and wrong for a product — the notification that makes
  it automatic is phase 8, and the test asserts the current behaviour explicitly rather than leaving it
  ambiguous.
- **Readers download whole segment files.** The design calls for lazy block-range fetches with a file
  cache; this copies the commit in full. Fine at test scale, not at product scale.
- **No search coordination.** Queries go to one shard through `SearchService`. Fan-out across shards and
  nodes is the `action/` layer, still deferred since phase 4.
- **Scale-to-zero is manual.** Nothing decides to stop or start a reader. That is a controller, and none
  exists.
- **Nothing about S3 or GCS** (D5/R11), unchanged.
- **No WAL** (phase 4), unchanged: durability is still "as of the last published commit".

## Next

Phase 6 — activation and failover under fault: lease expiry driving reactivation automatically, and the
paused-JVM zombie test that closes R9 rather than partially closing it.
