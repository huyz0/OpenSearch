# M18: the half of the fence nothing was testing

Fencing is §9.6 — "where this design loses data if it is wrong". It has two halves, and only one of them
had a test.

## The two halves

Publishing is **read manifest → upload segments → compare-and-swap**.

1. **The term check**, at the read. A writer that finds a *newer* term already in the manifest is refused
   outright. Covered by `testAZombieWriterCannotCorruptTheShardItLost` since phase 6.
2. **The compare-and-swap**, at the write. A writer that read while it was still the newest term holds a
   manifest that is genuinely older than itself; by the time it swaps, a successor has published. The term
   check cannot help — when the loser looked, there was nothing newer. Only the generation swap refuses it.

The second had no test. **The whole manifest CAS could be destroyed and 148 of 149 tests stayed green** —
verified, not assumed, by planting the defect and running the suite.

## Why it had no test, and what to do about it

The window is the width of a segment upload. On a filesystem that is microseconds, so the scenario is
effectively unreachable and a test would have to race and hope. `PausingBlobStore` holds it open on
demand instead: the first segment write blocks until the test releases it, and everything else runs
normally. Only `writeBlob` pauses, and only under a `t=` container — a WAL append is also a `writeBlob`,
and pausing one of those parks the wrong writer at the wrong moment.

The sequence is then exact rather than lucky:

1. the loser reads the manifest and parks inside the upload;
2. its lease lapses, a successor takes the shard at a higher term and publishes;
3. the loser wakes and finishes the publish it began while it was still the owner.

**The test asserts which half refused it.** The term check reports the term it lost to; the swap reports a
concurrent change. Without that assertion the test would also pass if the term check had done the work,
and the branch it exists to cover would still be untested — which is precisely the trap the old zombie
test fell into.

## And then on a bucket, where the fence is an HTTP conditional write

Both halves had only ever been demonstrated on a filesystem, where a compare-and-swap is a rename. The
claim the project makes is about an object store, and it rests on `compareAndSwapRegister` being
linearizable over HTTP — a GET for the ETag, then a PUT carrying it. A filesystem cannot demonstrate that.

The precedent for the transport mattering is not hypothetical: `IndexInputStream` had no mark support, so
publishing a segment was **impossible on S3 and perfect on disk**, and only running it on a bucket found it.

Same scenario, same planted defect, run against MinIO: refused, and refused by the conditional write. The
winner's document is then read back out of the bucket by a node that never held the shard — because a lost
swap would not look like an error, it would look like the winner's document having never been written.

## Canary

| Planted defect | Caught by |
|---|---|
| Re-read the manifest generation immediately before swapping | the new fs test, and the new bucket test — **and nothing else, on either store** |

That defect is the realistic one. It is what someone "fixing" a spurious CAS failure would write, it looks
like a tidy-up, and it silently converts the fence into an unconditional overwrite.

## A placement mistake worth recording

The bucket test first went into `ServerlessBucketContentionTests` and failed with
`availableProcessors is already set to [20], rejecting [20]`. That class runs under `processTest`, which
exists for tests that fork real JVMs and sets netty's processor count for the children; starting an
in-JVM node there re-sets it. The test is in-JVM plus a bucket, which is `ServerlessOnObjectStoreTests`'
shape, and belongs in `s3Test`. The build was telling the truth about what each task is for.

## What this does NOT establish

- **MinIO is still not S3.** This shows the fence holds against an S3 API with a real network in the
  window. Conditional-write linearizability on AWS S3, GCS or R2 under genuine concurrency remains
  unproven, as `r11-conformance.md` already says.
- **The window is opened by a test double, not by a slow upload.** A real publish of a large shard would
  hold it open for the same reason; nothing here measures how wide it actually gets.
- **Only one loser.** Two stale writers publishing into the same window is not tested.
- **Nothing about the WAL in this window.** The loser's log records at its own term are reclaimed by the
  successor's publish, which is covered elsewhere, and not re-checked here.
