# What the design costs, counted

The architecture is justified by a cost model: a steady-state pass is a handful of object-store
operations, §7's batched leases make that independent of how many shards a node holds, and block-range
reads fetch a fraction of a shard. Every one of those numbers was measured against a local filesystem,
where an operation is a system call and free. On an object store they are billed HTTP requests.

**Counted in operations, not seconds.** Wall-clock on a loopback MinIO says almost nothing about a real
network. A request count transfers: it is the same number against S3, and it is the number on the bill.

## Logical operations are not billable requests

A `compareAndSwapRegister` is one call and **two HTTP requests** on S3 — a GET for the current value and
ETag, then a conditional PUT. Counting it once understates exactly the operation this design leans on
hardest. `CountingBlobStore` now counts by kind and `impliedS3Requests()` applies what each kind costs.
That number is derived from reading `S3BlobContainer`, not from watching the wire: better than the
logical count, still an estimate.

## §7's claim is half true, and the measurement says which half

| Mode, per idle tick | 1 shard | 8 shards |
|---|---|---|
| **Batched** — compare-and-swaps | **1.0** | **1.0** |
| **Batched** — register reads | 2.0 | 16.0 |
| **Batched** — total implied requests | 4 | 18 |
| Per-head (the default) — compare-and-swaps | 2.0 | 9.0 |
| Per-head (the default) — total implied requests | 6 | 34 |

**The write side is exactly as advertised.** One lease renewal covers every shard: the compare-and-swap
count is *constant* at 1.0 per tick whether the node holds one shard or eight. That is §7's claim and it
holds precisely.

**The read side is not, and the RFC should not be read as saying it is.** Each held shard's head is still
read every tick, because losing a shard is something only the head can report. So an idle tick is O(shards)
either way — batching removes the expensive half (a CAS is two requests, a read is one) and turns 34
requests into 18 at eight shards. **Better by a constant, not by an order.**

**And the cheap mode is not the default.** A node built with the ordinary `MetadataPlane` constructor gets
per-head liveness and pays 5.7× more requests for 8× the shards. §12.1 has carried "two liveness modes
still coexist" as a tidiness item; it is a bill.

`testTheDefaultLivenessModeCostsACompareAndSwapPerShardPerTick` asserts that the cost *grows*, which is a
strange thing to want until you notice what it is for: it pins the fact. If someone makes batching the
default, that test fails, and the failure is the notification that the item is resolved.

## What a document costs

**Exactly one object-store write per document** before publication, on both a filesystem and a bucket —
one write-ahead log append, as designed. Publication then added 4 blob writes and 3 compare-and-swaps for
50 documents, so the amortised cost falls as batches grow.

Latency, for whatever a loopback is worth: 4.2 ms per document on a filesystem, 12.1 ms on MinIO.

## What an idle node costs

A node holding one shard and doing nothing: **5 requests per renewal, 360 renewals/hour, 1,800
requests/hour** at a 30 s TTL. That is the number the scale-to-zero question turns on, and it is per node,
not per shard-doing-work.

## Where the 69-second fleet test went

Not chased to the bottom, and the honest reason is that the operation counts made it less interesting: an
idle tick on a bucket is 27.6 ms at one shard and 62.6 ms at four. Most of the fleet test's wall-clock is
the 250 ms retry sleep in its own `putDoc` helper multiplied by more activation round trips, not the
system being 11× worse. **A wall-clock comparison between a filesystem and a bucket mostly measures the
test harness.** That is why this file reports requests.

## Two measurement bugs, both mine, both worth recording

- **An assertion that was arithmetically always true.** The first version read
  `blobWrites() - (blobWrites() - documents)`, which equals `documents` for every possible input and
  asserted nothing whatsoever.
- **Counting at the wrong moment.** Reading the write counter *after* the publishing tick reports 54
  writes for 50 documents — 50 log appends plus four segment files — and looks exactly like a write path
  that costs 1.08 operations per document for no reason. The count has to be taken before the publish,
  because "what a document costs" and "what a batch costs to publish" are different questions.

## Two things the full build found afterwards

Running everything at once — unit, process and S3 suites together on one machine — made the fleet tests
fail intermittently with `searched:3, unreachable:1`. Both causes were mine, and neither was a flake.

**One decision that should have been two.** I had bounded *every* forward by the lease TTL, with the
reasoning that there is no point waiting longer than the lease. That is right for a write: ownership may
already have moved, and a stale write is dangerous. It is wrong for a search, where a slow peer is slow
rather than wrong, and giving up at the TTL converts a complete answer into an incomplete one for no
correctness benefit. Search forwards now have their own, longer bound.

**A test tuned for speed that manufactured failures.** The fleet tests ran a three-second lease TTL. Under
a loaded machine a healthy node misses a renewal at that TTL, its lease genuinely lapses, and its peers
correctly stop believing in it — the system behaving exactly as designed, reported honestly as
`complete:false`, and asserted against by a test demanding *instant* complete coverage. The real property
is that a healthy fleet **converges**, so the coverage assertions are now `assertBusy`.

Relaxing a deadline is one edit away from deleting a test, so the fan-out defect was re-planted against
the relaxed version: a search that only ever looks at local shards never becomes complete, so it times out
rather than passing. **The deadline moved; the property did not.**

A third thing, smaller and worth fixing anyway: the node that knew why a shard was unreachable had logged
it inside a forked JVM whose output the harness captured and only printed on startup failure. Fleet
assertions now attach the relevant node's log tail when they report an incomplete answer, because the
interesting evidence is always in a process other than the one asserting.

## What this does NOT establish

- **MinIO on loopback is not S3 over a network.** Every latency here is a lower bound and probably a
  useless one. Only the request counts transfer.
- **Small numbers only.** 8 shards, 50 documents, one node. Nothing measured at the scale the design
  argues about — phase 9's 1,500 indices were counted on a filesystem and have not been recounted.
- **Nothing about search.** Query and fetch costs are unmeasured, and the block-cache and read
  amplification figures in `block-reads-notes.md` are all filesystem numbers.
- **No concurrency in the measurement.** These are single-threaded steady-state counts; contention adds
  retries, and a lost compare-and-swap costs its two requests anyway.
