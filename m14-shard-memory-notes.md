# M14 (finished) — what an open shard costs, measured

M14's row had one item left unmeasured across every earlier pass at it: shard-count scaling and per-index
memory. Everything else it once listed as remaining had already shipped elsewhere and the row just hadn't
been told — M42 shipped name-index/prefix search, `ServerlessCostTests` already measures S3 and not only
the filesystem, `ServerlessBucketContentionTests` already combines contention with object storage. That
drift is fixed in place; this note is about the one thing that was genuinely still open.

## The question, stated precisely

Core's own `rfc-100m-index-architecture.md` measured 150,888 B and 3.0 file descriptors per **gated**
index — a descriptor sitting in `ClusterState` with no shard open, the lightest state that design has.
There is no equivalent lightweight state to measure here: an index with no shard open costs this shell one
register read to describe and nothing else. The number that actually bounds how many indices a node can
serve at once — what `maxShardsHeld` (M13) is a bound *on* — is what one **open** shard costs. That number
had only ever been reasoned about, never measured.

## The numbers

| | heap | file descriptors |
|---|---|---|
| open writer shard | ~67–70 KB | 3.0 |
| open reader shard | ~50 KB | not separately measured |

Two independent runs of the writer measurement: 67,392 B/shard and 69,436 B/shard for a first batch of 30,
with the second batch of 30 costing *less* per shard both times (63,475 and 61,212) — flat or falling, not
rising, across five consecutive full runs of the suite. File descriptors were exactly 3.0/shard in every
run, no variance at all.

## A comparison worth stating plainly

An open shard here — a real Lucene `IndexShard`, a real engine, a real translog — costs **less than half**
what core's own gated index costs, and a gated index has no shard open at all. That is not this design
being more efficient at the same job; it is a different job. Core's gated index still carries a full
`IndexMetadata` in `ClusterState` — settings, compiled mappings, alias entries, routing table slots — and
the 150,888 B is layered on top of that graph. This design has no `ClusterState` and no `IndexMetadata`
graph behind an open shard at all: the 67 KB is the *entire* cost of serving that shard, not an addition
to something heavier already resident. The comparison says more about what each design carries by default
than about which shard implementation is lighter.

## What "flat" means here, and why nothing is asserted on an absolute byte count

A wall-clock or byte-count threshold would make this suite a JVM-version detector rather than an
architecture check — compressed oops, heap layout and JIT warmup move the absolute number across
environments that changed nothing this design controls. `ServerlessScaleTests` already asserts on trends
rather than absolutes for exactly this reason; these tests follow the same discipline. What is asserted is
the marginal cost of the next batch of shards against the first batch — the shape a hidden
`O(open shards)` structure (a scan over every already-open shard triggered by opening one more) would
produce, and a flat per-shard cost would not.

## The honest limits of a heap measurement inside a shared test JVM

`Runtime`'s heap accounting is an estimate, not an audit. Object headers, compressed oops and JIT-compiled
code are not uniform across JVMs, and other test classes loaded earlier in the same JVM leave garbage that
`System.gc()` is asked, not guaranteed, to collect. Two defenses against that, both real:

- **A "real consumption" floor**, asserted before any ratio is trusted. A measurement stuck near zero —
  GC noise swamping the signal, or a broken accounting path — would make a "does not grow" or "reader is
  cheaper" comparison pass vacuously regardless of which way it was wrong. Both tests assert the batch
  actually cost more than 100 KB before comparing anything, which 30 real Lucene shards will clear by
  construction (one segment file each is already more than that).
- **The same JVM, back to back**, for the reader-versus-writer comparison specifically. Comparing across
  two separate test runs would let ordinary JVM-to-JVM noise get mistaken for the difference the test is
  actually about; measuring both in one run, one after the other, removes that variable.

## What this does not measure

- **How many shards a real deployment could hold.** That needs a heap sized like a production node, not a
  3 GiB test-worker JVM shared with every other test class in the suite. The number here is marginal cost
  per shard, which is what a capacity calculation would multiply by a node's actual heap — not a claim
  about how many shards fit in this specific test run.
- **Cost under real segment data.** Every shard measured here is freshly created and empty; a shard
  holding gigabytes of segments costs more for reasons this measurement is not about (Lucene's own
  per-segment structures), and that cost scales with data, not with how the shard is held open.
- **Non-Linux file descriptor counts.** `/proc/self/fd` is Linux-only; the file-descriptor test skips
  itself elsewhere via `assumeTrue`.

## Canaries

Measurement tests, not behavior tests — there is no single togglable code path controlling "shards scale
linearly" or "a reader is cheaper than a writer" to plant a defect in and revert. What *is* a classic
canary is the guard against a vacuous measurement:

- **123 — a broken heap measurement reporting no consumption is not caught.** `usedHeapBytes()` forced to
  return 0 unconditionally. Caught: the "real consumption" floor fails before any ratio is even computed.

The comparison assertions themselves (marginal cost flat, reader cheaper than writer) are validated by
having measured real, unrigged numbers that could have come out either way and did not — five consecutive
full-suite runs, all with the same shape (flat or falling marginal cost, reader consistently lighter).

M14 is done.
