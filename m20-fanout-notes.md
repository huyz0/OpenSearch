# M20: what a query costs, and both fan-outs made concurrent

Two gaps, done in the order that makes the second one honest: measure first, then change, then say what
the measurement did and did not show.

## What a query costs

The cost suite counted writes and idle ticks and had never counted a query. Three cases, because they are
three different prices — conflating them is how the first version of this test went wrong, calling a node
"cold" while another node was still the live owner, so it measured forwarding and reported it as the cost
of opening a shard.

Identical on both stores, which is itself the point: the cost model transfers.

| case | requests | per shard | data reads |
|---|---|---|---|
| **warm** — shards open here | 3 | constant | 0 |
| **forwarded** — someone else owns them | 10 | 3.3 | 0 |
| **cold reader** — nobody owns them | 41 | 13.7 | 12 |
| **second query on that node** | 5 | constant | 0 |

The third row is the scale-to-zero number: what the first query costs on a node that holds nothing, with
no writer alive. It was a guess until now.

**Every measured search asserts `"complete":true`.** Without that guard the numbers are the cost of *not*
answering — and that is not hypothetical: the first run measured a "cold" search at 8 requests with zero
data reads because the node had the `ingest` role, reader placement had no candidate, and the search
honestly returned nothing. The assertion turns that from a plausible number into a failure.

## Both fan-outs were sequential

Search visited its shards one after another; `_bulk` dispatched its per-shard groups the same way. Both
were recorded as gaps in their own notes and neither was fixed. Latency was the *sum* of the shards rather
than the slowest of them, which is a rounding error on a filesystem and the difference between a query
that scales with an index and one that does not against an object store — 41 requests per cold shard, one
shard at a time.

`Fanout` runs them concurrently, bounded, and two decisions in it are load-bearing:

- **Not the pool the caller is on.** A shard query hands work to the SEARCH pool and blocks on it, so
  fanning out there deadlocks once the fan-out is wider than the pool. Search coordinates on GENERIC.
  Bulk had the same trap one level up and I walked into it: the bulk request already ran on WRITE, and
  the first version submitted its groups to WRITE and waited — fine until enough concurrent bulk requests
  hold every WRITE slot while waiting for groups that need WRITE slots to start. The coordinator moved to
  GENERIC; the writes stayed on WRITE; nothing on WRITE waits on WRITE.
- **Chunked rather than semaphored.** A permit-waiting task still occupies a thread, which is the cost
  being avoided. Chunking creates only as many tasks as can run, at the price of a barrier per chunk —
  recorded rather than hidden.

## A real bug the concurrency exposed

Three shards of one index opened at once and two lost a check-then-act race in `ShardReconciler`:
`indexService(index)` then `createIndex(...)`, with `ResourceAlreadyExistsException` for the losers. It
surfaced as **two unreachable shards**, which reads like a routing problem and is not one. Safe by luck
while opens were serial. The acquisition is now locked per index; `IndexService.createShard` is itself
`synchronized`, so shard opening stays parallel.

## The test asserts concurrency, not speed

`RendezvousBlobStore` makes each shard's first read block until every shard has arrived, and **fail if
they do not**. Sequential cannot finish; concurrent cannot fail. No threshold to tune, and no stopwatch to
be unlucky with — the `_bulk` notes already record a wall-clock comparison that measured a network hop and
reported it as the cost of batching.

**The first version of it proved nothing.** A timed-out waiter was allowed to proceed, so run one at a
time each shard arrived, waited out its timeout, carried on and answered: the search completed and every
assertion passed with the fan-out forced to width 1. Failing on timeout is the whole discriminating power
of the class.

| Planted defect | Caught by |
|---|---|
| Fan-out width forced to 1 | the rendezvous test — **only after the timeout was made fatal** |

## What the measurement did NOT show

- **The request counts are unchanged by this work, and that is correct.** Concurrency moves latency, not
  the number of requests. Nothing here claims a cheaper query; the claim is a query whose latency stops
  being the sum of its shards, and that claim is carried by the rendezvous, not by a number.
- **No wall-clock figure is published.** At three shards the noise exceeds the effect — the same warm case
  measured 316ms and 14ms in one run. A latency claim needs many more shards and a quieter machine than a
  build agent, and inventing one from this data would be the mistake these notes have twice recorded.
- **The chunk barrier is unmeasured.** At the default width of 8 it only bites above 8 shards, and nothing
  here runs that wide.
- **Bulk's concurrency has no equivalent rendezvous test.** The deadlock it would have had is reasoned
  about and avoided, not demonstrated.
