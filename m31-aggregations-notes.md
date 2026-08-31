# M31 — Aggregations, and the four nulls under them

Aggregations were the last thing the search surface refused outright, and the reason recorded in M19 was
honest: a shard's terms aggregation holds *that shard's* counts, and returning one would be answering with a
number computed over a fraction of the index.

Combining them is not summing. It is merging ordered buckets, deciding which terms survive the merge, and
carrying the error bounds that say how wrong the answer might be. So the reduce here is
`InternalAggregations#topLevelReduce` — **the code a classic coordinator runs** — rather than a second
implementation that would agree with it right up until somebody used an aggregation it had not met.

## The shape of it

- A shard's `QuerySearchResult` already carried its aggregations; `ShardQuery` takes them with
  `consumeAggs()`, which is single-use and throws on a second call, and that is how it makes ownership
  unambiguous.
- `ForwardedSearchResponse` carries them over the wire. `InternalAggregations` is `Writeable` and its
  concrete types are in the node's named-writeable registry, which the transport already had, so this cost
  a field rather than a serialisation format.
- The fan-out reduces once, finally. A classic coordinator reduces in batches as results arrive, to bound
  memory; this fan-out already holds every shard's answer before it merges hits, so a partial reduction
  would save nothing and add a state machine.
- Pipeline aggregations come from the request's own tree, so one the caller asked for runs rather than
  silently disappearing.
- Rendering is `InternalAggregations#toXContent` — every aggregation type knows its own output, and a
  hand-written renderer would agree with them until it did not.

## Four nulls, invisible until something asked

Every one of these was a placeholder that had been correct for as long as no aggregation ever reached a
shard. The REST layer's 501 was holding them all up.

**`ValuesSourceRegistry` was null** in the `IndicesService` construction, with a `// ValuesSourceRegistry`
comment beside it. It is how an aggregation resolves a field's values source, so the first terms
aggregation to reach a shard was a `NullPointerException` from inside `QueryShardContext`.

**`BigArrays.NON_RECYCLING_INSTANCE` cannot allocate.** That constant is built with a null circuit-breaker
service, so the first component to actually ask it for an array gets a `NullPointerException` rather than an
array. Nothing noticed because nothing allocated: a query needs no scratch space, and an aggregation is the
first thing here that does. The node now has a real `BigArrays`. Its breaker is
`NoneCircuitBreakerService`, so nothing is bounded — which is honest for a node whose memory limits are not
modelled anywhere, and a different thing from being unable to allocate at all.

**`SearchService` was given its own `BigArrays` with a null breaker service.** An aggregator asks its search
context's allocator for the breaker and calls `getBreaker` on it *before it has counted a single document*,
so this was a null pointer at the top of every aggregation regardless of the first fix. It now gets the
node's.

**`OriginalIndices.NONE` broke the shard request cache.** Fine for as long as the shard request was only
ever read in this process; the cache serialises it to build a key — which it does for a `size: 0`
aggregation, exactly the shape most aggregations have — and writing `NONE` trips an assertion inside
`OriginalIndices`. The request now names the index it came for, which is also simply what it is.

That is four defects found by one feature, each of which would have been found by a user instead.

## The fixtures cannot be answered by one shard

Twelve documents over three shards, four of each colour, so **no single shard holds four of anything**. A
count that came from one shard, or a merge that took the first answer and discarded the rest, is a
different and visibly wrong number. The sum test uses amounts 1..12 summing to 78, of which a single shard
holds at most a third.

One test splits the shards across two nodes so the buckets have to survive the transport. One asks with
`size: 0` and asserts hits are empty and aggregations are not — no hits does not mean no aggregations, and
a terms aggregation over an index with `size: 0` is the ordinary way to ask for one. One aggregates over a
filtered query and asserts it counts the matches rather than the index.

## Canaries

| Defect | Caught by |
| --- | --- |
| Only the first shard's aggregations are used | 4 tests |
| A shard's aggregations are never collected | 5 tests |

## What the search surface refuses now

`search_after`, `collapse` and `suggest`, and they are asserted to be refused in the same test that used to
assert sort and aggregations were — updated rather than deleted, because that test is the one that would
otherwise still be describing a surface that has moved.

## What is still missing

- **Accuracy is core's, including its limits.** A terms aggregation over high cardinality is approximate in
  the same way and for the same reasons it is on a classic node; `doc_count_error_upper_bound` is reduced
  and reported by core, not by anything here.
- **No memory bound.** The breaker is a no-op, so a large aggregation can exhaust the heap rather than being
  refused. That is the same position the node was already in for everything else, now reachable by a new
  route.
- ~~Cost unmeasured.~~ **Measured**: over three locally-held shards, a search costs **3 object-store
  requests and the same search with two aggregations costs 3** — identical on a filesystem and on a bucket,
  with zero data-blob reads. Which is the claim the design makes: the reduce runs on the coordinating node
  over answers the shards computed from segments they were already reading, so it adds nothing to what the
  deployment pays its object store. Measured against the same search *without* the aggregations, because
  "3 requests" means nothing alone and "the same 3" means everything.
- **`_mget`, `_source` filtering and multi-index search** remain absent.

259 tests green across `test` (224), `pluginTest` (2), `processTest` (13) and `s3Test` (20), none skipped,
MinIO live. `server/` untouched.
