# M34 — A node that refuses rather than dies

M31 shipped aggregations and ended with a line in the notes: *"No memory bound. The breaker is a no-op, so
a large aggregation can exhaust the heap rather than being refused."* This is that line, closed.

Every component in the shell was constructed with `NoneCircuitBreakerService`, which accounts nothing and
refuses nothing. It was invisible while the surface allocated nothing worth bounding. An aggregation over a
high-cardinality field is the ordinary way to exhaust a node's heap, and a node that dies is worse for every
other request than a node that refuses this one.

The node now builds one `HierarchyCircuitBreakerService` and hands it to `IndicesService`, `SearchService`,
the network module and its own `BigArrays`.

**Core's hierarchy, with core's defaults, deliberately.** The parent limit, the request and field-data
children and the thresholds between them are numbers OpenSearch has tuned against real workloads. Inventing
different ones here would be inventing a different product, and the interesting question is not whether the
numbers are right but whether the wiring is real.

So the tests set an absurd limit by configuration — `indices.breaker.request.limit: 1kb`, with
`use_real_memory: false` so the limit means what it says rather than depending on what the JVM running the
build has done with its heap — and assert three things: the aggregation is refused, the caller is told it
was the breaker, and **the node is still serving afterwards**. A breaker that took the node down with the
request would be a more expensive way of doing the thing it exists to prevent.

A third test runs with no limit configured and asserts ordinary aggregations are unaffected, because
turning real accounting on is exactly the kind of change that quietly starts refusing things.

## A search nothing answered was an empty result

Wiring the breaker exposed a second thing. The fan-out treats an unanswered shard as a hole in the coverage
it reports, which is right while *some other shard* answered. When none did, there is nothing to report
coverage over, and the response was:

```json
{"_shards":{"total":2,"searched":0,"unreachable":2},"complete":false,"hits":{"total":{"value":0},"hits":[]}}
```

A confident empty answer wearing a disclaimer — 200, zero hits, and a flag most clients never read. Exactly
what D2 exists to prevent, arrived at from a direction nothing had come from before.

The first failure is now carried out of the fan-out and becomes the response when no shard answered. **As
itself**, if it is a runtime exception: a circuit-breaking exception still answers 429 and a plugin's
authorization refusal still answers 403, rather than every failure collapsing into one status. Where there
was no exception — no node serving any shard at all — the message says that instead.

## Canaries

| Defect | Caught by |
| --- | --- |
| The breakers are no-ops again | 2 tests |
| A search nothing answered is reported as an empty result | 2 tests |

## What is still missing

- **Nothing exercises the parent breaker or field data**, only the request child. The others are core's and
  are now wired, which is a different claim from tested.
- **The bulk and write paths are unaccounted.** A write allocates through the engine, which has its own
  accounting, but nothing here adds a request-level estimate the way an aggregation does.
- **No `_nodes/stats` to read the breakers from**, so an operator can see a refusal and not the trend that
  led to it.

271 tests green across `test` (236), `pluginTest` (2), `processTest` (13) and `s3Test` (20), none skipped,
MinIO live. `server/` untouched.
