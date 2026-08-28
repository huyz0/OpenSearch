# M11 — The data path

- Code: `DocumentHandler`, `SearchHandler`, `DocumentRouting` in `serverless/shell/.../rest/`
- Tests: `ServerlessDataPathTests` (6)
- Result: **90 tests, 0 failures**; `check` green on both projects.

## The milestone, in one exchange

```
PUT  /library?shards=1
PUT  /library/_doc/book-1?refresh=true   {"msg":"the object store is the truth","n":1}
GET  /library/_search?q=msg:truth
  → {"_shards":{"total":1,"searched":1,"unreachable":0},"complete":true,
     "hits":{"total":{"value":1},"hits":[{"_id":"book-1","_source":"..."}]}}
```

Everything M10 built sat behind a Java method until this existed. It is now reachable by a user with
nothing but HTTP — and the write is durable before the response is sent, so a document written this way
survives its node dying before any publication. That is asserted, not assumed.

## Three things the responses say that they did not have to

**`_shards` coverage, always.** A node serves the shards it holds, and with no cross-node fan-out yet it
may hold some of an index and not the rest. A hit count without coverage would be a confident answer
computed over a fraction of the data, indistinguishable from a complete one — the failure `HANDOFF.md`
records eight times. So `total`, `searched`, `unreachable` and `complete` are always present. Canary:
hard-coding `complete: true` fails the test with `{"total":3,"searched":1,...,"complete":true}`.

**The owner, on a misdirected write.** A node that does not own the shard answers 421 and names the node
that does. Same shape as a lost activation race: losing is a routing instruction, and telling the caller
who won is what stops it guessing.

**What was actually guaranteed.** A successful write reports `"durable": "write-ahead log"` rather than
just `"created"`, because the segment it will live in may not be published yet and "created" alone would
let a reader assume the usual meaning.

## Routing mirrors the data plane's own function

`DocumentRouting` reproduces `OperationRouting.calculateScaledShardId`, which is private. It has to keep
mirroring it: a node that hashed documents differently from the data plane would write to one shard and
search another, and the symptom would be **a search returning nothing rather than an error**. Split
indices are explicitly not handled, and the class says so.

## Three bugs found, each silent

**`RST_STREAM: Stream cancelled`.** Both handlers blocked an HTTP thread waiting on work dispatched to
the search pool. The client saw a connection reset, not a diagnosable error. Both now hand off to
`GENERIC`/`WRITE` and answer from there.

**`No search context found for id [1]`.** A single-shard request resolves to query-*and*-fetch, so the
hits were already present and the reader context already freed; asking for it again produced a 404 that
reads like the document is missing rather than like the caller fetched twice. The handler now uses the
fetch result when it is there.

**Documents with no `_source`, reported as success.** `FetchPhase` was constructed with an empty
sub-phase list — the sub-phases are what load the source. Every hit came back with an id and no body,
and nothing failed. It is now built by `SearchModule`, as `Node` does it.

## The cross-node half, and a design correction

The first cut forwarded searches to `head.ownerNodeId` — the shard's **writer**. That was wrong, and
wrong in a way the RFC had already argued against: it couples search capacity to write capacity and
makes per-index search scale-to-zero meaningless (goals 2 and 3 of `rfc-serverless-opensearch.md` §2),
and §6 of the metadata-plane RFC says reader activation needs no coordination at all. Routing reads to
the writer reintroduces exactly the coupling the design removes.

So searches route by **reader placement** instead:

- **Rendezvous hashing** over the live `search`-role leases, not modulo. With `hash(shard) % n`, one
  node joining reshuffles nearly every shard and discards every cache; highest-random-weight moves
  about `1/n` of them. Since the entire point of placement is that a node keeps being asked for what it
  already holds, that difference is the feature.
- **Placement is a hint.** A node asked for a shard it does not hold opens it from the manifest and
  serves. Correctness never depends on the routing being right — a stale or unlucky decision costs a
  cold read. The moment placement became *required*, this would have reinvented the stateful cluster it
  exists to escape: agreement on placement, drain-before-move, handoff.
- **The owner remains a last resort**, for the case where no node advertises `search` at all.

Writes still forward to the owner, which is correct: a write must reach the node holding the shard-head,
because that is what compare-and-swap established.

Peers are resolved from their **leases** — a node's lease already records the transport address it
bound, so membership doubles as the address book and there is no separate discovery. One consequence,
recorded because it changes a measured number: a node now renews its lease every tick in *both* liveness
modes, not only under §7's batching, because a node that has not published its address is not merely
invisible but unroutable. Per-shard mode therefore pays `shards + 1` writes per tick rather than
`shards`; batched mode is unchanged at 1, and `ServerlessBatchedLeaseTests` asserts both.

## Canaries — the two cross-node properties

| Canary | Failure produced |
|---|---|
| No fan-out (local shards only) | `searched:1 ... complete:false ... value:3` of 10 |
| A node refuses a shard it does not hold (placement becomes a requirement) | cold search node serves `value:0` |

Worth noting what the failures looked like: even broken, the responses reported `complete:false` rather
than a confident wrong total. The coverage field did its job on the way down.

## What M11 does NOT establish
- ~~**Readers download whole segment files.**~~ **Closed** by lazy block-range reads — a reader
  fetches ~16% of a shard to open and answer a query, and 0 bytes on a repeat. See
  [`block-reads-notes.md`](block-reads-notes.md).
  ranges a query needs, which makes placement carry more weight than it should. Lazy block-range reads
  with a file cache are phase 5's deferred item and the thing that would make cold reads cheap enough
  that placement stopped mattering much.
- **Nothing keeps a placement warm.** Readers are opened on demand and never proactively; there is no
  controller deciding what to pre-warm or when to release a cold shard.
- **Fan-out is sequential.** Shards are queried one after another, not in parallel, so a wide index
  pays the sum of its shard latencies.
- **No `_bulk`.** One document per request, and therefore one object-store PUT per document — M10's
  missing batching, now reachable from outside.
- **`q=field:value` only.** No query DSL body, no aggregations, no sort, no pagination beyond `size`.
- **No `GET /{index}/_doc/{id}`.** Documents are searchable but not directly retrievable.
- **No deletes**, matching M10: the WAL does not log them either.
- **Nothing about S3 or GCS** (D5/R11), unchanged.
