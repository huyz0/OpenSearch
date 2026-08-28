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

## What M11 does NOT establish

- **No cross-node forwarding.** A write to the wrong node is refused with the owner named, not proxied.
  Transport forwarding using the address in each node's lease is the remaining half of R3.
- **No cross-node search fan-out.** A search covers the shards *this node holds*; coverage is reported
  precisely so that is visible rather than hidden.
- **No `_bulk`.** One document per request, and therefore one object-store PUT per document — M10's
  missing batching, now reachable from outside.
- **`q=field:value` only.** No query DSL body, no aggregations, no sort, no pagination beyond `size`.
- **No `GET /{index}/_doc/{id}`.** Documents are searchable but not directly retrievable.
- **No deletes**, matching M10: the WAL does not log them either.
- **Nothing about S3 or GCS** (D5/R11), unchanged.
