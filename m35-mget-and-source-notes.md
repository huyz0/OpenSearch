# M35 — `_mget`, and a feature that turned out to already exist

Two items from the "remaining" list, and they turned out to be different kinds of missing.

## `_source` filtering was never missing

It works, and nothing here made it work. A shard's half of a search runs through the same
`SearchService` a classic node uses, so the fetch phase applies `FetchSourceContext` before the hit is ever
returned — the coordinating node has nothing to filter and no way to get it wrong. `_source: false`,
`_source: ["msg"]` and `_source: {"excludes": [...]}` all behave.

**Which is exactly why it needed a test.** "It works because we reused the engine" is a claim about a code
path nobody had run. On a surface that is an explicit allowlist, an untested feature is indistinguishable
from one that quietly stopped working — and the notes had it listed as absent, which is its own kind of
wrong.

## `_mget` was missing

`POST /_mget` and `POST /{index}/_mget`, taking either `docs` naming an index and id each, or `ids` against
the index in the path.

**Not a loop over the get endpoint.** Documents in one request belong to different shards on different
nodes, so fetching them one after another costs the sum of their round trips; they go out through the same
bounded fan-out a search uses, so the cost is the slowest of them. That is `_bulk`'s argument on the write
side, for the same reason: a batch API whose latency is the sum of its items is a batch API in name only.

**Each item answers for itself**, which is the other half of why a multi-get is worth having. A document
that does not exist is `"found": false` beside the ones that do. An item naming an index that does not
exist gets an error *for that item* — deliberately not `"found": false`, because an index that is not there
is a different answer from a document that is not in one, and collapsing them tells a caller their data was
deleted.

**Every item asked for appears in the answer, in order.** The fixtures ask for more documents than exist
and assert one entry per request; an item silently disappearing is the failure this shape exists to
prevent, and it is the one a hand-written loop cannot make.

**The refusals are the get endpoint's refusals.** Both route through `ShardOperations`, so a document a
single get would decline to answer from a stale copy is declined here in the same words — a batch API
quietly having weaker guarantees than the single one is a thing that happens, and it should not happen by
accident.

## A flake, found on the way and fixed

`ServerlessGetTests.testAGetSeesAWriteThatASearchCannotYet` failed once during a full run and passed on its
own every time after. Its fixture writes *without* `refresh` and asserts the document is not yet
searchable — and the default `index.refresh_interval` is one second, so a scheduled refresh could land
inside the window. Its own assertion said so: *"the fixture is meaningless unless the write really is
unsearchable"*.

Roughly one full run in fifteen. A test that proves nothing one run in fifteen is a test that lies one run
in fifteen, so the index now sets `index.refresh_interval: -1` and the absence of a refresh is guaranteed
rather than likely — which is what the test meant all along.

## Canaries

| Defect | Caught by |
| --- | --- |
| An item that failed is reported as not found | `testABadItemDoesNotLoseTheGoodOnes` |
| Only the first document asked for is answered | 3 tests |

## What is still missing

*Written at the time. For the current position, which later milestones have moved, see
[`serverless-status.md`](serverless-status.md).*

- ~~`_mget` does not honour `_source` filtering per item.~~ **Closed**: both the single get and each
  multi-get item honour `_source`, `_source_includes` and `_source_excludes`, and a document in a multi-get
  may name its own — which is the reason that API takes objects rather than ids, since one request can want
  the body of one document and only a field of another. Core's own `XContentMapValues.filter` does the
  work, so wildcards and dotted paths behave as they do everywhere else.

  **It shapes the response and does not make the read cheaper**, and the test says so alongside the rest.
  The document is fetched whole either way; for a forwarded get the whole source has already crossed the
  internal network before any filtering runs. A search is the other way round — filtering there happens
  inside the shard's fetch phase, before a hit is ever returned. That asymmetry is real and cannot be fixed
  without pushing the context through the forwarded-get request.
- **No `docs[]._routing`**, because the shell routes by id and nothing else.
- **`search_after`, aliases and `ignore_unavailable`** remain absent.

276 tests green across `test` (241), `pluginTest` (2), `processTest` (13) and `s3Test` (20), none skipped,
MinIO live. `server/` untouched.
