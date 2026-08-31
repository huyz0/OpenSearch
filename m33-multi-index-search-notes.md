# M33 — Searching several indices, and refusing to guess which

`GET /a,b/_search` treated `a,b` as one index name and answered 404. Naming several indices is how anybody
with time-partitioned data searches, so this is a hole with a well-defined floor and a well-defined
ceiling, and the ceiling is the interesting part.

## What is now supported

A comma-separated list, resolved by looking each name up: **one register read per name, no listing.** Every
shard of every index named is asked in **one fan-out** — six shards across three indices go out together,
not three searches run in sequence and stitched, which is the difference between a latency of the slowest
shard and a latency of the sum.

The window is cut **once, over everything**, so page two of a search over three indices is the page two it
would be over one. `testThePageIsCutAcrossTheWholeSet` sorts by a field whose values interleave the indices
and asks for `from: 3, size: 4` — a search that ran per index and concatenated returns a different four.

Coverage is reported over the whole set: `_shards.total` counts every shard asked for, so "complete" still
means complete.

## What is refused, and why that is the point

**A pattern.** Resolving `logs-*` means enumerating the deployment's indices, which is the operation §6.3
declines to offer on a request path — the same reason `/_serverless/indices` answers 501, and the same
reason `RefusingIndexNameExpressionResolver` refuses rather than resolving against a node-local view.

A wildcard *could* have been implemented by matching against the indices this node happens to know about.
It would have worked in every test, on every small deployment, and returned a silently partial answer on a
large one — a search over "all my logs" that covered the third of them this node had opened. Refusing by
name, with the cost named in the refusal and an instruction to name the indices, is the honest version.

The refusal applies inside a list too, where letting one through would have been easiest:
`/logs-a,logs-*/_search` is refused whole.

**An index in the list that does not exist**, rather than dropped. A search over three indices where one is
missing, answering 200 and looking complete, is the confident empty answer this surface exists to avoid.
Classic OpenSearch has `ignore_unavailable` for the other behaviour; offering the flag without the
distinction behind it would be worse than not offering it.

## A hit did not know where it came from

`SearchHit`'s index field is `transient` and is set from the shard target, which a full search sets during
the fetch phase and this one does not. It never mattered while a search covered one index, because the
handler could name it from the request.

With several indices that would have printed `"_index":"logs-a,logs-b"` on every hit — a lie that reads
like a formatting detail. `ShardQuery` now stamps each hit with its shard, so a hit reports the index it
actually came from, over the wire as well: `SearchShardTarget` is serialised with the hit and carries the
`ShardId`.

## Canaries

| Defect | Caught by |
| --- | --- |
| Only the first index named is searched | 2 tests |
| A pattern is treated as a name | `testAnIndexPatternIsRefused` |

## What is still missing

- **No `ignore_unavailable`**, deliberately, until there is a reason to distinguish "I know one of these
  might not exist" from a typo.
- **No aliases**, which are the other way a caller names a set of indices, and which would need somewhere
  to store the mapping.
- **`_mget` and `_source` filtering** remain absent.
- **`search_after`** remains refused: deep paging needs the last hit's sort keys to become a per-shard
  cursor, which is a different mechanism from merging a window.

268 tests green across `test` (233), `pluginTest` (2), `processTest` (13) and `s3Test` (20), none skipped,
MinIO live. `server/` untouched.
