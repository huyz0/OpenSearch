# M19: a query language, and the merge that had never worked

Search was `?q=field:value` — one `matchQuery` on one field — plus a `POST /{index}/_search` route that
accepted a body and never read it.

## What was already there, and what was missing

The node has a real `SearchService` and a real mapper; per-shard execution has worked since phase 5. What
was missing was the parsing, the merge, and an honest answer for the parts that cannot be merged.

Parsing is nearly free: `SearchSourceBuilder.fromXContent` with a registry from `SearchModule`. So a
`bool` with a `must_not`, a `range`, `match_all` — the whole language — now works because the language was
never the hard part.

## The merge was broken, and no test could have noticed

Hits were appended in the order the shards were visited and returned **in full**: a three-shard search for
`size=10` returned up to thirty hits ordered by shard number. Not a page, and not relevance-ranked.

It survived because every existing search test either fits on one shard or only counts totals. And it
could not have been fixed without a second bug being fixed first — **a score never crossed the network**.
`ForwardedSearchResponse` carried an id list and a source list, so the coordinating node had nothing to
rank on. It now carries whole `SearchHit`s, which are already `Writeable` and already carry score, source
and sort values: less code, and the only version that can be merged.

A third one underneath that: **fetched hits arrive with a score of `NaN`**. In a full OpenSearch search
`SearchPhaseController` copies scores across while merging shards; `ShardQuery` queries a shard directly
and has to do it itself. Invisible while hits were concatenated — an absent score changes nothing if
nothing sorts.

Each shard is now asked for `from + size`, because any one of them might own the whole page, and the
window is applied once, globally.

## Refused, not ignored

`aggs`, `sort`, `search_after`, `collapse`, `suggest` and `profile` return **501 with a reason**. All of
them parse happily and run happily on a single shard; none survives being run on several and having the
answers concatenated. Returning one shard's buckets as if they were the index's would be a number that is
wrong and looks right, which is the worst thing this system could hand anyone. Each is cheap to lift
later, one at a time, and each will need its own merge and its own test.

## Two bugs this surfaced

- **The transport's named-writeable registry was empty.** A search body crossing the network is a
  `SearchSourceBuilder` whose query serializes as a named writeable, so the receiving node answered
  `Unknown NamedWriteable category [QueryBuilder]`. Local shards answered fine and remote ones dropped
  out of the coverage count — the failure shape this design reports rather than hides, which is how it
  was found in one run rather than in production.
- **`_source` was returned as an escaped string.** It was written with `builder.field(String, String)`,
  so every client had to parse the document a second time. Now raw JSON, and hits carry `_score`.

## Canaries

| Planted defect | Caught by |
|---|---|
| Concatenate shards in visit order, no window | order/size and paging |
| Silently ignore aggregations instead of refusing | the refusal test |
| Ask each shard for `size` rather than `from + size` | paging — **only after the fixture was fixed** |
| Leave fetched hits unscored | order/size and the source/score test |

## The fixture that proved nothing

`testFromPagesThroughTheMergedResult` first spread ten documents over three shards and asserted two
disjoint pages. It passed with each shard asked for `size` instead of `from + size`, because no shard held
more than a page's worth and truncating it lost nothing. The window only matters when a single shard could
supply the whole page, so the fixture now puts every candidate on one shard — chosen by routing, not by
hoping — and the canary produces an empty second page.

## What this does NOT establish

- **No aggregations, sort, `search_after`, `collapse`, or `suggest`** — refused, per above.
- **Fan-out is still sequential.** A three-shard search is three round trips one after another. This is
  the same gap `_bulk` has for its per-shard dispatch, and it is now the most visible performance issue in
  the system.
- **`total` is a sum of per-shard totals**, which is right for a match count and would not be right for
  anything requiring deduplication.
- **No search cost measurement**, on either store. The cost suite counts writes and idle ticks; what a
  query costs is still unknown.
- **No `_msearch`, no scroll, no point-in-time, no highlighting tested**, and no multi-index search — the
  route is still one index at a time.
