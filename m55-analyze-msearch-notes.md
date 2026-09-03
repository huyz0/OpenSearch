# M55 (finished) — `_analyze` and `_msearch`

The last two endpoints both compared serverless products ship and this did not.

## `_analyze`, and a fourth stale reason

The refusal this replaces read: *"analysis runs inside a shard's mapping, and a node that does not hold a
shard of this index cannot answer for it."* Analysis needs the mapping, and any node can read one — which is
the same observation that made `_field_caps` shard-free in M53. So the reason was wrong in the way three
earlier ones were: true-sounding, never checked.

The analyzer comes from a mapper service built from the descriptor, so an index whose shards are all dormant
answers — the normal resting state here, and the state a client is most likely to be in when it is trying to
work out why a query matches nothing.

**Why a client calls this at all** is worth stating, because it decides what may be approximated: a query
matching nothing is usually an analysis disagreement, and this endpoint is how somebody sees one. So
`analyzer_used` is reported on every answer, and **a custom tokenizer-and-filter chain is refused rather than
substituted**. Accepting `tokenizer` and analysing with the index's analyzer instead would show a caller the
analysis of something other than what they asked about — to somebody who called this endpoint precisely
because they do not believe what they are being told about analysis.

`explain` is refused for the same kind of reason: it reports each step's intermediate tokens, which this does
not collect, and returning the final list while accepting the flag would answer a different question.

## `_msearch`, and what it does not save

Several searches in one request. **The saving is round trips, not shard work**: each search still fans out to
the shards it names, so ten searches cost one HTTP request and exactly the same object-store traffic as ten
requests would. The endpoint's name suggests a bulk discount it does not give, and both the class
documentation and the tests say so rather than letting a reader assume otherwise.

**One search's failure is that search's failure.** A batch whose third query is malformed returns results for
the other nine and an error object in the third slot, at HTTP 200 — the same accounting `_bulk` uses, so a
caller pairing responses to requests by position still can. A body that is not newline-delimited pairs at all
fails the *request*, because there is nothing to pair.

**The response body is `SearchHandler`'s.** `renderInto` was extracted so a query answered here and the same
query answered by `_search` produce the same object. A second renderer would agree with the first until
somebody changed one of them, and the difference would surface as two response shapes for one query.

## Two bugs the probe caught before the tests existed

**The query registry.** The first version parsed sub-queries with `NamedXContentRegistry.EMPTY`, which fails
every query with *"named objects are not supported for this parser"* — a message that reads like a malformed
request and is not. Queries name types that only the node's populated registry can resolve.

**The size default.** The second version answered with correct totals and empty `hits` arrays, because
`SearchSourceBuilder` starts at size `-1` and `_search` applies its default separately. A response saying
"one hit matched" and carrying none is exactly the shape of answer this project treats as worse than an
error, and it also produced a misleading `Unknown NamedWriteable category` failure downstream that looked
like a registry problem and was not.

Both were found by driving a running node and reading the bodies, not by a test that asserted what I expected
to see.

## Canaries

- **188 — a field's analyzer is ignored in favour of the index default.** Caught: a keyword field's value
  comes back tokenised.
- **189 — a custom tokenizer is accepted and the index analyzer used instead.** Caught.
- **190 — one bad search fails the whole batch.** Caught.
- **191 — `_msearch` counts hits and returns none.** Caught. This is the canary for the size-default bug
  above, so the defect that shipped in the first draft cannot come back unnoticed.

## What this does NOT establish

- **No custom analysis chains.** `tokenizer`, `filter` and `char_filter` are refused, so an operator designing
  a custom analyzer cannot test it here before setting it in a mapping. That is the most useful thing this
  endpoint could do that it does not.
- **No `explain`.**
- **`_msearch` runs its searches in sequence**, not concurrently. Ten searches take as long as ten searches;
  the round trip is what is saved. Running them concurrently is possible and is not done here, because each
  search already fans out concurrently across shards and stacking a second layer of concurrency needs a
  thought about the search pool that this milestone did not have.
- **`max_concurrent_searches` is not honoured** — there is nothing to bound, since nothing runs concurrently.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M55 is done. Still open from the comparison: index and component templates, and ingest pipelines.
