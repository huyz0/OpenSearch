# M60 (finished) — `_explain`, and the last refusal that described nothing

Asked for as a question — *brainstorm how we can support explain, and if it is not possible or it impacts
scalability then don't do it.* The answer was that it is possible and it does not, for a reason worth writing
down: **an explain is cheaper than the search whose score it explains.**

## The reason it was refused was true of nothing

> explaining a score needs the scorer for one document on one shard; the fan-out here merges hits rather than
> exposing per-shard scoring internals

Both halves fail. The first names the requirement — the scorer for one document on one shard — and that is
exactly what is available: a shard is open, its searcher is reachable, and Lucene's `explain` is a method on
it. The second describes a fan-out that an explain does not do. **A search contacts every shard because any
of them might hold a hit. An explain names a document, and a named document lives on exactly one shard.** No
fan-out, no merge, one term lookup and one `explain` call.

This is the seventh instance of the same pattern this surface keeps producing: a reason that was true of one
endpoint, copied to another it was never true of. It is also one M59 got wrong in the other direction — its
notes called this "the one endpoint from the comparison whose original reason survived contact." It did not.
Reading the route list is what produced that sentence; reading `TransportExplainAction` is what corrected it.

## What core actually needs, and what this shell replaces

`TransportExplainAction` puts all of its cluster work in `resolveRequest` (alias filters, routing-required)
and `shards` (operation routing). Both are *which shard answers* — the one job this shell already does for
itself, from the shard-head. What is left is `shardOperation`, and it touches three things:

- `searchService.createSearchContext(new ShardSearchRequest(shardId, now, AliasFilter.EMPTY), NO_TIMEOUT)`
- `indexShard.get(new Engine.Get(false, false, id, new Term("_id", Uid.encodeId(id))))`
- `context.searcher().explain(context.query(), docIdAndVersion.docId + docBase)`

None of which reads cluster state. `ShardExplain` copies that sequence rather than reinterpreting it, and is
shared by the local path and the forwarded one for the reason `ShardQuery` is: two implementations of "explain
on one shard" stay consistent right up until they matter.

## The half that would have been easy to lose

**A document that exists and does not match is a 200, not a 404.** Lucene returns a non-matching explanation
— `"no matching term"`, value `0.0` — and that is the more useful half of what the endpoint is for. *Why did
my document not match* is the question a caller cannot already answer with a get. Collapsing it into "not
found" would answer the easy half and drop the hard one, which is what canary 209 plants.

The explanation for a non-match is terse. That is Lucene, not this shell: there is no scoring to break down.
Terse is not empty — it still separates a document that failed the query from one that is not there.

## Which copy scored it, and why that had to be said

**A score is not a property of a document.** It is a function of the whole shard's term and document
frequencies. In a cluster this never comes up: every copy of a shard holds the same segments, so every copy
computes the same score, and the classic endpoint has no reason to say which one answered.

Here a shard may have no owner and be read from its last published commit. That explanation is a true account
of *that commit* and will not be the number a search returns once a writer holds newer segments. So the
response carries `realtime` and `_node`, meaning exactly what they mean on a get.

**Routed to the owner, following a get and not a search.** `handleSearch` opens a shard it does not hold as a
reader, because a search's answer is a set of hits and a slightly older set is a partial answer with a number
attached. A score is not like that — a plausible number computed from the wrong statistics looks exactly like
a right one. So `handleExplain` refuses when the head is stale, as `handleGet` does, and sends the caller back
to re-read the head.

The explanation crosses the wire whole, through core's own `Lucene.writeExplanation`. Flattening a nested
explanation to text on one node so the other could hand it back would make a forwarded explain read
differently from a local one.

## No match-all default

`_count` with no body means "how many documents are there", which is a question. "Explain this document
against nothing in particular" is not one, and answering it with `match_all` would return a constant score
that reads like a real result. A body without a `query`, and a request with neither body nor `q=`, are both
400s that say what to send.

`q=` goes through the same parser search's `q=` uses. Explaining a query the caller did not write is worse
than refusing, because the explanation would be correct about the wrong query.

## Canaries

- **209 — a document that exists and does not match is reported as a 404.** Caught.
- **210 — the description is written here rather than taken from Lucene.** Caught: the term `title:sea` is
  Lucene's own text and appears nowhere in this shell.
- **211 — a missing document is reported as present with no explanation.** Caught.
- **212 — the published-commit copy claims to be realtime.** Caught, and the reason the disclosure test is not
  vacuous: without the dormant-shard case every path exercised is realtime, so a hardcoded `true` would pass.
  That was the first version of the test, and it proved nothing.
- **213 — the explanation is flattened on the wire.** Caught: the forwarded case asserts the nested `details`
  came back, which a bare value-and-description would not satisfy.
- **214 — the forwarding node claims it did the explaining.** Caught.

## What this does NOT establish

- **`_source` and `stored_fields` on the response are not served.** Classic returns the document alongside the
  explanation when asked. Here a caller who wants both makes two requests, which costs one extra round trip
  and nothing else.
- **Rescorers are not applied.** Classic runs `rescorer.explain(...)` over the base explanation. Nothing on
  this surface builds a rescore context, so there is none to apply; if rescoring is ever served, this is the
  second place to change.
- **Alias filters are not applied.** `AliasFilter.EMPTY` is passed, which is correct only because aliases here
  are sets of index names with no filter — the same gap M59 recorded, showing up in a second place.
- **`?routing=` is not honoured**, consistent with the rest of this surface, which routes by id alone.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M60 is done. `_explain` is served, and the four-way comparison's list of things refused for a reason that
survives reading has one entry left: `_rank_eval`.
