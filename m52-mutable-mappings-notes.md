# M52 (finished) — the gap a competitive comparison found

M51 ended with a comparison of this shell's REST surface against AWS OpenSearch Serverless and Elastic Cloud
Serverless. It produced one finding worth acting on before any of the others:

> **Both products let you change an index after you create it. We don't.**

Mutable mappings appear in AWS's permitted-operations table (`aoss:UpdateIndex` covers `PUT|POST
<index>/_mapping`) and inside Elastic's "index-level settings remain yours" carve-out. Ours were fixed at
creation, with no route to change them — and with no `_reindex` to escape through, the only remedy for a
missing field was to delete the index and its data.

Two independent vendors, on two different engines, treating something as non-optional is the strongest signal
a comparison can produce. That is what this milestone took.

## The design: nothing here decides what a legal mapping change is

The merge is core's own `MapperService#merge`. Adding a field succeeds; changing an existing field's type is
refused, and the refusal carries core's own words — *"mapper [msg] cannot be changed from type [text] to
[long]"* — rather than a paraphrase. Re-wording it would mean maintaining a second account of the type system
that could drift from the real one.

This is the same shape as `if_seq_no` in M48: the comparison is the engine's, and this shell only carries the
question to it.

**Validating without a shard.** `IndicesService#createIndexMapperService` builds a mapper service from index
metadata alone — it is how core validates a mapping before any shard exists. So the update works on whichever
node the request lands on, whether or not that node holds a shard of the index. A mapping is a property of the
index, and there is no reason a client should have to reach a particular node to change one.

**The write is a compare-and-swap.** Two clients adding different fields at the same time is exactly the race
this design keeps an object store to arbitrate. The loser re-reads, re-merges and retries, so both fields end
up present; only after four attempts is the caller asked to retry. Nothing is silently dropped and nothing is
silently overwritten.

**A mapping version had to be added**, and it is not bookkeeping. Core decides whether to re-apply a mapping
to an open shard by comparing `IndexMetadata#getMappingVersion` — an unchanged version means "nothing to do"
and the new source is ignored. A shell that edited the mapping and left the version alone would write the
change to the object store, hand it to every node, and have every node decline to apply it. The version lives
on the descriptor, because the descriptor is the only thing here that knows an update happened.

**Open shards had to be told.** `ShardReconciler#refreshMapping` applies the change to whatever this node
already holds. The reconcile pass opens shards that are missing and closes shards that are no longer ours; it
had never had a reason to notice that an index it already holds says something different than it did. A
mapping update is the first change of that kind, and the node that accepted it is the one node guaranteed to
be holding stale metadata the instant it succeeds. Every other node reads the new descriptor when it next
opens a shard — there is no push, because there is nothing to push from.

**The test that matters is the query, not the mapping read.** Storing a new mapping and reading it back proves
only that a string round-tripped. `testAFieldAddedToALiveIndexIsImmediatelyUsable` writes a document using the
new field and runs a *range* query against it, which only matches if the shard already open and already
serving traffic indexed it as a long. A shard that kept its old mapping would index it as text and return
nothing, with no error anywhere.

**One shape bug the work introduced and a test caught.** Core's merged mapping source nests everything under
the mapping type (`_doc`); OpenSearch's own `GET /{index}/_mapping` does not show that level, and this shell's
stored mappings never had it. Storing core's form verbatim would have silently changed the shape of every
mapping a client reads back, the first time it updated one. It is unwrapped once, at the point of storage,
rather than special-cased in every reader.

## The rest of what the comparison found

**A bug, not a gap: the point-in-time API was spelled Elasticsearch's way.** OpenSearch creates a PIT at
`POST /{indexes}/_search/point_in_time`; this shell shipped `POST /{index}/_pit`, which is Elasticsearch's
endpoint, on a fork of OpenSearch. A correctly written OpenSearch client got *"no handler found for uri"*.
Both spellings are routed now — removing the old one to fix a compatibility bug would have broken this
shell's own callers in order to do it.

**`_count`**, which both products ship. It was refused with a message telling the caller to send `_search`
with `size=0` and read `hits.total` — accurate, and work this handler was already doing and could simply
report. A count with no body means all of them; the total is exact rather than a lower bound, because this
shell always tracks it; and `_shards` is reported for the same reason a search reports it, since a count
assembled from some of the shards is a different number from one assembled from all of them.

**`GET|HEAD /{index}/_source/{id}`**, which AWS ships. The same read a `_doc` fetch already makes, rendered as
the document alone. A missing document is still a 404 — an empty body with a 200 would be indistinguishable
from a document that happens to be empty.

**Eight endpoints that were falling through to core's default 400.** The comparison found `_field_caps`,
`_validate/query`, `_resolve/index`, `_rank_eval`, `_aliases`, `/{index}/_alias/{name}`, `_search/pipeline`
and `_cat/templates` answering *"no handler found for uri"* rather than the 501-with-a-reason this surface
promises. Being absent is a decision; looking like a typo is not. Each now refuses with the reason it is
refused for, and `_field_caps` says plainly that it is the largest of the remaining gaps rather than dressing
its absence up as a principle.

**A stale message from M50, found while fixing the count.** The search handler's "send a search body" refusal
still said *"or use the q=field:value shorthand"* after M50 replaced `q=` with core's real query-string
parser. The M50 edit had used a plain string replacement without asserting it matched, so it silently did
nothing. Every other edit in that milestone asserted; this one did not, and that is exactly the difference.

## Canaries

- **173 — an open shard is not told the mapping changed.** Caught: the range query finds nothing.
- **174 — the mapping version never moves.** Caught by the same test, which is the point of having it: core
  declines to apply a mapping whose version has not changed, so this fails identically to never writing it.
- **175 — the merge is not core's; the caller's mapping is taken as given.** Caught: the type conflict is
  accepted.
- **176 — core's mapping-type wrapper leaks into what clients read.** Caught.
- **177 — OpenSearch's PIT spelling is unrouted again.** Caught.
- **178 — a count fetches the hits it will throw away.** **Did not fire**, and is recorded rather than
  dropped. The count renderer never emits hits, so whether they were fetched is invisible from the response;
  the assertion that looked like it covered this covered only the renderer's shape. The `size(0)` is a real
  saving and it is untested. Proving it needs an observation this suite does not currently make — object-store
  request counting reaches the wrong layer, since fetching hits from an already-open shard is local Lucene
  work.

An earlier attempt at 175 also failed to fire: swapping `MergeReason.MAPPING_UPDATE` for `MAPPING_RECOVERY`
changes nothing, because the refusal of a conflicting field type comes from the field mapper's own merge and
not from the reason. The comment claiming otherwise was corrected rather than left standing.

## What this does NOT establish

- **Settings are still write-once.** `PUT /{index}/_settings` remains unrouted, and both compared products
  ship it. It is the same shape of work as this milestone — compare-and-swap the descriptor, apply to open
  shards through `IndexService#updateMetadata` — but settings changes have their own validation rules
  (static versus dynamic) and are not a mapping's problem. Next, if anything is.
- **`_field_caps` is the largest gap left**, and both compared products ship it. Clients use it to discover
  what they can query. It is now an honest 501 rather than a 400, which is not the same as being built.
- **No differential test against a live OpenSearch, AWS, or Elastic instance.** Everything about "what a
  client expects" comes from published documentation and from core's own source in this repo.
- **A mapping update races with a shard opening elsewhere.** A node opening a shard between the swap and its
  own next descriptor read opens at the old mapping; it corrects on its next pass. Bounded by the reconcile
  interval, and no write is lost by it — an unknown field is indexed dynamically and re-indexed correctly on
  the next write, but a range query in that window may miss it.
- **A count against an index whose shards are all dormant is a 500**, not a 503 or an activation. That is
  `_search`'s pre-existing behaviour, which `_count` now inherits by being the same handler; it is consistent
  rather than good, and it is a worse first impression on an endpoint that is new. Left alone here because
  changing it changes search.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M52 is done.
