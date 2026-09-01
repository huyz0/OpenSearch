# M15 — production surface, taken in parts

The RFC's M15 row had drifted worse than most: it contradicted itself in the same sentence ("Bulk index
and delete are done" — three clauses later — "bulk delete... are not"), claimed auth and multi-tenancy
were still missing three milestones after M24 shipped them, and listed "classic create-body envelope, or
an explicit decision that D2 makes it a non-goal" as an open question when D2's own table already answers
it: "Not a drop-in for existing clients." The row is corrected in place in `rfc-serverless-shell.md`.

Ground truth, checked rather than assumed: auth (done, M24-M28), bulk index and delete (both done), delete
draining a live writer (done, just not by literal draining — the head clears before the bytes, so a
writer loses ownership on its next tick, and the residual publish race is bounded and self-heals on the
next index of the same name). What's genuinely left: delete-by-query, an update API, the `cluster/config`
register named in §9.3 and never built (parts 2 and 3, below), and one thing the row never named at all.

## The thing the row never named

`if_seq_no`, `if_primary_term` and `version` were not refused. They were read nowhere and so silently
ignored — a client asking `PUT /alpha/_doc/1?if_seq_no=5&if_primary_term=1` for a compare-and-swap got an
unconditional overwrite, with no indication anything was different from what it asked for.

That is a worse failure than every other refusal on this surface, because it looks like success. D2's
rule is "unimplemented endpoints return 501 with a reason — never an empty success," and a parameter
silently dropped is the parameter-level version of the same mistake: not an empty success, but a
*different* success than the one asked for, presented identically.

Both write shapes carried the hole. The single-document path reads the three parameters now and refuses
with a 501 naming why — `WalRecord` records document state, not history, so there is no sequence number
to condition on. Bulk carries the same fields on the action line (`_seq_no`, `_primary_term`, `_version`,
`version_type`) rather than the query string, and is refused the same way, per item, with the same
reasoning `create` already gets ("requires version-conditional writes, which this system does not have").

## Found while testing the fix

Writing the negative case — assert the refused write didn't happen — needed a `GET` on a document in an
index that had never had anything written to it. That path threw. `doGet`'s "no owner" branch assumed
"nobody owns it, so the published commit is the current state" and opened a reader on it — which is right
once something has been published, and wrong for an index that has *never* had a writer activate: nothing
has been published, `openReader` refuses with its own internal "cannot serve as a reader" message, and
nothing above it was catching that specific case. It surfaced as a 500.

The fix checks for this directly rather than catching the exception: when there is no owner, look at
whether anything has ever been published before trying to open a reader on it. If not, and nobody owns
the shard, there is no writer that could publish soon either — the owner check already ruled that out —
so the honest answer is `found: false`, not an internal-invariant leak wearing a 500.

The check this replaces exists for a reason that does not apply here. `openReader`'s refusal protects the
*search* fan-out, where "unpublished" must not collapse into "empty": a live writer might be mid-first
publish, and answering emptily there would be the confident wrong answer this design refuses everywhere
else. A `GET` in the no-owner branch has already ruled that case out by construction — no owner means no
writer is coming — so the two call sites needed different answers to the same internal state, and sharing
one refusal between them was the bug.

## Canaries

- **102 — a conditional single write is silently accepted.** Caught.
- **103 — a conditional bulk action line is silently accepted.** Caught.
- **104 — a fresh-index get 500s instead of 404ing.** Caught.

## Part 2 — an update API

`POST /{index}/_update/{id}` merges a partial document into what's there, using core's own
`XContentHelper.update` — the same utility core's `_update` uses internally, recursing into nested
objects rather than overwriting them whole, and reporting whether anything actually changed as a side
effect of doing the merge rather than needing its own comparison pass.

**Not a transaction, and the javadoc says so before anything else.** The read and the write are two
separate calls with nothing holding the document still in between: a write landing in that window is
silently overwritten, exactly as two plain `index` calls racing each other would be. Classic
OpenSearch's `_update` avoids this by retrying under `if_seq_no`/`if_primary_term` — precisely the
version model `WalRecord` does not have (M15 part 1). This is the honest version of the feature without
one, and it says so rather than implying a safety it doesn't provide.

**One gate, not two.** `ShardOperations.get()` and `.index()` each wrap themselves in their own
`ActionGate` call, under their own action names. Calling them from `update()` would have run a filter
chain twice per request — once as `indices:data/read/get`, once as `indices:data/write/index` — and
never once as `indices:data/write/update`, which is the name a security filter meaning to gate updates
specifically would be looking for. `update()` calls the private, ungated `doGet`/`write` primitives
directly and wraps the whole operation in one gate instead — the same shape `search()` already uses to
compose `describeOnce` and the fan-out under one name. `ServerlessAuthorizationTests` now sends an
update through its filter-surface sweep and asserts the action name arrives exactly once, under its own
name.

**`script` and `scripted_upsert` are refused**, not approximated — the same reason and the same "no
engine registered" shape as every other script surface on this node.

**`detect_noop`'s test asserts on the engine, not the label.** A response saying `"result":"noop"` proves
nothing about whether a write happened; a version of this code that labelled every write "noop" while
still writing would pass a test that only read the field back. The shard's own max sequence number moves
on every real write and nowhere else, so the test asserts on that — and its pair, `detect_noop:false`,
asserts the number *does* move, so the first assertion isn't just an artefact of a shard that never
writes.

### Canaries

- **105 — a scripted update quietly accepted.** Caught.
- **106 — a conditional update quietly accepted.** Caught.
- **107 — a missing document written anyway with no upsert.** Caught.
- **108 — `detect_noop` never skips the write.** Passed at first: the test asserted the response label,
  not the effect. Caught once the test asserted the engine's sequence number instead.
- **109 — the merge does not recurse into nested objects.** Caught.
- **110 — an update gated under the wrong action name.** Caught.

## Part 3 — the `cluster/config` register

`GET/PUT /_cluster/settings` is now the one piece of `/_cluster/*` that is not refused, and the reason it
can be is exactly what makes the rest of that namespace refused: it genuinely is one register, the same
shape as an index descriptor, rather than genuinely cluster-wide state a single node cannot honestly
answer for.

**A store, not a settings service.** Core's `ClusterSettings` validates a key against a registry of
definitions plugins and modules contribute. Building that here would mean either faking a registry this
shell does not have or reaching into core's, and either way inventing a guarantee — "this key is known and
this value is legal" — nobody has actually checked. This register holds whatever an operator puts in it
and hands back whatever is there, the same as an index's mapping JSON is stored without this shell
checking it against Lucene's field-type rules.

**Persistent only.** A `transient` block that is not empty is refused — not silently dropped, not silently
treated as persistent. Not an oversight: transient cluster settings are a footgun even where they exist
(reset on a full restart, and the source of "why did this change back" confusion), and this design never
had a restart to reset one on to begin with. An *empty* transient block is allowed, because plenty of
client libraries send one by default whether or not the caller asked for one, and refusing that would
break ordinary use for no honesty gained.

**Scalar values only, and this one took real investigation to get right.** The obvious plan — treat a null
value as "remove this key," merge everything else via `Settings.Builder.put(Settings)` — has two traps,
both found before they shipped rather than after:

1. Core's own merge does not strip a null-valued key from the map; it stores the null. A register that
   only ever merged would accumulate one dead tombstone key forever, per removal, with no visible symptom
   until someone read the raw register and wondered why a "removed" setting was still there as `null`.
   Fixed by rebuilding the merged result from scratch after every update, keeping only keys whose value is
   still non-null.
2. Once that rebuild exists, it needs to tell a genuine list-valued setting apart from a scalar — and
   `Settings`'s own `getAsList` cannot do that from outside the class: called on a plain scalar key, it
   still returns a non-null one-element list (by design, for comma-delimited value support), so it cannot
   be used to detect "this key is really a list" without a false positive on every scalar. Rather than
   silently drop a list's values during the clean rebuild — the exact same class of loss the rebuild exists
   to prevent for scalars — a list value is refused at the door, before it ever reaches the register.

**Found by testing gating, not by testing the register itself.** Extending `ServerlessAuthorizationTests`'
filter-surface sweep to include a `_cluster/settings` write tripped core's own "must not be a transport
thread" assertion — the PUT branch ran its object-store compare-and-swap inline, on the thread that should
have been reading the next request. Every other write handler on this surface had already learned not to
do that; this one had not, because nothing had ever driven a write through it inside a running node until
the sweep did.

**Retried under contention, not merely once.** §9.3's whole argument for giving settings its own register
is that the writer population is operators at human-scale, so a retry loop rather than a single
compare-and-swap attempt costs one extra read on the rare occasion two operators collide, not a storm.
Tested with eight threads racing genuinely concurrent writes to eight different keys, asserting every one
survives.

### Canaries

- **111 — a removed key leaves a null tombstone in the register.** Caught.
- **112 — a second write replaces the register instead of merging.** Caught.
- **113 — a non-empty transient block is silently accepted.** Caught.
- **114 — a list-valued setting is silently accepted.** Caught.
- **115 — a lost CAS race is not retried.** Caught.

M15 is done: delete-by-query is the one item left, tracked separately.

## Part 4 — delete-by-query, and M15 is done

`POST /{index}/_delete_by_query` removes every document a query matches. It's the largest piece of M15
by a wide margin, and the design question it turned on wasn't "how do you delete documents" — that part
is trivial, reusing the ordinary write path — it was "how do you find them all without the multi-shard
merge machinery `_search` already has."

## Why `_search`'s `search_after` merge is the wrong tool here

`_search`'s cross-shard paging exists to produce one *globally ordered* page across shards, and that needs
a sort whose values are comparable shard to shard. Delete-by-query does not need an order at all — only
that every match is visited exactly once. Reaching for the same merge machinery anyway would mean solving
a harder problem (a meaningful cross-shard order) to answer an easier one (exhaustive coverage).

So it walks one shard at a time, sorted by `_doc` — native Lucene document order. Two properties make it
the right choice for exactly this: it needs no fielddata (unlike sorting by `_id`, which real OpenSearch
disables by default for cost reasons), and it is natively exhaustive *within* one shard's frozen reader,
which is all a per-shard walk needs. What it deliberately is not is a meaningful order *across* shards —
and that was never required.

## Evaluated once, against a frozen view

The set of documents to delete is decided against the last published commit as of when the operation
starts, using the same point-in-time mechanism `search_after` paging already has. A query re-evaluated
against a moving index could match a document twice, across two pages, or never, depending on exactly when
a write landed — the same reason `_pit` exists at all.

**The real cost of this, tested rather than asserted.** A write that is acknowledged and refreshed —
searchable to a live query *right now* — is not touched by a delete-by-query running at that same moment,
if the writer has not yet published it. That is not a bug; it is the same boundary a plain `search_after`
export of matching ids would have. `testAnUnpublishedWriteSurvivesEvenThoughItMatches` proves both halves
in one test: a live search finds both documents, and the delete-by-query only removes the published one.

Deletes themselves go to *live* state, not the frozen view — matching is frozen, deleting is not. A
document rewritten between the query freezing and its delete reaching the shard is still deleted, exactly
as a plain delete would be; there is no version check to make that a conflict here any more than anywhere
else on this surface (M15 part 1).

## Gated once, not once per document

The first version called the already-public, already-gated `ShardOperations.delete()` once per matched
document — which would have run the filter chain once per document under `indices:data/write/delete`,
and never once under a name describing the operation as a whole. `BulkHandler` had already stated the
reason this is wrong, for the same shape of problem: *"filtering each document under
`indices:data/write/index` instead would be stricter but would show a filter an action name it never
registered for, which for a filter that only guards bulk is a hole rather than a difference."*

The fix moved the whole operation onto `ShardOperations` — not the REST handler — as
`ShardOperations.deleteByQuery`, gated once under core's own `indices:data/write/delete/byquery`
(`DeleteByQueryAction`, which turned out to already exist in `server`, not a reindex module the shell
cannot reach). Internally it calls the private, ungated `doDelete` primitive per match. This is the exact
shape `update()` (M15 part 2) and `search()` already use: a composite operation lives on
`ShardOperations`, gated once, calling ungated primitives underneath — not assembled from public,
individually-gated pieces in the REST layer. `ServerlessAuthorizationTests`' filter-surface sweep now
sends a delete-by-query and asserts its action name arrives.

## Two defects its own tests found before either shipped

**No way to ask for a refresh, at all.** Every other write endpoint on this surface accepts `?refresh=true`
and delete-by-query simply had nothing — the first version of the tests failed with search-based
verification showing stale results, because deletes default to `refresh=false` (correctly: refreshing per
document would be absurd for a bulk operation) and nothing ever made them visible. Fixed by threading a
`refresh` parameter through and piggybacking it onto the *last* delete issued for each shard — one refresh
per shard for the whole operation, reusing the exact transport plumbing that already carries a refresh
flag to a forwarded write's owner, so it works whether the shard is local or remote without any new RPC.

**The tests almost didn't require publishing before matching.** Every write in the fixtures used
`?refresh=true`, which makes a document searchable on the *live* shard — and does nothing for the
*published* commit a point in time reads from. The first run of every non-trivial test reported
`{"matched":0,"deleted":0}` against data that was plainly there, because nothing had ever been published.
A `publish()` helper was added to make every test explicit about the step it depends on, which is the same
distinction the class javadoc leads with.

## What is honestly out of scope

Only `query` is read. `script`, `sort`, `size`, `from`, `aggs`, `search_after`, `post_filter` and
`_source` are all refused explicitly rather than silently ignored — the endpoint decides how it walks a
shard and does not offer a caller a window into that. There is no implicit `match_all`: a caller who means
to delete everything says so. Single index only, matching the constraint `PointInTime` itself already has
(one index per view).

### Canaries

- **116 — the walk stops after the first page instead of continuing.** Caught.
- **117 — `max_docs` is ignored.** Caught.
- **118 — a `sort` in the body is silently accepted.** Caught.
- **119 — a missing `query` is silently treated as `match_all`.** Caught.
- **120 — the frozen view is never released.** Caught.
- **121 — refresh never rides any delete, even the last one.** Caught.
- **122 — gated under the wrong action name.** Caught, once the authorization sweep actually exercised
  the endpoint — it could not have caught anything before that.

## M15 is done

Conditional-write refusal, an update API, the `cluster/config` register, and delete-by-query. 22 canaries
across the four parts (102–122, plus the two found and fixed along the way), all caught.
