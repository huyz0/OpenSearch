# M50 (finished) — the audit's fix list, taken

M49 ended with a re-audit of the REST surface. It found three groups of drift away from the shell's own
stated posture — *unimplemented endpoints return 501 with a reason, and nothing silently returns a wrong
answer* — and this milestone is that list, worked through.

**The audit was done by reading every handler's `routes()` and then driving a running node over HTTP.** That
matters, because half of what it found is invisible from the source: a route that is not registered, a
parameter that is not consumed, a status core rewrites on the way out. M47's own lesson was that assuming
from endpoint names gets it wrong; this went one step further and refused to assume from method signatures.

## The one that was a wrong answer rather than a refusal

`PUT /{index}` took the shard count from a `?shards=` query parameter and treated the **entire request body**
as the mapping. A client sending OpenSearch's own create-index envelope therefore got a one-shard index whose
mapping was the envelope — and was told `"acknowledged": true` and `"shards_acknowledged": true` for it.
Shard count is fixed at creation, so the loss was not recoverable without deleting the index and its data.

That was the only place on this surface where a caller was misled about their own data rather than refused,
and it was the failure `NotImplementedHandler`'s own javadoc says the surface exists to avoid.

Both shapes are now read. A body carrying `settings`, `mappings` or `aliases` at the top level is the classic
envelope; anything else is the bare mapping this handler has always taken, so the shell's own callers and the
`?shards=` parameter are unaffected. The two cannot be confused — a mapping's top level is `properties`,
`_source`, `dynamic` and the like.

Three things inside the envelope are decisions rather than plumbing:

- **`number_of_shards` is honoured**, normalised through core's own `normalizePrefix` rather than a guess at
  what a client might have written.
- **`number_of_replicas` is refused** unless it is zero, with the real reason: a shard here has one writer and
  its durability is the object store, not a second copy. Accepting it would record a number nothing acts on
  and report a redundancy this deployment does not provide that way — the same failure as losing the shard
  count, quieter and no more honest.
- **Every other setting is carried through** into the descriptor's `extraSettings`, which already existed and
  already layers into `IndexMetadata`. The data plane under this shell is core's, so `refresh_interval`,
  analysis settings and the rest genuinely apply.
- **`aliases` is refused**, pointing at `PUT /_alias/{name}`. An alias silently not created is a search that
  silently matches nothing later.

## Refusals that had stopped being true

M48 gave the single-document path conditional writes. `_bulk` went on telling callers that `create`
*"requires version-conditional writes, which this system does not have"* and that *"every write is a plain
overwrite and every delete a plain removal."* Both were true when written; neither survived M48. A stale
refusal is worse than an unexplained one, because a caller can act on it — this one would have told a reader
that optimistic concurrency was unavailable deployment-wide.

The obstacle was never a missing capability. It was that the batch path took `WalRecord` — the write-ahead
**log** record — as its input type, and a log record describes what happened, so there was nowhere in it for a
condition, which describes what must be true before anything happens. M48 recorded this precisely and left it;
this milestone does the contained refactor it named. `ServerlessNode.BulkOperation` carries the record and its
condition, `ForwardedBulkRequest` carries both across the wire, and `bulkOperations` applies them.

So `_bulk` now does:

- **`create`**, which is one constant on the call the write path already makes — `Versions.MATCH_DELETED`
  rather than `MATCH_ANY`. The engine compares against its own live version map under the per-document lock it
  already holds. Nothing reads first, which would be a race rather than a check. The same constant gives the
  single-document path `PUT|POST /{index}/_create/{id}`.
- **`if_seq_no` / `if_primary_term` per item**, compared by the same engine call the single-document path uses.
- **A lost condition as a 409** carrying `version_conflict_engine_exception`, not a 400 carrying
  `operation_failed`. A client retrying needs to tell "your condition did not hold" from "your document was
  malformed", and only one of those is worth retrying.

Two refusals remain, and now say what is true of them:

- **`update` in a batch** is a partial merge, which has to read the current document before it can write one;
  this path applies a batch without reading it. The message points at `POST /{index}/_update/{id}`, which does.
- **External versioning** (`_version`, `version_type`) asks for a version model this system does not keep —
  the same refusal the single-document path makes, for the same reason.

**A regression this milestone introduced and a pre-existing test caught.** Moving `_seq_no` and
`_primary_term` out of the refused set left them silently ignored on an action line, because the request
spells a condition `if_seq_no`/`if_primary_term`. `ServerlessDataPathTests` failed on exactly that, which is
what it was written for. They are now refused by name, pointing at the right spelling.

## Endpoints that were absent and did not say so

D2 promises a 501 with a reason. Ten paths delivered one; everything else a client reaches for fell through to
core's default handler and came back as `400 {"error":"no handler found for uri ..."}` — a bare string, the
wrong status, and indistinguishable from a typo.

**What is now served** rather than refused, because it costs nothing and was only ever unrouted:

| Route | Was | Note |
| --- | --- | --- |
| `POST /{index}/_doc` | 400 no handler | An id the caller does not supply is generated, as `_bulk` has always done. Routing is a function of the id, so it has to exist before the request can be placed. |
| `PUT\|POST /{index}/_create/{id}` | 400 no handler | Above. |
| `HEAD /{index}` | 405 | An existence check. `GET` on the same path was routed and `HEAD` was not, so the one path that exists to answer this could not. |
| `GET\|POST /_search` | 404 "no such index: _search" | Swallowed by `GET /{index}`, which read the endpoint as an index name. A bare `/_search` now means `*`, the prefix pattern this shell already resolves with one bounded listing. |
| `GET /{index}/_mapping`, `_settings` | 400 no handler | Projections of the descriptor `GET /{index}` already reads, so neither adds an object-store request. |
| `q=` without a colon | 400 | It was a hand-rolled split on the first colon, so `q=field:value` worked and `q=hello` did not. It is now core's own query-string parser, which is what `q=` means in OpenSearch; the colon form still parses, as the syntax it always looked like. |

**What is now refused explicitly**, each with the reason it is refused for rather than a generic one:
`_count`, `_refresh`, `_flush`, `_forcemerge`, `_analyze`, `_explain`, `_termvectors`, `_msearch`, `_reindex`,
`_update_by_query`, `_search/scroll`, `_template`, `_index_template`, `_component_template`,
`_ingest/pipeline`, `_scripts`, `_ilm/policy`, `_tasks`, `_nodes/stats`, `_cluster/allocation/explain`, and the
rest of `_cat`. "Not here" and "not here *because*" are different answers, and only the second tells a caller
what to do instead — `_count` points at `_search` with `size=0`, `scroll` points at a point in time,
`_reindex` points at `search_after` plus `_bulk`.

A refusal on a path with a placeholder in it needed one more fix: `BaseRestHandler` rejects a request whose
parameters were not all read, so `/{index}/_count` came back as *"unrecognized parameter: [index]"* instead of
as the refusal it is. `NotImplementedHandler` now consumes its parameters.

## Shapes

- **`GET /{index}`** returned `{index, uuid, shards, has_mapping}` — a flag saying a mapping existed without
  letting the caller see it, and with `_mapping` and `_settings` unrouted there was no path by which a client
  could read back anything it had configured. It now answers OpenSearch's shape, keyed by index name, with the
  mapping itself and settings nested under `index`.

  **`aliases` is absent rather than empty**, and that is deliberate. Resolving an index's aliases needs a
  reverse lookup this design does not offer — aliases are found by name, and enumerating them is the inventory
  operation `/_serverless/indices` refuses for the same reason. `"aliases": {}` would be a confident empty
  answer. A caller that needs them asks `GET /_alias/{name}`.

- **The 501 and 421 bodies** built their own envelope with `"error"` as a bare string — the exact shape M47
  removed everywhere else, missed in these two because they are the only refusals that do not go through the
  shared helper. They now render the same nested `error` object as everything else, which was M47's whole
  point: a caller should not be able to tell "refused on purpose" from "something threw" by the envelope.

## Canaries

Every claim above is pinned by a test over HTTP, and every test by a planted defect:

- **157 — the create envelope is taken as the mapping again.** Caught: the shard count comes back as 1.
- **158 — a replica count is accepted and ignored.** Caught: the create succeeds.
- **160 — `create` is an ordinary overwrite** (`MATCH_ANY`). Caught: the second create returns 201.
- **161 — bulk conditions are accepted and ignored.** Caught: a stale condition writes.
- **162 — the refusal handler stops consuming its parameters.** Caught: `/{index}/_count` is a 400.
- **163 — `q=` reverts to the colon split.** Caught: a bare term is a 400.
- **164 — `HEAD /{index}` is unrouted.** Caught: 405.
- **165 — the mapping is not rendered.** Caught: `GET /{index}` carries no mapping.

## What this does NOT establish

- **No comparison was run against a live standard OpenSearch.** Every "what a client expects" here is from the
  documented API and from core's own source in this repo, not from a differential test against a running
  instance. That is the honest limit on the word "compatible" throughout.
- **`update` in `_bulk` is still refused**, and that is a real gap for a client that batches updates. Closing
  it means a read per item inside the batch, which changes what a batch is — it currently applies without
  reading — so it is a design decision rather than plumbing, and it is not made here.
- **Settings are write-once.** They are honoured at creation and readable afterwards; `PUT /{index}/_settings`
  is not routed, and a settings service is the feature M47 named and this milestone still does not build.
- **`aliases` at create time is refused, not implemented.**
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M50 is done.
