# M61 (finished) — the audit below the route table, and what it took

[`api-compatibility-audit-2026-09.md`](api-compatibility-audit-2026-09.md) was the first comparison to read
every handler for which parameters it honoured, refused, silently dropped, or rejected as "unrecognized
parameter", and to compare every response to the shape core's own `Rest*Action` renders. M47–M60 had made the
route table close to both vendors; this is what was underneath it. Every finding in sections A–E of the audit
is closed here, and every closure is pinned by a test that would have failed before it.

## The two that stopped real clients at the door

**`typed_keys`.** The official Java client puts `typed_keys=true` on every search unconditionally. No shell
search handler consumed it, so `BaseRestHandler` refused every one of that client's searches as
"unrecognized parameter". OpenSearch Dashboards sends `track_total_hits` and `max_concurrent_shard_requests`
on every server-side search, with the same result. The shell had never been asked a search by either.

**The fix is not a list of parameters.** `SearchHandler` now hands the request to core's own
`RestSearchAction.parseSearchRequest` — the same move M50 made for `q=` — so every URL parameter core parses
into the `SearchSourceBuilder` (`sort`, `_source`, `stored_fields`, `docvalue_fields`, `track_total_hits`,
`track_scores`, `timeout`, `terminate_after`, `explain`, `version`, `seq_no_primary_term`, `df`,
`default_operator`, `analyzer`, `lenient`) is honoured by being forwarded to every shard exactly as a body
field would be. URL wins over body for `size`, `from` and `q`, which is core's precedence; this handler had it
the other way round. What core parses into the *request* rather than the source is then answered on its own
terms: `scroll`, `routing` and `search_pipeline` change what the answer would be and are refused with a
reason; `preference`, `request_cache`, `batched_reduce_size` and the other coordinator hints are consumed
without effect, because there is one copy of every shard and a fixed fan-out width, and a hint that cannot be
followed is not a reason to refuse the search that carried it. Strictness is kept for what nothing knows: a
misspelt parameter is still a 400.

## Core's response object, so core's renderer

The search response used to be written field by field, and every field the renderer did not write was
silently absent — `highlight`, `_explanation`, `_version`, `matched_queries`, `inner_hits` were all computed on
the shard and dropped on the way out. The fan-out now builds a real `SearchResponse` and calls its own
`innerToXContent`. That one change is what makes `typed_keys` render (`InternalAggregation` reads it off the
params), makes `rest_total_hits_as_int` work, writes `_score: null` for an unscored hit the way every client's
parser expects, echoes `pit_id`, and lists `_shards.failures` — which never existed here: a shard that could
not be reached was counted and its reason only logged. The plugin `Client`'s search answer is built by the same
method, so a plugin and a user see one response.

**Four things the shard always reported and the wire dropped** now travel: whether it timed out, whether
`terminate_after` stopped it, whether its total is exact or a lower bound, and its best score.
`timed_out: false` and `relation: eq` had been stated as constants, and they were true only because the
information that could make them false was thrown away between `ShardQuery` and the response.
`track_total_hits` is honoured as the caller set it; it used to be forced to `true` after parsing, so the
cheaper count Dashboards asks for cost the full one.

## Confidently wrong answers, each with its canary

- **`POST /_index_template/_simulate` stored a live template named `_simulate`.** Only the `/{name}` forms
  were refused; the nameless form — the one in the spec — matched `POST /_index_template/{name}`. Templates
  now refuse underscore names the way indices and repositories already did, and the nameless form is
  refused with the simulate reason.
- **A body `pit` block was parsed and ignored**, so a client paging a point in time by the book got a live
  search that looked frozen. Both spellings are honoured; a body `keep_alive` extends the view, since the
  deadline is one register write. Core's delete (`DELETE /_search/point_in_time` with `pit_id[]`, and
  `_all`) and list (`GET .../_all`) are served in core's shapes; the shell's `/{id}` form is kept. A
  keep-alive over the maximum is refused rather than clamped.
- **A get through any action filter lost its concurrency token.** The gate's read-back rebuilt the document
  with the three-argument constructor, whose defaults are the unassigned sentinels, so with the security
  plugin installed every `GET` reported `_seq_no -2` and the conditional write built on it lost.
- **Alias options were dropped with an acknowledgement on all three spellings**; the index-scoped `PUT`
  never parsed its body at all. `filter`, `routing`, `is_write_index` and `is_hidden` are refused with the
  reason each cannot be kept. The `indices`/`aliases` array forms, which resolved to the literal string
  `"null"`, are read; `must_exist` is honoured.
- **Restore accepted 18 body fields and acted on 3.** The allowlist that turned an unknown key into a 400
  told a caller the known keys were honoured. The 15 it did not read are now refused, each with its reason;
  create checks unknown keys as restore always did; `include_global_state` is reported as `false`, which is
  what happened, rather than echoed.
- **Bulk action lines dropped `op_type`, `routing`, `pipeline`, `require_alias`.** `op_type: create` was an
  overwrite. It is a create now; `routing` and `require_alias` are per-item refusals; `pipeline` runs, per
  line or from the request, compiled once per batch, with a dropped document reported as `noop`.
- **Templates stored `template.aliases` and `data_stream` and never read them**; both are refused.
  `version` and `_meta` are stored and returned, which is all core does with them.
- **`_msearch` skipped the refusals `_search` makes** and silently dropped a named index it could not find.
  Index resolution is one method now, shared by both; `collapse`, `suggest`, `profile` and `search_after`
  get the same answers inside a batch as outside one; `took` comes first and each item carries `status`.
- **`_field_caps` ignored its body.** **`GET /_list/indices?size=`** was consumed and ignored; refused like
  `next_token`. **`_ingest/pipeline/_simulate`** misrouted onto `{id}`; served (see below).
  **`/_snapshot/{repo}/_verify`** tried to create a snapshot named `_verify`; served. **`GET /_nodes/os`**
  answered "no live node matches [os]"; a metric is a 501 naming what `/_nodes` does report.
- **The local write path showed a filter the pre-pipeline document** and wrote the post-pipeline one; the
  forwarded path did the opposite. Both show what lands.
- **A second identical aggregation search on a shard failed with "Unknown NamedWriteable category".** Found
  by the typed-keys canary, not by the audit: the shard request cache reads a cached result back through the
  registry `IndicesService` was built with, and that registry was empty. It is the search module's now.

## Parameters, by policy rather than by accident

The same parameter used to be a 400 on one endpoint (`routing` on `_doc`), silently dropped on another
(`routing` in `_bulk`) and a 501 on a third (`version` on `_doc`). Every handler now sorts what it is sent
into three buckets, and the bucket is written down where it is decided:

- **Honoured**, where the mechanism already existed: `refresh=wait_for` on every write (it asks for what
  `refresh=true` does, and was a boolean parse failure); `op_type`; `retry_on_conflict` on `_update`;
  `preserve_existing` on `_settings`; `create=true` on a template; `min_score` and `terminate_after` on
  `_count`; `q=` and its companions on `_explain`, `_validate/query` and `_delete_by_query`; the
  `_field_caps` body; `GET /{index}/_settings/{name}`.
- **Consumed as a hint**, with the reason stated once in the handler: `preference`, `realtime`, `refresh` on
  a get; `timeout`, `wait_for_active_shards`, `master_timeout`, `cluster_manager_timeout`, `local`,
  `flat_settings`, `include_defaults`, `expand_wildcards` and the rest of the fleet-shaped parameters an
  admin call carries.
- **Refused with a 501 and a reason**, where accepting would change the answer: `routing` everywhere,
  `version_type` (its sibling `version` always had a careful refusal; it had "unrecognized parameter"),
  `stored_fields` on a get, `_source` on an update or explain (the document is not carried back),
  `require_alias`, `slice`, `indices_boost`, `wait_for_completion=false` on a delete-by-query, `verbose` on a
  simulate, `include_unmapped` and `index_filter` on field caps.

## Shapes with no reason to differ

`_delete_by_query` answers core's `BulkByScrollResponse` fields (`matched` stays as the older name for
`total`). `_update` answers 201 when the upsert created the document. `_mget` items carry `_version`,
`_seq_no` and `_primary_term`. `GET /{index}/_source/{id}` answers 400 for `_source=false` and a JSON error
for a missing document, both core's own. Writes report `forced_refresh`. `_count` reports
`terminated_early` and failures. `_analyze` reports `positionLength`. `_cat` renders `yaml`, `cbor` and
`smile` through the channel's own builder. `GET /` is core's `MainResponse` field for field, and `HEAD /` —
every client's `ping()` — exists. `_cluster/health` reports `discovered_cluster_manager` and
`active_shards_percent`.

## Both vendors ship it and this did not

`GET /_cat/indices/{pattern}` is served under the bounded-prefix rule `_list/indices` already applied; the
bare form stays refused. `POST /_ingest/pipeline/_simulate` runs a stored or inline pipeline over the
documents in the body — the same compiler, over documents instead of a write. `POST /_analyze` without an
index answers from the node's own analyzers. Search templates, data streams, and the other spec paths that
fell to core's default are refused with reasons.

## The test that stops this recurring

Forty-one spec paths answered core's default 400 while a sibling path answered 501 with a reason, found by a
forty-line script over `rest-api-spec`. `ServerlessSpecCoverageTests` is that script as a test: the spec is
copied onto the test classpath from `:rest-api-spec` so it cannot drift, every non-deprecated method and path
is sent to a running node, and none may answer core's no-handler 400 or a 405. It would have caught the
simulate bug, the PIT delete gap and the alias spellings before they shipped.

## What this deliberately still did not do — taken by M62

Every item below was closed in [`m62-remaining-compatibility-notes.md`](m62-remaining-compatibility-notes.md);
the list is kept as it was written, because the reasons are what M62 is about.

- **`GET /{index}` has no `creation_date` and no `version.created`.** The descriptor does not record when
  an index was created, and adding the field is a change to the metadata format rather than to a handler.
- **`GET /_alias/{name}` keeps its `{alias, indices[]}` shape.** Core's is keyed by index name, which this
  shell's own callers do not parse; the index-scoped spelling answers core's shape.
- **`_resolve/index` does not list an index's aliases**, and `GET /{index}/_alias` is refused: both need the
  reverse lookup this design does not keep.
- **`_rank_eval`, search pipelines, search templates, data streams, stored scripts** stay refused, each with
  the reason it always had or a truer one.
- **No comparison was run against a live vendor endpoint.** The vendor claims are their documentation as
  fetched on 2026-09-04; the client claims are the clients' own sources on their main branches.

M61 is done: 479 tests, 21 of them new, and every path in the spec answers for itself.
