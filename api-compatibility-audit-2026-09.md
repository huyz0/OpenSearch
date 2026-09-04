# API compatibility audit, September 2026 — standard OpenSearch, AWS OpenSearch Serverless, Elastic Cloud Serverless

A fresh four-way comparison of the shell's REST surface, taken after M60. Unlike the earlier comparisons this one
goes below the route table: every handler was read for which parameters it honours, refuses, silently drops, or
rejects as "unrecognized parameter", and every response was compared to the shape core's own `Rest*Action`
renders. Every claim below cites the line it was read from. Nothing was run against a live vendor endpoint.

**Ground truth used**

- Standard OpenSearch: `rest-api-spec/src/main/resources/rest-api-spec/api/*.json` in this repo (165 APIs),
  diffed programmatically against every `Route(...)` in `serverless/shell/.../rest/*.java` and every refusal
  path in `ServerlessNode.java:806-1013`; plus core's handlers under `server/src/main/java/org/opensearch/rest/`.
- AWS OpenSearch Serverless: the supported-operations table at
  `docs.aws.amazon.com/opensearch-service/latest/developerguide/serverless-genref.html`, fetched 2026-09-04.
- Elastic Cloud Serverless: `elastic.co/docs/deploy-manage/deploy/elastic-cloud/differences-from-other-elasticsearch-offerings`,
  fetched 2026-09-04.
- Clients: `opensearch-java` `SearchRequest.applyQueryParameters` and `PingRequest._ENDPOINT` (main branch);
  OpenSearch Dashboards `get_default_search_params.ts` (main branch). Fetched 2026-09-04.

> **Status, after M61:** every finding in sections A–E below is closed, and each closure is pinned by a test
> in `ServerlessVendorCompatibilityTests` or `ServerlessSpecCoverageTests`. G1 (the spec-driven test) exists;
> G2 (a shared parameter-policy helper) is done by convention rather than by a helper — each handler sorts its
> parameters into honoured, hint and refused at the top of `prepareRequest`, with the reason beside it. What is
> deliberately left after M61 is listed at the end of [`m61-api-compatibility-notes.md`](m61-api-compatibility-notes.md),
> and was then taken in full by [`m62-remaining-compatibility-notes.md`](m62-remaining-compatibility-notes.md):
> creation metadata, core's alias shapes and the reverse lookup, stored scripts, search templates, rank-eval,
> search pipelines, rollover and data streams are all served.

**Verdict.** The route table is now close to both vendors — M47–M60 did that work. What the deeper read found
is different in kind: **the official Java client and OpenSearch Dashboards cannot search this shell at all**,
because of two query parameters the shell rejects; **several requests are answered confidently and wrongly**
(a simulate that persists, a frozen search that is live, an alias filter that is dropped); and the M47 rule —
a shell-specific shape with no architectural reason to differ — still has around thirty instances left in
responses rather than routes. Everything in sections A–D costs nothing architecturally. Section E is the
short list of features both vendors ship and this does not; section F is what should stay refused.

---

## A. Client blockers — fix first

These are not shape drift. Each one stops a real client at the door.

| # | What | Evidence | Fix |
|---|---|---|---|
| A1 | **`typed_keys` is a 400.** `opensearch-java` puts `typed_keys=true` on every search unconditionally (`SearchRequest.applyQueryParameters`, main). No shell search handler consumes it, so `BaseRestHandler` rejects the request (`server/.../BaseRestHandler.java:117-128`). The official Java client cannot run a single search here. | `SearchHandler.java:94-98` consumes only `index`, `q`, `size`, `from`, `pit`, `ignore_unavailable`. Aggregations are rendered with `ToXContent.EMPTY_PARAMS` at `SearchHandler.java:791`, so even if consumed the `type#name` keys would not be produced (`InternalAggregation.java:364` reads the flag off params). | Consume `typed_keys`; pass `request` params (or a `MapParams` with `typed_keys`) into the aggregation render. Same in `MultiSearchHandler`. |
| A2 | **Dashboards' default search params are a 400.** `get_default_search_params.ts` (main) sends `max_concurrent_shard_requests`, `ignore_unavailable=true`, `track_total_hits=true` on every server-side search, plus `timeout` from `getShardTimeout` when configured. Only `ignore_unavailable` is consumed here. | `SearchHandler.java:94-98`. | Consume `track_total_hits` and honour it (see B2), consume `max_concurrent_shard_requests` and `timeout` as hints (the fan-out concurrency is fixed at `Fanout.java:50`; `timeout` is already forwarded per shard through `shallowCopy`). |
| A3 | **`HEAD /` is unrouted.** `opensearch-java` `PingRequest` is `HEAD /`; `opensearch-py` `ping()` is the same. Core registers both methods (`RestMainAction.java:63`). | `ServerlessRootHandler.java:55` registers `GET /` only. | Add the HEAD route. |
| A4 | **`GET /` is not the root document clients parse.** Core emits `cluster_uuid` and `version.{distribution,number,build_type,build_hash,build_date,build_snapshot,lucene_version,minimum_wire_compatibility_version,minimum_index_compatibility_version}` (`MainResponse.java:113-131`). The shell emits `version.number` only, adds `node_id` and `flavour`, and a different `tagline`. Dashboards reads `version.number` and `version.distribution` from this document. | `ServerlessRootHandler.java:61-72`. | Render `MainResponse`'s field set from `Build.CURRENT` and `Version.CURRENT`; keep `flavour` as an extra if wanted. |
| A5 | **A standard client cannot delete a point in time.** Core's `delete_pit` is `DELETE /_search/point_in_time` with body `{"pit_id":[...]}`, and `DELETE /_search/point_in_time/_all` (`RestDeletePitAction.java:57`, `DeletePitRequest.java:88-117`). The shell routes only `DELETE /_search/point_in_time/{id}`. The body form is a 400 "no handler"; `_all` matches `{id}` and returns **404 `no such point in time: _all`** having deleted nothing. `GET /_search/point_in_time/_all` (`RestGetAllPitsAction.java:88`) is a 405. | `PointInTimeHandler.java:81-91`, `:172-181`. | Route the body form and `_all`; the reaper already lists PIT records every tenth reconcile pass, so `_all` is a listing that exists. Render `{"pits":[{"successful","pit_id"}]}` (`DeletePitInfo.java:80-83`) and, on create, `{"pit_id","_shards":{...},"creation_time"}` (`CreatePitResponse.java:86-98`) instead of `{pit_id,index,shards,keep_alive_millis}` (`:161-169`). |
| A6 | **A search that quotes a PIT in the body runs live.** Core's spelling is a body block `"pit":{"id":...,"keep_alive":...}`; `SearchSourceBuilder.fromXContent` parses it into `pointInTimeBuilder` (`SearchSourceBuilder.java:1420`). The shell reads only its own `?pit=` query parameter and never `source.pointInTimeBuilder()`, so the frozen view is ignored and the search runs against the current shards while the caller pages what it believes is a consistent set. `pit_id` is also never echoed in the response (core: `SearchResponse.java:343-345`). | `SearchHandler.java:172` is the only `pit` read; grep for `pointInTimeBuilder` in `rest/` is empty. | Read `source.pointInTimeBuilder()`, treat it exactly as `?pit=`, and either honour the body `keep_alive` extension or refuse it explicitly; echo `pit_id`. |

## B. Confidently wrong answers — bugs regardless of compatibility

Each of these returns success (or a real-looking error) for something that did not happen. This is the failure
this surface has been arranged to avoid, and each is the same shape as the stale-reason pattern M50–M60 kept
finding: a check that exists on one path and was never copied to its sibling.

| # | What | Evidence |
|---|---|---|
| B1 | **`POST /_index_template/_simulate` persists a live index template named `_simulate`.** Only the `/{name}` forms are refused (`ServerlessNode.java:989-991`). The nameless form — the one in `indices.simulate_template.json` — matches `POST /_index_template/{name}` with `name="_simulate"`, passes the `index_patterns` check, and is stored with `{"acknowledged":true}`. `IndexAdminHandler.java:150-163` has an underscore backstop for index names; `TemplateHandler` has none. | `TemplateHandler.java:70`, `:149-153`, `:160-166`. |
| B2 | **`track_total_hits` is forced to `true` after the body is parsed**, so a body or Dashboards asking for `false` or a ceiling pays the full count and is told `"relation":"eq"`. M47 documented `eq` as honest because the flag "was already forced" — it is, but by this line, not by anything architectural. | `SearchHandler.java:161`; `MultiSearchHandler.java:267`; `:725`. |
| B3 | **A get through any installed `ActionFilter` loses its concurrency token.** When a filter is registered, `ActionGate.run` round-trips the answer through `view.read(...)` (`ActionGate.java:184-188` is the filter-free fast path only); the get view rebuilds the document with the 3-argument constructor, which defaults `_seq_no` to `-2`, `_primary_term` to `0` and `_version` to `-1`. With the security plugin installed, `GET` and `_mget` return tokens a following `if_seq_no` write will always lose. | `ShardOperations.java:372-378`; `ServerlessNode.java:2779-2787`; rendered `GetHandler.java:196-200`. |
| B4 | **Alias options are dropped on every spelling and acknowledged.** `filter`, `routing`, `index_routing`, `search_routing`, `is_write_index`, `is_hidden` are never read: `PUT /_alias/{name}` reads only `indices` from the body; `PUT /{index}/_alias/{name}` never parses the body at all (parsing is gated on `indexParam == null`); `POST /_aliases` reads only `alias` and `index` per action. A filtered alias becomes an unfiltered alias with `{"acknowledged":true}`. The array forms `indices`/`aliases` in `_aliases` produce `cannot alias [null]: no such index`. | `AliasHandler.java:94-107`, `:370-378`, `:413-429`, `:467-472`. |
| B5 | **Restore accepts 18 body fields and acts on 3.** The allowlist at `SnapshotHandler.java:78-97` turns an unknown key into a 400, which tells a caller the known keys are honoured. `restore()` reads `indices`, `rename_pattern`, `rename_replacement` and nothing else; `index_settings`, `partial`, `include_aliases`, `include_global_state`, `ignore_index_settings`, `settings`, and the rest are silently ignored. On create, `include_global_state` is read (`:172`) and echoed back (`:646`) without anything being captured. | `SnapshotHandler.java:504-560`. |
| B6 | **Bulk action lines drop `op_type`, `routing`, `pipeline`, `require_alias`, `retry_on_conflict`.** The parser matches a fixed key list with no else-branch. `{"index":{"_id":"1","op_type":"create"}}` is executed as an overwrite; create-ness comes only from the action name (`:571,583`). `pipeline` works on `_doc` (`DocumentHandler.java:106`) and not on `_bulk`, on the same node. | `BulkHandler.java:523-544`. |
| B7 | **Templates store `template.aliases`, `version`, `_meta`, `data_stream` and never read them.** They round-trip through `GET` verbatim so they look honoured; `TemplateResolver` reads only `settings`, `mappings`, `index_patterns`, `priority`, `composed_of`. | `TemplateHandler.java:160`, `:195-199`; `TemplateResolver.java:105,123-129,158,167`. |
| B8 | **The search renderer discards work the shard already did.** `highlight`, `explain`, `version`, `seq_no_primary_term`, named queries (`matched_queries`) and `inner_hits` are all applied by `SearchService` on the forwarded source, then the hand-rolled hit renderer writes only `_index`, `_id`, `_score`, `fields`, `sort`, `_source`. A search UI's highlighting silently vanishes. | `SearchHandler.java:737-783` vs `SearchHit.java:667-762`. |
| B9 | **`_shards.failures` never exists and `timed_out` is a literal `false`.** A shard that fails is counted in `failed` and logged (`SearchFanout.java:460`, `Fanout.java:84`) but its exception never reaches the caller. `timeout` is now forwarded per shard through `shallowCopy` (`SearchFanout.java:178-180`), so the comment at `:686-688` saying nothing enforces one is stale. | `SearchHandler.java:686-698`. |
| B10 | **`_msearch` skips the refusals `_search` makes.** `whatCannotBeMerged` runs in `SearchHandler.java:163` only; `MultiSearchHandler` calls `SearchFanout.run` directly, so `collapse`, `suggest`, `profile` become silent no-ops there and `search_after` gets no sort/from/arity validation. Per-line header keys other than `index` (`indices`, `preference`, `routing`, `search_type`, `ignore_unavailable`) are parsed into a map and never read. | `MultiSearchHandler.java:162`, `:233-244`, `:256-268`. |
| B11 | **`_field_caps` ignores its body.** `POST /{index}/_field_caps {"fields":["a"]}` returns every field: `request.content()` is never referenced, `fields` is read from the query string only. | `FieldCapabilitiesHandler.java:92`. |
| B12 | **Three misrouted paths give wrong errors.** `POST /_ingest/pipeline/_simulate` → 400 `invalid_pipeline` (matched `{id}`); `GET /_ingest/pipeline/_simulate` → 404 `no such pipeline: _simulate`; `POST /_snapshot/{repo}/_verify` → tries to create a snapshot named `_verify` and fails on `missing_indices`; `GET /_nodes/os` → 404 `no live node matches [os]`. | `PipelineHandler.java:63-64`, `:128-136`, `:154`; `SnapshotHandler.java:121`, `:160-169`; `NodesHandler.java:144-149`, `:208-214`. |
| B13 | **`GET /_list/indices/{p}?size=N` consumes `size` and ignores it.** | `ListIndicesHandler.java:84`. |
| B14 | **The local write path shows an action filter the pre-pipeline document and writes the post-pipeline one**; the forwarded path passes the post-pipeline source. A redaction filter inspects a different document from the one that lands, depending on which node the request reached. | `DocumentHandler.java:380-383` vs `:279-282`. |
| B15 | **PIT `keep_alive` is silently clamped to one hour.** Core requires the parameter; here a larger value is reduced with no error. | `PointInTimeHandler.java:58`, `:98-101`. |
| B16 | **`slice` in a search body always fails with a Lucene-internal message** ("cannot be used outside of a scroll context or PIT context") because `ShardQuery` opens a plain reader context even on the frozen path. Should be a 501 with a reason, like `collapse`. | `ShardQuery.java:174-196`; `SearchFanout.java:520-522,543-545`; `SearchService.java:1732-1735`. |

## C. M47-class shape fixes — a value already computed, rendered under the wrong name or not at all

| Endpoint | Shell | Core | Evidence |
|---|---|---|---|
| `_delete_by_query` | `{"matched","deleted"}` | `took, timed_out, total, deleted, batches, version_conflicts, noops, retries{bulk,search}, throttled_millis, requests_per_second, throttled_until_millis, failures[]` | `DeleteByQueryHandler.java:182-190`; `BulkByScrollTask.java:380-395`. `matched` is core's `total`. Also `query` is required here (`:108-117`) where core defaults to `match_all`. |
| `_update` on upsert-create | 200 | 201 | `UpdateHandler.java:288`; `DocWriteResponse.status()`. |
| `_update` | no `get` block for `_source` | `get{...}` | `UpdateHandler.java:272-285`; `_source*` params are 400. |
| `_mget` items | no `_version`, `_seq_no`, `_primary_term` | present | `MultiGetHandler.java:274-303` (single `GET` does emit them). |
| `GET /{index}/_source/{id}` on `_source=false` or missing doc | 404, empty `text/plain` body | 400 "fetching source can not be disabled" / 404 JSON error | `GetHandler.java:180-185`; `RestGetSourceAction.java:88-92,123-129`. |
| `_msearch` | `{responses, took}`; items lack `status` | `{took, responses[{...,status}]}` | `MultiSearchHandler.java:147-187`; `MultiSearchResponse.java:192-201`. |
| hit `_score` when NaN; `hits.max_score` when none | key omitted | `null` | `SearchHandler.java:727-735`, `:744-746`; `SearchHit.java:691-695`. |
| `_count` | no `terminated_early`, no `failures` | present when relevant | `SearchHandler.java:621-635`; `RestCountAction.java:118-131`. |
| `_analyze` tokens | no `positionLength`, no `attributes` | present | `AnalyzeHandler.java:270-294`; `AnalyzeAction.java:420,428`. |
| `_validate/query` | `explanations[].error`, `_shards` all 0, extra `validated` | `explanations[].explanation`, real counts | `ValidateAndResolveHandler.java:138-164`. |
| `_resolve/index` | index entries lack `aliases`, `data_stream` | present | `ValidateAndResolveHandler.java:174-206`. |
| `GET /{index}` settings | no `creation_date`, no `version.created`; `number_of_replicas` hard-coded `"0"` | present | `IndexAdminHandler.java:369-388`. |
| `_cluster/health` | no `discovered_cluster_manager`, no `active_shards_percent`; `yellow` never returned; unscoped call omits every shard counter | present | `ClusterHealthHandler.java:213`, `:319-388`. |
| `GET /_alias/{name}` | `{alias, indices[]}` | `{index:{aliases:{name:{}}}}` (the index-scoped form already does this) | `AliasHandler.java:534-544` vs `:318-329`. |
| write responses | extra `_shard`, `_node`, `durable`; no `forced_refresh` | `forced_refresh` when refresh forced | `DocumentHandler.java:446-474`; `BulkHandler.java:397-422`; `DocWriteResponse.java:335-343`. Extras are additive and harmless; `forced_refresh` is one boolean already known. |
| URL `size`/`from`/`q` precedence | body wins; `q` dropped when a body exists | URL wins | `SearchHandler.java:110`, `:155-160`; `RestSearchAction.java:276-287`. |
| `_cat` `format=yaml|cbor|smile` | plain text | that format | `CatTable.java:82`. |

## D. Silent drops and "unrecognized parameter" 400s that should be deliberate answers

The dominant mode across the surface is `BaseRestHandler`'s 400 for any parameter a handler did not consume. That
is honest but blunt: a client library sets these parameters from a spec, not by hand, and a 400 for `preference`
looks like a broken server rather than a design choice. Three buckets, each needing a different answer:

**Honour — the mechanism already exists, only the parameter is missing.**
`_search`: `sort`, `_source*`, `stored_fields`, `docvalue_fields`, `track_total_hits`, `track_scores`, `timeout`,
`terminate_after`, `explain`, `version`, `seq_no_primary_term`, `df`, `default_operator`, `analyzer`, `lenient`,
`rest_total_hits_as_int`, `typed_keys` (all parse into the same `SearchSourceBuilder` the body already fills —
`RestSearchAction.java:276-352` is the reference). `_count`: `min_score`, `terminate_after`, `df`,
`default_operator`. `_doc`/`_bulk`/`_update`: `refresh=wait_for` — every write here is visible after
`refresh=true`, so `wait_for` is satisfiable by the same refresh; today it is a 400 from `paramAsBoolean`
(`DocumentHandler.java:99`, `BulkHandler.java:107`, `UpdateHandler.java:113`, `DeleteByQueryHandler.java:73`;
`Booleans.java:126-134`). `_update`: `retry_on_conflict` (the handler's own javadoc at `UpdateHandler.java:33-36`
describes the retry loop it refuses to run), `_source*`. `GET`/`_mget`: `stored_fields`, `realtime` (the answer
already reports `realtime` as an output at `GetHandler.java:206`). `_explain`: `_source*`, `stored_fields` (the
`get` block). `_analyze`: `char_filter` as a query parameter (the body form is already checked). `_field_caps`:
the body. `_msearch`: `indices`, `ignore_unavailable` per line. `_validate/query`: `q`. Templates: `create=true`
(refuse the overwrite; today `PUT` is an unconditional upsert at `TemplateHandler.java:160`).

**Consume and ignore, with the reason stated once in the handler — hints that core itself may not honour.**
`preference`, `request_cache`, `batched_reduce_size`, `max_concurrent_shard_requests`, `pre_filter_shard_size`,
`allow_partial_search_results`, `search_type`, `ccs_minimize_roundtrips`, `master_timeout`,
`cluster_manager_timeout`, `timeout` on admin calls, `wait_for_active_shards` (there is one writer per shard; the
only legal value is `1`), `local`, `flat_settings` (or honour it: it is a rendering choice), `include_defaults`.
`_cluster/health` already does this for `wait_for_no_relocating_shards` (`ClusterHealthHandler.java:111-114`) and
says why; that is the pattern.

**Refuse with 501 and a reason — things whose absence changes the answer.**
`routing` everywhere (documents route by id; Elastic Serverless refuses custom routing too — this is a legitimate
vendor position, but today it is a 400 on `_doc` and silently dropped in `_bulk` and `_mget`), `op_type` on `_doc`
(or honour: `create` already exists via the `/_create/` path), `version_type` (its sibling `version` gets a
careful 501 at `DocumentHandler.java:114-130`; `version_type` gets "unrecognized parameter"), `scroll`,
`expand_wildcards` and `allow_no_indices` (the pattern rules here are different and documented), `slice`,
`indices_boost` (`ShardQuery.java:188` hardcodes `1.0f`), `conflicts`/`slices`/`wait_for_completion`/
`requests_per_second` on `_delete_by_query` (there is no task), `include_unmapped` (already done, keep),
`remove_index` in `_aliases` (already done, keep).

## E. Features both vendors ship and this shell does not — the M52 signal

M52 set the rule: two vendors on two engines treating something as non-optional is the strongest signal a
comparison produces. After M60 the list is short.

| Feature | AWS | Elastic | Here | Cost |
|---|---|---|---|---|
| `_cat/indices` and `_cat/aliases` | yes (`aoss:DescribeIndex`, `aoss:DescribeCollectionItems`) | yes — the two `_cat` endpoints it keeps | 501 (`ServerlessNode.java:951`, `:956`); `_list/indices/{prefix}*` serves the bounded version under a name no client knows | Serve `GET /_cat/indices/{pattern}` with the same bounded-prefix rule and cap `_list/indices` already applies (`ListIndicesHandler.java:118-121`); keep the bare form refused. `_cat/aliases` needs the reverse lookup this design does not have — keep refused, but `_cat/aliases/{name}` is one register read. |
| `_ingest/pipeline/_simulate` | yes | yes | misrouted (B12) | Pipelines are already compiled at `PUT` via `Pipeline.create` (`PipelineHandler.java:126`); simulate is `pipeline.execute(doc)` per document, no state. |
| `_analyze` with no index | yes | yes | 400 no handler | `TransportAnalyzeAction` handles `indexService == null` from the global `AnalysisRegistry`; the node already has one. |
| `_search/pipeline` | yes | n/a | 501 "not implemented here" | Same decision as painless (M57) and ingest-common (M58): `search-pipeline-common` is a module; the reason given is not architectural. Optional. |
| `_rank_eval` | yes | yes | 501 with a real reason | Leave; the reason at `ServerlessNode.java:996-1001` is a design position, not a stale one. |
| `_search/template`, `_render/template`, `_msearch/template` | Mustache ships as a plugin on AWS | yes | 400 no handler | `lang-mustache` is a module; same shape as M57. Optional. Should at least be a 501. |
| `_scripts/painless/_execute` | no | yes | 501 | Leave. |
| `_reindex`, `_update_by_query`, scroll | no | yes | 501 with reasons | Leave — AWS makes the same call. |
| `_data_stream` | no | yes (its lifecycle model) | 400 no handler | Refuse explicitly. |

## F. Refusals that are correct and should stay

`_cluster/state`, `_cluster/stats`, `_nodes/stats`, `_tasks`, `_cluster/reroute`, `_cluster/allocation/explain`,
`_cat/shards|segments|recovery|allocation|thread_pool|pending_tasks`, `_open`/`_close`, `_shrink`/`_split`/`_clone`,
`_rollover`, `_forcemerge`, `_flush`, `_refresh` (per-index), stored scripts, external `version`,
`number_of_replicas > 0`, static index settings, `transient` cluster settings, `_dangling`, `_remote/info`,
`_ilm`. Every one of these is refused by at least one of the two vendors as well, and each carries a reason that
is true of it specifically. Nothing in this audit argues with them.

## G. Two structural fixes so this class of gap stops recurring

1. **A spec-driven test.** The diff in this audit was produced by a forty-line script over
   `rest-api-spec/.../api/*.json`. Forty-one spec paths currently fall to core's default 400 "no handler found
   for uri" while a sibling path is an explicit 501 — `GET /_cat/indices/{index}` next to a refused
   `GET /_cat/indices`, `GET /_nodes/{id}/{metric}` next to a served `GET /_nodes/{id}`, `HEAD` on every
   `NotImplementedHandler` path (`NotImplementedHandler.java:56-62` registers four methods, not five). M50
   found this category by driving a node by hand; a test in `ServerlessApiCompatibilityTests` that walks every
   non-deprecated spec path and asserts "served, or 501 with a reason, never 400 no-handler" would have caught
   B1, B12 and A5 before they shipped. The full list is in the appendix.

2. **One parameter policy per handler instead of three accidental ones.** Today the same parameter is a 400 on
   one endpoint (`routing` on `_doc`), silently dropped on another (`routing` in `_bulk`) and a 501 on a third
   (`version` on `_doc`). A small helper — `Params.honour(...)`, `Params.hint(...)` with a reason,
   `Params.refuse(...)` with a reason — called at the top of each `prepareRequest` would make the bucket a
   decision written down, and would let the spec test above also assert that every spec parameter of a served
   API lands in one of the three buckets.

---

## Appendix — spec paths that answer core's default 400 today

Produced by diffing `rest-api-spec` against the shell's routes and refusals. `REFUSED` siblings are shown for
context; `UNROUTED` is the gap.

```
cat.*                  UNROUTED  GET /_cat/{aliases,allocation,count,fielddata,indices,recovery,segments,shards,snapshots,templates,thread_pool}/{x}
                       UNROUTED  GET /_cat/segment_replication[/{index}]
cluster.*              UNROUTED  GET /_cluster/state/{metric}[/{index}], GET /_cluster/stats/nodes/{id}, decommission/*, voting_config_exclusions, routing/awareness/*
dangling_indices       UNROUTED  DELETE|POST /_dangling/{uuid}
delete_pit             UNROUTED  DELETE /_search/point_in_time           (body form)
delete_all_pits        UNROUTED  DELETE /_search/point_in_time/_all      (matches {id} → 404)
get_all_pits           UNROUTED  GET /_search/point_in_time/_all         (405)
get_script_context     UNROUTED  GET /_script_context
get_script_languages   UNROUTED  GET /_script_language
indices.add_block      UNROUTED  PUT /{index}/_block/{block}
indices.analyze        UNROUTED  GET|POST /_analyze
indices.clear_cache    UNROUTED  POST /_cache/clear
indices.*data_stream*  UNROUTED  all
indices.delete_alias   UNROUTED  DELETE /{index}/_aliases/{name}
indices.put_alias      UNROUTED  PUT|POST /{index}/_aliases/{name}, PUT /{index}/_alias[es], POST /_alias/{name}, PUT|POST /_aliases/{name}
indices.exists_alias   UNROUTED  HEAD /_alias/{name}
indices.get_alias      UNROUTED  GET /{index}/_alias
indices.*_template     UNROUTED  GET|PUT|POST|DELETE|HEAD /_template[/{name}]   (legacy templates)
indices.flush/refresh/forcemerge/recovery/segments/shard_stores/upgrade  UNROUTED  the index-less forms
indices.get_field_mapping  UNROUTED  GET [/{index}]/_mapping/field/{fields}
indices.get_mapping    UNROUTED  GET /_mapping
indices.get_settings   UNROUTED  GET /_settings[/{name}], GET /{index}/_settings/{name}
indices.put_settings   UNROUTED  PUT /_settings
indices.rollover       UNROUTED  POST /{alias}/_rollover/{new_index}
indices.simulate_template  UNROUTED  POST /_index_template/_simulate      (persists a template — B1)
indices.stats          UNROUTED  GET [/{index}]/_stats/{metric}
indices.validate_query UNROUTED  GET|POST /_validate/query
ingest.processor_grok  UNROUTED  GET /_ingest/processor/grok
ingest.simulate        UNROUTED  GET|POST /_ingest/pipeline[/{id}]/_simulate   (nameless form misroutes — B12)
msearch_template, search_template, render_search_template  UNROUTED  all
mtermvectors           UNROUTED  all
nodes.*                UNROUTED  GET /_nodes/{id}/{metric}, hot_threads, usage, reload_secure_settings, stats/{metric}...
ping                   UNROUTED  HEAD /                                  (A3)
put_script             UNROUTED  PUT|POST /_scripts/{id}/{context}
rank_eval              UNROUTED  GET|POST /_rank_eval
reindex/update_by_query/delete_by_query _rethrottle  UNROUTED
remote_store.*         UNROUTED  all
search_pipeline.get    UNROUTED  GET /_search/pipeline
search_shards          UNROUTED  all
snapshot.cleanup/clone/status/verify  UNROUTED  all   (verify misroutes — B12)
tasks.cancel/get       UNROUTED  POST /_tasks[/{id}]/_cancel, GET /_tasks/{id}
termvectors            UNROUTED  GET|POST /{index}/_termvectors          (no id)
wlm_stats_list         UNROUTED  GET /_list/wlm_stats
```

Shell-only routes with no spec entry (kept on purpose, per M52/M59): `POST /{index}/_pit`, `DELETE /_pit/{id}`,
`DELETE /_search/point_in_time/{id}`, `PUT|GET|DELETE /_alias/{name}` (name-scoped body form),
`/_serverless/*`, `GET /_list/indices/{index}`, `POST /{index}/_settings`, `PUT|POST /{index}/_mappings`,
`POST /_ingest/pipeline/{id}`.
