# M47 (finished) — API compatibility that costs nothing architecturally

Grounded in a real audit (an agent read the actual handler source, not memory) rather than assumption:
this shell was never trying to be a drop-in for real OpenSearch — D2 says so explicitly — but a real audit
of the gap turned up a mix of two very different kinds of difference. Some are the deliberate, load-bearing
refusals D2 and §6.3 exist for: conditional writes (no version model), non-prefix wildcards (no listing on
a request path), a cluster-wide surface (no cluster-wide state). Those are correct and untouched. The rest
were shell-specific inventions with no reason to differ at all — a field named `searched` where real
OpenSearch calls the identical count `successful`, a response missing `took` that costs one
`System.nanoTime()` call to add, an error rendered as a bare string where every uncaught exception on the
same surface already rendered the real nested object. This milestone closes the second kind and leaves the
first exactly alone.

## The line, stated precisely

Every fix here is a response-shape change: a field renamed, a field added from a value already computed,
an envelope restructured to match what core's own exception renderer already produces elsewhere on this
same surface. **Nothing here adds a new object-store request, a new listing, or a new piece of state a node
has to hold.** That is the test each fix had to pass before it qualified for this milestone rather than
being deferred as a real feature (see "What this deliberately still does not do," below, for the ones that
didn't pass it).

## The one fix with the widest reach: the error envelope

Every deliberate refusal on this surface — every 501, every 400 naming why — went through one shared
helper, `IndexAdminHandler.error(channel, status, type, reason)`, and it rendered `{"error": "<type>",
"reason": ..., "status": N}`. Meanwhile every *uncaught* exception on the identical surface already
rendered correctly: `new BytesRestResponse(channel, e)` is core's own `OpenSearchException` renderer, and it
has always produced `{"error": {"root_cause": [...], "type": ..., "reason": ...}, "status": N}` — `error` an
object, not a string. A client that parsed errors structurally rather than only checking the HTTP status
broke on precisely the responses that were trying hardest to explain themselves, and worked by accident on
the ones that weren't. One function fixed, and every deliberate refusal across every handler renders the
real shape now — the reason this is called out first: it is the single highest-leverage change in this
milestone, touching every error response on the entire surface at once.

## Search response

- **`took`** — measured with `System.nanoTime()` around the fan-out only, the same boundary a classic
  node's own `took` uses (parsing and validation happen first and are not counted).
- **`timed_out`** — always `false`, stated as an honest constant rather than omitted: nothing on this path
  enforces a search timeout yet, so there is nothing that could make this `true`, and a caller checking the
  field before trusting a result deserves a real answer rather than an absent key that happens to read as
  falsy.
- **`_shards`** — renamed to real OpenSearch's own field names. `searched` is gone; `successful` is what it
  always meant. `unreachable` is gone; `failed` is what it always meant. `skipped` (a count, always `0`
  here — nothing on this path skips a shard the way a throttled search elsewhere can) is added new.
- **The shell-specific list of index *names*** dropped by `ignore_unavailable` — which has no real-OpenSearch
  equivalent at all, since `_shards.skipped` there is a shard *count*, a different concept — is renamed from
  `skipped` to `skipped_indices` so it no longer collides with the real field of the same name that now
  always exists.
- **`hits.total.relation`** — always `"eq"`, because `trackTotalHits(true)` was already forced on every
  search this handler runs; `"value"` was never a lower bound, so `"relation"` never had anything else to
  say. Real OpenSearch reports `"gte"` once a search stops counting past a configured ceiling — nothing here
  has that ceiling yet, so `"eq"` is the honest constant, the same shape `timed_out` takes.
- **`hits.max_score`** — computed from the hits already being rendered; omitted when there are none, matching
  real OpenSearch's own omission in that case.
- **`complete`** stays. It has no real-OpenSearch equivalent — a classic coordinating node always fans out
  to every shard it can resolve, so "how much of the index answered" is not a question it ever has to
  answer — and this shell's own architecture (a node serving only the shards it holds, or reachable through
  placement) makes it a real, load-bearing question `_shards.total`/`_shards.successful` alone cannot
  answer honestly. Kept on purpose, not an oversight.

## Write responses

`PUT`/`POST`/`DELETE /{index}/_doc/{id}`, `POST /{index}/_update/{id}`, and each successful item of
`POST /_bulk` now report `_shards: {total: 1, successful: 1, failed: 0}` — always these exact numbers,
since every one of these operations touches exactly the one shard it was routed to. `PUT /{index}` now also
reports `shards_acknowledged: true` alongside the existing `acknowledged`, real OpenSearch's second flag
distinguishing "the descriptor exists" from "every shard is allocated and ready" — the two are always true
together here, since creation is one put-if-absent, not a routing decision that can lag it.

## Cluster settings

`PUT /_cluster/settings` was missing `acknowledged` entirely — a plain oversight, not a design decision.
Added, and added **only** to the write path: real OpenSearch's own `GET /_cluster/settings` has no such
field, and inventing one there would be a value nothing backs.

## What this deliberately still does not do

Every one of these was considered and set aside because closing it would mean building a real feature, not
adjusting a response shape — the line this milestone stays on the right side of, stated in the intro.

- **`_version`, `_seq_no`, `_primary_term` on any document response.** This shell has no version model at
  all — `WalRecord` records document state, not history, which is the entire reason conditional writes are
  refused rather than silently dropped. Adding these fields without real semantics behind them would be
  worse than omitting them: a client that stored a returned `_seq_no` and later sent it back as
  `if_seq_no`, expecting the compare-and-swap it implies, would get the same 501 refusal that exists today —
  but only after being handed a number that looked load-bearing and was not. A real per-shard monotonic
  counter is architecturally cheap to add (an O(1) increment per write, no listing, no cross-node state) and
  is the natural next step if conditional writes themselves are ever wanted — but that is a real feature
  with its own scope, not a shape fix, and is not part of this milestone.
- **"created" vs. "updated" as an honest distinction**, on both the single-document write path and bulk's
  `index` action. Both always report `"created"`, even when overwriting an existing document. Telling the
  two apart honestly needs the write path to know whether the document existed before this write — not free
  the way every fix above was, and deferred for the same reason `_seq_no` was.
- **Index create's real envelope** (`{"mappings": ..., "settings": ..., "aliases": ...}`, real
  `settings.index.number_of_shards`). D2 names this one directly already — "the classic create-body
  envelope was never an open question." Accepting it would mean parsing and honouring a settings object this
  shell has no settings service behind (`ClusterConfig`'s own scope note: "a store, not a settings
  service"), which is a real feature, not a shape fix.
- **Alias and point-in-time path/body shapes.** Real OpenSearch's alias surface (`POST /_aliases` with an
  actions array) and PIT surface (`/{index}/_search/point_in_time`) are different enough in shape from this
  shell's own that matching them would mean redesigning working, tested endpoints for naming symmetry alone
  — not in scope here.
- **Endpoints that do not exist at all** (`_count`, `_msearch`, `scroll`, `_mapping`, `_settings`,
  `_reindex`, `_update_by_query`, templates, `_tasks`, auto-id `POST /{index}/_doc`, bare `GET /_search`) —
  unaffected by this milestone; each is either genuinely out of scope or a future addition to the allowlist
  D2 describes, not a shape fix on something that already exists.

## Canaries

- **143 — the error envelope reverted to a flat string.** Caught by
  `testDeliberateRefusalsUseTheRealErrorEnvelope`.
- **144 — `_shards` omitted from a document write response.** Caught by
  `testDocumentOperationsReportShards`.
- **145 — `hits.total.relation` omitted.** Caught by `testSearchResponseCarriesRealOpenSearchFields`.
- **146 — `shards_acknowledged` omitted from index creation.** Caught by
  `testIndexCreateReportsShardsAcknowledged`.
- **147 — `acknowledged` rendered unconditionally, including on `GET /_cluster/settings`.** Caught by
  `testClusterSettingsAcknowledgedIsWriteOnly`.

Five planted, five caught. Every field-name rename (`_shards.searched`→`successful`,
`_shards.unreachable`→`failed`, the `skipped`→`skipped_indices` collision) was additionally caught by the
**existing** regression suite once the correct escaped-quote grep pattern was used to find every reference —
five pre-existing tests across four files (`ServerlessDataPathTests`, `ServerlessFleetTests`,
`ServerlessMultiIndexSearchTests`, `ServerlessAuthorizationTests`) had to be updated to the new field names,
which is itself evidence the rename was real and not merely additive.

M47 is done.
