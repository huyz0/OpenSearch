# The serverless shell: where it actually is

Per-milestone notes (`m10`…`m64`) recorded what was true when each was written. They live on
`feature/serverlessnode`, which this branch supersedes. **This document supersedes all of them.**
If a milestone note and this disagree, this is right.

**Ported from `feature/serverlessnode`, where it was `serverless-status.md`.** That branch was
built on a fork of OpenSearch; this one is built directly on `opensearch-project/OpenSearch`
`main`. Three kinds of claim below were re-checked against that move and corrected: what core
costs, what the tests are, and the alias-option gap. Everything else is carried over as it was
written and was **not** re-verified line by line -- treat an unfamiliar claim here as a lead, not
as a warranty.

## What works

**Documents.** Write, get, delete, `_bulk`, `_mget`, with `_source` filtering on reads. The routes an
OpenSearch client actually calls: `PUT|POST /{index}/_doc/{id}`, `POST /{index}/_doc` for a generated id, and
`PUT|POST /{index}/_create/{id}` for create-if-absent — which is one constant on the write path's own engine
call (`MATCH_DELETED`), so the comparison happens under the per-document lock the engine already holds rather
than as a read followed by a write. `_bulk` does `create` and per-item `if_seq_no`/`if_primary_term` too, and
reports a lost condition as a 409 (`m50-api-compatibility-notes.md` (on `feature/serverlessnode`)). Every write, get
and bulk item reports `_seq_no`, `_primary_term` and `_version`, and **conditional writes work**:
`if_seq_no`/`if_primary_term` on a write, a delete or an `_update` are passed to the engine, which performs
the compare-and-swap against its own live version map; a lost race is a 409. The numbers survive a
failover — the write-ahead log records each operation's sequence identity and a successor replays it as
that operation rather than as a new one, so a token minted before a writer died is still honoured by its
successor (`m48-sequence-numbers-notes.md` (on `feature/serverlessnode`)). `version` (external
versioning) stays refused: it asks this system to order writes by a number the caller maintains and this
system does not keep, which is a different thing from comparing against one the engine assigned. `_update` does a
partial-document merge (core's own recursive merge, so a nested object is not silently overwritten), with
`doc_as_upsert` and `upsert` for a missing document and `detect_noop` skipping a write that changes
nothing — read then written back, not a transaction on its own: nothing holds the document still between
the two, so an unconditional update can still lose a race. The caller can close that window itself now, by
passing `if_seq_no`/`if_primary_term` and retrying on a 409 — which is the compare-and-swap classic
OpenSearch's own `_update` performs internally. `_delete_by_query` removes every document a query
matches — evaluated once, against a frozen point-in-time view, then walked one shard at a time in native
Lucene `_doc` order (always available, no fielddata) and deleted through the ordinary write path. A write
acknowledged but not yet published when the operation starts is not touched by it, the same boundary a
plain `search_after` export of matching ids would have. Routing is resolved
once per request, so a multi-get's object-store cost is the shards it touches and not the documents it asks
for: ten documents cost 4 requests and twenty cost 4, against 30 for the same ten fetched one at a time. A write is durable in a write-ahead log before it is
acknowledged, and replayed by a successor. **A single write no longer costs an object-store PUT of its
own**: concurrent writes to one shard share the PUT already in flight for it, so a shard's write cost is
set by the store's write latency rather than by its write rate — 96 documents from 16 writers cost 12 PUTs
against MinIO where one writer pays 96. Nothing is acknowledged before the blob carrying it lands, so the
durability contract is the one it was. **A writer that has lost its shard but not yet noticed is fenced
out of that log**: it refuses to write past the lease deadline it last published for itself, which needs no
I/O and so is checked on every write; and a successor seals the log at takeover, durably, so nothing
appended after ownership moved is ever replayed — by that successor or by any node after it
(`m49-fencing-notes.md` (on `feature/serverlessnode`)). A get is routed to the shard's owner so it sees writes a search
cannot yet; an unowned shard is served from its published commit, so a get works against an index that has
scaled to zero.

**Search.** The full query DSL, sorting, aggregations, `search_after`, `_source` filtering, several
indices named in one request, and **prefix patterns** (`logs-*`) — resolved by a paged listing, a thousand
names per request, each page resuming where the last stopped, so its cost is set by a cap (10,000 by default)
rather than by the population. A pattern matching more than the cap is refused rather
than truncated, because an answer that stopped at a limit looks exactly like a complete one. Shards fan out
in a sliding window (`serverless.search.fanout.concurrency`, 16); a shard with no owner whose published
**pruning digest** rules the query out is skipped (`_shards.skipped`); and a search that would open more cold
shards than `serverless.search.max_activations_per_query` (1,024) is refused unless it passes
`allow_partial_activation=true`. The window is cut once over everything; coverage
(`_shards` and `complete`) is reported on every answer, and a search *no* shard could answer is an error
rather than an empty result. `ignore_unavailable` turns a named index that cannot be reached into a
`skipped` entry rather than a failure, and only when the caller asked for that.

**API compatibility.** Measured below the route table, not just at it (`m61-api-compatibility-notes.md` (on `feature/serverlessnode`)).
The official Java client and OpenSearch Dashboards can search this shell: search parameters go through core's
own `parseSearchRequest`, the response is a real `SearchResponse` rendered by core, so `typed_keys`,
`rest_total_hits_as_int`, `highlight`, `_explanation`, `_shards.failures`, `timed_out` and `pit_id` are what
core would answer. Every path in `rest-api-spec` is served or refused with a reason -- `ServerlessSpecCoverageTests`
walks the spec against a running node and fails on core's default 400 or a 405. Every handler sorts its
parameters into honoured, consumed-as-a-hint, or refused-with-a-reason, and nothing is accepted and silently
dropped: alias options, restore fields, bulk action-line fields and template blocks that cannot be kept are
refused, not acknowledged. `refresh=wait_for`, `op_type`, `retry_on_conflict`, a body `pit` block, the
standard point-in-time delete and list, `_ingest/pipeline/_simulate`, `_analyze` without an index and
`_cat/indices/{prefix}*` are served. M62 (`m62-remaining-compatibility-notes.md` (on `feature/serverlessnode`))
took what M61 left: an index records when and by what it was created; `GET /_alias/{name}` is core's shape
and `GET /{index}/_alias` answers the reverse question from a hint each descriptor carries, verified against
the alias records it names; **stored scripts** live in a register with a change marker and resolve through
one overridden `ScriptService` method; **search templates** (`lang-mustache`), **ranking evaluation**
(`rank-eval`) and **search pipelines** (`search-pipeline-common`) are served through the modules chosen by
name, the way painless and ingest-common were; **rollover** is one compare-and-swap of the alias after the
index is created, with `max_age` and `max_docs` evaluated against what is recorded; **data streams** are
aliases with a generation, written through to their newest `.ds-` backing index and rolled over in place.

**Point in time.** `POST /{index}/_pit` freezes each shard's commit into a record and returns an id; a
search quoting it reads that commit however far the writer has moved on, which is what makes paging with
`search_after` return a consistent result set rather than a moving one. The garbage collector treats a
live view's blobs as referenced — without that the sweep would collect a caller's commit halfway through
their paging. The keep-alive is absolute rather than sliding, and a released or expired view answers 404
rather than emptily. A search over a view fans out by the same **reader placement** a live search uses —
the coordinating node tries the shard locally, then the placement-preferred peer over the wire, then opens
it itself as the guaranteed fallback — rather than opening every shard unconditionally on whichever node
happened to answer the request.

**Aliases.** A name that stands for some indices, created in the same register namespace as an index
descriptor — so an index and an alias cannot take the same name, and resolving a name is one read rather
than a miss on one namespace followed by a lookup in another. One level; an alias does not name another
alias.

**Indices.** Create, describe, delete, alias. Storage is keyed by index uuid, so a name reused after a delete
cannot inherit the previous index's data. Deletion reclaims the bytes, and a sweep collects shard
containers no index owns.

**Cluster config.** `GET/PUT /_cluster/settings` — §9.3's `/cluster/config` register, the one piece of
`/_cluster/*` that is not refused, because it genuinely is one CAS register rather than genuinely
cluster-wide state. Persistent settings only (`transient` is refused if non-empty — no cluster manager
means no restart to reset one on), scalar values only (a list cannot be told apart from an explicit
removal through `Settings`'s public surface without risking silent data loss), and no validation against
a settings registry: this is a store, not a settings service, the same as an index's mapping JSON is
stored without checking it against Lucene's field types.

**Snapshot and restore.** `PUT /_snapshot/{repo}` registers a repository — a namespace within this
deployment's own object store, not a distinct storage backend, because there is only one object store to
put a snapshot in — upserting on a repeat call, matching real OpenSearch. `remote_store_index_shallow_copy`
is a repository setting, not a per-snapshot choice, verified against real OpenSearch's own source: standard
(the default, matching real OpenSearch's own default) copies each shard's blobs into the repository's own
storage at capture time, real cost, in exchange for a snapshot whose lifecycle owes nothing to the index it
was taken from; shallow references the live shard's own blobs, free to take, and depends on that shard's
storage continuing to exist — the garbage collector and index deletion alike now protect a live shallow
snapshot's blobs, keyed by uuid rather than name so an unrelated later index reusing a deleted one's name is
never mistaken for what a snapshot actually captured. `PUT /_snapshot/{repo}/{name}` needs an explicit
`indices` list (comma string or array) — this surface does not enumerate a deployment's population any more
than `_search` across several indices does. Restoring always creates a new index, since shard storage is
keyed by an index's own uuid; `rename_pattern`/`rename_replacement` is the real Java-regex mechanism, and a
restore refuses the whole request rather than half of it if a target name collides. Every request and
response shape — `GET /_snapshot` with no name, `wait_for_completion`, the `SnapshotInfo`/`RestoreInfo`
envelopes — matches real OpenSearch's own `_snapshot` API; every operation is synchronous regardless of
`wait_for_completion`, since there is no background task registry to run one against. Classic OpenSearch's
repository-plugin API surface (`RepositoriesService`) is untouched and still yields null to a plugin — this
is a shell-native surface, not a workaround. Not yet built: a repository genuinely on a distinct backend
(S3 while the deployment runs on GCS, say) — shallow-vs-standard turned out orthogonal to that question and
didn't require answering it.

**Scaling to zero and back.** A node takes a shard by compare-and-swap on a register, renews a lease,
publishes commits to the object store, and releases shards that have gone idle. There is no cluster
manager, no consensus process, and no cluster-wide state. **What an open shard costs is measured**: ~67 KB
of heap and 3 file descriptors for an open writer shard, ~50 KB for a reader (no translog, no indexing
buffers) — flat per-shard across two independent 30-shard batches, not growing with how many are already
open. Lighter than core's own measured 150,888 B for one *gated* (shard-closed) index, because this design
carries no `ClusterState`/`IndexMetadata` graph behind an open shard at all.

**Maintenance the deployment does for itself.** A reconcile pass lets go of readers whose commit has been
superseded, so nothing serves a stale commit indefinitely; closes the shards it is holding for a view whose record has gone, every pass and
for nothing when it holds none; reaps expired point-in-time records every tenth pass, because that half is
a listing across the whole deployment and paying one every thirty seconds per node to be told a feature is
unused is a bill rather than a safeguard; and sweeps the garbage left by what it has just published. The sweep waits until a
blob has been unreferenced across two passes, because a reader may still be reading it — the grace is what
makes running the collector on a schedule different from running it by hand. Eviction under cap pressure
has **hysteresis** — a full node clears down to a floor below the cap (5% by default) on the first pass a
burst of arrivals needs, rather than one slot per arrival, so the O(open shards) victim search runs once
per burst rather than once per arrival — and a **per-index pin**, exempting an index's shards from both
cap-pressure eviction and idle release on the node that pins it. Pinning is a node-local runtime setting,
the same shape as the shard cap or the idle timeout, not a durable deployment-wide record; nothing has
asked for the durable version yet. See `m13-hysteresis-notes.md` (on `feature/serverlessnode`).

**Identity across a hop.** A thread-context header a plugin sets travels with a forwarded write and
arrives on the node that owns the shard — core's own mechanism, the one the security plugin uses to move an
authenticated user between nodes. What does not happen is the receiving node running the action filters
again: the decision belongs to the node the request reached.

**TLS on the transport, from a plugin.** A plugin supplies a `SecureSettingsFactory`, the shell passes it
to the network module, and core's own `SecureNetty4Transport` carries the traffic — proved by a write
forwarded between two nodes over a real handshake. The shell had been passing an empty collection here, so
a plugin naming the secure transport was told the type did not exist: TLS through a plugin was impossible
rather than undemonstrated. Requiring a client certificate is a deployment's decision expressed
entirely in that provider, and it works: a node presenting one is served and a node presenting none is
refused. What still does not travel is the *caller's* identity — the peer is authenticated as a node, and
a filter on the receiving side would not know who the request came from.

**Plugins.** Installed from `plugins/` through core's own loader, or named on the classpath by
configuration. `createComponents`, REST handlers, the request wrapper, `onNodeStarted`, analysis, mappers,
search plugins, network plugins and their transport interceptors, system-index declarations, and
`ActionFilter`s all work. A plugin's **own transport actions** run too — built by resolving each
constructor against what the node can supply, since Guice is not on offer here — so an action is reachable
from the plugin's REST handler, from its own code through the client, and from another node. The `Client` and the `NodeClient` a handler receives are the same allowlist.

**Authentication and authorization.** The shell's own authentication is a plugin, depending on `:server`
and not on the shell. HTTP Basic at the request seam, accounts in a system index unreachable over REST,
salted PBKDF2, one configured account in the keystore checked before the store so an unreadable object
store is not a locked door. A separate plugin's `ActionFilter`s can refuse by action name and caller —
proved by a reader being denied a write an administrator is allowed — and can *rewrite* what a search or a
get returns, which is what field- and document-level security are made of: a filter that removes a field
from every hit removes it from `_search`, `_doc/{id}` and `_mget` alike, and one that drops a hit outright
drops it from the count as well. Coverage is not the filter's to change — a filter that dropped a hit has
not made a shard unreachable, and a redaction must not be able to masquerade as a partial answer.

**Bounds.** Real circuit breakers, so an aggregation past the limit is refused and the node keeps serving;
and core's indexing pressure on the write paths, so a batch larger than the node's budget is rejected with
a 429 rather than accepted into memory it does not have. A node at its shard limit evicts its least
recently used idle shard to make room rather than refusing, and `GET /_serverless/stats` reports the
counters those refusals are decided from — breakers, in-flight write bytes, and what the node is holding.
`?nodes=_all` (or a list of ids or names) asks every live node over the authenticated transport and answers
with each node's own document under its id, plus `_nodes.total/successful/failed` and a `failures` array
naming the ones that did not answer and why — the nodes worth asking about are the ones most likely to time
out, so a fan-out that quietly returned only what it could reach would let an operator read "everything is
fine" off a report missing the node that is not. `/_nodes/stats` stays refused, for a reason that is now
about schema rather than reach: it means core's indices/os/jvm/fs/transport/thread_pool numbers, which this
shell does not produce.

**Analysis and batched search.** `GET|POST /{index}/_analyze` shows what the analyzer does to a string — a
field's own analyzer, a named one, or the index default — read from the mapping, so a dormant index still
answers. A custom tokenizer chain is refused rather than substituted: a caller debugging analysis must not be
shown the analysis of something else. `_msearch` runs several searches in one request, each answered in the
body `_search` would have produced, with one search's failure kept to its own slot. The saving is round trips,
not shard work (`m55-analyze-msearch-notes.md` (on `feature/serverlessnode`)).

**Discovery.** `GET /{index}/_field_caps` reports what a client can query — field types, and whether each is
searchable and aggregatable — read from the mapping rather than from a shard, so it answers for an index whose
shards are all dormant. Where two indices map a field differently both types come back, each attributed to the
indices using it. `GET /_list/indices/{prefix}*` (and the bare form, meaning every index) **pages**, with
core's `size` (500 by default, 5,000 at most) and `next_token`; each page resumes in the store's own listing,
so page two hundred costs what page one did. `_cat/indices`, whose contract is the whole answer, still
**refuses a prefix matching more than the cap rather than truncating** -- which is why the walk is `_list`
and not `_cat`: OpenSearch added `_list` for exactly this reason (`m53-discovery-notes.md` (on
`feature/serverlessnode`)).

**Ingest pipelines, and dynamic mapping.** `_ingest/pipeline/{id}` stores a pipeline in a register and
compiles it at `PUT`, so one naming a processor this deployment does not have is refused where the operator is
standing rather than at the first write that uses it. `?pipeline=` runs it before the document is routed or
written, and a dropped document is reported as dropped. `index.default_pipeline` and `index.final_pipeline`
are honoured too, with core's precedence: a pipeline named on the request displaces the index's default and
never its final, `_none` opts out of the default alone, and the default runs before the final. They are
resolved against the index actually written to, so a data stream write uses its backing index's settings.
Separately: a document carrying a field the mapping
does not describe now grows the mapping and is indexed — before M58 an index created without an explicit
mapping could not accept a single document (`m58-ingest-notes.md` (on `feature/serverlessnode`)).

**Scripts.** Painless runs here: script queries, `script_score`, `script_fields`, scripted aggregations and
scripted updates including `ctx.op` for noop and delete. The engine is chosen by name and registered directly,
the same way this shell has always chosen its transport — "no modules are loaded" was about discovery from
disk, not about the code in `modules/`. Stored scripts were refused at M57 because `ScriptService` reads them
out of cluster metadata; M62 overrode the one method that does so, and they live in a register now (see
above and `m62-remaining-compatibility-notes.md` (on `feature/serverlessnode`)).

**Why a document scored what it did.** `GET|POST /{index}/_explain/{id}` accounts for one document's score
against one query, in Lucene's own words. It is served because it is not a fan-out: an explain names a
document, a named document lives on one shard, so it is one term lookup and one `explain` call — **cheaper
than the search whose score it explains**, which has to ask every shard. A document that exists and does not
match is a 200 saying why, not a 404; that is the half a get cannot answer. The response says which copy
scored it, because a score is a function of the whole shard's term and document frequencies and a published
commit's are not a live writer's (`m60-explain-notes.md` (on `feature/serverlessnode`)).

**Aliases, in OpenSearch's own spellings.** `PUT|GET|HEAD|DELETE /{index}/_alias/{name}` alongside the
name-scoped form this shell shipped with. `POST /_aliases` serves the atomic move it exists for — actions on
one alias are one register, so the whole move is one compare-and-swap — and refuses actions spanning several,
which would be several registers with no transaction over them. `_validate/query` parses a query and says that
is all it did; `_resolve/index` reports which names are indices and which are aliases
(`m59-alias-shapes-notes.md` (on `feature/serverlessnode`)).

**Templates.** `_index_template` and `_component_template` store in registers like every index descriptor, and
a new index inherits from the highest-priority template whose patterns match — its components merged in order,
its own block on top, and whatever the request said on top of that. A missing component fails the create
rather than being skipped, because an index quietly missing the settings a component was meant to contribute
shows up much later as a query matching nothing
(`m56-templates-notes.md` (on `feature/serverlessnode`)).

**Indices are mutable.** `PUT|POST /{index}/_settings` changes any *dynamic* setting on a live index and
refuses a static one, because classic changes those only on a closed index and there is no closed state here —
storing one would leave a value nothing acts on. `IndexScopedSettings` decides which is which, so there is no
second list to drift (`m54-mutable-settings-notes.md` (on `feature/serverlessnode`)).
`PUT|POST /{index}/_mapping` adds fields to an index that already exists — the merge
is core's own `MapperService`, so adding a field succeeds and changing a field's type is refused in core's own
words, and a shard already open and serving traffic picks the change up. The write is a compare-and-swap on
the descriptor, so two clients adding different fields both win
(`m52-mutable-mappings-notes.md` (on `feature/serverlessnode`)). `PUT /{index}` reads OpenSearch's own create envelope — `settings.number_of_shards` is honoured,
`mappings` is the mapping, and any other setting is carried into the descriptor and layered into
`IndexMetadata`, because the data plane under this shell is core's and understands them. The older bare-mapping
body with `?shards=` still works. `GET /{index}`, `/_mapping` and `/_settings` answer OpenSearch's shape, so
configuration can be read back; `HEAD /{index}` answers whether an index exists. `GET|POST /_search` with no
index searches everything, and `q=` is core's query-string parser rather than a lookalike.

**Cluster-shaped endpoints, where they can be answered honestly.** `GET /_nodes`, `/_nodes/{id}` and
`_cat/nodes` are projections of the lease registry — the address book write forwarding already routes through,
so a node reporting the fleet is reporting what it computes on every heartbeat. `GET /_cluster/health`,
`/_cluster/health/{index}` and `_cat/health` answer with **green redefined to mean what it can mean here**:
every shard asked about can be served. A shard nobody owns is *dormant*, not unassigned — it is the healthy
resting state, and reporting it as a fault would make `wait_for_status=green` block forever in a system where
shards activate on demand. `dormant_shards` carries the real information, `replication: "object-store"` says on
every answer that green does not mean copies exist, and the unscoped form reports `complete: false` rather than
counting shards it did not examine (`m51-cluster-endpoints-notes.md` (on `feature/serverlessnode`)).

**Nothing looks like a typo.** Every endpoint a client reaches for either answers or refuses with the reason
it is refused for. An index name beginning with an underscore is an API this shell does not implement and says
so, rather than being reported as a missing index — and the same backstop covers node selectors and repository
names. A genuinely missing index is still a 404.

M63 (`m63-review-fixes-notes.md` (on `feature/serverlessnode`)) took every finding of the September review
([`serverless-index-review-2026-09.md`](serverless-index-review-2026-09.md)): names are validated before they
become paths, the system-index guard checks the written target, transport is authenticated with a deployment
secret, generations never restart, shards and heads are identified by uuid, the WAL ordinal is seeded and a
write is acknowledged only if the lease was valid on both sides of the append, the action gate covers the
whole surface, no thread pool waits on itself, mapping and settings changes reach every node, `_bulk` grows
a mapping, `_delete_by_query` is conditional with `version_conflicts` and `conflicts=`, every write is under
indexing pressure, the search coordinator's working set is bounded and inside the breakers, and error
bodies name a blob rather than a path. A second pass turned the last hot-path listings into reads: membership
is a `members` register maintained only on join and leave, the WAL is truncated from the writer's own
ordinals, a sweep runs only when a manifest lost a file, and the manifest records every file's length so a
reader opens without listing. Register reads followed: one head read per shard per pass shared by the
heartbeat, the hints and the publish; writes and gets route by the owner last seen and read the head only
when that node refuses; a publish swaps over the generation it remembers. M64
(`m64-deep-review-fixes-notes.md` (on `feature/serverlessnode`)) took every finding of the deep review
([`serverless-deep-review-2026-09.md`](serverless-deep-review-2026-09.md)): the acknowledgement fence is
explicit — a lapsed lease suspends every shard until the heads are re-verified, a per-shard fence spans
apply→append→acknowledge and publish→release→close, the holder treats its lease as lapsed a margin early,
a forwarded write is accepted only for a shard whose recent head names the owner, and a failed append fences
rather than closes; readers never serve a fenced writer's bytes and writers open lazily; hints are forgotten
on every forward failure; forwarded answers carry sequence identity and conflicts; alias and stream
mutations swap against the generation they read; snapshots capture settings and honour their swap on
restore; tombstones are quarantined and swept; forwarded requests carry a per-request MAC; the throttle can
lock neither a shared address nor the recovery account.

## What is deliberately refused

These are decisions, not gaps. Each answers 501 with a reason.

| Refused | Because |
| --- | --- |
| Alias options: `filter`, `routing`, `index_routing`, `search_routing`, `is_write_index`, `is_hidden` | An alias here names indices and nothing else. Refused on all three doors -- the index-scoped spelling, the alias-scoped one, and an action inside `POST /_aliases` -- and pinned by a canaried test. This was once the last place something was accepted and not honoured: a filtered alias was created unfiltered and answered `{"acknowledged": true}`. |
| `_cat/indices` past the pattern cap | `GET /_cat/indices` is served as the empty prefix under the same capped resolution every pattern takes, and refused past the cap rather than truncated: its contract is the whole answer, and a page that looks complete and is not is worse than a refusal. `GET /_list/indices` walks any number of indices by `next_token` instead. |
| `POST /_data_stream/_modify` | A data stream here is an alias with a generation, so its backing indices are the alias's members and change by rolling it over, not by being reassigned underneath it. |
| Patterns that are not a prefix (`*-2026`, `lo*s-a`, `logs-?`) | A prefix can be answered by one bounded listing; these cannot be answered by a listing at all, only by reading every index name in the deployment and matching each. Supporting a wildcard syntax whose cost depends on where the caller put the star would be worse than the split. |
| Enumerating indices (`/_serverless/indices`) | An inventory operation, not a serving one — it is asked to return everything, where a pattern is capped and refuses when the cap is exceeded. Look an index up by name, or run an offline inventory. |
| `/_cluster/*` except `settings` | There is no cluster-wide state; a node-local answer would mislead. |
| `collapse`, `suggest`, `profile` | Each needs cross-shard machinery that does not exist yet. |
| Two request wrappers, or two plugins contributing one setting | Which one wins would depend on load order. |
| `index.search.default_pipeline` | A search pipeline is applied by the search path and this shell's does not read it, so the value would be stored and never run. Name it on the request (`?search_pipeline=`). The two *ingest* pipeline settings were refused here for the same reason until the write path started reading them. |
| `IndexNameExpressionResolver` for plugins | A node's `ClusterState` holds only the indices it has open, so resolving would answer a wildcard with a subset — which for privilege evaluation fails open. |
| Scripting from a plugin's own engine | The shell registers painless and mustache by name; a plugin cannot contribute a third engine, and gets core's own "no lang registered" for one. |
| On-behalf-of and service-account tokens | They delegate authority, and this deployment authenticates without a general authorization model to delegate. |

## What is genuinely missing

**Open, and mine to do:**

- Filters run once, on the node the request reached, and not again on the node a write is forwarded to.
  That is a design choice — re-running every filter per hop would make one that counts or rate-limits wrong
  — but it does mean a plugin cannot enforce at the shard. (A caller *does* travel: a thread-context header
  set by a plugin arrives on the far side, which is core's own mechanism and is tested.)
- A plugin's action can now be run on a node the plugin chose, and the hop is authenticated from the
  first request rather than from the end of startup. The check first asked the built registry whether an
  action was a plugin's — but a `HandledTransportAction` registers its handler from its own constructor,
  those constructors run inside the loop that builds the registry, and the registry is assigned only when
  the loop returns. The transport has been accepting since well before. So each plugin action was live and
  unchecked for the length of that loop. It reads the declared names instead, which come off
  `getActions()` and are known before the transport accepts anything. This entry
  used to say a plugin could not forward one at all; that was wrong, and testing it is what found out.
  Every piece was already present — the projected cluster state carries every live member, a
  `HandledTransportAction` registers its own handler everywhere, and the thread context travels — so a
  plugin doing what it does on a classic node already worked. What was missing was that nothing signed the
  hop and nothing checked it: **any process that could reach the transport port could invoke any plugin's
  action**, including the credential API an auth plugin ships. A transport interceptor now signs a plugin
  action on the way out with the deployment's MAC and refuses an unsigned one on the way in, reusing the
  shell's own signing rather than a second scheme. Plugins need no shell API for any of it.
  One caveat: members reach the projected cluster state on an activation or on the periodic resync — ten
  minutes at the default interval — so a freshly started node holding nothing may not see a peer yet.
- A point in time is still node-local once opened: the record is in the object store and any node can serve
  it, but placement is only a hint, so two nodes can still each end up opening one view under an unlucky or
  stale routing decision — narrowed, not closed, by giving frozen search the same placement a live search
  already had. No slicing.
- The swap/seal fencing window is **closed**, by fencing the write-ahead log itself rather than recording
  where it ended (`rfc-fencing-closure.md`). A takeover occupies the predecessor's next log slot with a fence
  -- a blob of no bytes, written put-if-absent -- so exactly one of {the predecessor's append at that slot,
  the fence} lands. Whatever landed is ahead of every fence and is replayed, and a write whose record landed
  is now acknowledged whatever its writer's lease says afterwards; nothing can land after a fence. That
  needed four things the log did not have: a writer stops for good after any failed or ambiguous append, so
  its names are contiguous; every writer fences its own term before writing, which is the same-term reopen
  case done the same way, and confirms the head still names it; truncation always keeps a term's highest
  record, so a successor can find the slot; and fences are never deleted. A node that wins a shard it
  already holds at an older term now reopens it at the new term -- before, its later writes landed behind
  the seal and were never replayed, acknowledged and lost. **What remains is assumption, not window**: that
  the store's put-if-absent on one key is linearizable (conformance checks it on the filesystem, MinIO and
  RustFS 1.0.0; R11 is open for real S3, GCS and Azure), and that an append whose outcome is unknown -- a
  timeout -- may or may not be in the log, which is what "unknown" means. The log's cutoff no longer rests on
  clocks; lease expiry still decides when a takeover may begin. Pinned by `FsLogFencingTests` and
  `S3LogFencingTests` (the same history on MinIO and on RustFS single-node and erasure: an append before the
  fence is kept, one after it is refused, a refused writer never writes again, truncation cannot hide the
  slot, a fence survives its term being dropped, a same-term reopen fences its predecessor, a superseded
  writer stops, and a concurrent writer racing two takers loses nothing acknowledged and replays nothing
  refused) and by `ServerlessWriteFencingTests` end to end (an append between the swap and the fence is
  acknowledged and replayed; a write that landed before a takeover is acknowledged and kept). Removing any
  one of the fence, the poisoning, the kept record, the kept fence, the own-term fence, the acknowledgement
  change or the reopen at a newer term fails at least one of them. An append is still one conditional PUT;
  a takeover costs 8 requests against the seals' 9 on the log (6 listings and 2 PUTs, against 6 listings, 2
  PUTs and a GET), plus the head read a writer makes after fencing its own term.
- `update` inside `_bulk` is refused: it is a partial merge, which has to read the current document before
  writing one, and the batch path applies without reading. Closing it changes what a batch is, so it is a
  design decision rather than plumbing. `POST /{index}/_update/{id}` does the merge.
- Static index settings cannot be changed at all, by any route. Classic OpenSearch offers close-change-open
  and there is no close here, so the only way to change one is to create a new index. Dynamic settings change
  freely (M54).
- A walk over indices is not a snapshot. `GET /_list/indices` by `next_token` returns every index that
  exists for the whole walk exactly once, in name order; one created or deleted behind the cursor is not
  seen, one ahead of it is seen or not depending on when its page is read. Core sorts `_list/indices` by
  creation time; there is no creation-ordered index over a hundred million names here, so `sort` is accepted
  and the order is by name.
- A freeze and the sweep no longer race. Freezing reads one manifest per shard and only then wrote the
  record, so files the early shards froze could stop being referenced and be swept before the record
  naming them existed. The wall-clock floor on unreferenced blobs makes a *short* freeze safe and only a
  short one: at `MAX_SHARDS` the manifest reads alone pass the default sixty-second floor at any per-read
  latency above about fifteen milliseconds. Snapshots already had the guard — an index named by a capture
  that has not recorded its commits sweeps nothing — and points in time now do too, by writing a marker
  before reading anything and replacing it under the same id.
- The whole-deployment orphan sweep (shard containers no index owns) is still manual; the per-shard sweep
  runs on every pass. **Deleting an index while it is being published to lands in exactly that gap**: the
  delete purges the shard data, and a writer that had already begun a publish completes it afterwards,
  leaving blobs under the deleted uuid. Nothing serves them — no descriptor, no head, and a uuid no later
  index can be minted with — so no answer is wrong, but nothing automatic reclaims them either. `deleteIndex`
  used to claim the next index of that name cleared them on creation; it does not, and cannot, because the
  uuid in the path means the new index writes somewhere else. A reader whose node cannot reach the object store for longer than the grace can lose
  the commit it is reading — it fails with an error rather than answering wrongly, but it is a real limit.

**Open under D4 (each needs a narrow shared interface in `server/`, which D4 permits and nobody has built):**

- `onIndexModule` fires only for plugins loaded from disk. Firing it for a plugin passed in as an instance
  needs `PluginsService` to accept instances.
- The orphan sweep is not resumable; making it so needs paged `children()` on `BlobContainer`.

**Blocked on something I do not have:**

- **R11 — provider CAS linearizability — remains the top risk and is unclosed.** The whole safety argument
  rests on `compareAndSwapRegister` being genuinely linearizable. What exists now: a linearizability
  checker that records a concurrent history and asks whether any sequential order explains it — the
  question itself rather than a proxy for it — canaried against stores that really misbehave, and passing
  against two unrelated S3 implementations (MinIO and SeaweedFS). What does not exist is an account. No
  fake can close this; pointing the checker at a real provider is the whole remaining cost.

  The blast radius is now measured rather than assumed (`ServerlessStoreDeviationTests`). A stale head
  read costs nothing, because ownership is decided by the swap. An ambiguous outcome costs liveness. Two
  winners on one generation cost data — and that measurement found that the publish fence only refused a
  *strictly newer* term, so a second writer at the same term could produce a commit assembled from two
  nodes' segments. A manifest now records who wrote it and refuses a foreign writer at the same term.

## Decisions a reader should know about

- **`server/` is no longer untouched, and the exact extent is six files and 364 lines** -- the
  object-store compare-and-swap register, and the pair of engine hooks that replay the write-ahead
  log during recovery. See [`README.md`](README.md) for the file-by-file account. The direction rule
  still holds and is still a build task: no file under `server/` may reference
  `org.opensearch.serverless`. D4 permits narrow shared interfaces, and these are them.
- **One build-tooling file is touched too, and it is not part of that count.** `doc-tools/missing-doclet`
  reported every record's accessors and canonical constructor as undocumented, which no amount of writing
  could satisfy: a record's `@param` tags are the only place those can be documented, and the doclet never
  looked at them. That made the strictest level unreachable for any module using records. It now skips
  those implicit members and checks the record's own `@param` coverage instead — which nothing did before,
  so the check is stricter than it was, not looser. The serverless modules stay at `parameter`, the
  strictest level, and above core's own `server/`, which sits at `class`.
- The REST surface is an allowlist. Anything not registered answers 404 or an explicit 501, never an empty
  success.
- Storage layout changed in M32 (uuid in the path); an existing deployment's data is not found by a node
  running this build. Pre-release.
- The authentication plugin authenticates and ships no policy beyond one rule: only the configured account
  may manage accounts. It is one `if`, and is named as one.
- **Response shapes match real OpenSearch wherever matching costs nothing architecturally** (M47) — field
  names, envelopes, defaults that were shell-specific inventions with no reason to differ. Every deliberate
  refusal, not just uncaught exceptions, now renders the real `{"error": {"root_cause": [...], "type": ...,
  "reason": ...}}` object; search responses carry `took`, `timed_out`, real `_shards` field names,
  `hits.total.relation`; writes carry `_shards`; index creation carries `shards_acknowledged`. What did
  **not** change: the endpoints and request shapes D2 and §6.3 refuse on purpose — conditional writes (no
  version model), non-prefix wildcards (no listing on a request path), a cluster-wide surface (no
  cluster-wide state) — and `_version`/`_seq_no`/`_primary_term` and "created vs. updated," both deferred
  because closing them needs a real feature (a version model), not a response-shape change. See
  `m47-api-compatibility-notes.md` (on `feature/serverlessnode`).

## What a correctness review found

A deliberate review, after the work above was already committed and green. Five real defects and one
hardening. **Two of the five had been introduced earlier the same day**, which is the first thing worth
recording: the newest code was the least reviewed code, and reviewing it as hard as the old code is what
caught them before they shipped rather than after.

| Finding | Class | Where |
| --- | --- | --- |
| A plugin's action was reachable and unauthenticated while the node started | security hole | `PluginHopAuthentication` |
| The sweep could delete files a freeze was still reading | data loss | `GarbageCollector` / `PointInTime` |
| `deleteIndex` claimed a cleanup mechanism that does not exist | false safety argument | `MetadataPlane` |
| A freeze in progress pinned nothing, so deleting its index took its files | wrong answer | `pinnedByView`, both copies |
| A capture marker was usable as a view: zero hits, `"complete": true` | wrong answer | `pointInTime`, `_pit` list and `_all` |
| A frozen shard whose view holds no commit for it now refuses | hardening, not a live bug | `openFrozenView` |

**Three of the five are one shape: a guard that exists on the snapshot-capture path and was never mirrored
onto the point-in-time path.** Snapshots have had a two-phase capture guard since they were written — an
index a capture has named is pinned in full, and sweeps leave it alone. Points in time did not, and every
consequence of that absence was a separate bug. If a third pin-like mechanism is ever added, check it
against both.

**Every finding came from running or probing, not from reading.** The first fix for the freeze/sweep race
was dead code whose own test passed: the capture marker did not round-trip, because the parser refuses a
record holding no shards, so it read back as the placeholder the plane substitutes for anything unreadable
— and the protection observed in the test came from the placeholder path, which stops the sweep for every
shard of every index rather than the one being frozen. A probe printing the record after a round trip is
what exposed it. Reading the code confirmed the wrong belief three times.

**The last finding came from searching for the bug class rather than the next file.** After four findings
shared a shape, grepping every consumer of `livePointsInTime` was more productive than continuing linearly:
two of the nine had never been checked and both were wrong. Worth doing deliberately rather than after the
fourth instance.

**Two testing hazards, both of which produced a false green during the review.** Gradle's task cache does
not notice the `missing-doclet` jar changing, and did not re-run tests after a source change on two
occasions — so a canary reported success without having recompiled. Every canary here now runs with
`--rerun-tasks`. And Lucene's `ExtrasFS` drops an `extra0` into random directories, where the orphan sweep
is right to take it; that has broken three separate tests in `ServerlessStoreInvariantTests`. Assert on the
blob the test planted, never on an empty list.

**What the review checked and did not fault**, so a later reader knows where it has already been: the
write path from acknowledgement through publish to truncation, including that truncation deliberately lags
one publish; the lease and shard-identity fence on all three write paths; sealing at the swap against the
clock-skew budget `BlobLeaseMembership` documents; rollover's create-then-swap and its compensating delete;
alias hint ordering; search coverage (`answered == shards`, and the fan-out claims each shard's slot
exactly once, so it cannot be inflated); `_mget` never rendering an unreachable shard as `"found": false`;
snapshot restore, which fails loudly on a missing source blob rather than restoring a truncated commit;
mapping growth under concurrency; `_update`'s conditional write-back; the publish fence the collector's
first condition depends on, which is bypassable on its fast path and saved by the re-read after a failed
compare-and-swap; that a pipeline cannot run stale, because the marker register is read fresh on every
lookup; and that a conditional write keeps its meaning across a forward, conflict included.

## What an object-store cost audit found

A deliberate pass over every `put`, `list` and `get` this shell issues, asking of each whether it is the
minimum that preserves correctness. Two independent reviews of the audit followed, and **both corrected it
in ways worth recording, because the corrections are more instructive than the findings**.

| Finding | Class | Where |
| --- | --- | --- |
| A single write cost one PUT; concurrent writes now share one | cost, the dominant term | `WalGroupCommitter` |
| A seal deleted a concurrently-written seal it never merged, widening a replay cutoff | data loss | `WalStore#sealAt` |
| A zombie's records under a dead term were reclaimed once and then never | storage leak | `WalStore#onPublished` |
| A descriptor was read from the store on every write, get and search | cost, per request | `MetadataPlane#resolveForRouting`, `ServerlessNode#ensureIncarnationLive` |
| Deletes were issued one blob at a time where the container batches a thousand | cost | `ShardHeadStore`, `DescriptorStore` |
| A sweep read one index's descriptor once per shard rather than once per index | cost | `GarbageCollector` |
| Two cost assertions had been failing since the rebase, unnoticed because `s3Test` skips without an endpoint | stale test | `ServerlessCostTests` |

**The write path is priced by the store's latency now, not by the write rate.** Group commit batches only
what arrives while a PUT for that shard is already in flight — no timer, so a quiet shard pays exactly what
it paid before and a busy one stops paying per document. Measured: 3.3 documents per PUT on a filesystem,
8.0 against MinIO, and a remote bucket has longer round trips than loopback so 8.0 is a floor.

**Two of the three correctness findings came out of fixing something else.** The seal bug was found by an
adversarial reviewer arguing the *opposite* case — that taking the supersede set from a pre-write listing
would delete an unmerged seal. It is the post-write listing that can, which is what the code did. The WAL
leak was found by digging into one of the two stale cost tests rather than simply re-baselining it.

**What the reviews corrected, which is the part worth keeping.** The audit claimed two in-memory generation
memos bought nothing on S3, because the container's compare-and-swap re-reads anyway; they save one request
of three, and a test already pinned it. It ranked the read-then-CAS cost first when that path carries no
per-request traffic at all, and listed a method with no production callers among its hot costs. It proposed
a descriptor cache whose invalidation could not fire for the names a coordinator actually caches, and a
replacement for the head scan that would have let a node resume acknowledging on the strength of an absent
notification. **Ranking by "looks redundant" is what produced those; ranking by what a cost scales with —
requests, shards, indices, nodes, bytes — is what corrected them.**

**Two costs that scale with load rather than with the deployment, and only one is fixed.** A descriptor
read is gone from the request path — writes, search, multi-search, get, multi-get and the shard operations
behind them — answered from a read taken within the last second. Until recently only writes were; an
earlier version of this paragraph said all of them were, and the code never did.

**The argument that made that cache safe was wrong in one case, and acknowledged writes into deleted
indices.** It said a stale answer cannot misroute a write because the uuid is checked again where the write
lands. That holds when the old incarnation's shard is no longer open on the landing node, and not when it
still is: another node deletes and recreates an index, this node still holds the old shard open and routes
by the cached descriptor, whose uuid is the old one, so the check matches and the write lands — a 201,
durable in the log, in an index that had been deleted. `ServerlessRoutingFreshnessTests` missed it because it
wrote through `ShardOperations`, which read fresh. **Closed by an incarnation fence** rather than by giving up
the cache: a node answers from a shard — acknowledging a write after its log append, returning a get,
returning a search shard's hits — only within one second of having read that the shard's index still exists
under that uuid, by its own monotonic clock, and a REST delete (index or data stream) answers only after
1.25 seconds. So anything answered from an incarnation was answered before its delete returned: concurrent
with the delete, never after it, and durations rather than timestamps are compared so clock skew does not
enter. The fence costs one descriptor read per index per second of activity on a node, not one per request;
a refusal is a retryable 503 that also drops the coordinator's cached route. Pinned in
`ServerlessRoutingFreshnessTests` (a stale REST write is a 503; neither a get nor a search returns the old
incarnation's document; a delete takes at least the settle window) and `ServerlessRequestPathCostTests` (a
warm search, msearch, get and mget read no descriptor), each with a planted defect that fails it. The price
is that deleting an index takes at least a second and a quarter.

**The per-shard head scan is the other, and it is now fixed in deployment by a revocation carried on the
node's own lease.** A head is taken only from an owner whose lease the taker judges expired, by the taker's
clock; within the skew budget the owner has lapsed by its own clock too and re-reads every head, and the
scan existed for the case beyond it. The taker now swaps the owner's lease register, at the generation it
read, to a revoked copy *before* swapping the head. That swap and the owner's own renewal contend on one
register, so either the owner renewed first and the taker backs off, or the revocation landed first and the
owner's next renewal fails — which it now treats as a lapse (suspend acknowledgement, re-read every head),
where it used to be written over silently. The objection this file raised below — that a revocation marker
is not atomic with the swap that moves ownership — holds for a marker kept anywhere else; on the owner's own
lease it is the owner's renewal that it is atomic with, and that is the comparison that matters. A node
whose renewals keep succeeding has lost no head to a takeover, at one write per renewal however many shards
it holds, so `ServerlessBootstrap` runs the scan every five minutes as a backstop (for a head released by
someone other than its owner, which nothing in this shell does) and re-reads the descriptors of open
indices every thirty seconds rather than every pass — an index in use is re-read within a second by the
incarnation fence, which now applies what it reads. The library defaults stay per pass, because the tests
that encode the old bargain construct nodes directly. `ServerlessLeaseRevocationTests` pins it: an owner a
whole lease behind the taker's clock, which never scans on its own, learns of the takeover from its next
renewal and lets go; a live lease is never revoked; and with the production intervals a pass reads no
register at all, holding two shards or eight. A planted defect in each of the revocation, the owner's
reaction, the scan after it, and either interval fails a test. Whatever this leaves, the earlier text:

The per-shard head scan was **not** fixed: it can be put on an
interval, and that is built, measured and off by default, because the saving is bought with
ownership-detection latency and three existing tests encode the current bargain as a guarantee. Making it
genuinely flat needs a signal carrying evidence of ownership; a revocation marker is not written atomically
with the swap that moves ownership, so its absence proves nothing. `RegisterMap#assignments` already says
exactly that about the listing it maintains.

**Two more since**, both measured. Membership refresh no longer re-reads a lease whose own stamp says it
cannot have expired — six registers over five members on a cold refresh, one on a warm one, which takes the
fleet-wide term from quadratic to linear. And a cold read no longer pays one request per block: read-ahead
ramps while the reads stay sequential and drops back to one block on a seek, so a scan of 512 blocks costs
11 requests where it cost 512, while eight scattered reads still cost exactly eight.

**Deleting an index no longer needs a tombstone, where the store can delete conditionally.** A tombstone
existed for one reason: a register's generation lived in its body and restarted at 1 when the blob was
deleted and created again, so a compare-and-swap carrying a generation read before a delete landed on
whatever was created after it. Two changes remove the reason. A register now starts at a random 62-bit
generation (`BlobRegister#initialGeneration`), so a recreated register never reuses its predecessor's
numbers; and `BlobContainer#deleteRegisterIfUnchanged` deletes only at the generation the caller read, with
the store's own condition — `If-Match` on the ETag for S3 and Azure, a generation precondition for GCS, a
lock for the filesystem. A delete then writes a reclaim intent (`reclaim/<due-minute>/<name>#<uuid>`), deletes
the descriptor conditionally, and deletes only *this incarnation's* heads, conditionally — the earlier
by-name head delete could remove a head a recreated index had just won, which a third node could then take
while its owner still held the shard. Two minutes later a reclaimer, off the background pass and split by
rendezvous, drops the intent if the index turned out to live on, or else deletes any head a writer that missed
the delete re-created and purges the uuid's bytes again: the delete-during-publish leak this file lists
below is closed for the index-delete case. No quarantine, no marker, no claim, no `name_being_reclaimed`.

**Only where the store is seen to honour it.** A store can accept `If-Match` on a delete and ignore it, and
then a delete racing a write removes the write. So `ConditionalDeleteProbe` runs once per node against the
store in use: it moves a register on through a second container and deletes through the first at the stale
generation; a store that deletes it anyway fails, and that node keeps tombstones, markers and the sweep —
the fallback is the whole previous path, unchanged. **MinIO (`RELEASE.2025-04-22`) fails the probe**: it
ignores `If-Match` on `DeleteObject`, and a node on it stays on tombstones. **RustFS 1.0.0 passes it**
(`rustfs/rustfs@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff`), both as one node on
one drive and as one node over a four-drive erasure set (four tmpfs mounts, since it refuses drives sharing a
device). Against both layouts the full conformance suite passes, including a linearizability check extended
to histories mixing concurrent reads, swaps, create-if-absent and conditional deletes on one register — run 25
times over per layout, 100 rounds of four contenders each — with a planted store that deletes despite a stale
generation to show the check fails when it should. Nothing reported against earlier RustFS builds (a
conditional delete checking `If-Match` on arrival and then deleting whatever was current) reproduced on 1.0.0.
**That is evidence for RustFS 1.0.0, not for AWS, GCS or Azure**: R11 stays open for all three, since this
work had no account on any of them.

The extended check also found two defects in the filesystem store, fixed: a register read racing a delete
threw `NoSuchFileException` instead of answering "absent", and starting generations were drawn from the test
framework's per-thread seeded randomness, which handed two concurrently created registers the same one.
Starting generations now come from one process-wide `SecureRandom`.

**A compare-and-swap on a known version is one request.** With generations unique across incarnations, no
two versions of a register have the same body, so an S3 ETag identifies a version as exactly as its
generation does. `S3BlobContainer` remembers the ETag of the version it last read or wrote, and a swap
against that version is one conditional PUT instead of a GET and a PUT; a stale memory costs a refused PUT
and a read, never a wrong write. Register requests are now counted in the S3 client's request metrics, which
they were not. Measured on MinIO with those metrics (`ServerlessDeleteCostTests`, in `s3Test`):

| operation | before | after |
| --- | ---: | ---: |
| compare-and-swap on a version the container just read or wrote | GET + PUT | PUT |
| compare-and-swap from a container that knows nothing | GET + PUT | GET + PUT |

| deleting a 1-shard index | at the delete | deferred | total | PUTs |
| --- | ---: | ---: | ---: | --- |
| tombstone path (fallback; already has the one-PUT swap), idle or with a writer | 7 | 8 | 15 | marker, tombstone; claim |
| conditional path, idle index | 8 | 6 | 14 | one intent; none |
| conditional path, a writer holding the shard | 5 | 10 | 15 | one intent; none |

Identical request counts on MinIO, RustFS single-node and RustFS erasure. The conditional path writes one
object where the tombstone path wrote three and spends nothing on quarantine or claims, and it purges an
index's bytes exactly once: at the delete when nothing held a shard, and otherwise at reclaim, after any
writer that missed the delete has stopped — a head of the dead uuid found then, re-created by such a writer,
also triggers the purge. An earlier version purged at the delete and again at reclaim, which cost 19 requests
per delete; the reclaim's extra listing of an already-empty bucket went too. Pinned by
`ServerlessConditionalDeleteTests`, each with a planted defect that fails it: a stale swap cannot land on a
recreated name (fails with generations restarting at 1); a head and a publish left by a writer that missed
the delete are reclaimed (fails with a no-op reclaim); an old incarnation's reclaim leaves the new
incarnation's head (fails with heads deleted by name); a store ignoring the condition fails the probe and
keeps tombstones (fails with a probe that trusts the store). The conformance suite gained a recreated
register never reusing a generation and a conditional delete refusing a stale one; both pass on the
filesystem and on MinIO.

**Background cost, measured flat in index count.** `ServerlessScaleMeasurementTests` (in `s3Test`) runs one
node against MinIO with the production intervals, holding 8 shards over 4 indices, beside `P` indices it never
touches and 50 deleted long enough ago that their tombstones are due. It then counts every request over 120
backstop passes — an hour at the 30-second backstop, including one tombstone sweep. Measured at `P` = 10,000
and 100,000, on the branch before this work (`57b924b9840`) and after it:

| | P = 10,000 | P = 100,000 | grows with P? |
| --- | ---: | ---: | --- |
| before: requests per node-hour | 11,834 | 101,834 | by exactly one register read per index |
| after: requests per node-hour | 930 | 930 | no |

The same run on RustFS 1.0.0, where the probe passes and the 50 deletions go the conditional way and are
reclaimed rather than swept, is flat too: 1,006 and 1,007 requests per node-hour at 10,000 and 100,000 indices
on one drive, 1,010 and 1,007 over the four-drive erasure set. The difference from MinIO's 930 is those 50
reclaims — an intent read and a descriptor and head check each — and the few-request spread between runs is
the thirty-second descriptor refresh, which runs on the wall clock.

Before, 1,834 of those were the per-pass head and descriptor scans of the held shards and `P` were the sweep
reading every descriptor. After, the head scan runs every five minutes, descriptors every thirty seconds, and
the sweep reads only the 50 deleted names. At S3 list prices ($0.40 per million GETs, $5 per million PUTs and
LISTs), per node, extrapolating the "before" line by its measured slope of one read per index:

| population | before: GETs/s | before: $/node-month | after: requests/s | after: $/node-month |
| ---: | ---: | ---: | ---: | ---: |
| 100,000 | 28 | ≈ $30 | 0.26 | ≈ $0.93 |
| 1,000,000 | 278 | ≈ $293 | 0.26 | ≈ $0.93 |
| 10,000,000 | 2,778 | ≈ $2,900, and the sweep never finishes | 0.26 | ≈ $0.93 |

The "before" cost is paid by *every* node, since each ran the whole sweep; at 10M indices one serial sweep
is 10M reads, 14 to 55 hours at 5 to 20 ms per GET, against an hourly interval. The "after" cost is per node
and independent of the population; deletion itself costs about three PUTs and three GETs per deleted index,
paid once by whichever node owns its hour's bucket. Two caveats: these are MinIO request counts priced at S3
list prices, not an S3 bill; and the test's pass also renews the lease, where production renews on its own
timer three times per TTL, adding about 240 PUTs per node-hour to both lines alike.

**Shard heads are spread over 256 key prefixes.** S3 scales request rates per key prefix, about 5,500
GETs a second each, and splits partitions on leading characters. A head was named `<index>#<shard>`, so a
deployment whose index names share a stem put its whole head traffic behind one prefix well before a million
shards. The name now leads with two hex digits of a hash of itself (`3f-logs-2026-01-01#0`); nothing lists
the container, so nothing needed the order. Index descriptors stay unhashed on purpose: `logs-*` is answered
by a prefix listing of them. **Not compatible with heads written under the old names** — a deployment
switching must start from an empty `shards/`, or two nodes would each find a shard unowned under a different
key. `ServerlessKeyLayoutTests` pins the spread (a year of daily indices over more than 200 of the 256
prefixes) and the key actually written.

**The per-node caches hold what is recent, not everything ever seen.** The routing cache and the head
sightings each dropped an entry only when a lookup happened to find it stale, so a name or shard touched
once stayed for the life of the process — every routing entry with its whole descriptor, mapping included.
Past 10,000 entries, each now drops what no reader would believe any more (a routing entry past its one
second, a sighting past a lease), at most once per that interval, so no answer changes and the size is set by
what was touched recently rather than by the population. `ServerlessCacheBoundTests` pins both, and fails
with either prune removed.

**The membership probe is capped for every request path, not only search.** Within a young snapshot a join
is detected by one read of the members index's generation, and that read was made on every call: every
health check a load balancer sends, every stats call and node listing. Search had capped it in its own
handler. The cap now lives in `BlobLeaseMembership#refreshIfOlderThan` — at most one probe a second per
node, wall-clock — and search's copy is gone. `ServerlessRequestPathCostTests` counts it: ten health checks
in well under a second read the members index at most once, and fail with the cap removed.

**The tombstone sweep no longer walks the population, and no longer loses a recreated index.** It used to
list every descriptor and read each one, on every node, hourly — a million reads per node per hour at a
million indices, to find the few hundred names deleted that hour — and it deleted the expired tombstones in
one batch at the end of that walk, with nothing checking at delete time that the name had not been recreated
since. At scale the walk takes hours, and a name recreated inside it lost its new descriptor; the collector
then deleted the new index's shards as orphans. Now a delete also drops an empty marker into an hourly bucket
under `tombstones/`, and the sweep reads only the markers in buckets old enough to hold an expired name — one
listing of the buckets, then a read per deleted name. Rendezvous hashing over membership gives each bucket to
one node, so the deployment pays once per deleted name rather than once per node. The unconditional delete
is made safe by a claim: the sweep first swaps the tombstone to a claimed one, a create refuses to swap over
a claim for a minute (`name_being_reclaimed`, 503), and the sweep deletes only within ten seconds of
claiming, by its own clock. What remains is a pause between that check and the delete landing, the same
residual every lease-timed write here carries. Tombstones written before markers existed have none and are
never swept. Pinned by `ServerlessTombstoneSweepTests`: register reads equal at populations of 10 and 200,
and each of the claim, the deadline, the create's refusal, the marker and bucket ownership has a planted
defect that fails a test.

**Cold activation: the first write waits for its shard, and the activation stopped repeating itself.**
Measured on RustFS 1.0.0 with a fixed delay injected before every object-store request
(`ServerlessColdActivationMeasurementTests`, in `s3Test`): a dormant 1-shard index -- published and let go
by a node that has since gone -- takes its first write and its first search on a fresh node, ten samples
each. The "before" is the same harness on `7bfec04d070`.

| delay | first write, before | first write, after | first search, before | first search, after |
| ---: | --- | --- | --- | --- |
| 20 ms | p50 1,008 ms, p99 1,049 ms; 79 requests; ~30 client attempts | p50 720 ms, p99 894 ms; 26 requests; 1 attempt | p50 232 ms, p99 361 ms; 9 requests | p50 247 ms, p99 992 ms; 7 requests |
| 100 ms | p50 4,473 ms, p99 4,742 ms; 85 requests; ~34 client attempts | p50 2,594 ms, p99 2,677 ms; 27 requests; 1 attempt | p50 1,082 ms, p99 1,552 ms; 10 requests | p50 797 ms, p99 935 ms; 7 requests |

What the recorded sequence showed, and what changed:

- The first write was answered 421, and the client retried until some pass had taken the shard. Each retry
  read the head again: about fifty of the 79 requests, and most of the wait. A write to a shard nobody owns
  now waits (up to 30 s) on the activation it causes and is written once the shard is open, into the
  incarnation it was routed for; one request, one attempt. The same holds for a shard whose head already
  names this node but which is still opening.
- One global `activationLock` became a single flight per shard -- every path that asks for a shard (the
  pass, a doubt, a waiting write) shares the one activation in flight -- on a bounded pool (8 by default,
  run on the generic pool's threads). A node taking a hundred cold shards no longer takes them one after
  another, and a write waiting for one shard no longer waits behind the rest. The shard cap is kept by
  reservation, so concurrent on-demand activations cannot overshoot it. Views are projected and applied
  under one lock, and a shard being opened is protected from a concurrent view's pruning.
- The takeover's fence reused none of the listing it had just made; now its first put-if-absent uses it.
  The open fenced every older term again, listing the log a second time; after a takeover at the same term
  it no longer does. The replay read the whole log twice -- once in recovery, once for records without
  sequence identity -- and now reads it once; its record reads run concurrently (8 at a time, the caller
  reading too); and the legacy seals directory is listed only when the log's root listing shows one.
- The lease was renewed on every activation. It is now renewed when less than half its TTL remains.
- A reader open resolved the index's uuid twice (the two-argument manifest lookup describes again), and
  the query it served read the descriptor a third time for the incarnation fence. The read the open makes
  now confirms the incarnation, as does activation's.

Pinned by `ServerlessActivationTests`: the first write is 201 on its first attempt; eight concurrent writes to
one cold shard cause one compare-and-swap of its head; two shards' activations are in flight together (each
one's head write waits for the other's to begin, so serialised activation deadlocks and fails); and **the
canary -- two nodes racing for the same six cold shards leave exactly one owner of each, and the loser holds
nothing**. `ServerlessSchedulerTests#testOnDemandActivationStopsAtTheCap` caught the first version of the
reservation overshooting the cap.

**Wide searches: a sliding window, a pruning digest, and an activation budget.**

- The fan-out handed the pool a chunk of shards and waited for the whole chunk before the next, so every
  chunk was as slow as its slowest member. It is now a sliding window: `concurrency - 1` pool workers and
  the caller each take the next shard the moment they finish one (`serverless.search.fanout.concurrency`,
  16). `FanoutTests` pins it with a first task that can finish only after the last one has started -- a
  chunk barrier deadlocks on it.
- Every publish writes a **pruning digest** into the manifest register it already writes: per point field,
  the minimum and maximum any document in the commit holds, read from the BKD roots
  (`PointValues#getMinPackedValue`). No Lucene change was needed. The digest rides last in the JSON, so a
  node from before it reads everything it knows and stops; the parser now skips any object it does not know.
- A search over more than one shard first asks, concurrently, which shards it can skip: a shard not open
  here as a writer, whose head names no owner -- so its published commit is everything it holds -- and whose
  digest rules the query out. A digest rules out only range and term queries on a digested numeric or date
  field, reached through `bool` `must`/`filter` and `constant_score`, and only when the query lies strictly
  outside `[min, max]` with both ends taken as inclusive; dates are parsed as the field parses them, with
  the request's single `now`. Anything else is a "maybe". Skipped shards are reported in `_shards.skipped`
  and counted as successful, as core's `can_match` counts them; if every shard would be skipped one is
  searched anyway, so aggregations have their shape. A query with nothing a digest could judge, a `global`
  aggregation or a suggester skips the phase entirely.
- A search that would open more shards not held here than `serverless.search.max_activations_per_query`
  (1,024) is refused with a 400 before any is opened, unless it passes `allow_partial_activation=true` --
  then the first ones within the budget are searched and the rest reported as failed shards. What the
  digest skipped does not count.
- The pattern cap went from 500 to 10,000: resolution now pages a thousand names per listing request, and
  the names' descriptors are read concurrently rather than one after another, which for five thousand names
  was five thousand round trips before the first shard was asked.

Measured on RustFS 1.0.0 (`ServerlessWideSearchMeasurementTests`): 5,000 hourly indices, each four
documents inside its hour, all published and let go; a `logs-*` search for a 250-hour window, no injected
delay. **4,749 of 5,000 shards pruned (94%)**, and the total exactly the window's 1,000 documents -- the
canary at scale, since a digest that skipped one shard too many is a wrong total.

| pass | latency | requests | of which |
| --- | ---: | ---: | --- |
| cold (no reader open) | 5.5 s | 16,513 | 6 listings resolving the pattern; 5,000 descriptor, 5,000 head and 5,000 manifest reads; 251 reader opens (~1,000 ranged reads) |
| warm (the window's readers open) | 4.9 s | 15,258 | the same resolution and pruning reads; no opens |

Without pruning the same search asks for 5,000 cold shards: past the budget, and past the node's shard cap of
1,000, so it is refused; with the budget raised it would be 5,000 reader opens. **What it still costs:** three
register reads per index per search, all of them repeated by the next search -- the routing cache holds a
descriptor for one second, and a digest is re-read every time because its shard could have been published
since. A digest cached against the manifest's generation, and descriptors held across queries for indices
that no longer change, are the next step; neither is needed for correctness.

Pinned by `PruningDigestTests` (the rules one at a time, forward-compatible parsing, and a simulation of
the old parser reading a manifest with a digest) and `ServerlessPruningTests`: the digest a publish writes;
six of eight daily indices skipped by a two-day range, with totals and aggregations identical to the same
search with pruning off; an owned shard never skipped, whatever its digest; the budget refusing, then
answering partially on request; and **the canary -- forty random ranges, inclusive and exclusive, on
boundaries and off, as numbers and as date strings, over random data, each total checked against brute
force.** A digest that skipped one shard it should not have is a wrong total.

**Paged listing.** `BlobContainer#listBlobsByPrefix(prefix, startAfter, limit)` lists one page of names after a
cursor, and `BlobContainer#children(startAfter, limit)` does the same for child containers:

| backend | how a page after a cursor is read |
| --- | --- |
| S3 | `StartAfter` and `MaxKeys` on `ListObjectsV2`; children resume at `<name>0`, just past the cursor's subtree |
| GCS | `startOffset` (inclusive, so the cursor is dropped) and a page size |
| filesystem | a directory walk holding only `limit` names in a bounded heap |
| Azure | the continuation marker is opaque and cannot be built from a name, so a page pages through from the prefix, skipping up to the cursor and stopping when full: bounded in memory, but a late page pays for the listing before it |
| anything else | the default: list every match and sort |

The production store wrapper (`ObjectStores.Metered`) had silently dropped every listing method it did not
override -- including `listBlobsByPrefixInSortedOrder`, so the capped pattern listing was never capped in
production -- and `deleteRegisterIfUnchanged`, so **the conditional-delete path of the last round never ran
in production**: the probe failed on the wrapper's `UnsupportedOperationException` and every node stayed on
tombstones. Both are delegated now.

Uses: `_list/indices` pages with `size` and `next_token` (core's contract and response shape); prefix
patterns resolve past one page; `DescriptorStore#listPage` resumes in the listing instead of listing the
whole container per page; and the orphaned-shard sweep became resumable -- a slice claims up to a budget of
shard containers by moving a cursor register past them *before* judging any, so racing slices do not
duplicate work and a restart resumes where the last slice stopped. The background runs one slice of 100
hourly per node, off the pass. The conformance suite gained the paged walk (every blob once, in order,
cursor excluded), changes between pages (seen only ahead of the cursor), and paged children skipping the
cursor's subtree; all pass on the filesystem and on RustFS.

Measured on RustFS 1.0.0 (`ServerlessListingMeasurementTests`), a `_list/indices` walk of **100,000
indices** in pages of 5,000, end to end through REST: 20 pages, **every page 6 listings and 5,000 GETs**,
the first and the last alike; p50 2.4 s and max 3.3 s per page. Between two pages a third of the way in, the
canary created and deleted an index on each side of the cursor: the walk returned every index that existed
throughout exactly once, the one created ahead of the cursor, and neither the one created behind it nor
the one deleted ahead of it -- the documented semantics. A walk is not a snapshot.

**Found failing before this work.** `ServerlessInstalledPluginTests` fails on Windows because a plugin jar is
still open when the test directory is removed; it fails identically on `7bfec04d070`. The two cost tests that
failed alongside it are fixed in the next round, below.

**What it adds to the flat background cost.** The orphan slice, hourly per node: a cursor read and write,
the pins' two listings, one child listing and at most a descriptor read per index among 100 containers --
about 105 requests per node-hour at worst, independent of the population. Measured with
`ServerlessScaleMeasurementTests` on RustFS 1.0.0 after this work: **1,016 requests per node-hour at both
10,000 and 100,000 indices** (1,007 before), the nine extra being the slice over the test's eight shard
containers.

**The two red cost tests, bisected.** Both had been failing since before the last round, and both now pass,
for different reasons:

- `ServerlessCostTests#testWhatAGetCosts` first failed at `5cd68a7a96b`, which moved gets onto the one-second
  routing cache and the incarnation fence's one-second confirmation. Inside a warm second ten single gets now
  read nothing, and neither does a multi-get, so "a multi-get costs well under ten singles" compared zero with
  zero. An intended cost reduction: the test is re-baselined to what still has to hold -- neither path reads
  more than the two registers an entry expiring mid-loop can cost, however many documents are asked for -- with
  the reason written beside it.
- `#testWhatASearchCosts` first failed at `fad3afbf5`. A regression, and a real one: a node that had just
  started was missing from its own membership view -- renewing its lease did not put it there, and the view is
  refreshed at most once a second -- so a search it coordinated found no reader for a shard nobody owned,
  itself included, and answered 500. A node's own view now includes the lease it last renewed, while it is
  unexpired -- added when the view is read, so a refresh still reads the lease and subscribers still hear the
  join.

Both pass on the filesystem and on MinIO, with the rest of `ServerlessCostTests`.

**Nothing between a node and its store can stand an interface default in for the backend again.**
`BlobContainerWrapperGuardTests` walks the shell's compiled classes, finds every `BlobContainer` it declares,
and fails if any of them inherits one of the interface's default methods rather than overriding it -- so a
method added to `BlobContainer` later cannot be silently dropped by a wrapper that predates it. The wrapper it
exists for, `ObjectStores.Metered`, now delegates every one of them, including the metadata-carrying reads
and writes, the conditional write and the asynchronous sorted listing it still inherited. The same class builds
stores through the production factory (`ObjectStores.create`) on a filesystem and on an S3 API and drives each
path through the whole chain: register create, read and swap, the conditional delete, put-if-absent, the
bounded and paged listings (each counted as one listing), paged children, and the node's conditional-delete
probe -- which passes through the chain on RustFS and correctly fails on MinIO. Its canary is the wrapper as it
stood at `7bfec04d070`, which the guard flags method by method; putting the bug back into `Metered` fails both
the guard and the filesystem chain test.

**A digest bug the rollup tests found, in last round's code.** A digest's bounds were normalised through
`c ? Double.valueOf(x) : Long.valueOf(y)`, and a conditional expression mixing the two boxes promotes both to
`double`. Every bound was stored as a double, so a long above 2^53 was rounded -- and rounded down is narrower,
so a shard holding exactly its maximum could be skipped by a query for that value. Dates, far below 2^53, were
unaffected. Branches now, and `PruningDigestTests#testALargeLongBoundIsKeptExactly` pins a maximum of 2^60 + 1,
through the manifest's JSON and back.

**Wide searches read a few registers, not three per index.** Last round a `logs-*` search over 5,000 hourly
indices read a descriptor, a head and a manifest for every one of them: 15,258 requests warm. Every publish now
also records its digest in a **digest rollup** -- indices grouped by the first five characters of their names,
each group spread over 64 registers, each holding per index its uuid, shard count and, per shard, the digest of
what it has published and the term of any writer holding it. A search over a prefix of five characters or more
that matches at least 32 indices (`serverless.search.rollup.min_indices`) reads the group's 64 registers first,
drops every index they rule out before reading anything of it, and skips the per-shard checks for indices the
entry already settles may match. And the incarnation fence accepts the routing read resolution just made, when it
found the same uuid within the fence's second -- a read is a read whoever made it -- so a kept index's descriptor
is read once per search, not twice.

What keeps a rollup safe, since a stale one could otherwise skip a shard that matches:

- every searchable document of an index is covered by its entry before it becomes searchable. A publish widens
  the entry before it writes its manifest; a writer records its term before it opens the shard, and a shard
  with a writer is never ruled out;
- letting go clears the term only if it is still that writer's, so a predecessor's late clear cannot unmark a
  successor;
- within an incarnation an entry only widens -- a union over the fields both sides digest, coarsened outward
  (dates to the hour) so an appending shard rewrites it about once an hour rather than every publish;
- an entry for another uuid is replaced by the new incarnation's first publish or writer, before which the new
  incarnation has nothing searchable;
- a shard with no digest yet is never ruled out.

A stale entry is therefore wider than the truth or describes nothing searchable, never narrower. The options
proposed and not taken: a longer-lived descriptor cache is unsafe past the incarnation fence's second -- a
recreated index could be judged by its predecessor's digest -- and would still leave the head and manifest
reads; per-time-bucket rollups need the coordinator to know an index's time bucket from its name, which a
name-grouped register does not.

Measured on RustFS 1.0.0 (`ServerlessWideSearchMeasurementTests`, 5,000 hourly indices, a 250-hour window, no
injected delay):

| pass | before (last round) | after |
| --- | ---: | ---: |
| cold: no reader open | 16,513 requests, 5.5 s | 1,828 requests, 2.3 s |
| warm: the window's 251 readers open | 15,258 requests, 4.9 s | 322 requests, 1.12-1.21 s over three runs |

The warm 322: six listings to resolve the pattern, the group's 64 rollup registers, one descriptor read for each
of the 251 indices searched, and one members read. The cold pass adds the 251 reader opens. **The request target
-- warm under 1,000 -- is met; the latency target, under a second, is not quite.** Timed phase by phase, about
0.7 s of the warm second is listing the pattern's 5,000 names: six pages of a thousand, one after another,
because each resumes where the last stopped. The rollups took 80 ms, the descriptor reads 35 ms and the 251
queries 160 ms. Listing ranges of the prefix concurrently, cut at a previous listing's split points, was built
and measured: on RustFS ten concurrent listings took as long as six serial ones -- it serves listings one at a
time -- so it cost four requests and saved nothing, and was taken out. What would remove the listing is keeping
each group's names in its rollups, written before a descriptor or alias is created so a name can be missing only
by not existing; that puts a register write on index creation, which this round did not want to pay without a
store where listing is the bottleneck to justify it.

**What the rollups add elsewhere.** A writer's mark is one register swap at activation and one at an idle
release -- a read as well when this node's copy of the bucket is stale. A publish writes the rollup only when its
coarsened digest widens what the entry holds: a shard appending in time order about once an hour, a shard whose
values do not move never. Idle nodes publish nothing, so the flat background cost is unchanged.

Pinned by `DigestRollupsTests` (widening only, replacement by a new incarnation, a writer's mark and a stale
clear, unknown shards never ruled out, concurrent publishes all landing, coarsening only widening) and by
`ServerlessPruningTests`: the forty-range conservativeness canary again with the rollups consulted for every
pattern; an index deleted and recreated with a document inside the range is found; a shard with a writer is
not ruled out; and the canary's own canary -- an entry planted narrower than its shard makes the total wrong,
which is what the canary's totals would catch.

**Still open, in the order the cost model ranks them:** the per-shard head poll above, which needs a signal
carrying evidence of ownership rather than a cheaper timer; and the sweeps that still walk the whole
population — points in time, snapshots — on a slow cadence. Neither is a request-path cost.

**And the one that is not a cost at all.** Every argument above assumes `compareAndSwapRegister` behaves
on the store underneath. The checker passes against MinIO and SeaweedFS and has never been pointed at a
real cloud provider. That remains the largest open risk on this branch.

It is no longer *ungrounded*, though, which is a different thing from being closed. AWS documents both
halves of what this design actually needs. For mutual exclusion: "If multiple conditional writes or copies
occur for the same object name, the first write operation to finish succeeds. Amazon S3 then fails
subsequent writes with a `412 Precondition Failed` response." For the reads and listings every other path
depends on: since December 2020, "all S3 GET, PUT, and LIST operations ... are now strongly consistent", in
all regions, list operations included. So the register is a documented primitive rather than an assumed
one, and the listing-driven paths — WAL replay, prefix resolution, every sweep — rest on a stated guarantee
rather than on a hope.

**What that does not establish, and it matters.** A specification is what a provider commits to, not
evidence that an implementation delivers it under partition, throttling or failover — which is precisely
what a linearizability checker is for. It says nothing about the S3-compatible stores that are not S3, and
`ObjectStores` will point this shell at any of them. It is a floor under the argument, not a proof.

**Reading the contract found a real defect that no amount of testing against MinIO would have.** The same
page documents two more outcomes of the same races: `409 Conflict` "in the case of concurrent requests",
and, for `If-Match`, `404 Not Found` when a concurrent delete wins or the current version is a delete
marker. `S3BlobContainer` mapped only `412` to a conflict and threw the other two, so a lost race against a
routine deletion — an index delete removing shard heads, a tombstone sweep, a node releasing its lease —
surfaced as "the object store is broken" rather than "someone else owns this now". All three mean the write
did not happen, so all three are now conflicts. The conformance endpoints do not produce 409 or 404 for
these races, which is exactly why the specification was worth reading.

## What a fleet run under failure found

Six shell JVMs against RustFS 1.0.0 with a million indices, 200 writes a second, and seven ten-minute scenarios
of deliberate failure, every write attempt recorded in a client ledger and every acknowledged one read back
afterwards (`:serverless:testkit:fleetTest`; the method and every figure are in
[`scale-test-results.md`](scale-test-results.md)).

| scenario | acked | lost | refused but visible | takeover against the 30 s TTL |
| --- | ---: | ---: | ---: | --- |
| steady | 102,331 | 0 | 0 | |
| kill -9 of the busiest node | 99,719 | 0 | 0 | 267 of 267; p50 59.4 s, p99 120.6 s |
| pause past the lease | 93,573 | 0 | 0 | 209 of 250 while frozen; p50 44.0 s; the zombie acknowledged nothing |
| partition from the store | 95,407 | 0 | 0 | 0 of 2,120 writes acknowledged after its lease ran out |
| 503 SlowDown, 60 s | 5,033 | 0 | 0 | recovered 20 s after the burst |
| rolling restart | 66,102 | 0 | 0 | each node back in 5-12 s |
| delete/recreate storm | 77,807 | 0 | 0 | 32,335 writes to deleted names refused, none visible |

**No acknowledged write was lost.** The runs found five defects first, and each was fixed with a canary that fails
without it:

- **A shard could wedge with a head and no writer**, its acknowledged writes in the log and unreadable for as long
  as the owning node lived. A reader and its replacing writer share a `ShardId`, and the stale-reader pass closed
  by id after a store round trip in which a write had made the shard a writer. Reader releases now check and close
  in one step, and a node named owner with nothing open re-takes the shard instead of answering "retry" or refusing
  it at the shard cap (`ServerlessReaderBecomesWriterTests`, `ServerlessEvictionTests`).
- **Forwarded writes deadlocked across nodes**: coordinators' WRITE threads waited on owners that applied forwarded
  writes on WRITE. Owners now use pools that forward nothing further (`ServerlessForwardingTests`).
- **One mget's items raced to open one reader** and all but one failed; reader opens are single-flight.
- **A filesystem store listed half-written blobs**, which the log's fencing took for fences; exclusive writes now
  publish by hard link (`FsBlobContainerTests`, and `FsLogFencingTests`, which had been failing about one run in ten).
- **A release could skip its publish** for an operation still in flight at the previous flush.

**What it measured that is still open.** A wide `logs-*` search fails outright at a million indices: RustFS cannot
list a million names under one prefix, so the listing the resolution depends on times out server-side. Idle, a node
costs about 190 store requests per held shard per hour, most of them the 30-second descriptor refresh. A shard
nobody owns can briefly be read from a commit behind its log after a lapsed owner gives back its head unpublished;
every such read in these runs resolved once the shard was taken. And a fleet-wide SlowDown stalls writes for the
client's whole timeout rather than shedding them early.

### Goal 9: those items, and a starved pool

Goal 9 closed or measured each of them, re-running the fleet at a million indices after each change; the detail
is in [`scale-test-results.md`](scale-test-results.md#goal-9-the-five-open-items-and-what-a-starved-pool-had-been-hiding).
**Every run kept 0 lost and 0 refused-but-visible, and a new stale-read check found 0 searches answering from a
commit behind the log.**

| | before | after |
| --- | --- | --- |
| a shard nobody owns, read behind its log | possible | refused: manifests record the log position they cover (`shard_behind_log`) |
| kill -9 takeover p50 / p99 | 59.4 s / 120.6 s | **35.3 s / 43.2 s**: survivors take a dead node's shards when its lease runs out |
| wide `logs-*` at a million indices | the store listing failed | resolved from rollups in about 0.2 s, no listing |
| idle requests per held shard-hour | 190 | 86-106 where shards stayed held; the 30 s descriptor re-read is a five-minute backstop, and changes arrive through the fence every operation passes |
| SlowDown | writes stalled to the timeout | refused early with 429 and `Retry-After`, never applied |

The runs also found that **shard activations starved the GENERIC pool**: writes waiting for an activation filled
every thread, and the activations they waited for queued behind them for minutes. That was most of every earlier
run's steady-state errors; activations now have their own pool (`serverless_activation`). Fixed besides: the global
checkpoint never advanced, so a writer's safe commit could name deleted files and fail its engine; a failed writer
kept its head; a mapping change made through another node failed the owner's next write; a conflicting dynamic
mapping addition blocked every later one on that node.

Still open: rollup "owned" marks left by crashed owners are never cleared, so a wide search cannot rule those shards
out; survivors at their shard cap re-take a frozen node's shards slowly; the idle figure still mixes in the cost of
going idle, so the 5× target is not yet shown; and the write limiter sheds load on this single-drive store even
without injected throttling, and took 116 s to recover from a SlowDown burst.

### Goal 11: a verdict that needs no forensics, measured clean

The fleet ledger now decides what the read-back cannot reach from the store -- commit, then log -- and answers
verified, lost with its evidence, or unreached, which is inconclusive rather than a loss; the 415 writes two earlier
runs reported lost all verify, and canaries cover a planted loss, a handover mid-check and an unreadable store. On a
fresh 1M-index bucket, drained first: 0 lost, 0 refused-but-visible, 0 unreached in every scenario; a one-month
`logs-*` search fits under the 10,000-index cap (about 1,000 indices not ruled out); the write limiter, which had been
cutting on single slow appends, settles at limits of 777-1,024 and refuses nothing in steady state; idle cost on nodes
that kept their shards is 141 requests per held shard-hour (1.35x below Goal 8, short of 5x); the drained janitor
costs about 1,750 requests per node-hour. Open: steady write p50 of 3-5 s now that nothing is refused, SlowDown
recovery, kill -9 p99 of 63 s. Details in [`scale-test-results.md`](scale-test-results.md#goal-11-a-verdict-that-needs-no-forensics-measured-clean).

### After Goal 11: latency, two stale reads, and a loss traced to writes past a fence

**Steady write p50 fell from 3-5 s to about 0.3 s.** The cause was a local fsync, under the shard's exclusive guard,
of a cache that every open rebuilds from the store anyway.

**Two ways a reader could serve a commit behind an acknowledged write, with no failure, are fixed**, each with a
test that failed first:
- a reader trusted "nobody owns it" while a writer came and went unpublished;
- a reader stayed on a commit that a successor had replaced and trimmed the log behind.

**One run lost three acknowledged writes** (`run1790974395481`). Each lost seqNo had been acknowledged twice: a
superseded writer's record landed after its successor had fenced the log, and the next successor replayed it first,
skipped the newer write as already processed, and trimmed it. Records could land past a fence three ways, all now
closed, each with a test that failed first:
- pipelined appends;
- a takeover skipping terms whose only fence was the writer's own;
- writes reaching a reader's writable engine.

An open now refuses, naming both operations, when the log holds one seqNo twice. The final run was clean in all six
scenarios.

**Open:**
- SlowDown recovery;
- kill -9 takeover, now slower: p50 52-99 s and p99 84-288 s;
- many readers refusing where nobody is writing;
- `logs-0000120`, which refuses to open on its recorded collision until repaired.

Details in [`scale-test-results.md`](scale-test-results.md#after-goal-11-latency-two-stale-reads-and-a-loss-traced-to-writes-past-a-fence).

### Goal 10: cleaning up after a crash

A crashed node left every shard it held named to it -- heads, claims, rollup entries -- and the only cleanup was a
takeover. Now ([`scale-test-results.md`](scale-test-results.md#goal-10-cleaning-up-after-a-crash)):

- **A dead owner's shards are given back, not taken, when everything they hold is published**: lease revoked, head
  moved to the next term with no owner, older terms sealed, then the mark and claim cleared. Only a shard whose log is
  ahead of its commit is taken and replayed. Pause re-takes went from 44 of 376 to 141 of 147; kill -9 takeover to
  p50 28.9 s, p99 36.7 s.
- **A janitor on every node** settles claims of nodes long dead, marks with a dead owner or none, and replays and
  gives back shards left behind their log -- a bounded amount a pass, split between nodes by hash
  (`serverless.janitor.interval_millis`, default one minute). Old commits are judged from their commit point rather
  than replayed.
- **An index that holds nothing keeps no rollup entry.** The biggest leftover was not a crash's: dormant indices
  opened only to be read left entries with no digest, which no wide search can rule out -- over half of the `logs-`
  group. Those are removed, by a swap a writer's mark always wins. A `logs-*` search for next month went from 63,020
  indices it could not rule out to 34,647 over one run, and falling.

Correctness held in every run: 0 lost, 0 refused-but-visible, 0 silently behind; two runs whose ledgers reported
losses had every write found on inspection. Steady write latency was higher in these runs (p50 4-8 s) while the
janitor worked through the backlog on a single-drive store, and should be measured again once it is gone.

## How it is tested

671 tests in `:serverless:testkit:test` and 37 in `s3Test`, plus `pluginTest` (a real plugin installed from its
assembled zip), `processTest` (forked JVMs), `tlsTest` (a real TLS handshake, security manager off),
`s3Test` (against live MinIO and SeaweedFS endpoints, which assume-skips with no endpoint
reachable) and `fleetTest` (a fleet under load and failure against a real store; see above).

Core's own suite passes on this base too: `:server:test` is 20,643 tests, 0 failures. That matters
here because the 364 lines this branch adds to `server/` are additive, and this is the evidence
rather than the assumption.

Both dependency-direction rules are enforced as build tasks:
`:serverless:shell:checkServerDoesNotReferenceServerless` and
`:serverless:auth:checkAuthDoesNotReferenceTheShell`.

Every load-bearing claim has a planted-defect canary: the defect is introduced, the failing test is watched,
and the defect reverted. A compile failure does not count as caught. Two canaries in this run passed, which
is how a vacuous test and a piece of dead code were found.
