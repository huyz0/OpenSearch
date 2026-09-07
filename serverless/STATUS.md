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
acknowledged, and replayed by a successor. **A writer that has lost its shard but not yet noticed is fenced
out of that log**: it refuses to write past the lease deadline it last published for itself, which needs no
I/O and so is checked on every write; and a successor seals the log at takeover, durably, so nothing
appended after ownership moved is ever replayed — by that successor or by any node after it
(`m49-fencing-notes.md` (on `feature/serverlessnode`)). A get is routed to the shard's owner so it sees writes a search
cannot yet; an unowned shard is served from its published commit, so a get works against an index that has
scaled to zero.

**Search.** The full query DSL, sorting, aggregations, `search_after`, `_source` filtering, several
indices named in one request, and **prefix patterns** (`logs-*`) — resolved by one bounded listing whose
cost is set by a cap rather than by the population, measured at 1 object-store request against 3 indices
and 1 against 63, on a filesystem and on S3 alike. A pattern matching more than the cap is refused rather
than truncated, because an answer that stopped at a limit looks exactly like a complete one. Shards fan out concurrently; the window is cut once over everything; coverage
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
indices using it. `GET /_list/indices/{prefix}*` lists indices through one bounded listing and **refuses a
prefix matching more than the cap rather than truncating**, which is why it is `_list` and not `_cat`:
OpenSearch added `_list` for exactly this reason (`m53-discovery-notes.md` (on `feature/serverlessnode`)).

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
| Enumerating indices past the pattern cap | `GET /_list/indices` and `GET /_cat/indices` are served now, as the empty prefix under the same bounded listing every other prefix takes -- one capped `listBlobsByPrefixInSortedOrder`. Past the cap they are refused rather than truncated, which is the point: a page that looks complete and is not is worse than a refusal. |
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
- A plugin's action can now be run on a node the plugin chose, and the hop is authenticated. This entry
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
- One fencing window is narrowed but not closed. The log is now sealed at the compare-and-swap that
  transfers ownership rather than later, when the shard is opened, so the stretch in which a predecessor's
  appends still fall in front of the cutoff is one blob write instead of a shard creation, a local-file
  drop and a translog bootstrap. It cannot be closed by moving the seal any further: a swap and a seal are
  two object-store operations with no transaction spanning them. Closing it entirely would need the
  register itself to carry the seal.
- `update` inside `_bulk` is refused: it is a partial merge, which has to read the current document before
  writing one, and the batch path applies without reading. Closing it changes what a batch is, so it is a
  design decision rather than plumbing. `POST /{index}/_update/{id}` does the merge.
- Static index settings cannot be changed at all, by any route. Classic OpenSearch offers close-change-open
  and there is no close here, so the only way to change one is to create a new index. Dynamic settings change
  freely (M54).
- There is no cursor over indices. `GET /_list/indices/{prefix}*` answers in one bounded page or refuses;
  paginating a deployment whose indices fit under no usable prefix is not possible, because resuming a listing
  needs `start-after` and core's `BlobContainer` does not expose it.
- The whole-deployment orphan sweep (shard containers no index owns) is still manual; the per-shard sweep
  runs on every pass. A reader whose node cannot reach the object store for longer than the grace can lose
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

## How it is tested

549 tests in `:serverless:testkit:test`, plus `pluginTest` (a real plugin installed from its
assembled zip), `processTest` (forked JVMs), `tlsTest` (a real TLS handshake, security manager off)
and `s3Test` (against live MinIO and SeaweedFS endpoints, which assume-skips with no endpoint
reachable).

Core's own suite passes on this base too: `:server:test` is 20,643 tests, 0 failures. That matters
here because the 364 lines this branch adds to `server/` are additive, and this is the evidence
rather than the assumption.

Both dependency-direction rules are enforced as build tasks:
`:serverless:shell:checkServerDoesNotReferenceServerless` and
`:serverless:auth:checkAuthDoesNotReferenceTheShell`.

Every load-bearing claim has a planted-defect canary: the defect is introduced, the failing test is watched,
and the defect reverted. A compile failure does not count as caught. Two canaries in this run passed, which
is how a vacuous test and a piece of dead code were found.
