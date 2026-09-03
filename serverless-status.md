# The serverless shell: where it actually is

Per-milestone notes (`m10`…`m41`) record what was true when each was written, which is the point of them.
Their "what is still missing" sections do not — those age badly, and several had been closed by later work
while still reading as open. **This document supersedes all of them.** If a milestone note and this
disagree, this is right.

## What works

**Documents.** Write, get, delete, `_bulk`, `_mget`, with `_source` filtering on reads. The routes an
OpenSearch client actually calls: `PUT|POST /{index}/_doc/{id}`, `POST /{index}/_doc` for a generated id, and
`PUT|POST /{index}/_create/{id}` for create-if-absent — which is one constant on the write path's own engine
call (`MATCH_DELETED`), so the comparison happens under the per-document lock the engine already holds rather
than as a read followed by a write. `_bulk` does `create` and per-item `if_seq_no`/`if_primary_term` too, and
reports a lost condition as a 409 ([`m50-api-compatibility-notes.md`](m50-api-compatibility-notes.md)). Every write, get
and bulk item reports `_seq_no`, `_primary_term` and `_version`, and **conditional writes work**:
`if_seq_no`/`if_primary_term` on a write, a delete or an `_update` are passed to the engine, which performs
the compare-and-swap against its own live version map; a lost race is a 409. The numbers survive a
failover — the write-ahead log records each operation's sequence identity and a successor replays it as
that operation rather than as a new one, so a token minted before a writer died is still honoured by its
successor ([`m48-sequence-numbers-notes.md`](m48-sequence-numbers-notes.md)). `version` (external
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
([`m49-fencing-notes.md`](m49-fencing-notes.md)). A get is routed to the shard's owner so it sees writes a search
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
asked for the durable version yet. See [`m13-hysteresis-notes.md`](m13-hysteresis-notes.md).

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

**Indices.** `PUT /{index}` reads OpenSearch's own create envelope — `settings.number_of_shards` is honoured,
`mappings` is the mapping, and any other setting is carried into the descriptor and layered into
`IndexMetadata`, because the data plane under this shell is core's and understands them. The older bare-mapping
body with `?shards=` still works. `GET /{index}`, `/_mapping` and `/_settings` answer OpenSearch's shape, so
configuration can be read back; `HEAD /{index}` answers whether an index exists. `GET|POST /_search` with no
index searches everything, and `q=` is core's query-string parser rather than a lookalike.

## What is deliberately refused

These are decisions, not gaps. Each answers 501 with a reason.

| Refused | Because |
| --- | --- |
| Patterns that are not a prefix (`*-2026`, `lo*s-a`, `logs-?`) | A prefix can be answered by one bounded listing; these cannot be answered by a listing at all, only by reading every index name in the deployment and matching each. Supporting a wildcard syntax whose cost depends on where the caller put the star would be worse than the split. |
| Enumerating indices (`/_serverless/indices`) | An inventory operation, not a serving one — it is asked to return everything, where a pattern is capped and refuses when the cap is exceeded. Look an index up by name, or run an offline inventory. |
| `/_cluster/*` except `settings` | There is no cluster-wide state; a node-local answer would mislead. |
| `collapse`, `suggest`, `profile` | Each needs cross-shard machinery that does not exist yet. |
| Two request wrappers, or two plugins contributing one setting | Which one wins would depend on load order. |
| `IndexNameExpressionResolver` for plugins | A node's `ClusterState` holds only the indices it has open, so resolving would answer a wildcard with a subset — which for privilege evaluation fails open. |
| Scripting | No engines registered; a plugin gets core's own "no lang registered". |
| On-behalf-of and service-account tokens | They delegate authority, and this deployment authenticates without a general authorization model to delegate. |

## What is genuinely missing

**Open, and mine to do:**

- Filters run once, on the node the request reached, and not again on the node a write is forwarded to.
  That is a design choice — re-running every filter per hop would make one that counts or rate-limits wrong
  — but it does mean a plugin cannot enforce at the shard. (A caller *does* travel: a thread-context header
  set by a plugin arrives on the far side, which is core's own mechanism and is tested.)
- A plugin's action runs on the node the request reached: nothing forwards one, and no identity crosses a
  transport hop, so an action that must run somewhere particular cannot ask.
- A point in time is still node-local once opened: the record is in the object store and any node can serve
  it, but placement is only a hint, so two nodes can still each end up opening one view under an unlucky or
  stale routing decision — narrowed, not closed, by giving frozen search the same placement a live search
  already had. No slicing.
- One fencing window is still open: the shard-head is won at the compare-and-swap, but the log is sealed
  slightly later, when the shard is opened, so a predecessor's appends in between are still replayed.
  Closing it means sealing at the swap, which the path that opens a shard from projected truth rather than
  from an acquisition does not have. The unbounded half is closed.
- `update` inside `_bulk` is refused: it is a partial merge, which has to read the current document before
  writing one, and the batch path applies without reading. Closing it changes what a batch is, so it is a
  design decision rather than plumbing. `POST /{index}/_update/{id}` does the merge.
- Settings are write-once. They are honoured at creation and readable afterwards, but `PUT /{index}/_settings`
  is not routed and there is no settings service behind it.
- `GET /{index}` omits `aliases` rather than reporting an empty object: resolving an index's aliases needs a
  reverse lookup this design does not offer, and an empty one would be a confident wrong answer.
- The whole-deployment orphan sweep (shard containers no index owns) is still manual; the per-shard sweep
  runs on every pass. A reader whose node cannot reach the object store for longer than the grace can lose
  the commit it is reading — it fails with an error rather than answering wrongly, but it is a real limit.

**Out of scope while `server/` may not change:**

- `onIndexModule` fires only for plugins loaded from disk. Firing it for a plugin passed in as an instance
  needs `PluginsService` to accept instances.
- The orphan sweep is not resumable; making it so needs paged `children()` on `BlobContainer`.
- `/_nodes/stats` stays refused with the rest of the cluster surface — a node cannot speak for another
  without cluster-wide state. `GET /_serverless/stats` answers for the node it was sent to, which is the
  honest scope.

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

- `server/` is untouched, and a build task fails if any file under it references `org.opensearch.serverless`.
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
  [`m47-api-compatibility-notes.md`](m47-api-compatibility-notes.md).

## How it is tested

437 tests across five Gradle tasks — `test`, `pluginTest` (a real plugin installed from its assembled zip),
`processTest` (forked JVMs), `tlsTest` (a real TLS handshake, security manager off) and `s3Test` (against
live MinIO and SeaweedFS endpoints) — none skipped.

Every load-bearing claim has a planted-defect canary: the defect is introduced, the failing test is watched,
and the defect reverted. A compile failure does not count as caught. Two canaries in this run passed, which
is how a vacuous test and a piece of dead code were found.
