# The serverless shell: where it actually is

Per-milestone notes (`m10`…`m40`) record what was true when each was written, which is the point of them.
Their "what is still missing" sections do not — those age badly, and several had been closed by later work
while still reading as open. **This document supersedes all of them.** If a milestone note and this
disagree, this is right.

## What works

**Documents.** Write, get, delete, `_bulk`, `_mget`, with `_source` filtering on reads. Routing is resolved
once per request, so a multi-get's object-store cost is the shards it touches and not the documents it asks
for: ten documents cost 4 requests and twenty cost 4, against 30 for the same ten fetched one at a time. A write is durable in a write-ahead log before it is
acknowledged, and replayed by a successor. A get is routed to the shard's owner so it sees writes a search
cannot yet; an unowned shard is served from its published commit, so a get works against an index that has
scaled to zero.

**Search.** The full query DSL, sorting, aggregations, `search_after`, `_source` filtering, and several
indices named in one request. Shards fan out concurrently; the window is cut once over everything; coverage
(`_shards` and `complete`) is reported on every answer, and a search *no* shard could answer is an error
rather than an empty result. `ignore_unavailable` turns a named index that cannot be reached into a
`skipped` entry rather than a failure, and only when the caller asked for that.

**Point in time.** `POST /{index}/_pit` freezes each shard's commit into a record and returns an id; a
search quoting it reads that commit however far the writer has moved on, which is what makes paging with
`search_after` return a consistent result set rather than a moving one. The garbage collector treats a
live view's blobs as referenced — without that the sweep would collect a caller's commit halfway through
their paging. The keep-alive is absolute rather than sliding, and a released or expired view answers 404
rather than emptily.

**Aliases.** A name that stands for some indices, created in the same register namespace as an index
descriptor — so an index and an alias cannot take the same name, and resolving a name is one read rather
than a miss on one namespace followed by a lookup in another. One level; an alias does not name another
alias.

**Indices.** Create, describe, delete, alias. Storage is keyed by index uuid, so a name reused after a delete
cannot inherit the previous index's data. Deletion reclaims the bytes, and a sweep collects shard
containers no index owns.

**Scaling to zero and back.** A node takes a shard by compare-and-swap on a register, renews a lease,
publishes commits to the object store, and releases shards that have gone idle. There is no cluster
manager, no consensus process, and no cluster-wide state.

**Maintenance the deployment does for itself.** A reconcile pass lets go of readers whose commit has been
superseded, so nothing serves a stale commit indefinitely; reaps expired points in time and closes the
shards held for them; and sweeps the garbage left by what it has just published. The sweep waits until a
blob has been unreferenced across two passes, because a reader may still be reading it — the grace is what
makes running the collector on a schedule different from running it by hand.

**TLS on the transport, from a plugin.** A plugin supplies a `SecureSettingsFactory`, the shell passes it
to the network module, and core's own `SecureNetty4Transport` carries the traffic — proved by a write
forwarded between two nodes over a real handshake. The shell had been passing an empty collection here, so
a plugin naming the secure transport was told the type did not exist: TLS through a plugin was impossible
rather than undemonstrated. Node *identity* still does not travel with a forwarded request, so this
protects the wire without yet authenticating the peer.

**Plugins.** Installed from `plugins/` through core's own loader, or named on the classpath by
configuration. `createComponents`, REST handlers, the request wrapper, `onNodeStarted`, analysis, mappers,
search plugins, network plugins and their transport interceptors, system-index declarations, and
`ActionFilter`s all work. The `Client` and the `NodeClient` a handler receives are the same allowlist.

**Authentication and authorization.** The shell's own authentication is a plugin, depending on `:server`
and not on the shell. HTTP Basic at the request seam, accounts in a system index unreachable over REST,
salted PBKDF2, one configured account in the keystore checked before the store so an unreadable object
store is not a locked door. A separate plugin's `ActionFilter`s can refuse by action name and caller —
proved by a reader being denied a write an administrator is allowed — and can *rewrite* what a search or a
get returns, which is what field- and document-level security are made of: a filter that removes a field
from every hit removes it from `_search`, `_doc/{id}` and `_mget` alike.

**Bounds.** Real circuit breakers, so an aggregation past the limit is refused and the node keeps serving;
and core's indexing pressure on the write paths, so a batch larger than the node's budget is rejected with
a 429 rather than accepted into memory it does not have. A node at its shard limit evicts its least
recently used idle shard to make room rather than refusing, and `GET /_serverless/stats` reports the
counters those refusals are decided from — breakers, in-flight write bytes, and what the node is holding.

## What is deliberately refused

These are decisions, not gaps. Each answers 501 with a reason.

| Refused | Because |
| --- | --- |
| Index patterns (`logs-*`) | Resolving one means enumerating the deployment, which §6.3 does not offer on a request path. A wildcard matched against what a node happens to know would be silently partial. |
| Enumerating indices | Same rule, stated directly. |
| `/_cluster/*` | There is no cluster-wide state; a node-local answer would mislead. |
| `collapse`, `suggest`, `profile` | Each needs cross-shard machinery that does not exist yet. |
| Two request wrappers, or two plugins contributing one setting | Which one wins would depend on load order. |
| `IndexNameExpressionResolver` for plugins | A node's `ClusterState` holds only the indices it has open, so resolving would answer a wildcard with a subset — which for privilege evaluation fails open. |
| Scripting | No engines registered; a plugin gets core's own "no lang registered". |
| On-behalf-of and service-account tokens | They delegate authority, and this deployment authenticates without a general authorization model to delegate. |

## What is genuinely missing

**Open, and mine to do:**

- Node-to-node forwarding carries no identity, so filters run on the coordinating node only and the
  transport port must be treated as trusted infrastructure. Making identity travel without node
  authentication would make that assumption load-bearing.
- Dropping hits on a per-document rule is possible by the same mechanism as redacting them, and is not
  demonstrated. Only redaction has a test.
- No action registry, so a plugin's own transport actions (Security's config-update API) cannot run.
- A point in time is node-local once opened: the record is in the object store and any node can serve it,
  but two nodes serving one view open it twice. No slicing.
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
  rests on `compareAndSwapRegister` being genuinely linearizable. The conformance suite runs against MinIO
  and passes; it has never run against real S3, GCS or R2, and that needs credentials. Everything else in
  this document is downstream of that assumption.

## Decisions a reader should know about

- `server/` is untouched, and a build task fails if any file under it references `org.opensearch.serverless`.
- The REST surface is an allowlist. Anything not registered answers 404 or an explicit 501, never an empty
  success.
- Storage layout changed in M32 (uuid in the path); an existing deployment's data is not found by a node
  running this build. Pre-release.
- The authentication plugin authenticates and ships no policy beyond one rule: only the configured account
  may manage accounts. It is one `if`, and is named as one.

## How it is tested

310 tests across five Gradle tasks — `test`, `pluginTest` (a real plugin installed from its assembled zip),
`processTest` (forked JVMs), `tlsTest` (a real TLS handshake, security manager off) and `s3Test` (against a
live MinIO) — none skipped.

Every load-bearing claim has a planted-defect canary: the defect is introduced, the failing test is watched,
and the defect reverted. A compile failure does not count as caught. Two canaries in this run passed, which
is how a vacuous test and a piece of dead code were found.
