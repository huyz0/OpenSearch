# The serverless shell: where it actually is

Per-milestone notes (`m10`…`m36`) record what was true when each was written, which is the point of them.
Their "what is still missing" sections do not — those age badly, and several had been closed by later work
while still reading as open. **This document supersedes all of them.** If a milestone note and this
disagree, this is right.

## What works

**Documents.** Write, get, delete, `_bulk`, `_mget`. A write is durable in a write-ahead log before it is
acknowledged, and replayed by a successor. A get is routed to the shard's owner so it sees writes a search
cannot yet; an unowned shard is served from its published commit, so a get works against an index that has
scaled to zero.

**Search.** The full query DSL, sorting, aggregations, `search_after`, `_source` filtering, and several
indices named in one request. Shards fan out concurrently; the window is cut once over everything; coverage
(`_shards` and `complete`) is reported on every answer, and a search *no* shard could answer is an error
rather than an empty result.

**Indices.** Create, describe, delete. Storage is keyed by index uuid, so a name reused after a delete
cannot inherit the previous index's data. Deletion reclaims the bytes, and a sweep collects shard
containers no index owns.

**Scaling to zero and back.** A node takes a shard by compare-and-swap on a register, renews a lease,
publishes commits to the object store, and releases shards that have gone idle. There is no cluster
manager, no consensus process, and no cluster-wide state.

**Plugins.** Installed from `plugins/` through core's own loader, or named on the classpath by
configuration. `createComponents`, REST handlers, the request wrapper, `onNodeStarted`, analysis, mappers,
search plugins, network plugins and their transport interceptors, system-index declarations, and
`ActionFilter`s all work. The `Client` and the `NodeClient` a handler receives are the same allowlist.

**Authentication and authorization.** The shell's own authentication is a plugin, depending on `:server`
and not on the shell. HTTP Basic at the request seam, accounts in a system index unreachable over REST,
salted PBKDF2, one configured account in the keystore checked before the store so an unreadable object
store is not a locked door. A separate plugin's `ActionFilter`s can refuse by action name and caller —
proved by a reader being denied a write an administrator is allowed.

**Bounds.** Real circuit breakers, so an aggregation past the limit is refused and the node keeps serving;
and core's indexing pressure on the write paths, so a batch larger than the node's budget is rejected with
a 429 rather than accepted into memory it does not have.

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

- No `Transport` has been substituted, so TLS through a plugin is *enabled* rather than demonstrated. This
  is the single largest gap in hosting OpenSearch Security: the shell can carry the parts of it that
  decide, and not the parts that protect the wire.
- Node-to-node forwarding carries no identity, so filters run on the coordinating node only and the
  transport port must be treated as trusted infrastructure. Making identity travel without node
  authentication would make that assumption load-bearing.
- Filters see requests, not responses — so document-level security and field redaction cannot work.
- No action registry, so a plugin's own transport actions (Security's config-update API) cannot run.
- No aliases, no `ignore_unavailable`, no point-in-time readers.
- `onIndexModule` fires only for plugins loaded from disk.
- The orphan sweep is not resumable and nothing runs it on a schedule.
- No `_nodes/stats`, so an operator can see a refusal and not the trend that led to it.
- `maxShardsHeld` refuses rather than evicting.

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

283 tests across four Gradle tasks — `test`, `pluginTest` (a real plugin installed from its assembled zip),
`processTest` (forked JVMs) and `s3Test` (against a live MinIO) — none skipped.

Every load-bearing claim has a planted-defect canary: the defect is introduced, the failing test is watched,
and the defect reverted. A compile failure does not count as caught. Two canaries in this run passed, which
is how a vacuous test and a piece of dead code were found.
