# Deep review of the serverless shell and auth plugin — implementation against goal

September 2026, after M63 and the two cost commits (`5339c922d80`, `a1422b2f83f`). Seven code-grounded
reviews ran in parallel, each reading the governing RFC sections for its area and then the source: the
goal match (shell RFC decisions and milestones), the metadata plane and control loop, storage and
publication, the auth plugin and plugin surface, the write and read paths, the search and admin surface,
and lifecycle, scale and cost. Every headline finding below was re-verified by reading the cited lines.
Paths are under `serverless/shell/src/main/java/org/opensearch/serverless/` unless stated. The seven full
reports, with every finding's interleaving and fix, are in the session scratchpad
(`review-A-goals.md` … `review-F-lifecycle.md`).

> **Status, the next day:** every finding below has been addressed in the working tree; what each fix is and
> how it was verified is in [`m64-deep-review-fixes-notes.md`](m64-deep-review-fixes-notes.md). The text
> below is left as written so the fixes can be read against the findings.

**The short answer to "can we fix anything":** yes, and some of it must be fixed before the durability
claim is true. Three independent reviews converged on one rule the code does not enforce — *a node
whose own lease lapsed has lost every shard it held* — and each found a different acknowledged-write-loss
window that follows from its absence. None needs clock skew. The rest is a long tail of real but
bounded defects, a cost model that has drifted from the RFC's at fleet scale, and a set of design
decisions the code silently no longer honours.

---

## 1. Does the implementation match the goal?

The central move holds. `ClusterState` is a node-local projection built from registers
(`LocalViewProjector`), applied through `ClusterApplier`, with the shard-head term fed as both primary
term and applying version; there is no `Coordinator`, `AllocationService`, discovery or Guice anywhere,
and the direction check and the control-plane absence scan are real tests. D2 (allowlisted surface,
501 with a reason) holds, pinned by the spec-coverage test. The register map, CAS activation with
loser-routes-to-winner, term-scoped data paths, the manifest-generation fence, the WAL seal and batched
node leases are all genuinely realised.

Where the code has drifted from the written decisions:

| Decision | Status | Evidence |
|---|---|---|
| §5.2 absence from a projected view is never a removal signal | **Contradicted** | `ServerlessNode.syncFrom` closes any open shard absent from the node's *claims listing*, which `RegisterMap` itself calls "a hint, not truth". A missed entry or a failed claim write after a won CAS releases a shard whose head still names the node. It also evicts every reader on a dual-role node per activation. |
| §9.3 no register for the membership list | **Contradicted** | The `members` register added in `5339c922d80` is exactly that: one register with the whole fleet as its writer set. `RegisterMap.java:31-33` now documents something false. |
| §9.3 no CAS on a request path | **Contradicted** (one path) | `growMapping` performs a descriptor CAS from the bulk and single-write paths, four immediate retries, no coalescing. |
| §6.3 data-plane actions reused unmodified | **Contradicted** | None of `TransportShardBulkAction`, the search phases or the replication family are used; the shell built ~19.6k lines of its own request path. The allowlist capped the surface, not the cost. |
| D4 / §8.1 four narrow interfaces added to `server` | **Missing** | `server/` has zero changed files; the status doc reinstated the pre-D4 rule and blames three limitations on it. |
| §6.2 `MetaStateService` never constructed | **Contradicted** | Constructed for real; the absence scan checks four classes and would not catch it. |
| §2.3 `NodeConnectionsService` real, driven by membership | **Contradicted** | A stub; the router opens connections ad hoc and never closes them. |
| §9.5 epoch piggybacked on transport responses | **Missing** | Freshness is polling only. |
| D5 provider-unverified durability must ship behind a flag | **Missing** | The daemon boots on S3 with no acknowledgement; the bootstrap javadoc still says filesystem-only. |
| §10.4/§10.5 roles from the lease, `DiscoveryNodes` from membership | **Partial** | `DiscoveryNodes` holds only the local node; every `DiscoveryNode` carries `BUILT_IN_ROLES`. Latent. |
| RFC header: `serverless-status.md` is the single source of truth | **Contradicted** | It contradicts itself in five places; the code agrees with the "works" half each time. |

Also absent from the metadata-plane RFC, and not deferred anywhere in writing: prune-before-activate and
the activation budget, the directory tier or any cross-node hint sharing, load-aware writer placement,
the mapping version in manifests, scheduled fleet-wide reconcilers, reader pins in the lease. The
control-cell-diet RFC is sidestepped correctly by the working-set-only projection.

---

## 2. Critical: the acknowledgement fence

Three reviews found the same missing rule from three directions. All CONFIRMED at the lines cited.

**C1. Lease renewed before heads are re-read.** `ServerlessNode.heartbeat` calls `renewOwnLease` first,
then reads descriptors, then heads, one GET each, sequentially. A node that paused past its TTL and lost
a shard to a successor renews its own lease successfully, is "alive" by its own clock for the length of
its heartbeat I/O, and `appendOrRelease` — whose only ownership check is `selfLeaseValidAt` — appends and
acknowledges a write behind the successor's seal. The forwarded-write handler accepts on `openShards`
alone and never reads a head. The owner hints added in `a1422b2f83f` widen this from "writes already in
flight" to "every write any coordinator with a stale hint routes there until it refuses".

**C2. Handover has no exclusion against request threads.** `BackgroundReconciler.letGo` (idle release,
eviction, shutdown) does publish → release head → close. A write that applies to the engine before the
head release and lands its WAL PUT after the successor's seal is acknowledged and never replayed.
`appendOrRelease` checks only the node lease, which is valid throughout.

**C3. Two clocks.** Acknowledgement is judged on the writer's clock (`selfLeaseValidAt`), takeover on the
taker's (`acquire` plus the oracle). The write-loss window equals the clock offset, not "skew beyond TTL"
as the design assumes.

**Fix, as one change.** In `heartbeat` and `activateWriter`, if the own lease has lapsed, release every
writer shard *before* renewing; the successor re-acquires at a bumped term and seals correctly. Add a
per-shard fence across apply→append→ack that `letGo` takes exclusively before releasing the head. Make
the forwarded write handler consult the recent head (age ≤ one renewal) and refuse when it does not name
this node. Give the holder and the taker asymmetric margins on the lease. The metadata review supplies
the two-clock test; the storage review supplies the write-during-letGo test. Both fail today.

---

## 3. High findings by area

### Durability and publication (storage review)

- **A reader silently serves a fenced writer's segment.** `BlockCacheDirectory.openInput` prefers a local
  file of the same name; the reader open path drops only `segments_*`. A node fenced after a local flush
  of segment `_k` and later serving the shard as a reader returns its own `_k.*` in place of the
  successor's published same-named files. Wrong results, no error. Fix: delete every manifest-named local
  file before bootstrapping a reader; drop stale local commits on the writer path too.
- **Orphan `IndexShard` on activation failure.** Any exception after `IndexService.createShard` in
  `ShardReconciler` leaves the shard registered; every retry hits "already exists" while the head keeps
  naming this node. A permanent single-shard outage until restart. Fix: `removeShard` on failure.
- **A head won but never opened is held unserved forever** (assignment PUT or `syncFrom` fails); the 503
  branch raises no doubt, so nothing re-acquires it.
- **The orphan-container sweep ignores snapshots** and deletes what a shallow snapshot pins and what
  `purgeShardData` deliberately preserved.
- **Brownout deviation.** The first failed WAL append *releases* the shard, so a fully cached writer stops
  serving reads; the RFC says reads degrade to staleness, never unavailability.
- **Seal min-merge** with the sealer's own-term entry can exclude legitimate records on a same-term reopen.
  Two-line fix in `WalStore.sealAt`.

### Write and read paths (write review)

- **Owner hints are never invalidated unless the hinted node answers "does not own".** After an owner
  dies and a third node takes over, every single-document write, get, update and explain through a
  non-owner answers 503 indefinitely, while `_bulk`, which reads the register, keeps working.
  `OwnerUnreachable` does not forget the hint. Fix: forget the hint on any forward failure and on
  `OwnerUnreachable`; bound a hint's age to one TTL.
- **Forwarded writes match shards by name only and accept reader shards.** `ShardRouter.handleIndex` and
  `handleBulk` use `localShard`; a forward to a node holding the shard as a reader fails with "open as a
  reader", not the load-bearing "does not own", so the stale hint sticks. `handleGet` already uses
  `writerShard`.
- **A recreated index with the same name can acknowledge writes into the old incarnation.**
  `BulkHandler.applyGroup` has no uuid filter and `ForwardedIndexRequest`/`ForwardedBulkRequest` carry no
  uuid, unlike the search path.
- **A forwarded get drops sequence identity.** `ForwardedGetResponse` renders `_seq_no: -2`,
  `_primary_term: 0`, `_version: -1`; a conditional write built from it always conflicts. The existing
  forwarded-get test never asserts `_seq_no`.
- **A forwarded bulk conflict is a 400.** `ForwardedBulkResponse` never serialises `conflict`, so a
  `create` on an existing id or a lost `if_seq_no` comes back `operation_failed` instead of 409.
- On the forwarded single-document path, a security plugin's 403 becomes 503 `forward_failed` and casts
  doubt on ownership; owner-side 429 does the same. `_index` differs between local and forwarded
  responses for alias and data-stream writes.

### Security (plugin review)

- **`_mget` bypasses the system-index guard.** `MultiGetHandler` takes `_index` from the body and calls
  `ShardOperations.get`, which has no `isSystemIndex` check. Any account can dump the credential index's
  password records. Not in the system-index test.
- **Snapshot and restore copy the credential index** under a renamed, readable index; no check on capture
  or on the restore target.
- **The transport secret is a static bearer token** on a cleartext-by-default transport: replayable,
  unbound to request, sender or time, cached for the process lifetime, no rotation, and no test sends a
  wrong or missing token. Holding it grants ungated, unguarded writes to every shard on the owner.
  Object-store read access is therefore cluster write access, which defeats §12's GET-only tier scoping.
- **Per-address lockout refuses correct passwords.** Five failures from a shared NAT or load-balancer
  address lock every account behind it; one wrong password a minute holds the cap forever; the recovery
  account is lockable by an unauthenticated caller, contradicting the design's stated intent.
- Cross-plugin constructor injection can hand another plugin the live `CredentialStore`. Filters run only
  on the coordinating node (documented). No security-posture section exists for the shell.

### Search and admin surface (search review)

- **The alias CAS token is read in a second register read after the value** (`AliasHandler`,
  `RolloverHandler`): two racing `_aliases` both succeed and one is lost; a delete in the window
  resurrects the alias or writes an alias record over an index descriptor.
- **Any alias mutation on a data-stream name rebuilds it as a plain alias** (two-argument
  `AliasRecord`), after which writes and rollover fail.
- **A data stream's backing index inherits none of its template's settings or mappings**: created with
  `CreateRequest(1, null, EMPTY)`, and the template glob is anchored against the `.ds-` name.
- **Template settings in core's own spellings are mangled** (`index.index.refresh_interval`;
  `number_of_shards` read bare so three shards become one). **`PUT _settings` commits an unparseable
  value** to the descriptor, then reports 400.
- **Restore discards the manifest CAS result** and counts the shard successful; a write that activates
  the shard during restore leaves it empty with `failed: 0`. Snapshots do not capture settings, so
  analyzers are lost, while `index_settings` on restore is refused "because settings come from the
  snapshot". The record is written after the manifests are read, so a concurrent delete yields a
  `SUCCESS` snapshot that cannot be restored.
- A shard `timeout` is a shard failure, so `timed_out: true` can never be produced despite M61's claim.
  Each shard evaluates `now` at a different instant. The fan-out pool can deadlock across two saturated
  search nodes until the 30-second forward timeout. A plugin's `Client` search silently searches only
  its first index. `StoredScripts.lookup` serves a cached hit without checking the marker, so an
  overwrite is stale elsewhere for up to a pass. `index.default_pipeline` is accepted and never read.
- Six more alias and data-stream races (a lost create strips the winner's hint, last-index-removed
  deletes a concurrent add, delete racing rollover, orphan `.ds-` on a failed create, user-created
  `.ds-` names wedge rollover) and a dozen admin divergences (timed-out health answers 200,
  `wait_for_nodes` strips the comparator, stats `writers` goes negative with a view open,
  `_resolve/index` renders streams as aliases).

### Lifecycle, scale and cost (lifecycle and metadata reviews)

- **The heartbeat's sequential head reads bound how many shards a node can hold.** At ~15 ms per GET on
  S3 the lease lapses under a healthy node once shards plus indices exceed roughly 1,200, inside the
  default cap of 1,000 shards. Combined with C1 this is not only a liveness cliff.
- **Fleet cost is quadratic in node count.** Every request-serving node reads every peer's lease every
  half-TTL. At the RFC's target that term alone is millions of GETs per second, against a design that
  budgets polling away. Expired leases are never pruned, so each refresh also pays a GET per dead node
  forever. The members register removed the LIST and kept every per-object read.
- **Truth is read on the request path**: a descriptor GET on every write and every search name.
- **Every owned shard reports `dormant` after one TTL** in cluster health, because `ShardHeadStore.renew`
  has no caller and the stamp is what health reads.
- **Each activation re-reads the node's whole working set** and rebuilds the `ClusterState`; each reader
  open does the same and reads the manifest three to four times; reader bookkeeping grows without bound.
- **Writers cold-start by downloading the whole shard**; the RFC promises seconds behind an indexing
  queue that does not exist. The reader and view cap is a hard-coded 1000 that refuses rather than evicts.
- **Brownouts have no backoff**: activation retries immediately, cold opens do not fast-fail, fan-out
  threads park for minutes. No test drives an object-store outage through a running node.
- **The 500 ms publish debounce** is ~21,600 PUTs per hour per continuously written shard, although the
  WAL already made the writes durable. Idle release always flushes and uploads even with nothing new.
- **Descriptor tombstones are permanent**, and the prefix cap counts them: `logs-*` is refused after 500
  deletions under that prefix.
- Template and script markers are write-then-bump, not atomic; a crash between leaves every node's
  cache stale until the next unrelated change. The sweep gate misses files freed by a view reaped on
  another node.
- Frozen views leak a local index directory per point in time. The block cache is one synchronized LRU
  of 256 MB outside the breakers with no disk tier and no single-flight.
- No production object-store request counters, publish lag, lease health or per-shard ownership listing;
  the counting store is test-only. Dynamic partitioning is not started for this node: fixed shard
  counts, no split or merge.

---

## 4. What the reviews checked and found sound

Apply-then-log with engine sequence identity, tombstones, one PUT per batch, the engine-owned CAS on
local and forwarded single-document writes, term-scoped paths with the manifest generation as the fence,
foreign-writer detection, WAL ordinal seeding and truncation from memory, `dropOlderTerms`, the recorded
lengths, the term-keyed block cache, the memoised publish, the recent-head reuse, the owner-hint
correction loop's happy path, PBKDF2 parameters and constant-time compare, the decoy timing, the
credential marker propagation, the bounded pools and queues, `ActionGate` semantics and its coverage of
every data endpoint, the sort merge with core's missing-value rule, the aggregation reduce under core's
bucket consumer, the placement-first fan-out, the spec-coverage test.

---

## 5. Fix order

Ranked by what each buys, with the reviews' own tests to pin each one.

1. **The acknowledgement fence** (C1–C3): release before renew on a lapsed lease, the per-shard fence in
   `letGo`, the head check on forwarded writes, holder/taker lease asymmetry. Two failing tests come with
   the reports.
2. **The two credential leaks**: `isSystemIndex` in `ShardOperations.get` (covers `_mget`) and on
   snapshot capture and restore target. Hours.
3. **Owner hints**: forget on `OwnerUnreachable` and on every forward failure; bound hint age; use
   `writerShard` in `handleIndex`/`handleBulk`; carry the index uuid on forwarded writes and filter bulk
   groups by it.
4. **`syncFrom` must not close on listing evidence**, and must skip readers; read the head before
   releasing.
5. **Reader correctness**: delete manifest-named local files before opening a reader; `removeShard` on
   activation failure; the seal min-merge fix.
6. **Forwarded response completeness**: `_seq_no`/`_primary_term`/`_version` on `ForwardedGetResponse`;
   `conflict` on `ForwardedBulkResponse`; keep a plugin's 403 and an owner's 429 as themselves.
7. **Alias and data-stream atomicity**: read the value and its generation in one read everywhere; keep
   the stream fields through every mutation; make backing indices inherit the template; the template
   settings spellings; parse before the settings CAS.
8. **Snapshots**: honour the restore CAS; capture settings; write the record before reading manifests.
9. **The heartbeat cliff**: renew the lease on its own timer, ahead of and independent of the head reads;
   parallelise or batch the head reads; renew head stamps or stop health reading them.
10. **The fleet cost model**: decide whether membership is a listing (the RFC), a sharded index, or an
    index rebuilt from a bounded listing; prune expired leases; take the descriptor read off the write
    and search path with a bounded-age cache; add epochs to forwarded responses.
11. **Throttle semantics**: per-account and per-address budgets that cannot lock the recovery account or
    a shared address; then a start-time refusal on a plaintext transport, a negative token test, and a
    per-request MAC with rotation.
12. **Brownouts**: activation backoff with jitter, fast-fail for cold opens, keep a writer shard readable
    when the log cannot be appended; a chaos test through a running node.
13. **Cost knobs**: a publish debounce in seconds; no flush-and-upload on an idle release with nothing
    new; one manifest read per reader open; delete frozen-view directories; a disk tier or at least
    single-flight for the block cache.
14. **The documents**: amend §6.3, decide D4, add the D5 boot flag, widen the absence scan and no-op
    `MetaStateService`, fix the five contradictions in `serverless-status.md`, and add the gaps this
    review names to "What is genuinely missing".

Items 1 through 8 are correctness and take days, not weeks. Items 9 and 10 are the scale work the
RFC's napkin math assumed was done. Everything after is cost, hardening and honesty.
