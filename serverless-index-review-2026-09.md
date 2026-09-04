# Review of the serverless index implementation — correctness, completeness, security, performance, scale, cost

September 2026, after M62. Six code-grounded reviews (metadata registers, write path and durability,
publication and readers, lifecycle and control loop, security, search path), each read the source rather
than the notes; every headline finding below was then re-verified by reading the cited lines, and one was
confirmed by running a probe against a node. Paths are under `serverless/shell/src/main/java/org/opensearch/serverless/`
unless stated. **CONFIRMED** means the code was read end to end for the scenario; **PLAUSIBLE** means the
mechanism is confirmed and the trigger is argued rather than reproduced.

> **Status, later the same day:** every finding below — C1–C3, H1–H12, M1–M18 — has been addressed in the
> working tree; what each fix is and how it was verified is in
> [`m63-review-fixes-notes.md`](m63-review-fixes-notes.md). The text below is left as it was written, so the
> fixes can be read against the findings.

The short version: the register-and-lease design is sound where it was measured, and most of what the
notes claim is true. What the reviews found is a layer below the notes — identity (name versus uuid,
node id versus ephemeral id, generation versus instance) is checked in the places the milestones tested
and assumed elsewhere; the REST surface grew faster than the authorization gate and the system-index
guard; and the control loop's per-pass cost is proportional to everything a node has ever touched rather
than what it holds.

---

## Critical

### C1. No name validation, and names are object-store paths — path traversal on the filesystem store. CONFIRMED by probe
- `RegisterMap.descriptorBlob` returns the name verbatim (`metadata/RegisterMap.java:297`); `TemplateStore.put` writes `PREFIX + name` (`metadata/TemplateStore.java:96`); the only check anywhere is `startsWith("_")`. Core decodes `%2F` in path parameters (`server/.../PathTrie.java:306`), and `FsBlobContainer` resolves with a bare `path.resolve(name)` and creates parent directories (`server/.../FsBlobContainer.java:233,344-350`).
- Probe: `PUT /..%2F..%2F..%2Fescaped-index` answered `200 {"acknowledged":true,...}` and wrote a file three directories above the object-store root. The template, script and pipeline variants failed only because the intermediate directory did not exist, and each 500 leaked the absolute filesystem path. `PUT /UPPER%20case%2Aname` was accepted; an index whose name contains `*` breaks every pattern rule this surface has.
- Consequences beyond traversal: `#` is the separator inside blob names (`RegisterMap.java:43`), so repository `a#b` snapshot `c` collides with repository `a` snapshot `b#c` (`:200-210`); a `/` in a name nests a blob under a subdirectory that flat listings never see, so it is invisible to patterns and to the garbage collector; on the FS store a prefix listing is a glob, so `GET /[ab]*/_search` matches `a…` and `b…` and `[` alone is a 500.
- Fix: apply core's `MetadataCreateIndexService.validateIndexOrAliasName` rules at every REST entry that names an index, alias, template, pipeline, script, repository, snapshot or data stream, and additionally reject `#`. Also reject names beginning with `.ds-` on plain index creation.

### C2. The system-index guard checks the raw path parameter only. CONFIRMED
- `rest/SystemIndices.java:106-109`: `request.param("index")` compared against the descriptor set. Nothing downstream re-checks: `SearchHandler.resolveIndices` splits comma lists, expands `prefix*` and follows aliases with no system check; `MultiSearchHandler`, `SearchTemplateHandler` and `RankEvalHandler` take indices from the body; `AliasHandler` only checks that an index exists before aliasing it. `isSystemIndex` has one caller, in `BulkHandler.route`.
- Scenario: any authenticated user reads the auth plugin's account index with `GET /alpha,.serverless_auth/_search`, `GET /.serverless*/_search`, an `_msearch` line, or by aliasing it first. The password hashes are `index:false` but sit in `_source` (`auth/.../CredentialStore.java:334`).
- Fix: resolve names to concrete indices in one place and refuse there; refuse aliasing or data-streaming a system index; re-check on the transport handlers (see C3).

### C3. Transport handlers are unauthenticated, unscoped, and cleartext by default. CONFIRMED
- `transport/ShardRouter.java:82-130` registers index, bulk, get, search, explain and frozen-search handlers that check only that the shard is local; `handleSearch` opens a shard it does not hold on request (`:196`). No caller identity, no action filter, no system-index check. The transport is plain netty4 unless a network plugin supplies TLS (`shell/ServerlessNode.java:281-282, 582`).
- Anyone reaching the transport port can write to or read any shard this node owns, including the auth index. The notes call the transport port trusted; nothing enforces it, and `_serverless/shards` and `_nodes` hand any authenticated user the addresses and lease details needed.

---

## High — correctness and durability

### H1. Register generations restart at 1 after delete-and-recreate: every compare-and-swap is open to ABA. CONFIRMED
- `DescriptorStore.deleteIfUnchanged` tombstones under CAS and then deletes the blob (`metadata/DescriptorStore.java:295-307`); both blob containers mint generation 1 on create. A generation therefore identifies a version of *some* register at that name, not of the index.
- Scenario: a mapping update reads index `x` (uuid U1, generation 1), does its merge; meanwhile `x` is deleted and recreated (U2, generation 1); the update's CAS succeeds and writes U1's descriptor over U2's. The garbage collector treats U2's shards as orphans of a different uuid and deletes them. Rollover's window spans a count fan-out, template resolution and an index creation, so it is seconds wide.
- Fix: keep the tombstone (or a monotone counter) so generations never restart, or carry the uuid in the CAS expectation.

### H2. In-process shard identity is by name, so a deleted-and-recreated index keeps serving from the old shard. CONFIRMED
- Shard heads are keyed by name (`RegisterMap.java:308`); the write, get and search paths match open shards by name and number (`rest/DocumentHandler.java:282`, `shard/ShardOperations.java:227-231`, `rest/SearchFanout.java:597`); a writer notices the delete on its next heartbeat, every TTL/3 (`ServerlessNode.java:1550-1557`).
- Scenario: delete `logs`, recreate it, write within ten seconds: the write lands in the old uuid's shard, is acknowledged, and is gone. `refreshReaders` keeps a reader whose manifest has vanished (`reconcile/BackgroundReconciler.java:220-224`), so searches return the old index's documents. M32's uuid-keyed storage protects the object store path; it did not reach the in-process identity.

### H3. The WAL ordinal restarts at zero on a same-term reopen and overwrites acknowledged records. CONFIRMED
- `store/WalStore.java:65` is a fresh `AtomicLong` per instance; `:132-133` writes `%020d` names with `failIfAlreadyExists=false`. `ShardReconciler.releaseShard` drops the cached store (`:640`), and every release that does not touch the head — append failure (`ServerlessNode.java:1816`), self-lease expiry (`:1933`), fenced publish (`BackgroundReconciler.java:885`), restart with the same node id — reopens at the same term and replays records 1..N, after which new writes overwrite blobs 1..m. A crash before the next publish loses the originals.
- Fix: seed the ordinal from the container's last name, and write with `failIfAlreadyExists=true`; or bump the term on every local release.

### H4. A forwarded create is applied as an overwrite. CONFIRMED
- `ForwardedIndexRequest` carries `requireAbsent`; `ShardRouter.handleIndex` calls the five-argument `node.index(...)` that has no such flag (`transport/ShardRouter.java:132`; `ServerlessNode.java:1647`). `PUT /{index}/_create/{id}` against a remote shard silently overwrites; data-stream writes through the name are create-only, so every forwarded data-stream write is affected. No test covers create plus forward.

### H5. `_update` writes back unconditionally; `retry_on_conflict` can never trigger. CONFIRMED
- `shard/ShardOperations.java:1283` passes the caller's `ifSeqNo`/`ifPrimaryTerm` (unassigned for a plain update) rather than the sequence number it read. Two concurrent partial updates lose a merge; scripted deletes and upserts have the same shape. The handler's retry loop (`rest/UpdateHandler.java:278`) waits for a conflict that cannot occur.
- Fix: when the caller gave no condition, pass `existing.seqNo()`/`existing.primaryTerm()` (or `MATCH_DELETED` for a missing document); the engine's compare-and-swap already exists.

### H6. Two publishes of one shard on the same node fence the owner out of its own shard. CONFIRMED
- The backstop `publishAll` and the debounced `publishDirty` run on GENERIC with no mutual exclusion (`reconcile/ReconcileScheduler.java:245,296-302`); both read the manifest generation, both upload, the loser's CAS throws `StaleWriterException(term, -1)` (`store/SegmentPublisher.java:161-163`); `BackgroundReconciler.publish` treats any such exception as a zombie fence and releases the shard (`:880-888`). The head still names the node, so gets and writes answer 503 until the shard is re-acquired, restored and replayed. `m14-object-store-notes.md` records this race and fixed the test.
- Fix: a per-shard publish lock, and distinguish a same-term CAS conflict from a newer-term fence.

### H7. Readers reopen one commit behind, perpetually. CONFIRMED by reading, not covered by tests
- A reader skips restore but runs `bootstrapNewHistory`, which writes a local `segments_{N+1}` over the remote `segments_N` (`shard/ShardReconciler.java:329-340, 613`); release keeps the local directory; the writer's next publish is also `segments_{N+1}`; `BlockCacheDirectory.openInput` prefers the local file (`store/BlockCacheDirectory.java:157-160`), so the reopen loads the stale local commit and records the new manifest as current. Writers delete local files before restore; readers have no equivalent.

### H8. The block cache is keyed by index name and shard number, not uuid or term. CONFIRMED
- `ServerlessNode.java:684-690` scopes the cache as `indexName#shard`; `BlockCacheDirectory.java:165-173` adds file name and block index. Same-named files are different bytes across a recreate or a failover with bootstrap (the code's own comments at `SegmentPublisher.java:200-202` say so). A search node serves the old index's blocks to the new index's files with no error.

### H9. Restart with the same node id, and duplicate node ids. CONFIRMED mechanism
- `truthFor` and the heartbeat inherit heads by `ownerNodeId` alone (`MetadataPlane.java:799-800`, `ServerlessNode.java:1551`) while the oracle requires the ephemeral id (`MetadataPlane.java:84-86`), so a restarted node serves shards every peer sees as dead; a peer that acquires at term+1 seals a WAL the restarted node is still acknowledging into. Clean shutdown neither publishes nor releases heads (`ServerlessBootstrap.java:298-309`), so the ordinary Kubernetes restart takes this path. `BlobLeaseMembership.renew` notes a CAS conflict "means a duplicate node id" and force-writes anyway (`membership/BlobLeaseMembership.java:90-108`): two processes with a cloned data directory share heads, terms and writer ids, and the foreign-writer fence cannot tell them apart.

### H10. Thread-pool self-deadlocks under concurrent search. CONFIRMED
- The coordinator runs on GENERIC (`rest/SearchHandler.java:381`) and blocks on a latch for shard tasks it queued on GENERIC (`rest/SearchFanout.java:132,215`; `rest/Fanout.java`). GENERIC's maximum is bounded (`server/.../ThreadPool.java:265-267`); enough concurrent searches park every thread waiting for tasks no thread is free to run, and lease renewal shares the pool. Forwarded searches run on SEARCH (`ShardRouter.java:100-114`) and `ShardQuery.execute` forks the query phase to SEARCH and blocks on it without a timeout (`shard/ShardQuery.java:270,315`; `server/.../SearchService.java:844-848`); enough forwarded searches fill SEARCH and their own query tasks queue behind them forever. Multi-search, rank-eval, search templates and explain coordinate on SEARCH as well.

### H11. Roughly two-thirds of the REST surface bypasses the action gate. CONFIRMED
- Gated, directly or through `ShardOperations`: document CRUD, `_bulk`, `_search` (live and frozen), `_mget`, `_update`, `_explain`, `_delete_by_query`, index create/get/delete, snapshots, repositories, cluster settings. Not gated: `_msearch`, search templates, `_rank_eval`, `_rollover`, data-stream create and delete (which deletes backing indices), every alias endpoint, point-in-time create and `_all` delete, mapping and settings updates, templates, ingest and search pipelines, stored scripts, `_field_caps`, `_analyze`, `_cat/indices`, `_resolve/index`, `_serverless/shards`. A security plugin's privilege evaluation never runs for any of them. `ActionGate.run` also executes the original request closure, so a filter that rewrites a request (index substitution, document-level security query injection) is silently ignored (`shell/ActionGate.java:81-100`).

### H12. Alias updates read the value and its generation in two reads, and demote data streams. CONFIRMED
- `AliasHandler.swap` resolves the record, computes the new set, then reads the generation and CASes (`rest/AliasHandler.java:372, 402-403`; same at `:643-645`); a change between the two reads is overwritten with a successful CAS. Every alias endpoint builds a two-argument `AliasRecord`, so `PUT /x/_alias/<stream>` or `POST /_aliases` on a data stream strips its generation and timestamp field, and `DELETE /_alias/<stream>` orphans the backing indices.
- Fix: `Resolution` should carry the generation it read; alias endpoints must refuse data-stream records.

---

## Medium

- **M1. Acknowledged writes behind a successor's seal.** The fence checks the node lease before the PUT; a writer that pauses past a successor's seal lands a record the successor never replays and returns 201 (`ServerlessNode.java:1924-1941`; `WalStore.java:251-290`). M49 names the bound; the `WalStore` javadoc's claim that such a caller "never received an acknowledgement" is not true. Needs a post-append check or a conditional PUT against a fence object. CONFIRMED by design.
- **M2. `_delete_by_query` deletes unconditionally** against a frozen view and reports `version_conflicts: 0`; a document rewritten after the view was taken is deleted anyway. One WAL PUT and fsync per document, on one WRITE thread, no pressure accounting (`ShardOperations.java:988`). CONFIRMED.
- **M3. `_bulk` never grows a dynamic mapping**; `MAPPING_UPDATE_REQUIRED` is a per-item 400 while the single-document path grows it (`ServerlessNode.java:2150-2230` vs `:1757-1762`). CONFIRMED.
- **M4. Mapping and settings updates reach only the node that served them**, and `growMapping` returns without refreshing when another node already added the field (`ServerlessNode.java:1870-1872`), so writes carrying that field fail on the stale node until the shard reopens. CONFIRMED.
- **M5. A node holding a reader can never become the writer**; activation skips anything in `openShards()`, which includes readers, and the write path answers 421 until idle release (`BackgroundReconciler.java:774-780`). CONFIRMED.
- **M6. Indexing pressure is not applied to forwarded writes** on either side; the forward timeout equals the lease TTL, so a large forwarded bulk that times out is retried with duplicated auto-ids and spurious 409s (`DocumentHandler.java:326-410`; `ShardRouter.java:120-160, 308-311`). CONFIRMED.
- **M7. Realtime get from a non-owner.** The forwarded get answers from any local shard including a search reader, stamped `realtime: true` (`ShardRouter.java:164-175, 214-221`); the local get answers from a writer shard even when the head names another node (`ShardOperations.java:434-436`). CONFIRMED.
- **M8. Edge-triggered publishes are never swept**; only the backstop's result enters the watch set, so a burst-then-idle shard keeps merged-away segments forever (`BackgroundReconciler.java:179, 867-871`; `ReconcileScheduler.java:296-302`). CONFIRMED.
- **M9. `extendPointInTime` is a plain overwrite racing the reaper**, which reads, decides, then deletes; a caller told the extension succeeded can lose the view on the next page. PLAUSIBLE.
- **M10. `aliased_by` can under-approximate**: `noteAlias` gives up silently after four lost CASes (`MetadataPlane.java:394-414`), and the remove-after / add-before ordering has an interleaving that leaves a live alias unhinted. Stale hints accumulate on a name that keeps losing `createAlias`. CONFIRMED.
- **M11. Rollover and data-stream creation wedge after one lost step**: the index is created before the alias CAS; a lost CAS leaves the next name taken, so every later rollover answers "already exists" (`rest/RolloverHandler.java:264-277`; `rest/DataStreamHandler.java:181-191`). CONFIRMED.
- **M12. Tombstone and head permanence on crash paths**: a crash between tombstone CAS and blob delete kills a name forever; `deleteIndex` deletes heads rather than tombstoning them, so an in-flight acquire recreates one the next same-name index inherits. CONFIRMED (crash paths).
- **M13. Authentication is brute-forceable and cheaply denial-of-service-able**: no throttling or lockout, each miss costs a PBKDF2 derivation on a one-to-four-thread pool with an unbounded queue, Basic credentials in the clear unless a plugin adds TLS, username enumeration by timing (`auth/.../ServerlessAuthPlugin.java:272, 369`; `CredentialStore.java:231`). CONFIRMED.
- **M14. Sort merge inverts missing keyword values** (`SearchFanout.java:423, 438-442`) and compares mixed numeric types as strings (`:449`). CONFIRMED / PLAUSIBLE.
- **M15. Coordinator memory is unbounded and outside the breakers**: every shard returns `from+size` full hits held before the cut, forwarded responses copied again, bucket consumer a no-op (`SearchFanout.java:163-165, 296-297, 369`). CONFIRMED.
- **M16. Object-store I/O on the HTTP event loop**: membership refresh, name resolution, PIT read and extension all run inside `prepareRequest` (`SearchHandler.java:313-373`); only the fan-out is moved off. CONFIRMED.
- **M17. Stored-script compile failures are swallowed** when no context is given and the language is not mustache (`rest/StoredScriptHandler.java:160-175`). CONFIRMED.
- **M18. Error bodies leak internals**: thirty-one handlers fall back to `new BytesRestResponse(channel, e)`, which with core's default detailed errors carries object-store paths, node ids and addresses. CONFIRMED by probe.

---

## Scale and cost — where the bill is proportional to the wrong thing

| What | Cost today | Evidence |
|---|---|---|
| Heartbeat and every writer activation | one head read per assignment blob the node has ever claimed; `letGo` never forgets assignments | `MetadataPlane.java:784-800`; `BackgroundReconciler.java:637-664` |
| Opening the N-th reader on a node | N manifest reads plus a full view projection; `served`/`readerShards` never pruned | `ServerlessNode.java:1592-1608` |
| Sweep, per published shard per pass | reads every PIT record and every snapshot, plus a listing per term container | `reconcile/GarbageCollector.java:147, 160` |
| Index creation, rollover, data-stream creation | reads every index and component template: two listings plus ~2,000 reads at the cap | `IndexAdminHandler.java:482-486`; `TemplateStore.java:142-154` |
| `PUT` of a template, pipeline or script | reads every one to count them (also a TOCTOU cap) | `TemplateStore.java:90-97` |
| A stored-script lookup that misses | a full reload under a lock; any request naming a bad id triggers it | `script/StoredScripts.java:60-67` |
| A forwarded search | one head read and one lease read per remote shard, on top of a membership refresh; ~2 requests per shard hot | `SearchFanout.java:551`; `ShardRouter.java:247` |
| A 1,000-shard search | 125 sequential rounds of 8; the full source serialised per shard | `Fanout.java:50`; `ForwardedSearchRequest.java:66` |
| A wide PIT search | the whole record (every shard's manifest) forwarded per shard, re-read per shard opened; views opened on the coordinator as fallback are held until expiry and exempt from eviction | `PointInTime.java:703-712`; `SearchFanout.java:653-669`; `ShardReconciler.java` cap exemption |
| Block cache | heap-only, 256 MB global across up to 1,000 shards, one global lock, no single-flight on misses, no readahead: ~1,600 ranged GETs to scan a 100 MB file | `store/BlockCache.java:71,87`; `store/ObjectStoreIndexInput.java:74-96` |
| `GET /_data_stream/<prefix>*` | re-resolves all templates inside the per-stream loop | `DataStreamHandler.java:230-235` |

At a hundred nodes holding a thousand shards each, heartbeats alone are on the order of ten thousand GETs per second before any request traffic. The design's own rule — cost set by a cap, not by the population — holds for patterns and templates on the way in, and is broken by these loops on the way round.

---

## Completeness gaps worth naming

- Readers and frozen views have no cap; only demand-driven writer activation is bounded by `maxShardsHeld`.
- `_msearch`, `_msearch/template` and `_rank_eval` accept any number of lines and run them sequentially on a SEARCH thread; point-in-time creation has no count cap; `DELETE /_all/_pit` releases every caller's views.
- No I/O timeout on a shard query: a hung object-store read pins a SEARCH thread and its waiting GENERIC thread.
- Duplicate node ids, clock skew beyond the TTL, and a crash between the two halves of a delete are each tolerated silently rather than detected.

---

## What is sound

- The register model itself: put-if-absent arbitrates index/alias collisions; tombstone CAS is the linearization point for delete; head acquisition is read → held-check → CAS with re-read on loss; terms are monotone; an unreadable lease is treated as live.
- Publication order (files, then manifest CAS), same-term foreign-writer fencing, WAL truncation lagging one publish, seals durable and monotone.
- The sweep rule: dead terms only, references built from the live manifest, live views and shallow snapshots before listing, two-sighting grace, unreadable-but-ours pins everything.
- The write ordering apply → WAL put → sync → ack, with append failure releasing the shard; only applied items logged in a bulk; version conflicts propagated as 409 across the wire.
- Aggregation reduce is core's final reduce; merge details (ties, NaN scores, relation, `max_score`, `track_total_hits: false`) are right; what cannot be merged is refused rather than approximated.
- Password storage (salted PBKDF2-SHA512, constant-time compare, keystore-only bootstrap); every REST route authenticated; the plugin client is an allowlist.
- Eviction hysteresis, pinning, idle release ordering, deletion order, and the snapshot-pinned purge exemption behave as the notes claim.

---

## Recommended order

1. **Identity.** Validate names (C1); carry uuid or a non-restarting generation in every CAS (H1); match open shards by uuid (H2); key the block cache by uuid and term (H8); compare ephemeral ids, not node ids, when inheriting heads, and refuse a duplicate node id (H9).
2. **Durability.** Seed the WAL ordinal and fail on collision (H3); pass the create flag over the wire (H4); CAS updates against what was read (H5); lock publishes per shard and distinguish a conflict from a fence (H6); clean local commits before opening a reader (H7); close the seal window with a post-append check (M1).
3. **Authorization.** Enforce system indices on resolved concrete names in one place (C2); gate the ungated table (H11); require a transport identity, TLS by default, and re-check on forwarded requests (C3); throttle authentication (M13).
4. **Threads.** Take the coordinator off the pool its workers use, bound the waits, and move name resolution off the event loop (H10, M16).
5. **Cost.** Forget assignments on release; stop re-reading every reader's manifest per open; read the PIT and snapshot sets once per sweep pass; cache template resolution per pass; cap readers, views, msearch lines and PITs.
