# M48 (finished) — `_seq_no`, `_primary_term`, `_version`: assigned, survived, conditioned on

M47's notes said this shell "has no version model at all." That was wrong, and finding out how wrong is
what made this milestone small enough to do. All three fields were already being assigned on every write,
by core's own engine, and one of them was already load-bearing in production code. What was missing was
narrower and sharper: the numbers did not survive a failover, so they could not be handed to a client as a
compare-and-swap token.

## What was already true before this milestone

- **`_seq_no` was always assigned.** `ServerlessNode.index` calls core's `applyIndexOperationOnPrimary`,
  the path that generates a sequence number from `LocalCheckpointTracker`'s `AtomicLong`. No cluster
  manager is involved in that — the counter is node-local and seeded from the Lucene commit's own
  `max_seq_no`, which is why it never needed one.
- **It was already depended on.** `BackgroundReconciler`'s entire "is this shard dirty, should I publish?"
  decision has been `maxSeqNo` change-detection since M10.
- **`_primary_term` was already solved, and this is the architecturally interesting part.** It is the one
  field with a hard cluster-manager dependency in classic OpenSearch — `IndexMetadata.primaryTerm(shardId)`,
  incremented by the cluster manager during allocation. This shell replaced that with the shard-head's CAS
  generation, threaded through `ShardHeadStore` → `MetadataPlane.truthFor` → `LocalViewProjector` →
  `IndexDescriptor.toIndexMetadata` → `updateShardState`. Core's own requirement for a correct term is "a
  monotonic fencing token that strictly increases on every change of writership and is durable across the
  writer's death." A compare-and-swap register on an object store is exactly that.
- **`_version` was computed normally** by the engine's live version map plus Lucene doc-values.

So the fields were being generated, used internally, and then discarded at the REST boundary.

## The actual gap: the failover boundary

A successor inherited the published commit's sequence state correctly — it rides inside `segments_N`, which
`SegmentPublisher` uploads like any other file. But `WalRecord` stored only `{id, source, deletion}`, and
`ShardReconciler` replayed each record through `applyIndexOperationOnPrimary` again, so every replayed
operation was minted a **new** sequence number. Numbers were monotonic within one shard's lifetime on one
node and meaningless across a takeover. `WalRecord`'s own javadoc had said so all along: *"anything built
on sequence numbers (optimistic concurrency via `if_seq_no`, cross-cluster replication) cannot be layered
on this without changing it."*

### The ordering problem that made the obvious fix wrong

"Add `seqNo` to `WalRecord`" does not work as stated. `ServerlessNode.index` appended to the log **before**
calling the engine — so at append time the sequence number did not exist yet.

The fix was to reverse it: **apply, then log, then acknowledge.** The durability contract is unchanged and
is the one the javadoc always stated — the record is in the log before the write is acknowledged, and
acknowledgement is the method returning. It is also the order classic OpenSearch uses internally: Lucene,
then the translog, then fsync.

Reversing it opens one window that did not exist before, and it is handled rather than accepted: if the log
append fails *after* the engine applied the operation, reporting failure while leaving the operation in a
shard that will later publish would make the refusal a lie. So `appendOrRelease` **releases the shard**, and
a successor rebuilds from the log — which does not contain the operation, making the refusal true. Classic
OpenSearch makes the same choice, failing the engine outright when a translog write fails.

### Replay had to move into the engine

Core only accepts a caller-supplied sequence number under a recovery origin, and `ensureWriteAllowed` only
permits a recovery origin while the shard is still `RECOVERING`. `Origin.REPLICA` was not an option either
— it asserts the shard is *not* a primary, and these are primaries. The reconciler regains control long
after that window closes, so replay could not stay where it was.

`Engine#engineRecoveryOperations()` is the seam core provides for exactly this, and its javadoc describes
exactly this case, down to the fencing obligation. `ServerlessWriterEngine extends InternalEngine`
overrides it to return the log's records as `Translog.Operation`s; `IndexShard` feeds them through
`runTranslogRecovery` under `LOCAL_TRANSLOG_RECOVERY`, which preserves each operation's sequence number,
primary term and version. `ShardReconciler` arms a per-shard registry immediately before the engine is
built and disarms it immediately after, because only the reconciler knows whether it is opening a writer, a
reader or a frozen view — and replaying into either of the latter two would be wrong.

**A watermark had to move with it.** Replayed operations may overwrite or delete documents already in the
restored commit, and the engine's append-only fast path is only sound while every operation it sees sits
above `maxSeqNoOfUpdatesOrDeletes`. Peer recovery in classic OpenSearch raises that watermark before
replaying a primary's history; this does the same. Without it the engine asserts — which is exactly how it
surfaced, in five existing tests, rather than as silent corruption.

**Replay also got more idempotent, not less.** Before, replaying a record twice was safe but consumed two
sequence numbers. Now a replayed operation carries the number it already had, so the engine recognises an
already-processed operation and skips it. `WalStore`'s deliberately conservative truncation therefore costs
less than it used to.

## What shipped, by tier

**Tier 1 — the fields are returned.** `_seq_no`, `_primary_term` and `_version` on single-document writes
and deletes, on `_update`, on every successful `_bulk` item, and on `GET /{index}/_doc/{id}` (which is how a
client obtains a token without having written). They travel across a forwarded write too, so the fields do
not depend on which node the client happened to reach. Action filters are now shown the real values
instead of placeholders.

**A bug M47 explicitly deferred is fixed in passing.** A write always reported `"result": "created"` and
`201`, even when overwriting. The engine has always known which it was (`IndexResult#isCreated`); this path
simply never asked. Overwrites now report `"updated"` and `200`, on both the single-document and bulk
paths, matching classic OpenSearch.

**Tier 2 — they survive a failover.** Covered above.

**Tier 3 — conditional writes.** `if_seq_no`/`if_primary_term` on `PUT`/`POST`/`DELETE /{index}/_doc/{id}`
and on `_update`, including when forwarded. The comparison is entirely core's own — the parameters are
passed to `applyIndexOperationOnPrimary`, which compares against the live version map under the
per-document lock it already takes. Nothing here re-implements a compare-and-swap.

Three refusals were kept or added rather than dropped:
- `version=` (external versioning) is **still refused**, and the message now points at what does work. It
  asks this system to order writes by a number the caller maintains and this system does not keep; that is
  a different feature from comparing against a number the engine assigned.
- Half a condition (`if_seq_no` without `if_primary_term`, or the reverse) is a 400, matching core's own
  requirement that they come together.
- A lost race is a **409**, not a 503 — including when the write was forwarded. That case needed explicit
  handling: a `VersionConflictEngineException` arrives at the coordinator wrapped in a transport exception,
  and the existing catch would have reported it as stale routing, told the client to retry an operation
  whose whole point is that it must not be retried blindly, and cast doubt on an ownership that was never
  in question.

## Canaries

- **148 — the log stops carrying sequence identity.** Records written with the identity-less constructor.
  Caught by `testSequenceNumbersSurviveAFailover`: replayed documents come back renumbered.
- **149 — replay goes back through the primary path.** `engineRecoveryOperations()` returns an empty list,
  so the reconciler's legacy loop re-applies everything as fresh primary operations. Caught by the same
  test, and by the token test — the whole point of the milestone reverts together.
- **150 — the condition is accepted and ignored.** `ifSeqNo`/`ifPrimaryTerm` replaced with unassigned
  values at the engine call. Caught by the stale-token assertions, which is why every conditional test
  asserts a stale token is *refused* before asserting a current one succeeds.
- **151 — the update-or-delete watermark is not raised before replay.** `advanceMaxSeqNoOfUpdatesOrDeletes`
  removed. Caught by five pre-existing replay tests, loudly, via the engine's own assertion.

## What this does NOT establish

- **Sequence gaps are possible and are not filled.** If the engine applies an operation and the log append
  then fails, the sequence number is consumed but never replayed, leaving a hole. Classic OpenSearch plugs
  holes on promotion with `fillSeqNoGaps`; that method is not reachable from outside `IndexShard`'s own
  promotion path, so this shell has no analogue. The consequence is bounded and worth stating precisely:
  a hole stalls the *processed local checkpoint*, which nothing in this shell reads, and does **not**
  affect `if_seq_no`, because that compares against the live version map and Lucene doc-values rather than
  against the checkpoint. It would matter if the global checkpoint were ever given meaning here.
- **WAL-append fencing is still open, and the reference implementation has not closed it either.** A writer
  that has lost its shard-head but not yet noticed can keep appending correctly-tagged records at its own
  term, and a successor reading terms in ascending order will replay them. `plugins/serverless-storage`
  documents the identical hole in its own WAL (*"Do not treat this method as a complete fencing
  mechanism"*) and keeps a TLA+ model of the argument. Core's `engineRecoveryOperations()` javadoc warns
  about exactly this. Nothing in this milestone closes it; the term-scoped log narrows it and no more. This
  is the single most important caveat on `if_seq_no`'s safety here: the guarantee is sound against one
  writer and against a cleanly-failed-over writer, and is exactly as strong as the existing head-fencing
  against a partitioned zombie — no stronger.
- **No conditional writes in `_bulk`.** Bulk items still refuse `_seq_no`/`_primary_term` on the action
  line, and the `create`/`update` bulk actions remain refused. Their old justification — "which this system
  does not have" — is now false, and the remaining reason is narrower and honest: the batch path takes
  `WalRecord` as its input type, which is the *log* record, and threading a per-item condition through it
  needs an input type of its own. That is a contained refactor, not a design question, and it is not in
  this milestone.
- **Legacy log records are replayed the old way.** A record written before this change carries no sequence
  identity and cannot be replayed as the operation it was, so `ShardReconciler` still replays those as
  fresh primary operations. They get new sequence numbers, exactly as every record used to. Dropping them
  instead would turn a format change into silent data loss. A log containing both kinds replays the new
  ones during recovery and the old ones after, so their relative order is not preserved — which cannot
  arise except by upgrading a node mid-life, and is recorded here rather than defended.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M48 is done.
