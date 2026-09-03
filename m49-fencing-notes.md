# M49 (finished) — the two limits M48 disclosed, addressed

M48 ended with two named limits. This milestone took both. One of them turned out not to exist.

## Limit 1 — sequence gaps: already closed, and M48's note was wrong about it

M48's notes said a hole in the log leaves a permanent gap in the sequence space, that classic OpenSearch
plugs holes with `fillSeqNoGaps`, and that "*that method is not reachable from outside `IndexShard`'s own
promotion path, so this shell has no analogue.*"

The first two clauses are right. The conclusion is wrong, and it was wrong in a way worth recording,
because it was reasoned from the API surface rather than checked against the call graph.
`StoreRecovery#internalRecoverFromStore` calls `fillSeqNoGaps` itself, inside `IndexShard#recoverFromStore`
— which is the only way this shell ever opens a writer. The shell does not need to reach the method; the
recovery path it already uses reaches it. Replayed operations are in the engine by then, because
`ServerlessWriterEngine` feeds them in during recovery, so the gaps core fills are exactly the gaps the log
left.

**The severity was also overstated in the other direction, in the fix's favour, and that is worth being
just as explicit about.** Before checking, the case for filling looked stronger than it is: `ReadOnlyEngine`
*throws* when a commit's max sequence number is above its global checkpoint, and every reader shard and
frozen view opens through `ReadOnlyEngine`. But this shell constructs it with `requireCompleteHistory =
false` (`ServerlessNode#engineFactoryProviders`), which returns before that check. So an unfilled hole would
not have broken readers today either. Both the original note and the first draft of this one were wrong,
in opposite directions.

What shipped for limit 1 is therefore a test and no production code:
`testAHoleInTheLogDoesNotSurviveActivation` plants a log with sequence numbers 0 and 2 and asserts the
shard's local and global checkpoints reach 2 anyway. It is a characterization test — the behaviour lives in
core — and its value is that it fails if activation ever stops going through `recoverFromStore`. It was
canaried by disabling the `fillSeqNoGaps` call in `StoreRecovery` and observing the checkpoint stall at 0,
which is also how the actual mechanism was found: two earlier guesses at where the filling happened were
both wrong, and a stack trace settled it.

## Limit 2 — WAL-append fencing: two defences, doing different jobs

M48's disclosure was accurate: a writer at term T appends to `wal/t=T`, and a successor replays every term
it finds, so records the old writer appends *after* ownership moved replay as though they had been
acknowledged before it. The term-scoped key path separates the two writers' blobs; it never stopped anyone
from reading the older term, which is what recovery does.

### The bound: a node checks its own lease before appending

`BlobLeaseMembership` now keeps the expiry it last published for itself and answers `selfLeaseValidAt(now)`
with no I/O, so the write path can check it on every write. `ServerlessNode#appendOrRelease` refuses past
that deadline and releases the shard.

The reason this is on the write path rather than the heartbeat is the shape of the failure. A node learns
it lost a shard by reading the shard-head, which it does on its heartbeat — and a node that cannot reach
the object store cannot read the head *or* renew its lease. The case where the existing check is guaranteed
not to fire is precisely the case where a zombie is possible. A check costing an object-store read could
not be made per write, so it would not be made at all; a check against a number already in memory can be.

**This bounds and does not fence.** The check and the append are not atomic, so a pause between them long
enough to outlive the deadline still lands the append. That race cannot be closed from the writer's side.
It is closed on the reader's side instead.

Before the first renewal the check answers true: a node that has never published a lease has no deadline
to have missed, and refusing writes on that basis would break a node that writes before its first heartbeat
rather than protect anything. The fence engages once there is a lease to lose.

### The fence: a successor seals the log at takeover

`WalStore#sealAt(term)` snapshots how far the log had been written and records it durably under
`wal/seals/`. `WalStore#replayable(cutoff)` replays only records at or below the cutoff, and skips a term
absent from the cutoff entirely — which drops a zombie that starts a fresh term directory as well as one
continuing an old one. `ShardReconciler` seals immediately before recovery and replays under the result.

Everything acknowledged before the takeover is necessarily inside the snapshot, because a write is appended
before it is acknowledged. Anything appearing after it was written by a node that no longer owns the shard.
A write in flight at the instant of the snapshot is dropped too, and correctly: its caller never received an
acknowledgement.

**The seal has to be durable, and that is the whole design.** A cutoff held only in memory protects the one
recovery that took it. If that node then dies before publishing — which is the case where the log still
matters at all — the next successor takes its own snapshot, by which time the zombie's records have been
sitting in the log looking like ordinary history for as long as it took. Writing the snapshot down makes it
outlive the node that took it. Seals merge by taking the lowest position recorded for each term, because the
earliest seal was taken closest to the moment ownership actually moved and is the tightest true bound.
Re-sealing writes the merged answer and drops the superseded blobs, so one seal carries it; a crash between
the write and the drop leaves both, and merging them gives the same answer.

Seals live in their own directory so `replayable` cannot mistake one for a term and `dropOlderTerms` cannot
delete one while pruning. `deleteAll` does remove them, which is right: that is the log being destroyed
rather than truncated.

### A refactor this forced, and it is an improvement

`ServerlessWriterEngine` used to be handed a `WalStore` and call `replayable()` on it. It is now handed a
`ReplayLog` that returns records, because the cutoff belongs to one act of recovery and the `WalStore` is
the shard's live log, shared with the write path. The engine no longer needs to know a log exists — only
that someone can hand it operations to replay.

## Canaries

- **152 — the self-lease check always passes.** `selfLeaseValidAt` returns true unconditionally. Caught by
  `testANodePastItsOwnLeaseRefusesToWriteAndReleasesTheShard`: the write past the deadline succeeds.
- **153 — the seal is not durable.** `sealAt` ignores existing seals and uses only its own snapshot. Caught
  by `testARecordAppendedAfterTakeoverIsNeverReplayed`, which is built as three generations precisely so
  that an in-memory cutoff fails it: B seals, the zombie appends, B dies without publishing, and C replays
  the zombie's record.
- **156 — no seal is ever written.** The write in `sealAt` is skipped. Caught by *both* fencing tests, which
  is the check that the truncated-log test is not merely riding on the other one.
- **155 — an absent seal entry is treated as unbounded** rather than as "this term was empty". This is the
  rule that makes an empty seal authoritative, and **the canary did not fire**, which is worth recording
  rather than quietly dropping. The case it guards needs a term directory that did not exist when the seal
  was taken, and `FsBlobStore` keeps a directory after its blobs are deleted, so a truncated term is still
  listed and the seal still mentions it. On a real object store there are no empty directories, the term
  vanishes, and the rule is what stops a zombie recreating it. The rule is kept — it is strictly the safer
  reading — but it is **unproven here**, and it is unprovable against this store rather than merely
  untested. It belongs to D5 with everything else that needs a real provider.
- **154 — gaps are not filled.** `fillSeqNoGaps` disabled in `StoreRecovery`. Caught by
  `testAHoleInTheLogDoesNotSurviveActivation`: the local checkpoint stalls at 0 instead of reaching 2.

## What this does NOT establish

- **The window between winning the head and sealing is still open.** The shard-head is won in
  `MetadataPlane#activate`; the seal is taken later, when the shard is opened. Anything the predecessor
  appends in between falls inside the snapshot and is replayed. Narrowing it means sealing at the
  compare-and-swap, which the path that opens a shard from projected truth rather than from an acquisition
  does not have. What is closed is the unbounded half: after the seal the predecessor can append for as long
  as it likes and none of it is ever read, by anyone.
- **A zombie's writes to its own local Lucene are not fenced by any of this.** It cannot publish them —
  `SegmentPublisher` fails with `StaleWriterException` — and its log records are now sealed out. What it
  can still do is answer reads from its own copy until it notices, which is a staleness bound, not a
  divergence.
- **Sealing costs one object-store write per shard activation**, on the scale-from-zero path. It is a small
  blob and activation already does several round trips, but it is a new cost on a path where the previous
  cost was zero, and it is the reason `RendezvousBlobStore` had to be taught that a seal is not part of the
  request under test — activation writes to shard data now, and it did not before.
- **Clock skew is still assumed away.** The lease check compares this node's clock against a deadline this
  node stamped, so it is self-consistent; but the successor's decision that the predecessor is dead uses
  *its* clock against the same stamps. `NodeLease` and `BlobLeaseMembership` have always said so.
- **No conditional writes in `_bulk`**, unchanged from M48.
- **Nothing here is proven against a real object store** (D5/R11), unchanged.

M49 is done.
