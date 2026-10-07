# Acknowledging writes before the shard is open

**Status:** deferred; nothing built. Reopen if either holds:
- the lease is made much shorter, so the open rather than the lease is most of a failover's recovery;
- a workload dominated by blind writes needs sub-second time-to-first-ack after a failover.
**Question:** after a takeover, can the new owner acknowledge writes as soon as it holds the shard-head and has fenced
the older terms -- before the engine is open and the log replayed -- and finish the open in the background? A log-first
store (TurboPuffer's shape) acknowledges on the log alone; this shell acknowledges on the log too, but only after the
engine has applied the write.

## Where the time goes today

From the fleet harness, six nodes, cap 1,000, a node holding ~280-350 shards killed (`scale-test-results.md`):

| | p50 |
| --- | --- |
| kill to lease expiry | ~28 s |
| expiry to a survivor holding the head | ~11 s |
| head to first acknowledged write | ~11 s |
| kill to first acknowledged write | ~50 s |

One writer activation, averaged over a steady run: acquire ~115 ms (head swap, older terms fenced, claim written),
rollup mark ~60 ms, local view 15-700 ms (it waits on a lock shared with other activations), and the open ~1 s:
create shard ~260 ms, fence and establish ~90 ms, recover ~620 ms. Both of the 11 s waits are mostly queueing: a node
opens 8 shards at a time, and an acquisition waits behind the opens ahead of it in the same queue.

Early acknowledgement would take the open (and the view and the mark) out of the path to the first ack. It would not
touch the 28 s.

## How a write is acknowledged today

From the code (`ServerlessNode#index`, `#appendOrRelease`, `WalStore`, `ServerlessWriterEngine`):

1. The write passes the shard's fence and the publish guard, and checks the shard is open, is not a reader, is not
   write-fenced, and belongs to the live incarnation.
2. **The engine applies it first.** `applyIndexOperationOnPrimary` assigns the seqNo, checks `if_seq_no`/
   `if_primary_term` against the live version map, and assigns `_version`.
3. **Then it is logged.** The record carries id, source, seqNo, primary term and version (or a deletion marker).
   It is appended through the group committer: one PUT in flight per writer, put-if-absent ordinals.
4. Before the PUT: the node's own lease must be valid, and `wal.begun(term)` must hold. That is the term-exact
   check: this instance established this term and has not been poisoned.
5. The PUT lands; the write is acknowledged with the engine's `_seq_no`, `_primary_term` and `_version`.

Replay on open reads every non-fence record of every live term, after fencing. Records with a seqNo are fed to
core recovery as the operations they were, so `_seq_no` survives a failover. Core skips those at or below the
commit's checkpoint, and `ServerlessWriterEngine` refuses to open on two different operations with one seqNo. Records
without a seqNo -- an older format -- are applied after the shard starts, as fresh primary operations with new seqNos,
in log order.

So the log is not the source of seqNos: the engine is. That is the crux.

## Settling the questions

### SeqNos before replay

The new owner cannot number a write before it knows the highest seqNo in the commit plus the log. The commit's
manifest gives the published max; the log's tail is what replay reads. Learning it is exactly the read that recover
spends its time on.

Could the log ordinal plus the term stand in, with seqNos assigned at replay? Mechanically yes. **The shell already
has that format:** a record without sequence identity is applied at open as a fresh primary operation, in log order.
An early write would be logged in that shape and numbered when the open replays it.

What it does to the API:

- **The response cannot carry `_seq_no` or `_version`.** Both are unknown when the ack is sent. The REST contract
  promises them on every index, delete and bulk item. Returning `-2` (unassigned), or leaving them out, is a visible
  change that clients may parse.
- **`if_seq_no`/`if_primary_term` against an early-acked document cannot be answered** until the open has numbered
  it. A client that indexes and then updates conditionally on the response's `_seq_no` has nothing to send.
- **The numbering order is the log order**, not arrival order at the engine. Within one writer that is the same
  thing: one append in flight, in order. Across the cut-over to the opened engine, the open must apply every early
  record before the engine takes its first write. Otherwise a later write can be numbered below an earlier one, and
  core recovery and the checkpoint tracker assume seqNos follow application order on a primary.
- `_version` is reconstructed at replay from the document's history, not known at ack. For a blind index that is
  invisible except in the response.

### Which operations could be acknowledged early

| operation | needs engine state? | early ack? |
| --- | --- | --- |
| index with an explicit id, no conditions | no | yes, without `_seq_no`/`_version` in the response |
| index with an auto-generated id | no | yes, the same way |
| delete by id | **yes**: `found`/`not_found` (200 or 404) comes from the engine | only if the response stops saying whether it was found |
| `op_type=create` / `_create` | **yes**: must know the id is absent | no |
| `if_seq_no`/`if_primary_term` | **yes** | no |
| `_update` (doc, script, upsert) | **yes**: a realtime get, then a conditional write | no |
| get (realtime) | **yes**: the live version map | no; it would also miss early-acked documents until the open applies them |
| bulk | per item | only if every item is early-ackable, or by splitting the bulk into acked and waiting parts |

So "blind index only" is early-ackable. A delete that does not report found is ackable too.

That covers the harness's load, and probably most log ingestion. It does not cover anything that reads before it
writes. During the open, a realtime get for a document acked a second ago would 404 or return the previous version:
read-your-writes, which the owner gives today, would be broken for the length of the open.

### The fence invariants

- **One append in flight** and **put-if-absent ordinals**: unchanged. Early records go through the same committer
  and the same `WalStore` instance.
- **The term-exact writer:** `begun(term)` is set by `establish`, which today runs inside the open. It would have to
  run at takeover, right after `fenceOlderTerms` at the head swap. It already can: it needs the log and the ownership
  check, not the engine. `establish` fences the term's own next slot and re-reads the head. A writer superseded
  before its first append poisons itself, as now.
- **The fenced marker:** unchanged; it is about older terms.
- **The in-log duplicate-seqNo refusal:** early records carry no seqNo, so they cannot collide in the log. They are
  numbered at open in log order, after every numbered record of earlier terms. This is the path legacy records
  already take.
- **New invariant -- the cut-over.** Early records and engine-numbered records of the same term must not interleave
  in a way replay cannot order. A successor replays the term in ordinal order: numbered records go to core recovery
  by seqNo, and unnumbered ones are applied afterwards as fresh operations. The pattern [early A] [numbered B]
  replays as B then A, which reverses them. If both name the same document, the wrong version survives.

  So the writer must never append a numbered record while an unnumbered one of its term is unapplied. Early writes
  stop, and further writes wait, the moment the open begins applying the early records. The engine takes writes only
  once every early record is applied, and from then on the term is all numbered. One switch, under the fence write
  lock.

  It is enough within one instance. A same-term reopen fences the term first, as `establish` does today.

### Failure cases

- **The owner dies after early acks, before its open finished.** The early records are in the log, below no fence
  of its own. The successor fences the term at its swap, then replays: the unnumbered records are applied as fresh
  operations, in order. Nothing acknowledged is lost. The successor numbers them; the dead owner never did. The
  only visible effect is that those documents get their first `_seq_no` from the successor, which the client never
  saw anyway.
- **The owner dies mid-cut-over,** after applying some early records and numbering later ones. The successor
  replays the term in ordinal order. The early records come first, because every numbered record of the term was
  appended after the switch. Applying them as fresh operations after core's recovery puts them *after* the numbered
  ones. That is the reversal above, in a different place.

  So replay must apply a term's unnumbered prefix before its numbered suffix. Today unnumbered records are always
  applied last, which is right only because they all predate numbered records in older terms. That is a change to
  `replayRecordsWithoutSequenceIdentity`'s ordering, and it is the riskiest part.
- **Readers during the background open.** `shard_behind_log` holds as it is. A reader's commit is behind the log
  whenever the term has landed past the published ordinal, and early records land past it. Readers would refuse
  (503) rather than serve a commit missing acknowledged writes, which is the property to keep. That means
  searches of a shard being opened after takeover fail for the length of the open, as they do now (there is no
  owner to forward to, and the commit is behind).
- **An ambiguous PUT** poisons the writer as now. An early write is never acknowledged on an ambiguous outcome,
  and it may still replay, which is the existing "refused may replay when ambiguous" assumption.
- **The lease lapses during the open:** writes are refused as now (the lease check precedes the append).

## The gain

Per shard, the time from holding the head to the first possible ack would drop from mark + view + open (about
1.2-2 s each) to establish (~90 ms).

What matters is the queue. Acquisitions sit behind opens in one per-node queue, 8 at a time. Acknowledging early
means a node needs only the acquire and establish (~200 ms) per shard before writes flow. For ~350 shards over five
survivors that is roughly 350 / 40 slots × 0.2 s ≈ 2 s, instead of ~18 s. Opens would continue behind it.

Estimate: head-to-first-ack p50 from ~11 s to ~1-2 s, and the head-held wait shrinks too. Kill-to-first-ack p50 from
~50 s to ~32-35 s, of which 28 s is the lease. For writes that are blind indexes only.

Most of that gain does not need early acks. It needs **acquisitions not to queue behind opens**: a separate, larger
pool for acquire+establish, with opens on their own pool. On its own that brings the head-held time down to ~1-2 s
after expiry. Writes still wait for their own shard's open (~1-2 s), so kill-to-first-ack p50 would be about
31-33 s, against 32-35 s with early acks. And it changes no API, no replay order, and no invariant.

## Recommendation

**Not worth building now.** It breaks `_seq_no`/`_version` in responses and read-your-writes during the open. It
refuses or delays every conditional, create, update and found-reporting delete. It adds a cut-over invariant and a
replay-order change on the path where data loss would hide. For blind-index load it buys perhaps 1-3 s of p50 over
the alternative, against a 28 s lease that dominates either way.

Do instead, in this order:
1. **Separate acquiring from opening.** A takeover burst acquires and establishes everything it will take within
   seconds, on a pool that does no opens. Opens run on their own pool, hottest first.
2. **Make the open cheaper** (Part A): recover and create-shard are most of it.
3. Revisit early acks only if a workload is blind-index-only, and the lease has been shortened to the point where
   the open is the larger part of recovery.
