# Deletes, and why they had to be in the log

The durability promise ran one way. Every acknowledged **write** survives its node dying — proved in one
JVM, across processes, and out of a bucket. There was no matching claim for deletions because there were
no deletions, and the moment there are, the existing machinery stops being merely incomplete and becomes
actively dangerous.

## The failure this exists to prevent

Replay is an idempotent redo of state: a successor rebuilds a shard by applying every record in the
write-ahead log to whatever the last published commit contained. **A deletion that was acknowledged and
not logged is simply absent from that reconstruction, so the document it removed comes back.**

Nothing errors. The caller was told the delete worked. The failover reports a successful recovery. The
document is there. That is the exact failure class this project keeps hunting — a wrong answer wearing
the clothes of a correct one — and it would have been introduced by adding a delete endpoint without
touching the log.

`testASuccessorDoesNotResurrectADeletedDocument` is the test, and the `delete-not-logged` canary
(deletion applied, never logged) confirms it fails without the fix.

## Ordering is load-bearing

Records replay **in log order**, mixed, not grouped by kind. A document written, deleted, and written
again must end up present; one written and deleted must end up gone. Applying every write and then every
delete gets the first case wrong — it would delete the document it had just been told to re-create.

`testAReWrittenDocumentSurvivesItsOwnEarlierDeletion` pins it, and the canary that reverses replay order
fails it. (My first attempt at that canary did not compile, and a compile failure is not evidence a test
catches anything. It was rewritten to reverse the record list, which compiles and is meaningful.)

## Format compatibility, deliberately one-directional

The `deleted` marker is written **only for deletions**. A log produced before deletes existed has no such
field, and a reader that has never heard of it reads those records as index operations — which is exactly
what they are. `testALogWrittenBeforeDeletesExistedStillReadsAsWrites` pins that, because a format change
that quietly reinterpreted old records would turn every previously-written document into an ambiguous
one.

## What a delete reuses

Everything. A deletion routes by the same document hash, forwards over the same transport action, is
fenced by the same term and logged to the same file. Only the terminal call differs. The forwarded
request carries a `deletion` flag rather than getting a parallel action, and the transport action was
renamed from `document/index` to `document/write` to stop lying about what it carries.

A delete also raises the same publication edge as a write. A shard whose only recent change was a
deletion still needs publishing, or the tombstone lives nowhere but the writer's disk and its log.

## Statuses

- `200` with `"result":"deleted"` when a document was removed.
- `404` with `"result":"not_found"` when there was nothing to remove — a delete that removed nothing must
  not claim success.
- `durable: write-ahead log` on both, which means more for a deletion than for a write: the tombstone is
  in the log before the response exists.

## What this does NOT establish

- **No `_bulk` delete, no delete-by-query.** One document at a time, and the measured cost is one
  object-store write per deletion, exactly as for an index operation.
- **No `if_seq_no` / version-conditional delete.** A delete removes whatever is there.
- **Nothing about garbage collection.** A tombstone must survive until every commit that could contain
  the deleted document is gone; the GC sweep has not been re-examined in the light of deletions, and a
  sweep that reclaimed a segment a tombstone still needs would resurrect documents by a different route.
  **This is the obvious next thing to check.**
- **Not tested on a bucket or across processes.** The failover test runs two nodes in one JVM against a
  local directory; the bucket and multi-process suites do not delete anything.
