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

## Checking the sweep, and three tests that proved nothing first

The previous note named garbage collection as the next place a deletion could be undone: a tombstone has
to survive until nothing can bring the document back, and the collector reclaims segment blobs an older
term container holds and the manifest does not name.

Reading the code said the hazard did not exist. **Reading is not verifying, and the first three tests
written to verify it were vacuous** — each passed with the defect it was meant to catch planted in the
code. Recording how, because the failures are more instructive than the conclusion:

| Test as first written | Why it could not fail |
|---|---|
| A deletion survives a sweep | It published twice under one term. Every live file sat in the **current** term container, which the sweep skips entirely, so there was nothing dangerous to sweep. Deleting the collector's manifest check changed nothing. Now it fails over first, so the live commit at term 2 names files inherited from term 1 — collectable by term, saved only by being named. |
| A sweep does not touch the log | It asserted the returned list contained no "wal". Records live at `wal/t=N`, one level deeper than the sweep looks, so no plausible defect could reach them and the assertion was near-tautological. It now counts surviving records directly, and is checked against a sweep made recursive — the realistic future regression. |
| Replay order holds past ten records | Off by one. Filler took ordinals 1–8, the write landed on 10 and the delete on 11 — and `"10"` still sorts before `"11"`, so an unpadded ordinal kept the delete after the write and the test passed. The straddle has to be **9 and 10**, where `"10"` sorts before `"9"` and the delete arrives first. |

With those corrected, four canaries are caught: a collector that ignores the manifest, a collector made
recursive, replay ordered newest-term-first, and an ordinal that is not zero-padded.

### What the sweep turned out to be

Safe, for two reasons that are worth separating because only one of them is deliberate. Tombstones are
part of the published commit, so the manifest names them and the existing rule covers them — that is the
design working. The log is untouched because it sits in a different subtree *and* because the sweep only
looks one level deep — that is two accidents agreeing, and the test now notices if either stops holding.

### One number worth keeping

The live commit at term 2 names **3 files inherited from term 1** while the sweep collects 1 orphan. That
is the shape the collector's second condition exists for, and until this test existed nothing in the
delete suite produced it.

## What this does NOT establish

- **No `_bulk` delete, no delete-by-query.** One document at a time, and the measured cost is one
  object-store write per deletion, exactly as for an index operation.
- **No `if_seq_no` / version-conditional delete.** A delete removes whatever is there.
- **Not tested on a bucket or across processes.** The failover tests run nodes in one JVM against a local
  directory; the bucket and multi-process suites still delete nothing.
- **The log is never reclaimed across terms.** `onPublished` trims only the publishing writer's own term,
  so a dead predecessor's records are trimmed by nobody and replayed by every successor forever. Harmless
  today because replay is ordered and idempotent, and unbounded growth all the same.
