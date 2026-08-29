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
- ~~**The log is never reclaimed across terms.**~~ Closed below.

# Reclaiming the log across terms

The gap above, closed. `onPublished` still trims the publishing writer's own term a cycle late, and now
also drops every record under a term strictly below it.

## Why a lower term is safe to drop outright when the current one is not

The same-term rule is conservative because records are appended *before* being applied, so a record
present at the moment of a flush is not necessarily in that flush. That window does not exist for another
writer's term: a successor replays every older record **before** it starts, so by the time it publishes at
term T those records are in the engine and therefore in the commit.

The case that looks uncovered is covered by ordering rather than by timing. A zombie still appending under
its old term after the successor replayed has records that are genuinely not in the commit — and deleting
them loses nothing, because they were already destined to lose. Replay is ordered term-then-ordinal, so
`t=1` is always applied before `t=2`: for a shared document id the zombie's write is overwritten anyway,
and for an unshared one it wrote to a shard it no longer owned. That is what fencing means.

## Three canaries, all caught

| Planted defect | Caught by |
|---|---|
| No cross-term drop at all | both new reclamation tests |
| Drop the publishing writer's own term too (`>` instead of `>=`) | the existing one-cycle-lag test, and the new test's own-term assertion |
| A successor tidies up the log it just replayed (drop moved into `replayable()`) | `testReclaimedRecordsAreAlreadyInACommit`, and **only** that one |

The third is the one worth having. It is the tempting version of this change — clean up after reading —
and no test written before this round noticed it, including the end-to-end failover test. It is caught by
going three failovers deep with the middle two activating *without* publishing, which is only possible
because `activateWriter` and a reconcile tick differ: a tick publishes in the same pass it activates in.

## A test that had stopped meaning anything

`testAGarbageCollectionSweepDoesNotTouchTheWriteAheadLog` broke, correctly. It asserted the log was
non-empty after a sweep — but the log is now legitimately empty by then, reclaimed by the publish, and the
assertion could no longer tell that from a sweep having eaten it. Its other assertion, that no collected
name contains `"wal"`, never could: the collector names what it deletes `t=N/blob`.

Rewritten, it says less than it looked like it said, and that is the honest reading. The sweep skips any
container at or above the live manifest's term, so the **only** log records it could ever reach are ones a
publish has already superseded — exactly the ones `WalStore` deletes itself. Collecting them would lose
nothing. What the test defends is ownership, not data: two components must not both reclaim the log when
only one of them states a rule for when that is safe. It now plants a stale-term record, counts before and
after, and is checked against a sweep made recursive.

## What it costs

Measured, both stores, as listings on a publish carrying one document:

| | publish that reclaimed 3 dead terms | the next publish |
|---|---|---|
| s3 (MinIO) | 4 | **1** |
| fs | 4 | **4** |

On a bucket, emptying a prefix makes it stop existing, so a reclaimed term is not there to be listed again
and the cost falls by exactly one listing per dead term. On a filesystem the emptied directories remain and
are re-listed on every publish forever. That is a recurring toll, and it is tolerable only because the
filesystem store is a test fixture (D5) where a listing is a system call rather than a billed request. Both
numbers are asserted, so the fixture's behaviour cannot change quietly.

The first version of that measurement compared the activation tick against the following tick and reported
35 requests against 4. The difference was entirely a publish happening versus not happening — publication
is edge-triggered, and the second tick had nothing dirty. Both measurements are now publishes.

## Still open

- Nothing reclaims a shard's log if **no successor ever publishes** — an index written once and abandoned
  keeps its records until the index is deleted. Bounded by what was written, not growing, so it is a
  tidiness problem rather than a correctness one.
- A persistent zombie appending under an old term is reclaimed only when the live writer next publishes.
  Bounded by the publish interval, and every record it writes is one that loses on replay.
