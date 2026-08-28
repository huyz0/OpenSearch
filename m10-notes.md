# M10 — Durability

- Code: `serverless/shell/src/main/java/org/opensearch/serverless/store/Wal*.java`, publication in `BackgroundReconciler.tick`, `ServerlessNode.index`
- Tests: `ServerlessDurabilityTests` (5)
- Result: **84 tests, 0 failures**; `check` green on both projects.

## Two gaps, and they were different

**M10-a — publication was manual.** `publishShard` was only ever called by hand, so the loss window was
not "since the last commit"; it was **unbounded**. A node could index for hours and lose all of it.
A tick now publishes what changed, and publishes nothing when nothing did — an idle shard must not
upload a fresh commit every tick forever, which is an object-store bill for saying nothing happened.

It lives in `BackgroundReconciler.tick` rather than on its own timer so it inherits the tick's ordering:
a node that lost a shard released it earlier in the same pass, so it cannot reach the publish loop
holding something it no longer owns.

**M10-b — the WAL.** Even with automatic publication, everything written since the last publish was
lost. Phase 6's zombie test measured exactly that and recorded it as the WAL gap rather than a fencing
failure. A record is now durable before the write is acknowledged.

## The end-to-end test M10 exists for

```
A indexes 5 documents  →  a tick publishes them   (nobody calls publishShard)
A indexes 3 more       →  no publication covers these
A dies                 →  no release, no drain, no final flush
B takes over           →  serves 8
```

Phase 4 could not write this test; phase 6 could only measure its absence.

## Ordering, and why it is that way round

`ServerlessNode.index` appends to the WAL **then** applies to the engine. A crash between the two
replays a write the caller was never told about — harmless, because replay is idempotent. The reverse
order would acknowledge a write no successor could recover. Cheap to get backwards and expensive to
notice.

## Truncation lags one publish cycle, deliberately

Records are appended *before* being applied, so a record present when a flush starts is not necessarily
*in* that flush. Deleting what the current publish snapshotted could drop a write that had not landed.
So each publish deletes what the **previous** publish saw — a full cycle has elapsed, so the record was
applied and committed. Replaying a few extra records costs nothing because replay is idempotent.

## Canaries — each half isolated

| Canary | Failure produced |
|---|---|
| Tick never publishes | `a commit should now exist in the object store` |
| WAL never appends (publication left working) | `expected:<8> but was:<5>` — exactly the three unpublished writes |

Run separately: together they fail everything, and the second number is the one that shows what the WAL
specifically contributes.

## Two bugs found on the way

**The WAL held only the last write.** `reconciler.wal()` built a *new* `WalStore` per call, so the
append ordinal restarted at 1 every time and every record overwrote the same blob. The end-to-end test
caught it as `expected:<8> but was:<6>` — five from the commit plus the one surviving record. A
`WalStore` carries state (the ordinal, and the snapshot that makes truncation lag a cycle), so it is now
memoized per shard.

**The log claimed files it did not write.** Lucene's test framework plants junk files — `extra0` — in
directories precisely to catch code that assumes everything in a directory is its own. Replay was
parsing them. Records are now identified by name (a 20-digit ordinal), and the three cases are kept
distinct: **absent** is fine, **foreign** is skipped because a stray object must not brick a shard's
recovery, and **ours and unparseable** propagates with the blob named — a corruption error that does not
say which record failed leaves an operator nowhere to look.

## What M10 does NOT establish

- **This is a document-level redo log, not an operation log.** It restores document *state*, not
  *history*: sequence numbers are not preserved, so `if_seq_no` optimistic concurrency and anything
  replication-shaped cannot be layered on it unchanged. That is also what makes replay idempotent, so
  it is a trade rather than an oversight.
- **One blob per record.** The design's own principle is "batch writes, stream reads"; a group commit
  belongs here and is not built, so a write costs an object-store PUT.
- **Writes go through `ServerlessNode.index`**, which no REST endpoint calls yet. M11 is what makes this
  reachable by a user.
- **Deletes are not logged.** Only index operations. A delete since the last publish is still lost.
- **Nothing about S3 or GCS** (D5/R11), unchanged.
