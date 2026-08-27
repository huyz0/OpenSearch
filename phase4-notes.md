# Phase 4 — the write path

- Code: `serverless/shell/src/main/java/org/opensearch/serverless/store/`, plus roles on `ServerlessNode`
- Tests: `ServerlessWritePathTests` (5)
- Result: **45 tests, 0 failures**; `check` green on both projects.

## The gap phase 3 named, closed

Phase 3's failover test moved shard *ownership* but not shard *data*, and said so explicitly. That test
can now be written:

```
A indexes 3 documents, refreshes, publishes at term 1
A's lease lapses; B wins the head at term 2
B syncs → restores from the object store → serves 3 documents
```

B never spoke to A. There is no peer recovery and nothing is copied node to node — the object store is
the only thing between them. That is the architecture's central claim, running.

## Fencing (§9.6, R9) — partially closed

Two mechanisms, doing different jobs:

| Mechanism | Stops a zombie from... |
|---|---|
| Manifest CAS refuses an older term | ...being *believed* |
| Term-scoped container (`t=<term>/`) | ...*overwriting* a live writer's file |

Both are needed. The CAS alone would let a stale writer clobber a file another node is serving; the
prefix alone would let it claim its own commit is current. With both, a writer that wakes after a long
pause writes bytes nobody reads, and is told `StaleWriterException` if it tries to name them.

**Both halves were verified by making them fail**, separately:

| Canary | Failure produced |
|---|---|
| Fence check removed | `Expected exception StaleWriterException but no exception was thrown` |
| All terms share one container | `the two writers must not share a prefix` |

## Roles are lease attributes (§10.4)

`serverless.roles` defaults to `ingest,search`. A node advertising only `search` does not open writer
shards even when truth assigns one to it — and a node advertising `ingest` does, so the gate is not
vacuous. One binary; asymmetric scaling is two Deployments differing by one environment variable.

## A lesson that recurred

`FsBlobContainer` resolves blob names flatly against a single directory. A blob named `"t=1/_0.cfe"`
does not create `t=1/` — it fails to write. Hierarchy in the blob store is the **container**, not the
name. This was already learned once for the register map in phase 3 and learned again here; the comment
on `SegmentPublisher.termSegment` says "Learned twice" so it is not learned a third time.

## Restore needs a translog, and that is where data can be lost

Restoring segments is not enough: recovery reads a translog whose UUID matches the commit, and a node
taking over has neither. The reconciler now runs the same sequence `StoreRecovery` uses for snapshot
restore — `bootstrapNewHistory`, then an empty translog at the commit's checkpoint.

**This is the current durability boundary and it should not be glossed.** Operations the previous writer
accepted but had not committed and published are lost at failover. The window is "since the last
publish". A write-ahead log is what closes it, and this phase does not have one.

## What phase 4 does NOT establish

- **No WAL.** Named in the phase gate, not built. Durability today is "as of the last published commit",
  and publication is explicit rather than automatic. Both are phase 4½/5 work.
- **No `TransportShardBulkAction` wiring, and no REST write endpoint.** Writes go directly to
  `IndexShard`. Reusing the bulk action needs `NodeClient.initialize` and an action registry — plumbing
  that proves less than the segment path did, and deliberately deferred rather than half-done.
- **Publication is manual.** `node.publishShard(shardId, term)` is called explicitly. Hooking it to
  refresh or a commit policy is what makes durability continuous rather than on demand.
- **Nothing about S3 or GCS** (D5/R11), unchanged.
- **R9 is partially closed, not closed.** A zombie's *published* bytes are inert. A zombie still holding
  an open `IndexWriter` on a shared local directory is a different problem, and does not arise here only
  because each node has its own disk.
- **Scale.** One shard, three documents, two nodes.

## Next

Phase 5 — the search path: `search` role, reader shards over object-store segments, and scale-to-zero
verified by killing every search node and restarting. The restore path built here is most of what a
reader shard needs.
