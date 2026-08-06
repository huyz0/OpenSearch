# STATE

Read this first, every time. It is the only source of truth for where work stands, and it
is written to survive context loss: nothing here depends on remembering a previous session.

Updated: 2026-08-06

## Position

| | |
|---|---|
| Active round | 004, the mapping write path ([plan](004-mapping-write-path/plan.md)) |
| Next action | `/round-next`, task T48 |
| Last task | T47 complete |
| Branch | `feature/serverless` |

Rounds 001 to 003 predate this file and have no round directories. Their work is in the git
history and in [rfc-100m-index-architecture.md](../../rfc-100m-index-architecture.md).

**T-numbers are cited in permanent code comments, so check the tree before allocating them.**
This file used to say they ran to T22, which was wrong: the tree cites up to T39, and round 004
was planned as T23 to T32 on the strength of that sentence. Eight of those ten numbers already
belonged to earlier work, so the round was renumbered to T40 to T49 after T45 landed. To find the
next free number, do not trust this paragraph either:

```
git grep -hoE '\bT[0-9]{1,3}\b' -- server/src plugins/serverless-storage/src | sort -u -V | tail -3
```

## Progress

The 100M argument has no structural gap left: records live outside cluster state, creation
and deletion are off the serialized thread, resolution answers instead of silently
emptying, residency is bounded by construction, and mappings are carried.

Measured, with controls:

| | |
|---|---|
| heap per open gated index | 150,888 B |
| file descriptors per open gated index | 3.0 |
| open gated indices per node, 31 GiB heap | ~110,000 |
| creates per second, no declared mapping | 10,505 |
| creates per second, with a declared mapping | 2x to 10x slower, by warm-up; 80%+ of it the mapping store, of which T24 removed ~a seventh |
| deletes per second | 646 |
| eviction under CPU pressure | 19.6 to 1.5 per second |

The last cycle (T13 to T21) was mostly defects found while doing something else rather than
work that was planned: field parameters silently dropped from gated mappings, two blocking
round trips on the cluster manager update thread, and a headline throughput figure that only
held for a population nobody would create.

## Open, not blocked

Work that can start without asking anyone. Round 004 took the mapping cost item; T22 was
refiled as T23 in its plan. The rest are candidates for round 005:

- **Eviction under load**, the RFC's first order-of-work item. Since it was written the
  ceiling gained `indices.gated.max_open` and an eviction on the open path, which may have
  answered it. Whether it did is unmeasured.
- **T14**: cluster-state publication latency against cluster size, which decides whether
  wake and sleep need batching. Estimated at 50 to 200 ms and never measured.
- **T16**: computed placement under hot-tenant skew. A hash cannot know one tenant takes a
  thousand times the traffic, and K=3 gives room to choose rather than solving it.
- Pre-warm before rotation. 12.4% of shards lose all warm candidates at fleet-doubling.
- Manifest sharding aligned with the routing hash, so a coordinator warms its whole
  partition in one read.

## Blocked

Items needing a human decision. The loop skips these and continues with other work.

### B1. Fork or upstream contribution

Computed placement replaces OpenSearch's placement model for serverless indices rather than
optimising it. That is a hard sell upstream while being the obviously right answer for a
serverless-only system. The codec namespace question, reserving a high integer range versus
a distinct blob codec name, is downstream of this and straightforward either way.

Nothing in the current task list depends on the answer, so the loop can run without it.

## Environment

- `-Dbuild.docker=false` on every Gradle invocation. Docker is not running, and a dead
  daemon fails every task including `compileJava`.
- The box is shared and often at load average 20 or higher. Timing-sensitive tests fail
  under contention and pass alone; the rule for telling that apart is in `/round-next`
  step 4.
- Do not kill the unrelated `cargo-mutants` and `cargo test` jobs belonging to
  `lucene-rust`.
