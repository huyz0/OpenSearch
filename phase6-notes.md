# Phase 6 — activation and failover

- Code: `activateWriter` and `heartbeat` on `ServerlessNode`
- Tests: `ServerlessFailoverTests` (5)
- Result: **56 tests, 0 failures**; `check` green on both projects.

## Failure detection runs on the node that might be failing

There is no failure detector, no ping, no quorum, and nothing that decides another node is dead. A node
that is paused, partitioned or gone simply stops calling `heartbeat`, its lease lapses, and someone else
acquires by compare-and-swap.

`heartbeat` has two halves and both are load-bearing:

- **Renew** what this node still holds. A live node crossing three TTLs keeps its shard.
- **Release** what it has lost. A renewal that comes back empty means the head no longer names this
  node — which happens exactly when someone else won it — and the shard is closed.

Losing an activation race returns an empty `Optional`, not an exception. Losing is a routing
instruction: the winner's head is in the metadata plane, and the correct response is to route there and
never to retry activation elsewhere.

## The zombie test — R9's actual scenario

`kill -9` cannot produce a zombie: a dead process writes nothing. The dangerous case is a writer that is
**alive, paused past its lease, and still believes it owns the shard**. The test walks the whole
sequence and asserts each stage separately:

| Stage | Asserted |
|---|---|
| Zombie writes and publishes at term T | commit visible |
| Clock passes TTL; successor acquires | successor's term > T |
| Successor writes and publishes | manifest at successor's term |
| Zombie wakes and writes locally | **it succeeds** — nothing stopped it |
| Zombie tries to publish at term T | `StaleWriterException` naming term T |
| Successor's manifest re-read | unchanged, still successor's term |
| Zombie heartbeats | discovers the loss, releases the shard |
| Fresh reader serves the shard | pre-pause doc ✓, successor's doc ✓, **zombie's doc 0** |

The fifth row is the honest one. A zombie's local writes are not prevented — nothing could prevent them
without coordination on every write, which is the cost this design refuses to pay. What is guaranteed is
that they reach nobody: they cannot be published, cannot overwrite a live writer's blobs (term-scoped
containers, phase 4), and vanish when the shard is released.

**That distinction is the point of the test.** The zombie's document being lost is phase 4's WAL gap, not
a fencing failure, and a test that conflated the two would be reporting the wrong thing.

## Canaries — all three claims made to fail

| Canary | Failure produced |
|---|---|
| Heartbeat renews but never releases | `A did not release the shard it lost expected:<[[alpha][0]]> but was:<[]>` (two tests) |
| Renewal does not extend the lease | `B took a shard whose lease was being renewed` |
| (phase 4, still standing) manifest fence removed | `Expected exception StaleWriterException but no exception was thrown` |

Run one at a time, because the release canary also breaks the zombie test — a combined run would have
looked like one failure with two symptoms.

## What phase 6 does NOT establish

- **Heartbeats are called, not scheduled.** Tests drive `heartbeat(plane)` explicitly. Putting it on the
  thread pool is a few lines and deliberately not done here: a timer would make every test above
  time-dependent, and the logic is what needed proving.
- **The clock is injected.** Lease expiry is tested by moving a counter, not by sleeping. Real clock skew
  between a writer and its successor is untested, and the assumption is recorded in
  `BlobLeaseMembership`'s documentation rather than resolved.
- **Unpublished writes are still lost** — phase 4's WAL gap, unchanged and now precisely characterised:
  the window is "since the last publish", and the zombie test is what measures it.
- **No automatic reactivation.** Nothing notices an unowned shard and picks it up; a node must be told to
  activate. That is the background reconciler of §9, and it is phase 8.
- **Nothing about S3 or GCS** (D5/R11), unchanged. Lease expiry against a real provider's clock and
  conditional write is exactly what R11 would test.
- **Single shard, two nodes.** No fleet, no contention at scale.

## Next

Phase 7 — the REST surface: the allowlisted admin and stats APIs re-implemented against the metadata
plane, with everything else still answering 501.
