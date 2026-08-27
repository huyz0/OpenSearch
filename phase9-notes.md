# Phase 9 — scale validation

- Tests: `ServerlessScaleTests` (5)
- Result: **75 tests, 0 failures**; `check` green on both projects.
- Purpose: re-measure `plan-100m-index-implementation.md`'s targets on this shell.

Measurements assert on **trends and operation counts**, never on absolute milliseconds. A wall-clock
threshold would make the suite a machine-speed detector rather than an architecture check.

## Target 1 — creation cost is flat in the population ✅

The claim the whole architecture rests on: *index creation costs the same at 100 million indices as at
zero*. Classic OpenSearch cannot say this — the same document measured 7.4 ms/create at 200 indices,
10.3 at 1,000, 34.6 at 3,000 and 98.8 at 6,000, because `Metadata.Builder.build()` rebuilds six name
arrays and a sorted lookup across every index on every create.

Measured here, six batches of 250 from an empty deployment to 1,500:

```
ops/create    = [1, 1, 1, 1, 1, 1]
micros/create = [577, 630, 571, 583, 553, 461]
```

**One object-store write per create, whatever is already there.** The time series is flat and, if
anything, drifts down — which is filesystem cache warming, not the architecture.

## Target 2 — node residency tracks the working set ✅

1,000 indices exist. A node holding one shard of one of them has **one index in its cluster state**, and
does not know the other 999 exist. This is §5's central claim, asserted directly.

## Target 3 — steady-state read cost ❌ then ✅

**This is what phase 9 was for.** `truthFor` — which runs on every reconciliation tick, on every node,
forever — opened with `descriptors.listAll()`:

| Population | Ops to read one node's truth (it hosts 1 shard) |
|---|---|
| 200 | **401** |
| 1,000 | **2,001** |

`2N+1`. The exact O(population) shape this design exists to remove, reintroduced in its own code on the
hottest path it has. At 100M indices that is 200M operations per tick per node.

Worth being blunt about how this survived eight phases: every earlier test had a handful of indices,
where 2N+1 and O(1) are indistinguishable. Nothing was wrong with those tests; they were measuring
correctness, and this is not a correctness bug. It is only visible when you go looking for the shape.

### The fix

A per-node assignment listing at `assignments/<nodeId>/`, written after a successful compare-and-swap.
`truthFor` lists that instead of the world.

| Population | Ops after |
|---|---|
| 200 | **3** |
| 1,000 | **3** |

**The listing is a hint, not truth.** A shard-head is still the only thing that says who owns a shard;
the listing only narrows the search from "every index" to "the ones this node last claimed", and every
entry is verified against its head before being believed. Consequences, each tested:

- A **stale** claim (ownership moved away) costs one read and is ignored. Canary-verified: dropping the
  ownership check produced *"a stale claim must not make a node believe it still owns a shard"*.
- A claim whose **index was deleted** is skipped rather than throwing.
- A **missing** claim — the process died between the CAS and the write — is self-healing: the node does
  not see the shard, re-acquires it, and the write happens then. The order matters and is deliberate:
  writing the claim first would advertise a claim that was never won.

Claims are dropped on release, so a node that has churned through shards does not list a year of dead
ones to find today's.

## What phase 9 does NOT establish

- **The measurements stop at 1,500 indices**, not 100 million. What is shown is the *shape* — flat
  ops/create, constant truth-read cost — not the endpoint. A shape that holds to 1,500 can still break
  at 10⁸ on something not exercised here, and listing behaviour at scale is the obvious candidate.
- **`FsBlobContainer` only** (D5/R11). A local directory listing is not `ListObjectsV2`, and the
  assignment listing introduces a new dependence on listing performance that a real object store prices
  very differently. This is now a reason R11 matters more, not less.
- **No shard-count scaling.** One shard per index throughout. A node holding thousands of shards costs
  one listing plus 2 ops per shard, which is O(owned) but not free.
- **`GarbageCollector.collectAll` is still O(population)** by construction, and says so. It is a
  background sweep rather than a steady-state path, but it is the next thing that needs the same
  treatment.
- **Nothing about memory.** `plan-area-h` measured heap per index (698 B for a stub) as well as time.
  Residency is asserted as a count of indices in cluster state, not as bytes.
