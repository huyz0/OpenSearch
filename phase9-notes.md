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

## Follow-up: "list every index" was itself the wrong operation

The fix above removed the O(population) sweep from `truthFor`, but left the same sweep sitting behind
`GET /_serverless/indices` and the GC. Raised in review, and correct: **at 100 million indices, "list all
indices" is not a slow operation — it is not an operation.** The answer does not fit in a response,
cannot be consumed by a caller, and is stale before it finishes. Supporting it at all is what forces the
cost; making it faster would have been solving the wrong problem.

The worst offender was a single field:

```json
{ "count": 47000000, ... }
```

A total requires a full scan by definition. It is the kind of innocuous-looking field that silently
reintroduces exactly the cost the architecture was built to remove, and nobody can act on it anyway.

### What replaced it

| Operation | Shape |
|---|---|
| `GET /{index}` | point lookup, one read |
| `GET /_serverless/indices?size=N&after=<cursor>` | bounded page, cursor to resume |
| `DescriptorStore.listAll()` | offline sweeps only, documented as O(population) and barred from request paths |
| `GarbageCollector.collectPage(after, limit)` | one slice; `collectAll` is a loop over it |

The listing reports `size`, `has_more` and `next_after`. **It does not report a total**, and the test
asserts the absence of that field rather than its value — a regression here would be a field reappearing,
not a number changing.

Measured, page size 25:

| Population | Ops to read one page |
|---|---|
| 200 | 26 |
| 1,000 | 26 |

And the slope is exactly one read per index returned: a page of 5 costs 6, a page of 25 costs 26. The
test asserts the *slope* (`ops(25) − ops(5) == 20`) rather than the constant, because the constant is an
implementation detail and pinning it would make the test fail for reasons unrelated to the claim.

*One unexplained observation, recorded rather than smoothed over:* an early run reported 27 rather than
26 reads for a 25-item page, and I could not reproduce it. The slope assertion is unaffected by a
constant, but the population-independence assertion is exact, so if it recurs there is something real to
find.

### Second correction: pagination was still the wrong answer

Raised in review, and right: a cursor walk is better than a full scan, but **the serving path should not
offer index enumeration at all.** S3 is the model — it offers prefix listings and a scheduled inventory,
and there is no API that counts the objects in a bucket. Enumeration is a maintenance operation.

So `GET /_serverless/indices` is gone. The path is registered with an explicit refusal:

```
501  enumerating indices is a maintenance operation, not a serving one;
     look an index up by name, or run an offline inventory
```

`listPage` survives as a **maintenance primitive** — the GC sweep uses it — not as an endpoint. Nothing
on a request path can be pointed at a hundred million indices any more.

### Third correction: lifecycle must be CAS throughout, and it was not

Creation was put-if-absent and a mapping change was a compare-and-swap, but **delete was an
unconditional blob removal**. That made it the one lifecycle operation not ordered against the others: a
concurrent mapping update and a delete could both report success.

`deleteIfUnchanged(name, expectedGeneration)` fixes it. The object store has no conditional delete, so
it swaps the descriptor to a tombstone at the expected generation and then removes the blob — the swap
is the linearization point, and `get`/`listPage`/`listAll` all report a tombstoned index as absent.
Tested: a delete holding a stale generation loses to a concurrent update, and re-reading and retrying
succeeds.

### And the shard list is bounded, so it can stay self-contained

Every caller already derives a shard's identity from `descriptor.numberOfShards()` rather than by
enumerating anything — opening, collecting or describing an index's shards costs one descriptor read.
That only holds while the list fits in an object a single compare-and-swap can replace, so
`IndexDescriptor.MAX_SHARDS = 4096` now enforces it instead of hoping. An index that needs more shards
wants more indices.

**Ownership deliberately stays out of the descriptor.** The shard *list* belongs there — it changes only
on lifecycle operations, one writer at a time. Per-shard *ownership* does not: its writer population is
every node that might hold a shard of that index, and folding it into one object would put a thousand
nodes on one register, which is precisely the contention §9.3's register map exists to avoid.

### A smaller bug this turned up

`/_serverless/shards/{index}` returned **400** instead of 503 on a node with no metadata plane.
`BaseRestHandler` rejects a request whose parameters were not all consumed, and the early return bailed
out before reading `{index}` — turning a deliberate refusal into "contains unrecognized parameter", a
400 blaming the caller for our shortcut. Parameters are now read before any early return.

### The half that cannot be shown here

A page bounds two different costs, and only one of them is demonstrated. The descriptor **reads** are
bounded by `limit` on any backend — that is what the measurement above shows. The **listing** is bounded
natively by S3's `ListObjectsV2` (`start-after` + `max-keys`) and GCS's equivalent; `FsBlobContainer` has
no such primitive and enumerates the directory. So on a filesystem this is bounded in reads but not in
the listing itself, and the missing half is R11-shaped.

## What phase 9 does NOT establish

- **The measurements stop at 1,500 indices**, not 100 million. What is shown is the *shape* — flat
  ops/create, constant truth-read cost — not the endpoint. A shape that holds to 1,500 can still break
  at 10⁸ on something not exercised here, and listing behaviour at scale is the obvious candidate.
- **`FsBlobContainer` only** (D5/R11). A local directory listing is not `ListObjectsV2`, and the
  assignment listing introduces a new dependence on listing performance that a real object store prices
  very differently. This is now a reason R11 matters more, not less.
- **No shard-count scaling.** One shard per index throughout. A node holding thousands of shards costs
  one listing plus 2 ops per shard, which is O(owned) but not free.
- **A sweep is still proportional to the population** — correctly, since it intends to visit everything.
  What changed is that it is now resumable and sliceable (`collectPage`), so a fleet can share it. A
  single worker walking 100M indices is still a single worker walking 100M indices.
- **No name index or prefix search.** `plan-area-a-name-index.md` exists for this and is not built here;
  a cursor walk is not a substitute for finding indices by pattern.
- **Nothing about memory.** `plan-area-h` measured heap per index (698 B for a stub) as well as time.
  Residency is asserted as a count of indices in cluster state, not as bytes.
