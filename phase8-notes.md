# Phase 8 — reconcilers, GC, and the gossip gate

- Code: `serverless/shell/src/main/java/org/opensearch/serverless/reconcile/`
- Tests: `ServerlessReconcileTests` (5)
- Result: **61 tests, 0 failures**; `check` green on both projects.

> **Phase 7 was skipped.** The allowlisted admin/stats surface was not built. That matters for one
> claim below: §10.3 gates gossip on evidence "from phases 1–7", and phase 7's evidence does not exist.
> The gate is answered here with a measurement instead, and its limits are stated.

## The loop that notices

Phase 6 named this gap explicitly: leases could expire and a successor could take over, but nothing
*noticed* — a node had to be told to activate. `BackgroundReconciler.tick()` is the noticing, and it is
deliberately not privileged. Every node runs the same loop, any number may run it at once, and a node
that stops running it harms only its own shards.

One tick, in this order and for a reason:

1. **Heartbeat** — renew what is held, release what is lost. First, so a node stops serving a shard it
   no longer owns before doing anything else.
2. **Activate what is wanted but unheld** — losing is silent and retried next tick.
3. **Refresh hints** — last, so they reflect the tick's own effects.

Tested end to end: a node acquires, loses the shard to a thief, *releases it on the next tick without
stealing it back*, and re-acquires automatically once the thief's lease lapses.

## Garbage collection, and the mistake it is shaped to prevent

A blob is deleted only when **both** hold: it is in a term container strictly older than the live
manifest's term, **and** the manifest does not name it.

The second condition is the one that matters. Phase 4 made failover *inherit* segment files rather than
re-upload them, so files a live commit depends on routinely sit in older term containers. **Collecting
by term alone deletes exactly the data the current writer is serving.**

The first condition is why the live term is excluded outright rather than trusted to the reference
check: publishing uploads files and *then* swaps the manifest, so there is a window where a live
writer's files exist and are unreferenced.

Both rules were verified by breaking them:

| Canary | Failure produced |
|---|---|
| Collect by term alone, ignoring references | `GC deleted a file the live commit depends on: t=1/_0.cfe` |
| Live term no longer excluded | `the live term must never be collected` |

The second needed a test written for it — a blob written into the live term container that the manifest
does not name, standing in for a publish in flight. Without that the rule was unfalsifiable, since every
live-term file in the ordinary case *is* referenced.

## Routing hints

Soft state, and the word is load-bearing: a cache of shard-heads, not a truth. The test asserts that a
hint **stays wrong** after ownership moves and is corrected only by a refresh — because a hint that
updated by magic would be a truth, and this is not one. The rule in the class documentation: never write
based on a hint; ownership is a read away.

## The gossip gate (§10.3), answered with a number

A steady-state tick costs **3.0 object-store operations per shard**: 2 reads, 1 write, measured over 10
passes with a counting blob store.

The write is the per-shard lease renewal, and it dominates. That is a design finding, not just a
measurement: `rfc-serverless-metadata-plane.md` §7 already anticipates it — *"batch to one lease object
per node with shard-heads referencing the node lease"* — and that batching is **not implemented**. The
measurement is what makes the size of the prize concrete.

Extrapolating `N nodes × S shards × 3 ops / T seconds`:

| Fleet | Interval | Requests/s |
|---|---|---|
| 10 nodes × 10 shards | 1 s | 300 |
| 10⁴ nodes × 1 shard | 1 s | 3 × 10⁴ |
| 10⁴ nodes × 100 shards | 1 s | 3 × 10⁶ |

**Verdict: gossip stays unbuilt.** Two mitigations come first and are cheaper: batching lease renewal
per node (§7, removes the write, roughly a third of the cost) and lengthening the interval where nothing
needs sub-second propagation (§9.5). The row that breaks is 10⁴ × 100 at one second, and nothing built
so far approaches it.

> **Settled since (phase 7).** Phase 7 was built after this and does not move the number: its endpoints
> are operator-rate, not request-rate, and none runs on a data path. What is still genuinely absent from
> the evidence is the data surface — `_bulk` and `_search` over REST — which no phase has built.

**What this measurement cannot tell you:** it was taken against `FsBlobContainer` on one machine with
one shard. It counts operations, not latency, not throttling, and not what a provider does to a client
issuing 3 × 10⁶ req/s. Phase 7's REST traffic, which §10.3 expected to be part of this evidence, is not
in it at all.

## Follow-up: three of these gaps are now closed

Written after the fact, when phase 8 was revisited. The measurement above is what made the first one
worth doing, so it is recorded here rather than in a separate document.

### §7's batched lease renewal — built, and re-measured

A shard-head no longer has to carry its own expiry. `MetadataPlane(..., nodeLeaseLiveness = true)` gives
`ShardHeadStore` a `LivenessOracle`, and a head is then held for exactly as long as its owner's node
lease is. One renewal covers every shard the node holds.

Re-measured at 4 shards, reads and writes separated because only the writes were ever the claim:

| Mode | Reads/tick | Writes/tick |
|---|---|---|
| Per-shard expiry | 8 | **4** — one per shard |
| Node-lease liveness | 8 | **1** — one per node |

Writes stopped scaling with shard count, which is the whole of §7. Reads did not, and are not claimed
to: losing a shard is something only its head can tell you, so a node still reads each one. Turning that
into a push is what gossip would be for, and the gate above is unchanged.

The oracle takes the owner's **ephemeral id** as well as its node id. A node that restarted has the same
name and a new process, and treating it as alive would hand a shard back to something with no idea it
ever held one. Tested directly.

### A bug the multi-shard tests found

Every test before this held a single shard, which hid a real defect: `IndexService` is created once per
index but shards arrive one at a time, so by the second shard its metadata — including per-shard primary
terms — was stale. The shard opened at the old term and `updateShardState` then rejected the new one
with *"term is only increased as part of primary promotion"*. The reconciler now refreshes index
metadata before opening a subsequent shard.

Worth noting how it was found: not by review, but by the first test that happened to want four shards.

### GC sweep and lease tidying

`collectAll(plane)` sweeps every shard of every index, and `collectExpiredLeases` removes dead node
leases. The sweep is explicitly the small-deployment shape — a pass proportional to the whole population
is exactly what the metadata plane exists to avoid, and the right answer is a sharded sweep with each
worker taking a slice by hash. Named as such rather than presented as the design.

Lease tidying is tidiness, not correctness: an expired lease is already filtered on read, so leaving it
changes no answer. It is collected so a deployment that has cycled nodes for a year does not list a
year of dead ones to find the live few.

## What phase 8 still does NOT establish

- **Ticks are called, not scheduled** — same as phase 6. A timer would make every test time-dependent.
- **The batched path is opt-in**, and every earlier test still runs on per-head expiry. Both paths are
  live, which is one more thing than a finished system should have.
- **The sweep is not sharded**, per above.
- **No gossip**, by the gate above rather than by omission.
- **Nothing about S3 or GCS** (D5/R11), unchanged — and the gossip measurement is exactly the sort of
  thing R11 would change the shape of.
