# The serverless metadata plane: design and impact analysis

Branch `feature/serverless`, cut from `f5eaf8ada60` on `feature/pluggable-engine-per-shard-role`.

This proposes moving every durable piece of index and shard metadata into object storage, keeping nothing
on a node that cannot be discarded and rebuilt, and computing placement rather than storing it. A
serverless index would have no cluster state entry, no allocator involvement, and no durable local
footprint. A traditional index would behave exactly as it does on `main`.

That last sentence is what most of this document is about. The design is only affordable if the core
changes it needs are inert when the plugin is absent, and the existing branch already establishes the
pattern for that.

---

## 1. The target, as invariants

**I1. Durable state lives only in the object store.** Index descriptors, mappings, settings, shard heads,
commit manifests, segments, the write-ahead log, tombstones. Nothing else is a source of truth.

**I2. Every local structure is reconstructible.** Memory and local disk hold caches. Losing a node loses
speed, never data. There is a path from the object store alone back to any local structure.

**I3. Placement is computed, never stored.** Index-to-node and shard-to-node are a pure function of shard
identity and a membership epoch. No routing table is published for a serverless index.

**I4. Core is unchanged when the plugin is absent.** Not "close to unchanged." Identical, and demonstrable
by running the core suite with nothing registered.

I4 is the only one of the four that can be tested cheaply and continuously, which is why it should be the
one guarding the rest.

---

## 2. Where the branch stands against main

`main` is at `fae98a3a5f3` (2026-05-25), two months stale locally and worth refreshing before any
upstreaming conversation. This branch is 318 commits ahead.

| | count | |
|---|---|---|
| core production files changed from main | 301 | 16,734 insertions, 1,575 deletions |
| of which added | 49 | new seams and mechanisms |
| of which modified | 251 | |
| **modified core files that call a descriptor or gated seam** | **39** | the footprint this work owns |
| plugin (`serverless-storage`) | 708 files | 101,837 insertions |

The ratio is already the right one. Roughly a hundred thousand lines of behaviour sit in a plugin, and
thirty-nine core files carry a call into it.

The other 212 modified core files belong to the pluggable engine work this branch was originally cut for.
They were not audited here and are not this design's concern, but they are the reason the raw diff against
`main` looks larger than the metadata plane actually is. Any future attempt to upstream the metadata plane
alone will need that separation made real, not just asserted.

---

## 3. The seam contract

Every core change so far follows one shape. A static registry holding a nullable supplier, a `register`
that accepts null to clear, and a read path that falls through to the original code when nothing is
installed. `AbsentIndexDescriptorSuppliers.metadataOrDescriptor` is the canonical example:

```java
public static IndexMetadata metadataOrDescriptor(Metadata metadata, String indexName) {
    IndexMetadata published = metadata.index(indexName);
    if (published != null || isRegistered() == false) {
        return published;
    }
    return synthesisedMetadata(indexName);
}
```

With nothing registered this is `metadata.index(indexName)` and nothing else. That is the property I4
depends on, and it holds today at all thirty-nine sites.

Three rules the existing seams follow, which new ones must:

**Unset by default.** A static `AtomicReference`, cleared by registering null so a test can restore the
default rather than leaking a supplier into unrelated cases.

**A supplier that throws has no answer, rather than failing the request.** This is a degradation path
already; turning a plugin bug into a request failure makes the absence worse than it was.

**Absence returns null rather than throwing**, matching `Metadata.index`, because callers disagree about
what absence means. A bulk request fails one document, resolution reports no such index, routing reports
no shard available.

And one negative rule, which W4 paid for: **do not widen a hot core accessor.** `metadataOrDescriptor` is
deliberately a separate helper rather than folded into `Metadata.getIndexSafe`, because that accessor is
called from the cluster state applier thread and a remote lookup on that thread deadlocks: the lookup
needs the thread it is blocking to make progress. A separate helper means every caller is one somebody
chose.

That rule is about to matter much more than it has so far. See section 5.

---

## 4. What the new work needs from core

Five core changes. Three are new, one is a bug fix that belongs upstream regardless, and one is already
being built elsewhere.

| | change | location | shape | risk |
|---|---|---|---|---|
| **C1** | descriptor prefetch seam, plus the unsafe-thread guard | core, new class plus a hook in `TransportBulkAction` | **done** |
| **C2** | routing geometry on the descriptor | core, `IndexDescriptor` | **done** |
| **C3** | membership epochs and decommission | core, `ComputedPlacementMembership` and its service | **done**, T16 and T17 |
| **C4** | conditional create on S3 | `plugins/repository-s3`, `S3BlobContainer` | **done**, T4 |
| **C5** | on-demand shard materialisation | core, `IndicesClusterStateService` | **done**, this is what T39 built |

All five have landed. Section 11 records what each one turned out to involve, including the two places C2
corrected this section's own description of the problem.

Everything else is plugin-side:

| | change | location |
|---|---|---|
| **P1** | blob-backed `DescriptorStore` | replace the system index client with a `BlobContainer` |
| **P2** | name index fed from descriptor writes rather than cluster state | `nameindex`, plus an append-only change log |
| **P3** | write lease scoped to `(node, epoch)` rather than per shard | `shardstate` |
| **P4** | commit manifests batched per node | `manifest` |

P2 gates the *removal* of the system index, not the addition of the blob backend: three of
`DescriptorStore`'s eleven operations are prefix searches that a blob store cannot serve. T3 in section 11
has the breakdown.

### C2, and why it is not optional

`IndexDescriptor.toIndexMetadata` sets `numberOfShards` and nothing else about routing geometry, which its
own javadoc defends as deliberate minimalism. For plain custom routing that is sufficient: a document
routes by `murmur3(routing ?: id) % routingNumShards / routingFactor`, and with no split in the index's
history `routingNumShards` equals `numberOfShards` and `routingFactor` is 1.

It stops being sufficient in two cases, and both fail silently:

- `index.routing_partition_size` defaults to 1, so `isRoutingPartitionedIndex()` is false and
  `partitionOffset` is always 0. A user can set the setting at creation, have it accepted, and get
  documents on one shard instead of the partition set. No error anywhere.
- After a reshard, `routingNumShards` diverges from `numberOfShards`. The descriptor has nowhere to record
  the original, so documents route against the wrong divisor and searches return partial results.

Index-per-tenant makes the second case scheduled rather than hypothetical, because tenants grow across the
shard-count threshold by definition. Carry both fields now, while it is a field addition rather than a
migration of live tenants.

**Done.** Section 11's C2 entry records two corrections to the paragraph above: the divergence runs the
other way, and the setting alone does nothing because `IndexMetadata` holds the routing shard count as a
field.

### C3, and the conflict with autoscaling

`ComputedPlacementMembershipService` adds nodes and never removes them, and says so: a departed node stays
a member until something decommissions it, and decommission is not implemented. The asymmetry is
well-reasoned for the case it was written for. A node that is merely restarting must not have its shards
moved to nodes holding none of its data, because those recover empty while looking healthy, which is what
C13 hit and it is silent.

Under autoscaling with scale-to-zero, that reasoning inverts. Nodes genuinely leave, the member list
accumulates tombstones, and a growing fraction of rendezvous weight lands on nodes that no longer exist.
Requests to them fail and retry forever.

Epochs resolve both. A membership epoch is an explicit versioned node list. Joining and leaving each mint a
new one. The previous epoch stays readable, which serves the restart case the current design protects and
also gives warmth derivation something to compute against (section 5 of the earlier discussion, and below).
Decommission stops being a ceremony and becomes "did not appear in the last K epochs."

### C4, and the comment that is a trap

`S3BlobContainer.writeBlob` documents that it ignores `failIfAlreadyExists` because "the S3 API has no way
to enforce this due to its weak consistency model." Both halves stopped being true in December 2020 and
August 2024 respectively, and `compareAndSwapRegister` in the same file already proves it by using
`ifNoneMatch("*")`.

So the primitive exists and works, but the obvious API silently provides no uniqueness at all and no
error. That is the failure shape this area keeps producing, aimed directly at the design being built here.

Fix it or repoint the comment at the register API. Either way it belongs upstream on its own, independent
of anything in this document.

---

## 5. The one hard problem

The descriptor seam is synchronous:

```java
public static void register(Function<String, IndexDescriptor> supplier)
```

Behind a system index that is a sub-millisecond local GET, measured flat in shard count at 0.435 ms with
sixty shards (S23). Behind an object store it is a network round trip, twenty to a hundred milliseconds
cold.

That call happens at eleven sites on the write path, including `OperationRouting.indexMetadata`,
`TransportBulkAction.doRun` once per document, and `TransportReplicationAction.ReroutePhase`. Making those
sites do remote I/O synchronously is the exact thing W4 forbids, and the transport threads they run on are
not the cluster applier thread but are equally not somewhere to park for 80 ms.

There are three ways out and only one of them is good.

**Keep it synchronous and guarantee cache hits.** Works until it doesn't. The first request for a cold
tenant blocks a transport thread, and a burst of cold tenants blocks all of them. It converts a latency
problem into an availability problem.

**Make the seam asynchronous.** `void supply(String, ActionListener<IndexDescriptor>)`. Correct, and it
rewrites eleven core hot paths from expressions into continuations, one of which is a loop over every
document in a bulk request. That is a large, invasive core change and it lands on exactly the code T39 is
in the middle of.

**Resolve before entering the synchronous path.** A coordinator receiving a bulk or search extracts the
distinct index names it will need, resolves them asynchronously and in one batch, populates a
request-scoped cache, and only then runs the existing synchronous logic, which is now guaranteed to hit.

The third is the design. It is worth being explicit about what it buys:

- The eleven sites do not change at all. Their seam calls become cache reads.
- Batching comes free. A bulk touching M distinct indices does one multi-get or one parallel blob fetch
  instead of M sequential round trips, which is the dominant cost in multi-index bulk.
- The new core surface is one prefetch hook at a handful of request entry points, not a rewrite.
- The hook is itself a seam, inert when unregistered, so I4 still holds.

**What it does not cover, and this is where the deadlock comes back.** Not every descriptor read has a
request entry point. Background tasks, the cluster applier, recovery, and stats aggregation all reach for
metadata with no coordinator having prefetched anything. Those paths must either never touch a cold
descriptor, or resolve on a thread pool where blocking is acceptable and is known not to be needed for the
resolution to complete.

That enumeration is a prerequisite for C1, not a follow-up to it. An unenumerated caller is a deadlock
waiting for a cache miss, and cache misses are rare enough that it will not show up in testing.

### 5.1 The enumeration, done

Two seams reach the descriptor store, one directly and one through a synthesised metadata step:

- `AbsentIndexDescriptorSuppliers.supply`, `exists`, `expandPrefix`, `page`, `supplyAll`.
- `AbsentIndexRoutingSuppliers.resolve` and `resolveShard`, which call
  `supply(state, indexName, indexMetadata)` with null metadata for a gated index, which calls
  `synthesisedMetadata`, which calls the descriptor seam.

Two helpers that look like they reach it do not, and the distinction is worth stating because it is not
obvious from the call site. `allShards(ClusterState)` enumerates `state.metadata()` and calls
`supply(state, indexMetadata)` with metadata already in hand, so a gated index is never enumerated and no
lookup happens. `supply(ClusterState, IndexMetadata)` likewise. Only the name-keyed entry points hit the
store.

The classification. Thread context was verified from the enclosing type rather than assumed.

**Group A: cannot block. Six sites.**

| site | thread | path to the store |
|---|---|---|
| `DanglingIndicesState:195` | cluster applier, the class is a `ClusterStateListener` | direct `supply` |
| `IndicesClusterStateService:874` | cluster applier | `resolveShard` |
| `IndicesClusterStateService:896` | cluster applier | `resolveShard` |
| `RoutingNodes:382` | whichever thread first calls `ClusterState.getRoutingNodes()`, which includes the applier and, via `AllocationService:724`, the cluster manager | `localShards` |
| `ClusterStateHealth:103,187,264` | cluster manager, constructed from `AllocationService` | `resolve` per index |
| `ActiveShardCount:179` | dual: `ReplicationOperation` on a write thread, `ActiveShardsObserver` on the applier | `resolve` |

`RestoreService:981` looks like it belongs here, being inside a `ClusterStateUpdateTask.execute` on the
cluster manager thread, but it calls the no-argument `allShards(ClusterState)` and so never reaches the
store.

**Group B: has a request entry point, prefetchable.** `OperationRouting:346,541`,
`TransportReplicationAction:1052`, `TransportBroadcastReplicationAction:179`, `TransportUpdateAction:222`,
`IndexNameExpressionResolver:367,1245,1397`, `TransportAnalyzeAction:150`,
`TransportGetFieldMappingsIndexAction:121`.

**Group C: management or generic pool, blocking is acceptable.** The stats, cat, segments, recovery,
force-merge, upgrade, clear-cache and ingestion-state actions, plus `IndexPaginationStrategy:110` and
`ShardPaginationStrategy:123`. All reach the store through `allShards(state, concreteIndices)`, which calls
`resolve` once per named index. Blocking is fine here, but M named indices is still M round trips, so these
want the same batching C1 provides even though they do not need it for safety.

### 5.2 What Group A means for the design

Six sites is few enough to fix individually and too many to keep correct by discipline. Every future core
change that reads metadata on the applier or cluster manager thread re-opens the hole, and the failure only
appears on a cache miss, which is exactly the case testing does not produce.

So the mechanism should make it impossible rather than forbidden. **The descriptor seam should refuse to
perform I/O when called on a thread where blocking is unsafe**, returning null the way it already does for
an unregistered supplier or a throwing one. A warm descriptor still answers from cache, because that costs
no I/O. A cold one degrades to the same absence the seam already models, and every caller already handles
absence because that is the contract.

This is preferable to the enumeration alone for the reason T38 gave for landing site 1 with T39 rather than
before it: it turns a silent design failure into a loud one. An applier thread that quietly blocks for 80 ms
under load is invisible until it deadlocks. An applier thread that gets null and reports no shard available
is a bug report.

It also means C1's prefetch does not have to be exhaustive to be safe. Missing a Group B site costs a slow
request, not a stalled cluster.

Open question, not resolved here: whether the six Group A sites can tolerate absence semantically, or
whether some of them need the descriptor badly enough that returning null is itself a correctness bug.
`IndicesClusterStateService:874,896` is the one to check first, since T39 is building shard materialisation
on top of exactly that call.

### Warmth, while we are here

The related question of "which node has shard X warm" should not become stored per-shard state, because
that is the global structure this whole design deletes.

`ReaderCacheAffinityMetadata` stores it in `IndexMetadata` custom data today, and both consumers read it
from cluster state, which a serverless index does not have. The mechanism is unreachable for exactly the
indices it exists for.

With C3's epochs it does not need to be stored at all. Rendezvous-hash the shard against epoch N-1 to
compute where it used to be warm. That is O(nodes) of state for the whole cluster instead of O(shards),
and it is available to any coordinator in the nanoseconds a rendezvous lookup costs.

---

## 6. What cannot be made pluggable

Being honest about this list is the point of the exercise, because a design that claims zero core impact
and then quietly changes default behaviour is worse than one that names its exceptions.

**C4 changes behaviour for everyone.** `failIfAlreadyExists` starts being enforced on S3. Any existing
caller relying on the documented no-op gets a new failure mode. That is a bug fix and it should be argued
as one, upstream, on its own merits.

**C1 changes code shape at shared entry points.** The hook is inert, but `TransportBulkAction` and friends
grow a resolution phase that traditional indices pass through as a no-op. Inert is not the same as absent,
and these are the hottest entry points in the product. The no-op needs to be measurably free, not
argued to be.

**C5 is a second shard lifecycle.** The T39 design is right to make it a second trigger inside
`IndicesClusterStateService` rather than a parallel path, because two lifecycles that must agree is a worse
problem than the one being solved. But it is still new behaviour inside a class that every index depends
on, and it is the highest-risk item on the list by a distance.

**C3 is additive** and can be defended as such: epochs with a single entry behave as the current list does.

Everything else is plugin-side and genuinely optional at runtime.

---

## 7. What the architecture costs

Stated so they are decisions rather than discoveries.

**Write latency, or the purity of I2.** An acked write must be in the object store before the ack, which
puts a floor of roughly 50 to 200 ms on write latency. The alternative is acking from local disk and
flushing asynchronously, which gives millisecond writes and means local disk holds non-reconstructible
state for a bounded window. That is I2, given up for the last few seconds of writes. There is a middle
option, replicating the WAL in memory to K nodes before ack, which reintroduces node-to-node state. Pick
one deliberately.

**Object store operation rate becomes the capacity metric.** Per-shard leases at a million active shards
are roughly 50k PUT/s and about $22k/day in request charges alone. This is why P3 scopes the lease to
`(node, epoch)` and P4 batches manifests per node. The compromise is losing per-shard commit granularity:
a shard's durability point is tied to its node's commit cycle, and a shard cannot move mid-cycle without a
flush.

**Realtime GET only works on the writer.** Reading unrefreshed documents from the translog has no meaning
on a transient reader. Route realtime GETs to the lease holder, or drop the guarantee. Read-your-own-write
follows the same rule. This is a visible API change, not an internal one.

**Cold read latency is a cliff.** First query against a cold shard is object store round trips for segment
blocks. Prewarm covers the scale-out case; nothing covers the genuinely-cold-tenant case, because keeping
it warm is the cost being avoided. Publish warm and cold latency separately.

**Cross-index atomicity is gone.** Single-object CAS is all the object store offers. Alias swaps across two
indices are no longer atomic. A settings change across the whole population is one CAS per index. Templates
apply at creation only.

**Global answers become approximate or paginated.** Exact index count, `_cluster/health` over `*`, total
doc count. These are the APIs people reach for when something is already wrong, which is what makes this
the compromise most likely to surprise operators.

**Scale events invalidate cache proportionally.** S12 measured a single node joining leaving no shard
without a warm candidate, structurally, since one new node can displace at most one of K. Doubling the
fleet leaves 12.4% with none. Autoscaling policy becomes a correctness-adjacent concern: small steps, or
prewarm before shifting traffic.

**Creation loses its accidental rate limiter.** The serialised cluster manager was backpressure. Parallel
conditional PUTs remove it, so a runaway client can create indices as fast as it can open connections.
An explicit quota is now required where one used to be free.

**Creation needs an idempotency token.** With a cluster state update, an ambiguous timeout was resolvable
by reading the next state. With a conditional PUT, a timeout followed by 412 on retry is indistinguishable
from another client having taken the name. Put a request id or the client-chosen uuid inside the object so
the retry can tell "this is mine" from "this is theirs."

---

## 8. How to verify it

**I4 belongs in CI, not in review.** Run the core test suite against this branch with nothing registered,
and require it to pass identically to `main`. That single job is what keeps the two-sided design honest as
the seam count grows, and it is the only check here that scales without attention.

**The premise guard applies to every gated test.** Assert the index under test is actually gated before
believing a pass. This class has already shipped a headline result that was served by ordinary indices
auto-creation had quietly put back into cluster state.

**A blocking guard for C1.** Assert that descriptor resolution never happens on a transport thread, by
instrumentation rather than by inspection. The failure this prevents is rare by construction and will not
appear under test load.

**Latency arms, not latency claims.** The descriptor point lookup goes from 0.435 ms to an object store
round trip. Measure the cache hit rate under a realistic tenant access distribution before building on the
assumption that the cache absorbs it. T22 already found the read path had assumed uniform tenant access
once.

**A number nobody has measured yet: the concurrently-active fraction.** S3 puts a resident index at ~101 KB
for the parsed mapper graph, T20 puts an awake shard at 118 KB and 3.06 file descriptors. Call it 220 KB
per awake single-shard tenant, so a node giving 8 GB to this holds around 36k. Fleet size is then linear in
the active fraction: 1% of 100M tenants is roughly 28 nodes, 10% is roughly 280. Every other cost in this
design is flat or near-flat in population. This one is not, and it is the number the whole capacity model
turns on.

**Build with `-Dbuild.docker=false`,** or a dead Docker daemon fails every Gradle task including
`compileJava`.

---

## 9. Sequencing

**Blocked on T39.** C2 and C5 both touch files T39 is actively editing. Nothing in this branch should go
near `IndexDescriptor`, `IndicesClusterStateService`, `TransportBulkAction`,
`TransportReplicationAction`, or `OperationRouting` until it resolves one way or the other. It may be
reverted, in which case this branch's base is already correct.

**Startable now, disjoint file set.** C4 in `repository-s3`. P1's blob-backed store, since
`compareAndSwapRegister` with `ifNoneMatch("*")` already works on S3 and `FsBlobContainer` implements it
so tests run without a bucket. P2's change log and name index feed, entirely inside the plugin. None of
these files appear in T39's working set.

**Then, in order.** C1's prefetch seam, with the non-request-entry caller enumeration done first. C3's
epochs. C2 once `IndexDescriptor` is free. P3 and P4 last, because they are optimisations of a path that
has to work before it can be made cheap.

**Two blob-layer defects found before P1, both filed.** T2 checked whether the register API caps value size
(it does not, so a descriptor fits) and found two things worth fixing first: `FsBlobContainer`'s CAS does
not arbitrate across container instances, which silently disarms the test arm, and the create path does a
GET that the conditional PUT makes redundant. Section 11 has both.

**The uniqueness key must never include the uuid.** Two clients creating the same name with different uuids
would write different keys and both conditional PUTs would succeed. Name-only, always.

---

## 10. What this document does not establish

It is a design and a reading of the existing code, not a measurement. Nothing here was run.

The load-bearing unmeasured claims, in the order they would hurt:

- That the six Group A sites in section 5.1 can tolerate a null descriptor. The enumeration is done and
  the thread contexts are verified, but whether absence is semantically acceptable at each site is not.
- That the descriptor cache hit rate is high enough to hide an object store round trip under a real tenant
  access distribution.
- That the concurrently-active fraction is low enough for the fleet arithmetic in section 8 to be
  affordable.
- That the 212 core files outside the descriptor seams are genuinely unrelated. They were not audited.

Corrections go in this file with the reason they were wrong, kept rather than deleted, the way the spike
results do it.

---

## 12. Where this stands, and what is next

Twenty-one tasks landed. Working tree clean, everything below verified by a test that was checked against a
deliberately broken build before being believed.

**Done.** T1 (caller enumeration), T2 (register semantics), T3 (store audit), T4 (S3 conditional create),
T5 (create-only contract tests), T21 (Fs register lock), T22 (`createRegisterIfAbsent`), T6 (backend
interfaces), T7 (cache extraction), T8 (blob backend), T9 (idempotent creation), T10 (tombstones), T11 (cache instrumentation), T12 (change log), T16 (membership epochs), T15 (descriptor enumeration), T17 (decommission), T18 (derived warmth), T13 (name index feed), T14 (checkpoint store), T20 (cache hit rate).

Two commits are formatting-only sweeps, separated out rather than buried. **The branch base does not pass
`spotlessCheck`** in either `:server` or `:plugins:serverless-storage`, so a precommit build fails there
for reasons unrelated to any change. Worth fixing at the base rather than paying it per commit.

**All twenty-two tasks are done.** What remains genuinely unmeasured is one number rather than one task:
the wall-clock cost of a single object store round trip, which needs a real bucket. Every claim that
depends on it is expressed as a request count multiplied by an RTT, so supplying that one measurement
converts the whole T19 table into absolute figures without changing anything else.

| | task | note |
|---|---|---|

**The 19 `ServerlessStoragePluginTests` failures are fixed.** They were reported here as pre-existing and
unrelated, which was true and was not the same as harmless. Every one was `ClusterService is null` from
`createComponents`, because the test passed null for every collaborator and the production code had since
started registering a settings update consumer for T28's wildcard cap. The fixture was asserting against a
shape a real node never has, and thirteen call sites now pass a cluster service carrying the plugin's own
node-scoped settings.

That also buys a check rather than just a pass: `addSettingsUpdateConsumer` rejects a setting it does not
know, so the suite now fails loudly if the plugin ever registers a consumer for something it forgot to
declare in `getSettings`. That is a real startup defect, caught in a unit test instead of on a node.

Full plugin suite: **1,294 tests, 1 failure**, down from 19. The survivor is
`WalBatchingLatencyBenchmarkTests`, which passes in isolation and fails only under whole-suite load, the
same shape as `ComputedPlacementCostTests`. Both are latency benchmarks and both match this branch's own
recorded experience of blaming a timeout on the wrong cause when contention was the real one.

**`spotlessCheck` now passes** on `:server`, `:plugins:serverless-storage` and `:plugins:repository-s3`.
The two formatting-only commits cleared it; a precommit build no longer fails for unrelated reasons.

**T39 has landed and this branch is rebased onto it.** C2 (`routingNumShards` on the descriptor) and C5
(on-demand shard materialisation) are no longer blocked. C5 is what T39 built; C2 is now free to be done,
and section 4 explains why it is not optional once tenants start resharding.

**The largest unmeasured risk is unchanged.** Nothing here has been run against a real object store. Every
latency claim in this document is arithmetic, and T19 and T20 are the tasks that would make them evidence.
A blob backend that works against `FsBlobContainer` and has never seen a network is exactly the shape this
area calls "correct and unreachable".

**One pattern worth carrying forward.** Three separate defects on this branch were the same shape:
`FsBlobContainer` and `S3BlobContainer` disagreeing about a call, with every test running against the
former. The ignored `failIfAlreadyExists` flag, the per-instance register lock, and the container-root
`createDirectories`. Any new `BlobContainer` behaviour should be assumed to have this problem until a test
shows otherwise, and the S3 side needs an assertion on the request rather than on the outcome, because an
outcome assertion passes against a container that silently does nothing.

---

## 11. Implementation findings

Recorded as the tasks land. T1's result is structural enough that it lives in section 5.1 instead.

### T2: the register primitive, and two things it does that the design assumed away

**There is no value-size cap, so a descriptor fits.** The wire format is an 8-byte generation followed by
arbitrary bytes, in both `S3BlobContainer` and `FsBlobContainer`. `BlobRegister`'s javadoc calls a register
"a small blob" but nothing enforces it. S8 measured the serialised descriptor at well under a kilobyte, so
the uniqueness token does not need splitting from the descriptor body after all. That removes the caveat
section 9 attached to P1.

**The generation lives in the body, not in the ETag,** which is why `readRegister` reads the whole object
and why a CAS cannot be a single conditional PUT.

**A CAS is two round trips, and a creation does not need to be.** `S3BlobContainer.compareAndSwapRegister`
does a GET, compares generations as a fast-fail, then does the conditional PUT. Three round trips on
conflict, because the 412 handler re-reads to report the conflicting generation.

For creation specifically, `expectedGeneration` is `ABSENT_GENERATION` and the GET exists only to discover
that `currentETag == null`, which selects `ifNoneMatch("*")`. But `ifNoneMatch("*")` is create-if-absent
already, evaluated atomically by S3, and the method's own javadoc says the conditional PUT is the
authoritative check and the read is "purely as a fast-fail." So the GET on the create path is provably
redundant.

Skipping it halves the latency and the request cost of every index creation. At the scale this design
targets, index creation is the throughput story, so this is worth a dedicated `createRegisterIfAbsent`
rather than a comment. Filed as its own task.

**`FsBlobContainer`'s CAS does not arbitrate across container instances, which breaks the test arm.**
`registerLocksByBlobName` is a `private final` instance field holding a `ReentrantLock` per blob name. Two
`FsBlobContainer` objects over the same directory therefore share no lock. Both read the same generation,
both pass the equality check, both write, and the second silently wins.

Internal cluster tests run several nodes in one JVM with their own container instances, so a test asserting
"exactly one of N concurrent creators wins the name" would be measuring nothing. That is precisely what
task 5 exists to verify, and precisely the "fails by succeeding" shape this area keeps producing.

The channel is already open under `FileChannel.open`, so `FileChannel.lock()` on it would arbitrate across
instances and across processes. Filed as its own task, and it blocks task 5.

### T3: eight of eleven operations move, and the cache constants do not

`DescriptorStore`'s public surface, and what a blob backend can serve:

| operation | shape | blob-servable |
|---|---|---|
| `get(name)` | realtime point read | yes, a GET |
| `create(descriptor)` | create-if-absent | yes, `ifNoneMatch("*")` |
| `createAsync(descriptor)` | the same, async | yes |
| `put` / `putAsync` | upsert | yes, PUT or CAS |
| `putTombstoneAsync` | tombstone write, 5 attempts | yes, but see task 10 |
| `invalidate(name)` | cache only, no I/O | n/a |
| `indexExists()` | bootstrap probe | n/a, disappears |
| `readCount` / `evictionCount` | stats | n/a |
| `findByPrefix(prefix, after, size)` | prefix search | **no** |
| `findNamesForPage(...)` | pagination | **no** |
| `expandPrefix(prefix, limit)` | wildcard expansion | **no** |

**P1 and P2 are not independent, and section 9 understated that.** The blob backend can be added
alongside the system index, but the index cannot be removed until the name index serves those last three,
which are exactly the operations S13 identified as the part that cannot be partitioned. The ordering is
P2 gates the deletion, not the addition.

**The create-only contract already exists and carries over unchanged.** `create` uses
`IndexRequest.create(true)`, which T18 added for uniqueness. The semantics are identical to
`ifNoneMatch("*")`; only the enforcement mechanism moves from document-id uniqueness to a conditional PUT.
Nothing above the store has to change.

**Bootstrap circularity disappears for free.** There is no index to create, so no `ensureIndexExists` race,
no shard count fixed at creation, and no dependency on a cluster state entry to hold the metadata plane
that exists to escape cluster state. That was one of the four problems section 2 charged against the system
index, and the blob backend simply does not have it.

**The cache is stronger than section 5 assumed.** It is bounded by both entry count and retained bytes, and
it collapses concurrent reads of the same name through an in-flight future map, because T1 measured
sixty-four concurrent resolutions of one name issuing sixty-four reads. Request collapsing is exactly the
right thing to have in front of an object store: M concurrent readers of a cold descriptor cost one round
trip, not M.

**But `CACHE_TTL_NANOS` is one second, and that is tuned for a local index.** A descriptor accessed more
often than once per second re-reads every second. At 0.5 ms that is free. At an object store round trip it
is 20 to 100 ms, paid per active tenant per second per node, whatever the request rate above it.

The fix is not a longer constant chosen by feel. It is a long TTL plus explicit invalidation, and the hook
already exists as `invalidate(name)`. The change log in task 12 is what drives it. **That gives the change
log a second reason to exist, independent of feeding the name index,** and it should be built before the
blob backend is switched on rather than after.

**`COLLAPSE_WAIT_MILLIS` is 3,000 and should be re-derived rather than inherited.** It bounds how long a
waiter blocks before reading directly, and it was chosen against a sub-millisecond read so that a hung
reader could not take its waiters down with it. Against a 50 ms read the ratio inverts: the fallback
essentially never fires, so the blast-radius protection it was built for is no longer there. Whether that
matters depends on what a hung object store read looks like, which is not known yet.

### T4: the dropped flag was already a live bug, not just a trap

Section 6 listed C4 as a behaviour change to argue upstream on its own merits. It is stronger than that.
**Six callers already pass `failIfAlreadyExists = true` and have been getting nothing on S3**, all of them
writing state that must not be overwritten:

`BlobContainerManifestStore`, `BlobContainerBundleStore`, `BlobContainerCloneLineageStore`,
`BlobContainerShardPartitionStore`, `BlobContainerInPlaceSplitRangeStore`, and the pinned-timestamp
service.

Every one is protected on a filesystem repository, because `FsBlobContainer` throws
`FileAlreadyExistsException` for the same call, and unprotected on S3. So the divergence is invisible to
any test that runs against `FsBlobContainer`, which is all of them. The manifest one is the sharpest: a
commit manifest is a shard's durability record, and two writers racing on a generation silently lose one.

Implemented as `ifNoneMatch("*")` on the single-upload `PutObject` and on the multipart
`CompleteMultipartUpload`, translating 412 into `FileAlreadyExistsException` so callers need one catch
rather than two. The multipart case puts the precondition on completion rather than creation, because
completion is the request that publishes the key, and the existing abort-on-failure block discards the
parts of a losing upload.

**Who actually changes behaviour, checked rather than assumed.** The two `BlobStoreRepository`
verification writes look like the risk and are not: both write under a container keyed by a freshly
generated `UUIDs.randomBase64UUID()`, so the precondition can only fire on a UUID collision.

`RemoteStorePinnedTimestampService.pinTimestamp` and `cloneTimestamp` do change: re-pinning the same
timestamp for the same entity used to overwrite a zero-byte marker on S3 and now throws. That is not a
regression introduced here, because those calls already throw on a filesystem repository today. The change
makes S3 agree with the interface contract and with every other implementation of it. If idempotent
re-pinning is intended, the fix belongs at those call sites, which should pass `false`, and it is an
existing bug on filesystem repositories independent of this.

**Requires an endpoint that implements conditional writes.** Older S3-compatible stores ignore the
precondition header, and against those this silently reverts to last-writer-wins rather than failing. That
is the same dependency section 4 already records for name uniqueness, now on a second path.

Existing `repository-s3` unit tests: 75 run, 0 failures.

### T21: the Fs register lock, and a test that was checked against the bug

`registerLocksByBlobName` was a per-instance field keyed by blob name. Made static and keyed by the
resolved absolute path instead, because the unit of exclusion is the file rather than the container that
reached it. A `FileChannel.lock()` now spans the read and the write as well, taken only after the
in-process lock so it cannot throw `OverlappingFileLockException` when two containers in one JVM meet at
the same file. Read takes it shared, since the channel is read-only and an exclusive lock on it would
throw.

`FsBlobContainerRegisterTests` already had `testConcurrentPutIfAbsentRaceHasExactlyOneWinner` and
`testConcurrentWritersWithRetryNeverLoseAnUpdate`, and both passed against the broken code, because both
hold a single container and so exercised the per-instance lock. The new test spreads twenty contenders
across four containers over one directory, which is the shape an internal cluster test produces.

**It was run against the unfixed source before being believed.** Reverting only `FsBlobContainer.java` and
keeping the test gives:

```
testConcurrentPutIfAbsentAcrossSeparateContainersHasExactlyOneWinner FAILED
  java.lang.AssertionError: exactly one contender must win across separate containers expected:<1> but was:<2>
```

Two winners on one put-if-absent, which is the lost update. With the fix restored, 8 tests, 0 failures.
A concurrency test that has never been shown to fail is a test that asserts nothing, and this area has
shipped one of those before.

### T5: the create-only contract, asserted on both sides

Three tests, and each was run against a deliberately broken build before being believed.

**S3, on the request rather than the outcome.** `testSingleUploadSetsIfNoneMatchOnlyWhenFailIfAlreadyExists`
captures the `PutObjectRequest` and asserts `ifNoneMatch` is `"*"` with the flag set and null without it.
Asserting on an outcome would have been the weaker test: a container that ignores the flag still succeeds
whenever the key happens to be free, which is every time in a unit test. Deleting the `ifNoneMatch` line
gives `expected:<*> but was:<null>`.

**S3, on the error translation.** `testSingleUploadTranslatesPreconditionFailedToFileAlreadyExists` asserts
a 412 becomes `FileAlreadyExistsException` with the flag set, and stays a plain `IOException` without it,
so a 412 arriving for some other reason is not reported as a name collision that never happened.

**Fs, on the contract itself.** `testWriteBlobHonoursFailIfAlreadyExists` had no equivalent anywhere, which
is how the S3 divergence survived: the behaviour was only ever exercised where it already worked. It also
asserts the original survives the rejected write rather than being half-replaced, and that the flag being
off still overwrites, since the flag is a choice between two behaviours rather than a safety toggle.

`repository-s3` 51 tests, `FsBlobContainerTests` and `FsBlobContainerRegisterTests` green.

### T22: creation is one round trip, not two

`createRegisterIfAbsent(blobName, value)` on `BlobContainer`, defaulting to
`compareAndSwapRegister(blobName, ABSENT_GENERATION, value)` so every existing container keeps working,
overridden in `S3BlobContainer` to issue the conditional PUT alone.

The general CAS has to read first, because a CAS against an arbitrary generation must learn the current
one. Creation does not: the only thing the read tells that path is that `currentETag` is null, which
selects `ifNoneMatch("*")`, and that header is itself the atomic must-not-exist check. The existing
javadoc already called the read "purely as a fast-fail", so removing it costs no safety.

Conflict reports `ABSENT_GENERATION` rather than reading to discover the winner's generation. A caller
that lost a create race wants to know it lost; paying a round trip to decorate that would put back the
cost this removes. `BlobContainer`'s javadoc states the weaker conflict contract so no caller relies on it.

Asserted on round trip count, not on the return value, because a test checking only the result passes
against the default implementation, which is correct and twice as expensive. Removing the `@Override` makes
both tests fail. 53 tests, 0 failures with it restored.

Worth being precise about the failure mode: the mutated build fails with an NPE from the unstubbed
`getObject` rather than from `verify(never())`. The signal is still "a GET happened", which is what is
under test, but the assertion is not what reports it.

### T6: two interfaces, because there are two sets of possible implementations

`DescriptorBackend` holds the eight point operations. `DescriptorPrefixBackend` holds the three prefix
searches. Splitting them is the substance of this task rather than a stylistic choice.

Put together, a blob implementation has to throw from three of eleven methods, and the compiler cannot tell
anyone that. Split, "the object store cannot resolve `logs-*`" is a type-level fact, and the ordering
constraint T3 found is expressed in the type system rather than in a comment: the blob backend can be
added while the system index still answers prefix queries, and the system index cannot be removed until
something else does.

Caching, TTL, eviction and in-flight collapsing stay above the interface in `DescriptorStore`. A backend
does I/O only. That matters more under an object store than under a system index, because collapsing turns
M concurrent readers of one cold descriptor into a single round trip, and a per-backend copy of that is two
chances to get it subtly different.

One rename: `indexExists()` becomes `available()` at the interface, because the mechanisms do not resemble
each other. The system index must exist, be allocated, and carry W8's settings. A blob backend has nothing
to bootstrap at all.

**The SPI was checked by making `DescriptorStore` implement both interfaces**, which compiles with no
signature changes anywhere. An interface extracted from one implementation is a guess until the
implementation is made to satisfy it. 234 plugin tests, 0 failures.

### T7: the cache moved out, and the loader can no longer forget

`DescriptorCache` now owns the freshness window, the byte and entry bounds, eviction, and in-flight request
collapsing. `DescriptorStore.get` is `descriptorCache.get(name, this::readFromIndex)`, and `readFromIndex`
is reduced to one read with no caching of its own.

The shape matters as much as the move. The loader is handed a name and returns a descriptor or null;
counting the read and admitting the hit belong to the cache. A backend that forgets to do either is now not
expressible, which is the sort of omission that shows up as a performance mystery rather than a failure.

Everything carried over unchanged, comments included, because each of the following was paid for once:
unconditional admission (T3 measured that gating it on capacity was a freeze, not an eviction policy),
recency stamped on write and never on read (P1 measured a read-mutating LRU running backwards under
contention), two clocks per entry, refusing to cache an entry larger than the whole budget, and never
caching a miss.

`DEFAULT_TTL_NANOS` is now a constructor parameter rather than a constant, which is the one deliberate
change. T3 established the one second window is wrong by two orders of magnitude against an object store,
and the fix is a long window with explicit invalidation from the change log rather than a larger number
chosen by feel.

### T8: the blob backend, and a third Fs/S3 divergence

`BlobDescriptorBackend` implements the eight point operations over a `BlobContainer`, reusing
`IndexDescriptor`'s existing `Writeable` form so there is one serialisation rather than two that drift.

Keys are `descriptors/<name>`, flat and prefix-preserving. Not hashed, and the reason is not the obvious
one: S3 already partitions adaptively on observed key distribution, so diverse tenant names spread without
help, and the real hazard is monotonically increasing names landing in one partition. Hashing would buy
little and cost the only path from the object store back to the name index, which is what makes that tier
provably a cache rather than a second source of truth.

`create` is one `createRegisterIfAbsent`. `available()` is unconditionally true, because there is nothing
to bootstrap. `putTombstoneAsync` throws rather than writing under the descriptor prefix, since a tombstone
there makes every LIST return dead names and forces a read per key to filter them; that belongs under its
own uuid-keyed prefix and is its own task. Refusing loudly beats a version that would only be caught when a
rebuild resurrected deleted indices.

**A third way Fs and S3 disagreed.** `FsBlobContainer.compareAndSwapRegister` called
`Files.createDirectories(path)`, the container root, rather than the register key's parent. A blob name
carrying a path segment, which is how any store namespaces its keys, resolves below the container, so the
open threw `NoSuchFileException`. S3 has no equivalent failure because its keys are flat strings. Fixed to
create `registerPath.getParent()`.

That is the third instance of the same shape in this branch, after the ignored `failIfAlreadyExists` flag
and the per-instance register lock. The pattern is worth naming: **`FsBlobContainer` is the only
implementation every test runs against, so anywhere it is more permissive than S3 the divergence is
invisible, and anywhere it is less permissive it fails only when someone finally exercises the path.**

**The test framework caught a bad test.** `testKeysArePrefixPreserving` first asserted an exact blob count
and failed at 4 rather than 3, because Lucene's `ExtrasFS` deliberately drops an `extra0` file into test
directories to catch code assuming a directory holds only what it wrote. The assertion was doing exactly
that. Rewritten to assert which names a prefix selects, which is the property a rebuild actually needs.

8 backend tests pass, and the server-side `FsBlobContainer` suites still pass with the directory fix.

### T9: a conflict cannot say whose creation it was

`createIdempotently` returns `CREATED`, `ALREADY_MINE` or `TAKEN`. The middle case is the one that did not
exist before: a first attempt reached the store, its acknowledgement was lost, and the retry got a
conflict. Reporting that as a collision fails a creation that actually succeeded; reporting it as success
without checking hands the name to a client that never got it.

Resolved by reading the winner and comparing uuids, which works because the uuid is already a stable token
per creation attempt. The contract that makes it work is stated rather than assumed: **a retry must resend
the same descriptor.** A caller minting a fresh uuid each attempt is asking a question this cannot answer,
and is told the name is taken.

An absent descriptor after a lost create is `TAKEN`, not `CREATED`. Either it was created and deleted
between the two calls or the store lost the write, and in neither case does this caller hold the name.

### T10: the tombstone has to stay findable by name

The obvious layout is uuid-keyed, and it is wrong. `State.DELETED` exists so a node partitioned during a
delete consults the descriptor, finds the tombstone, and drops its local shard data instead of resurrecting
the index. That node looks the descriptor up **by name**, because the name is all it has. A uuid-keyed
tombstone is unfindable by the one reader it exists for.

So tombstones go to `tombstones/<name>`, and `get` reads `descriptors/<name>` first and falls through to
the tombstone. Live names still cost one round trip; only a name that is not live costs two, and creation
never comes through here at all, since it is a conditional write that never reads.

`putTombstoneAsync` writes the tombstone first and removes the live descriptor second, and the order is the
correctness argument. A crash between them leaves an index tombstoned but still listed, which resolves as
deleted and is repaired by repeating the delete. The other order leaves a name with no record at all: free
to recreate, with a partitioned node still holding shard data for the old uuid and nothing to tell it
otherwise. The tombstone write is an unconditional CAS rather than create-if-absent, because deleting an
already-deleted index has to succeed.

A LIST over `descriptors/` now returns live names only, which is what stops a rebuild resurrecting deleted
indices without needing a read per key to find out.

Six tests, and the mutation check is unambiguous: removing the descriptor delete fails four of them,
including the two that check resolution rather than layout. 14 backend tests pass.

### T11: a hit rate that distinguishes the two ways a lookup avoids a read

`readCount` and `evictionCount` existed; a hit rate did not, because `reads` counts backend trips rather
than lookups. Added `freshHitCount`, `collapsedWaitCount`, `collapseFallbackCount` and a derived
`hitRate()`.

Splitting the first two is the substance. A fresh hit costs nothing. A collapsed wait costs a full round
trip that another thread is paying for, so the caller still waited on the network, it just did not add
traffic. Folded together they report a cache as healthy while every request stalls behind one cold read.
Against a system index that difference was half a millisecond and nobody had to care. Against an object
store it is the difference between a served request and a stalled one.

`collapseFallbackCount` is the one that should stay near zero. It rising means the collapse wait is
shorter than a real read takes, which converts collapsing into duplicated work: every waiter times out and
then issues the read it was waiting to avoid. That is exactly how the 3-second default goes wrong against
an object store, and it is invisible without the counter.

Six tests, including the two cases where a low hit rate is correct rather than a fault: an expired entry
and an absent name, since a miss is never cached. A reader of the metric needs to know both.

### T12: the change log, and a swallow that hid its own failure

One object per entry under `changelog/<bucket>/<random>`. A single object appended under compare-and-swap
is the obvious design and would serialise every descriptor write in the cluster behind one key at roughly
one write per round trip, which is worse than the cluster manager this design removes from the creation
path. So appends are independent and nothing coordinates.

The cost is stated rather than discovered: **there is no total order.** Two entries in one bucket have no
defined order, node clocks disagree, and a slow appender can land behind a later one. Nothing may use a log
position as a version cursor. Both real consumers are fine with that, because the cache only drops an entry
and the name index applies name, uuid and liveness, which is last-writer-wins on a key rather than a
sequence. Buckets are zero-padded so lexicographic order is chronological, which is load-bearing: an
unpadded bucket 9 sorts after bucket 10 and a reader resuming from it skips the gap silently.

**A fourth Fs/S3 divergence, caught before it shipped rather than after.** The first version put the bucket
inside the blob name as `changelog/<bucket>/<uuid>`. That works on S3, where keys are flat strings, and
fails on a filesystem repository, where nothing creates the intermediate directory. Rewritten to nest
through `BlobPath` and take a `BlobStore`, which every implementation honours.

**And the reason it took a debugging cycle is worth more than the bug.** `append` swallows failures
deliberately, because the log is derived and losing an entry must not fail the descriptor write that
already succeeded. But a log that silently appends nothing reads exactly like a log with nothing to say:
four tests came back empty and the cause had already been logged and discarded. Added
`failedAppendCount()`, and every test that appends now asserts it is zero. Swallowing a failure is a
decision; making it unobservable is a defect.

### T16: an epoch is a version number plus the list it replaced

`ComputedPlacementMembership` already had a version. What it lacked was the previous member list and any
way to shrink.

**Retaining the predecessor is what makes warmth computable rather than stored.** "Which node holds shard X
warm" is O(shards) if it is written down, which is the global structure this design exists to delete, and
O(nodes) if it is derived: rendezvous the shard against the previous list and the answer is where it used
to live. One generation, not N. Two epochs answer "where was this before the change that just happened",
and a shard that has missed two membership changes has no warm holder worth chasing.

**`withoutNodes` exists now, and the asymmetry it was omitted for is still right.** A node absent because
it is restarting must keep its membership, because moving its shards to nodes holding none of its data
makes them recover empty while looking healthy. What changed is that "absent" stopped being one condition:
under autoscaling with scale-to-zero, nodes genuinely leave, and a list that only grows accumulates ids
that never come back until a growing share of rendezvous weight lands on nodes that do not exist. So
removal is expressible and the decision stays with the caller, which is the only place that can tell a
restart from a departure. `withNodes` still cannot shrink.

The existing `testMembershipNeverShrinks` javadoc said that if a removal API were ever added, that test
should force the question of what happens to the data first. It did. The test was renamed and its
reasoning updated rather than deleted, because the property it guards is still true and still the reason
`withNodes` is separate.

Two things that would have been quiet defects: the predecessor is serialised **after** the version rather
than beside the node ids, so a reader with the order wrong cannot silently transpose a membership with its
own predecessor and place every shard one epoch in the past while looking healthy. And it participates in
`equals` and `hashCode`, or a cluster state update carrying a corrected predecessor would be dropped as a
no-op.

17 membership tests, 0 failures.

### T18: warmth is a function of the previous epoch

`WarmCandidates.forShard` rendezvous-hashes a shard against the epoch before the current one. Wherever a
shard used to be placed is where its cache is, because deterministic placement is what put it there.

That replaces `ReaderCacheAffinityMetadata`, which had two problems rather than one. It is O(shards) of
recorded state, the global structure this design exists to delete, and both of its consumers read it from
`IndexMetadata` in cluster state, which a serverless index has no entry in, so it is unreachable for
exactly the indices it was built for. The derived version is O(nodes) for the whole cluster and any
coordinator can evaluate it in the time a rendezvous lookup costs.

`preferWarm` orders rather than filters, and that is the design decision worth stating. A warm-only list
empties out precisely when the fleet has just doubled, which S12 measured as 12.4% of shards having no
warm candidate at all, and that is the moment a router most needs somewhere to send a request. Departed
nodes are dropped, because their cache is unreachable and offering one sends traffic somewhere that cannot
answer, which is the failure a stale stored affinity record produces and the reason the stored version
needed a freshness check that this does not.

Six tests, one of which re-derives S12's structural claim independently: a single node joining leaves no
shard without a warm candidate, checked across two thousand shards, because one new node can displace at
most one of K.

### T17: decommission counted in observations, not elapsed time

A member absent from `DiscoveryNodes` across ten consecutive elected-cluster-manager observations is
removed with `withoutNodes`.

**Observations rather than a duration**, because a wall clock says how long a node has been away and not
whether anyone was watching. A cluster manager that was itself restarting would see a long absence for a
node that never left. Consecutive observations are absences somebody actually saw.

**The counter is in memory and losing it is the safe direction.** A cluster manager election resets every
count, which delays a decommission and can never cause one. That asymmetry is the whole design:
decommissioning a node that was merely restarting moves its shards to nodes holding none of its data and
they recover empty while looking healthy, which is C13 and it is silent. Waiting too long costs requests
that fail and retry, which is loud and self-correcting. Persisting the counter would trade a loud failure
for a quiet one.

Ten is stated rather than tuned, and should be re-derived against a real restart profile before anyone
relies on the exact value.

### T15: the path that makes I2 true rather than claimed

`DescriptorEnumerator` reads the full live name set from the object store alone. Invariant I2 says every
local structure is reconstructible; for the name index that was a claim until something could actually do
it. Without this path the name index is not a discardable cache, it is a second source of truth that has to
be protected, which is a different and much worse system.

**Not a query path, and it must not become one.** S13 measured an in-memory prefix match at 3.2 ms for
sixty-five thousand hits. A LIST pages a thousand keys behind a serial continuation token, so millions of
names is seconds to minutes. Rebuild and reconcile with it; answer nothing.

Parallelism comes from splitting the prefix space, not from paging one prefix, which the continuation token
makes impossible. One listing per starting character runs independently. Two details that would otherwise
be quiet defects: results are unioned through a sorted set rather than concatenated, so a future alphabet
change cannot silently duplicate a name into a rebuild; and names outside the split alphabet are swept
afterwards rather than assumed absent, because a rebuild that drops indices looks exactly like success.

This only works because T8 kept keys prefix-preserving and T10 moved tombstones out from under the
descriptor prefix. Hashed keys would leave no way to enumerate a range or to split the work, and tombstones
left in place would need a read per key to find out which names are live.

Six tests, including one asserting the parallel and serial passes agree.

### T20: the decision rule was stated first, and it failed

The rule, written before the numbers: the design claims the cache hides the backend round trip, which is
credible only if a cache holding a small fraction of the population serves the large majority of lookups
under skew. **If a bound of one tenth of the population cannot reach 90%, the prefetch in C1 is doing the
real work and the cache is not.**

100,000 tenants, Zipf s=1.0, 500,000 lookups, TTL long enough that this measures the capacity bound rather
than the freshness window:

| cache bound | share of population | hit rate | reachable ceiling | shortfall |
|---|---|---|---|---|
| 100 | 0.1% | 24.7% | 42.9% | 18.2% |
| 1,000 | 1.0% | 46.0% | 61.9% | 15.9% |
| **10,000** | **10.0%** | **69.3%** | **81.0%** | **11.7%** |
| 50,000 | 50.0% | 85.6% | 94.3% | 8.7% |

**69.3% at a tenth of the population. The rule was not met.** Roughly three lookups in ten reach the
backend, so under an object store roughly three in ten pay a round trip.

The ceiling column is what makes that number attributable. It is the share of lookups landing in the
`capacity` hottest tenants, which is what a perfect eviction policy would achieve. Two separate things
follow:

- **The workload caps it at 81%**, not 90%. Zipf s=1.0 over 100k tenants simply does not concentrate
  enough traffic in the top 10% for the design's assumption to hold at that bound. No cache policy fixes
  this.
- **The policy leaves 11.7% on the table.** Eviction drops the stalest tenth by *admission* order, because
  recency is stamped on write and never touched on read, which P1 and P2 established after a read-mutating
  LRU was measured running backwards under contention. So the cache is closer to FIFO than LRU. That gap
  is real and the trade behind it was deliberate.

The control arm confirms the fixture models what it claims: at the same bound, uniform access gives 9.2%
against skew's 68.9%. Skew is worth a factor of seven, which is also why measuring this uniformly would
have been useless.

**What this changes.** C1's prefetch moves from optimisation to load-bearing. A bulk touching M distinct
indices cannot rely on the cache for roughly three in ten of them, and those are sequential round trips
without batching. It also raises the value of T12's change log, since a longer TTL with invalidation
recovers hit rate that a short window throws away, and the window was not even the binding constraint in
this run.

**What it does not say.** No latency was measured, deliberately: timing this against a filesystem container
would report local disk dressed as an object store result. The absolute cost of a miss is still unmeasured
and still needs a real store. Real tenant traffic may also be more skewed than s=1.0, which would raise
every row; the exponent is an assumption, not a measurement.

The test now asserts a regression floor below the measured value rather than the rule it failed, and
records the ceiling and shortfall so a future change that improves the policy is visible as such. Lowering
the bar to make the rule pass would have been the wrong repair.

A third arm pins the tail: a tenant outside the working set reaches the backend on at least 40 of 50
lookups, so no per-tenant latency claim can be made from a fleet-wide hit rate.

### T14: the checkpoint stays an optimisation because something else can rebuild it

`BlobNameIndexCheckpointStore` parks a `CompactNameIndex` in the object store so a restarting tier loads
packed entries instead of replaying the population, which is the trade A16 already settled.

What makes it safe to call that an optimisation is T15. Without a path from the object store back to the
full name set, a checkpoint stops being a cache of a derivable thing and becomes the only copy, which would
give the name index its own durability problem. A lost or unreadable checkpoint is a slow start, never a
loss, and the class is written so that reads back that way.

Each checkpoint is its own object and the newest generation wins, rather than one key overwritten in place.
Overwriting leaves a window where a reader sees a half-written structure, and a half-understood name index
is a cluster that cannot find its own indices, which `NameIndexCheckpoint`'s version guard already refuses.
Generations are zero-padded for the same reason the change log pads buckets: names sort lexicographically
in a listing, so generation 9 must not read as newer than 10.

Writing twice at one generation fails rather than picking a survivor, because two writers at one generation
have disagreed about how far the feed has been consumed and keeping one silently would hide that. An
unreadable newest checkpoint falls back to a rebuild rather than to an older checkpoint, since an older
structure with a newer feed position skips everything between them.

Five tests.

### T13: the feed a gated index appears in, and a test that caught my own claim

`NameIndexService.apply(List<DescriptorChange>)` consumes the change log. The cluster state feed it had
cannot see a gated index at all, since one has no metadata entry by construction, so the service was blind
to precisely the indices it exists to resolve. That is the same shape as the cache-affinity record reading
`IndexMetadata` for indices that have none.

**The first version claimed to be order-insensitive and was not.** Applying in arrival order, a delete that
arrives before its own create resurrects the index: the delete finds nothing to remove and the create then
puts it back. With no order in the log that is a coin flip rather than an edge case. The test asserting
order-independence failed, which is what the assertion was for.

Fixed with two passes. The first collects every incarnation the batch deletes, the second applies creates
only for incarnations that survive it, so the outcome is a function of the batch's contents rather than its
sequence. A tombstone is terminal for the uuid it names, which is what T10 made it in the descriptor store,
so this is that rule read back from the log.

The uuid on a change is what keeps replay safe. A stale delete applied by name alone would remove an index
recreated after it, and delete-then-recreate is exactly the case that produces two entries for one name
with no order between them. That is a silent data-visibility loss, and it has its own test.

Six tests, plus a full pass over the descriptor, name index and placement suites: 384 tests, 0 failures.

### T19: round trips are measurable without a bucket; latency is not

This was recorded as blocked on infrastructure. That was half right and the half it got wrong was the
important one. Wall-clock latency does need a real bucket. **The round trip count does not, and it is the
half every claim in this document actually rests on.**

T22 said creation costs one request rather than two. T10 said a name that is not live costs a second read.
T8 said a live read costs one. None had been checked through the descriptor layer, only at the container
below it, and each is exactly what silently regresses when someone adds a read "just to check".

Measured by counting store operations against `FsBlobContainer` and converting to S3 requests using the
per-operation cost read off `S3BlobContainer`:

| operation | requests | at 20 ms RTT | at 50 ms | at 100 ms |
|---|---|---|---|---|
| create | 1 | 20 ms | 50 ms | 100 ms |
| read, live | 1 | 20 ms | 50 ms | 100 ms |
| read, absent or deleted | 2 | 40 ms | 100 ms | 200 ms |
| delete | 4 | 80 ms | 200 ms | 400 ms |

**The request column is measured. The millisecond columns are multiplication**, over a round trip time
nobody here has measured, and they are laid out so a reader can see which is which rather than having to
take the whole table on trust.

Two things this settles. T22's halving is real end to end, not just at the container: creation issues no
read at all, confirmed by mutation, since inserting a `readRegister` before the conditional write fails
`testCreatingAnIndexCostsOneRequest` with `expected:<0> but was:<1>`. And T10's tombstone fallthrough
costs exactly the one extra read it was designed to, on the not-live path only.

Delete at four requests is the one worth watching. It is a tombstone read, a CAS that is itself two
requests, and the removal of the live object. Nothing here needs it to be cheaper, but it is the operation
with the most room to grow silently.

**The cost table is read off `S3BlobContainer` rather than measured**, so it could drift. It cannot drift
quietly: the per-request behaviour it encodes is asserted directly in `S3BlobStoreContainerTests` and, as
of this task, server-side against the mock S3 fixture in `S3BlobStoreRepositoryTests`.

### T19b: the S3 test was in the wrong place

`createRegisterIfAbsent`'s tests captured the `PutObjectRequest` and asserted `ifNoneMatch` was set. That
proves the request-construction code is right and nothing more. Whether the store *enforces* the
precondition is a server-side question, and name uniqueness depends entirely on that enforcement: if the
header were ignored, every concurrent creator would win and those unit tests would still pass.

There was already a home for exactly this. `S3BlobStoreRepositoryTests.testCompareAndSwapRegisterUsesRealConditionalWrites`
runs against the `S3HttpHandler` fixture, which was extended to enforce real If-Match and If-None-Match
semantics, and its javadoc says it exists as "proof that the conditional-write guard is actually enforced
server-side, not merely that our request-construction code compiles". The new register API skipped it.
`testCreateRegisterIfAbsentIsEnforcedServerSide` now sits beside it, and both pass.

### Rebase onto T39: it lands, with no conflicts and nothing to repair

T39 landed on `feature/pluggable-engine-per-shard-role` at `1b02991d93a`, "a gated index takes a write,
and the eleven sites were fifteen". This branch was cut from its parent, so it rebased onto it.

**Twenty-eight commits replayed, zero conflicts.** The two sides turn out to have touched disjoint file
sets, which was checkable in advance and worth checking: T39 changed the descriptor seams, the residency
sites and `IndicesClusterStateService`, while this branch changed the blob layer, membership and the
descriptor store's backend. Even the two formatting sweeps missed T39's files.

Textual cleanliness was never the real question, though. The risk was semantic, and specifically that T39
might have widened `IndexDescriptor`, which several tests here construct directly. It did not, so every
one still compiles.

Verified after the rebase rather than assumed from a clean `git rebase`:

| | |
|---|---|
| `:server`, `:plugins:serverless-storage`, `:plugins:repository-s3` main and test compilation | clean |
| plugin unit suite | **1,299 tests, 0 failures** |
| server tests over the touched areas (`FsBlobContainer`, `ComputedPlacementMembership`, `OperationRouting`, `AbsentIndex*`) | 74 tests, 0 failures |
| `repository-s3` unit plus the mock-S3 register tests | 184 + 2, 0 failures |
| `spotlessCheck` on all three projects | clean |

**The one that mattered: `GatedEndToEndIT.testAGatedIndexCanBeWrittenToAndSearched` passes.** That is the
test T39 exists to make pass, and this branch changes `FsBlobContainer`'s register locking, the membership
custom's wire format and `NameIndexService`'s feed, any of which could have broken it. The two skips in
that class are pre-existing `@AwaitsFix` annotations, not new.

One failure appeared and was not a regression. `AwsS3ServiceImplTests.testIrsaCredentialsFromKeystore`
fails with "Unable to load region from any of the providers in the chain", and passes with `AWS_REGION`
set. It is an environment prerequisite that has nothing to do with either side, checked rather than
waved through, because "unrelated" is what four wrong diagnoses in this area looked like.

**What this does not clear.** C2 and C5 were blocked on T39 and are now unblocked, but neither is done.
C2 still needs `routingNumShards` and `routingPartitionSize` on the descriptor, and `IndexDescriptor.java`
is now free to take them.

### C1 and C2: the last two core changes

With T39 landed, all five core changes in section 4 are done. C3 was T16 and T17, C4 was T4, and C5 is
what T39 built. These are the remaining two.

**C2 carries the routing geometry, and I had the direction backwards.** `toIndexMetadata` set
`numberOfShards` and nothing else, so a synthesised index routed against its shard count and was never
partitioned. Both failures are silent: `index.routing_partition_size` was accepted at creation and ignored,
and a split index routed against the wrong divisor.

The correction the tests forced is worth recording, because section 4 states it wrongly.
`number_of_routing_shards` is the **pre-split** shard space, so it is at or above the shard count and a
whole multiple of it. Splitting grows the shard count toward it and leaves it alone, which is exactly what
keeps a document routing to the same place across a split. Section 4 says "after a reshard,
`routingNumShards` diverges from `numberOfShards`", which is true, but my first fixtures had it diverging
downward and `IndexMetadata`'s own assert rejected them.

A second correction from the same tests: `IndexMetadata` keeps the routing shard count as a **field** and
derives `routingFactor` from it. The builder does not read it back out of the settings, so putting the
setting does nothing at all. It has to go through `setRoutingNumShards`, which is what real creation does.
My first test built its fixture via the setting and got the shard count back, which is precisely the
mistake the descriptor was making.

Unset is stored as 0 rather than a copy of the shard count, so a descriptor written before these fields
reads as "never split, not partitioned" rather than as a coincidence. The 14-argument constructor stays as
a delegate meaning exactly that, so no caller churns, and the map codec reads both with `getOrDefault`
because descriptors written before them are still in the index.

**C1 is two halves, and the second is the one that makes the first safe to skip.**

`DescriptorPrefetch` resolves the descriptors a request needs at its entry point, batched, before the
synchronous path runs. Hooked into `TransportBulkAction.doExecute`, which is the last point that knows the
whole request: by `doRun` the work is per document, and a lookup issued from inside that loop is one round
trip per distinct index, sequentially. T20 is what makes this load-bearing rather than a nicety, since
roughly three lookups in ten still reach the store at a realistic cache bound.

It is written to stay an optimisation. A prefetch that fails, or a prefetcher that throws, completes the
listener normally so the request proceeds and resolves inline the way it always did. The alternative would
let a speculative read break the thing it speeds up.

The second half is the guard T1's section 5.2 asked for. `AbsentIndexDescriptorSuppliers.supply` now
refuses to reach a supplier at all on `clusterApplierService#updateTask` or
`clusterManagerService#updateTask`, returning the same absence every caller already handles. That matters
because prefetch cannot cover the six Group A sites, which have no request entry point, and six is few
enough to fix individually and too many to keep correct by discipline: every future core change that reads
metadata on one of those threads reopens the hole, and the failure only shows on a cache miss, which
testing does not produce. **Missing a prefetch site now costs a slow request rather than a stalled
cluster.**

Nine tests. Removing the guard fails two of them; the mutation returns a descriptor on the applier thread,
which is the stall.

### The wiring pass, and the count that prompted it

With every T-task and every C-change landed, I counted production callers of what had been built. Six
components had none:

`BlobDescriptorBackend`, `BlobDescriptorChangeLog`, `BlobNameIndexCheckpointStore`,
`DescriptorEnumerator`, `WarmCandidates`, and `DescriptorPrefetch`'s registrar.

**C1 was the sharpest case: it did nothing.** The seam landed in core and the hook landed in
`TransportBulkAction`, both correct, and nothing registered a prefetcher. The hook found nothing installed
and returned immediately on every request in every cluster. Nothing failed and nothing logged, because a
no-op prefetch is indistinguishable from a fast one. This is the failure this area's own record names
seven times over, and I had added six more instances of it in one branch.

**What the pass fixed.** `DescriptorGate` now registers a prefetcher that warms each name through the
store's cache, so C1 is real. The change log and the name index feed both hang off
`IndexDescriptorPublisher`, which is where every descriptor write already flows, so T12 and T13 stop being
capabilities nobody invokes.

Three of the components also asked for a `BlobStore` while the plugin resolves everything through
`resolveContainer` returning a `BlobContainer`. That mismatch is the same mistake wearing a different hat,
and they now take a `Function<BlobPath, BlobContainer>` which the plugin's own seam satisfies directly.

**`DescriptorGateReachabilityTests` makes the count a build step** rather than something found by grepping.
It asserts install registers every seam it owns and uninstall clears every one. Suppressing the prefetch
registration fails two of its three tests.

**What is still unwired, stated rather than left to be discovered again.**

| component | state |
|---|---|
| `BlobDescriptorChangeLog` | wired; appended on every descriptor write |
| `NameIndexService.apply` | wired, locally. **The cross-node tailer does not exist**: a remote node learns of a change only through a rebuild |
| `BlobDescriptorBackend` | **not selectable.** `ServerlessStoragePlugin` still builds the system-index `DescriptorStore`, and T3 is why: three of eleven operations are prefix searches a blob store cannot serve, so swapping needs a composite backend or the name index serving them first |
| `BlobNameIndexCheckpointStore` | **no caller.** Needs a scheduled writer and a load on startup |
| `DescriptorEnumerator` | referenced only for its prefix constant. **No rebuild path invokes it** |
| `WarmCandidates` | **no caller.** Needs a hook on the search routing path |
| P3, P4 | not started. Section 7's request-rate economics depend on both |

Four of the six are still unreachable. The difference from before is that they are now written down as such
in a table rather than discoverable only by counting, and the two that were most load-bearing are fixed.

### I1-I4: integration, and the defect it immediately found

The batch was: composite backend, selectable by setting, wired by the plugin, proved by an end-to-end test.
The first three landed. The fourth is muted with its finding recorded, which is the outcome this area's
rules prescribe when something cannot be driven to green in one sitting.

**I1, the composite.** `CompositeDescriptorBackend` takes point operations from one backend and prefix
searches from another, because T3 established a bucket cannot serve the latter. This is what
"make the backend selectable" actually requires; a straight swap would silently stop every wildcard
matching anything created afterwards.

**I2, the gate widened** to the two SPIs, with an overload preserving the one-store form so no existing
caller churns. The publisher dual-writes when the two halves differ, which is what keeps wildcards
answering while point reads move, and makes the switch reversible because the system index still holds
everything.

**I3, `serverless_storage.descriptor.backend`,** defaulting to `index`. Choosing `blob` and failing to
resolve a container throws rather than falling back, because quietly serving descriptors from the index
after the operator asked for the object store is a durability guarantee nobody would notice was missing.

**I4b: the plugin has never installed the descriptor plane in a real node.** `createComponents` decided
whether to install `DescriptorGate` from `SERVERLESS_STORAGE_ENABLED_SETTING.get(environment.settings())`.
That setting is `IndexScope`, and OpenSearch **rejects index-scoped settings in node settings outright**,
so the expression could never be true. The same condition also gated the shard suspension registry.

Nothing caught it because every gated integration test calls `DescriptorGate.install` by hand inside the
test method, so the plugin's own wiring was never the thing under test. A whole subsystem was correct,
tested, and unreachable in production. That is the same shape as the six components the caller count found,
at a much larger scale, and it was found only by writing a test that refused to install anything itself.

Fixed with a node-scoped `serverless_storage.enabled`. Existing tests are unaffected, and the reason is
worth stating: they never set it, and the old expression was always false, so their behaviour is identical.

**I4 itself is muted, one layer deeper.** With the gate installing, gated creation reaches the blob backend
and stops there. `BlobDescriptorBackend.createAsync` completes synchronously inside an already-completed
future, and `DescriptorGate.registerCreator` runs on the cluster state thread, which the gate's own comment
says must not block: *"registering the blocking put hung the node instead of failing, which is how the
constraint was found."*

The backend states that limitation in its own javadoc, written when nothing called it. This is the first
caller that actually needs it lifted, and lifting it means threading an executor into the backend. All four
test failures are downstream of the one cause: the descriptor never lands, so the wildcard finds nothing,
the delete finds no index, and the write leaves a shard locked.

**What the exercise demonstrated.** Twenty-two tasks of component work produced zero integration bugs
because there was no integration. One integration test produced two on its first run, one of them a
subsystem that could never have worked in production. Section 12's remaining items should be read with that
in mind: the unwired components are not nearly done.

### T7 verification, including the part that looked like a regression

247 descriptor and gated tests pass.
The full plugin suite reports 1,242 tests with 19 failures, all in `ServerlessStoragePluginTests` and all
with `ClusterService is null`. Those were checked against the base by stashing the change and re-running:
19 failures there too. Pre-existing and unrelated, but assumed-unrelated would not have been good enough,
because "an unexplained failure is a residency assumption until proven otherwise" applies to unexplained
passes on unrelated suites as well.
