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
| **C1** | descriptor prefetch seam | core, new class plus a hook at ~4 transport action entry points | new seam, inert when unregistered | medium |
| **C2** | routing geometry on the descriptor | core, `IndexDescriptor` | two fields, carried into `toIndexMetadata` | low |
| **C3** | membership epochs and decommission | core, `ComputedPlacementMembership` and its service | versioned node list, retained history | medium |
| **C4** | conditional create on S3 | `plugins/repository-s3`, `S3BlobContainer` | wire `failIfAlreadyExists` to `ifNoneMatch("*")` | low |
| **C5** | on-demand shard materialisation | core, `IndicesClusterStateService` | second trigger, not a parallel path | high |

Everything else is plugin-side:

| | change | location |
|---|---|---|
| **P1** | blob-backed `DescriptorStore` | replace the system index client with a `BlobContainer` |
| **P2** | name index fed from descriptor writes rather than cluster state | `nameindex`, plus an append-only change log |

P2 gates the *removal* of the system index, not the addition of the blob backend: three of
`DescriptorStore`'s eleven operations are prefix searches that a blob store cannot serve. T3 in section 11
has the breakdown.
| **P3** | write lease scoped to `(node, epoch)` rather than per shard | `shardstate` |
| **P4** | commit manifests batched per node | `manifest` |

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

`IndexDescriptor.java` is in T39's working set, so this waits for T39 to resolve rather than rebasing a
field addition through an active edit.

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

### T7 verification, including the part that looked like a regression

247 descriptor and gated tests pass.
The full plugin suite reports 1,242 tests with 19 failures, all in `ServerlessStoragePluginTests` and all
with `ClusterService is null`. Those were checked against the base by stashing the change and re-running:
19 failures there too. Pre-existing and unrelated, but assumed-unrelated would not have been good enough,
because "an unexplained failure is a residency assumption until proven otherwise" applies to unexplained
passes on unrelated suites as well.
