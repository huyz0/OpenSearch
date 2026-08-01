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

**One check before P1 is worth building.** The register API may cap value size. A descriptor is small, but
if the cap bites, split the uniqueness token from the descriptor body and keep the token name-only. The
uniqueness key must never include the uuid: two clients creating the same name with different uuids would
write different keys and both conditional PUTs would succeed.

---

## 10. What this document does not establish

It is a design and a reading of the existing code, not a measurement. Nothing here was run.

The load-bearing unmeasured claims, in the order they would hurt:

- That the request-scoped prefetch actually covers the callers that matter. The enumeration has not been
  done, and an unenumerated caller is a deadlock.
- That the descriptor cache hit rate is high enough to hide an object store round trip under a real tenant
  access distribution.
- That the concurrently-active fraction is low enough for the fleet arithmetic in section 8 to be
  affordable.
- That the 212 core files outside the descriptor seams are genuinely unrelated. They were not audited.

Corrections go in this file with the reason they were wrong, kept rather than deleted, the way the spike
results do it.
