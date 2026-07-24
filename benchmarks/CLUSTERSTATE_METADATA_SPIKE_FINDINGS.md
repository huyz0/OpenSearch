# Spike: pluggable per-index metadata residency, at 100M-tenant scale

## Context

This is the spike the previous review round called for before considering a formal core
RFC for a pluggable `IndexMetadata`/routing residency SPI (the "compact stub, materialize
lazily" idea aimed at letting both serverless-storage tenants and classic local-disk
indices scale index/shard counts into the millions without holding every tenant's full
metadata on every node). That round's adversarial review found two unresolved structural
problems in the original design and recommended measuring two things before writing an
RFC: the real heap payoff, and whether a partial/compact representation can actually serve
`IndexNameExpressionResolver`'s wildcard/alias resolution. This spike answers both with
code and numbers instead of estimates.

## 1. Can a compact stub be a real `Metadata`-lookup-compatible object, built outside core?

No -- confirmed by reading the actual classes, not by attempting and failing a prototype.

- `IndexMetadata`'s only full-data constructor is `private` (`IndexMetadata.java:1168`).
  The sole construction path is `IndexMetadata.Builder#build()`, which does substantial
  unconditional per-index work eagerly: settings validation (`INDEX_NUMBER_OF_SHARDS_SETTING`,
  `INDEX_NUMBER_OF_REPLICAS_SETTING`, routing-partition/virtual-shard checks), filling
  `inSyncAllocationIds` for every shard id, building `DiscoveryNodeFilters`, etc. There is no
  lazy or partial construction mode, and the private constructor means no subclass can exist
  outside `org.opensearch.cluster.metadata` -- which means outside OpenSearch core entirely,
  not just outside a plugin.
- `Metadata.Builder#buildIndicesLookup()` (`Metadata.java:1802-1858`) iterates the hard-typed
  `Map<String, IndexMetadata> indices` unconditionally, wrapping every entry in a real
  `new IndexAbstraction.Index(indexMetadata)` with no branch for a partial/stub entry.
  `Metadata`'s own constructor (`Metadata.java:322`) does the same kind of unconditional
  per-index iteration before that.
- `IndexAbstraction` itself is a public, freely-implementable interface (this was initially
  assumed to be part of the blocker; it isn't) -- but its `getIndices()`/`getWriteIndex()`
  methods return `List<IndexMetadata>`/`IndexMetadata`, hard-typed to the concrete class, so
  even a custom `IndexAbstraction` implementation still needs a real `IndexMetadata` instance
  to return, which loops back to the point above.

**Conclusion**: a compact stub cannot be built as an external plugin type. Achieving one
requires literal changes to `IndexMetadata.java` and `Metadata.java` in `server/`. This
matches and sharpens the previous round's finding -- it's not "probably needs core changes,"
it's "specifically blocked by one `private` constructor and two unconditional iteration
sites," which is a much smaller, more precisely scoped patch surface than "type-hierarchy
surgery" suggested.

## 2. What does the real object graph cost, and what's the realistic floor?

Two separate measurements, because they answer different questions.

### 2a. Allocation cost (GC churn / provisioning throughput)

`TenantIndexRetentionBenchmark`, run with the GC profiler:

```
gradlew -p benchmarks run --args=' TenantIndexRetentionBenchmark -prof gc'
```

| | bytes allocated per index (gc.alloc.rate.norm) |
|---|---|
| Real `IndexMetadata` + `RoutingTable` (1 shard, 1 alias) | **11,824.8 B/op** |
| Compact DTO (name, uuid, shard count, alias name only) | **32.0 B/op** |

This number includes transient garbage -- intermediate maps and builder objects inside
`IndexMetadata.Builder#build()` that never get retained. It's the right number for "how much
GC pressure does creating/updating N tenant indices generate" (e.g. mass provisioning,
bulk onboarding), not for "how much resident heap does holding N tenants cost."

### 2b. Retained heap (does it fit on a node)

`TenantIndexRetainedHeapEstimate`, a standalone diagnostic (not JMH -- builds N objects,
holds strong references, forces a full GC, reads the heap delta):

```
java -Xms4g -Xmx4g -XX:+UseSerialGC -cp <benchmarks runtime classpath> \
  org.opensearch.benchmark.clusterstate.TenantIndexRetainedHeapEstimate full 200000
java -Xms4g -Xmx4g -XX:+UseSerialGC -cp <benchmarks runtime classpath> \
  org.opensearch.benchmark.clusterstate.TenantIndexRetainedHeapEstimate compact 200000
java -Xms4g -Xmx4g -XX:+UseSerialGC -cp <benchmarks runtime classpath> \
  org.opensearch.benchmark.clusterstate.TenantIndexRetainedHeapEstimate realistic 200000
```

Measured on this machine (OpenJDK 21, SerialGC, 200,000-object samples):

| | bytes retained per index | extrapolated to 100M tenants |
|---|---|---|
| Real `IndexMetadata` + `RoutingTable` (baseline) | **~3,668 B** | **~341.6 GiB** |
| `RealisticCompactTenantIndex` (diff-plausible) | **~1,097 B** | **~102.2 GiB** |
| Compact DTO (theoretical floor) | **~161 B** | **~15.0 GiB** |

341.6 GiB for metadata alone, before counting `ClusterState`'s other fields, `DiscoveryNodes`,
per-node JVM/OpenSearch overhead, or any actual document data, is well beyond what a single
realistic cluster-manager node holds today (tens of GiB heap is already large for a
cluster-manager). This confirms the original problem statement with a real number rather
than architectural intuition alone: at 100M tenants, today's design does not fit on one node,
by roughly 5-10x even if the *entire* heap were dedicated to nothing else.

**The bare-DTO floor overstates the achievable win.** The first version of this spike stopped
at the 161 B floor and reported a ~22.8x reduction. That number doesn't survive a closer,
diff-protocol-grounded design pass. Reading `IndexMetadata.IndexMetadataDiff` (the object
actually sent over the wire and applied on every node, `IndexMetadata.java:1660-1705`) shows
several fields are kept *whole*, not shrunk further, because the diff protocol depends on
them: the full `Settings` object, the full alias map (`Map<String, AliasMetadata>`, needed
by both `IndexAbstraction.Alias` and the diff itself), and four per-field version longs plus
`state`. `RealisticCompactTenantIndex` keeps all of those real and unabridged, and only
drops what genuinely doesn't appear in the diff or in `IndexAbstraction`'s needs -- the four
`DiscoveryNodeFilters` objects (allocation-only, recomputable from settings on demand),
`mappings`/`customData`/`rolloverInfos` when empty, and `context`/`ingestionStatus`/
`splitShardsMetadata` when null -- plus it substitutes a two-field `CompactShardRouting` for
the real `RoutingTable -> IndexRoutingTable -> IndexShardRoutingTable -> ShardRouting` object
chain on the routing side.

That measured **~1,097 B/index -- ~102.2 GiB at 100M tenants**, a ~3.3x reduction from the
baseline, not ~23x. It's worth noting *why* the win is smaller than hoped: this spike's "full"
baseline was already a minimal, default-configuration tenant index (no mappings, no routing
filters, no rollover info, no custom data) to keep the comparison fair, and for a genuinely
minimal tenant index, most of what a compact scheme would drop was already absent or `null`
at zero extra cost. The dominant remaining cost isn't optional payload data being carried
unnecessarily -- it's largely fixed per-object overhead (Java object headers across roughly
half a dozen nested objects, `Settings`' internal map, the alias map, two `String`s for name
and uuid) that a compact-but-still-diff-compatible representation cannot shed.

**~102.2 GiB at 100M tenants is still a lot.** It's smaller than the 341.6 GiB baseline, and
it's a real, work-worth-doing reduction, but it does not comfortably fit "a single reasonably
large node" the way the bare-DTO floor suggested -- it's larger than most production
cluster-manager heaps run today, before counting anything else `ClusterState` or the JVM
needs. Section 3 revises the bottom line accordingly.

**Remaining caveat**: `RealisticCompactTenantIndex` is still a design sketch, not a proven
`IndexAbstraction`-integrated, wire-serializable type -- it wasn't built as an actual
`Diffable` implementation or run through real diff/apply/publish code, because doing that for
real means modifying `IndexMetadata.java`/`Metadata.java` in `server/`, which is out of scope
for a benchmarks-module spike (see section 1). Treat ~1,097 B as a well-grounded estimate of
the achievable size, not a verified floor.

## 3. Revised bottom line

The heap-payoff question is answered, and the honest answer is more modest than the first
pass of this spike suggested. The realistic, diff-plausible reduction is roughly **3.3x**
(3,668 B → ~1,097 B per index), not the ~23x a bare-identity DTO implied. At 100M tenants that
is 341.6 GiB → ~102.2 GiB -- a real improvement, and confirmation that today's design is
infeasible on a single node by a wide margin, but **not, by itself, enough to make 100M
tenants' metadata comfortably resident on one conventional cluster-manager node.** Even in the
best case this line of work can plausibly achieve, holding all 100M tenants' metadata still
needs either an unusually large single machine or horizontal partitioning across more than
one cluster-manager -- which means the "cell-based storage + true cluster-manager federation"
idea flagged as the long-term roadmap item in the original 144-idea brainstorm isn't just a
nice-to-have follow-on to this SPI, it's required regardless of whether the SPI ships, once
you're actually trying to reach 100M tenants rather than a more modest target this reduction
alone would cover comfortably (~1,097 B/index puts roughly 10-30M tenants within a large
single node's comfortable reach, depending on how much heap is dedicated to metadata versus
everything else).

This changes the recommendation from the first pass. Then: "pursue a core prototype, the
payoff looks worth it." Now: the payoff is real but partial -- worth pursuing as one piece of
a larger plan, not as a solution to the 100M-tenant target on its own. The next step is still
the same core-module work identified before (a genuine `server/`-local prototype of a
diff-compatible compact `IndexMetadata`, not a plugin), but it should be scoped and proposed
alongside the horizontal-partitioning story from the start, rather than as a standalone RFC
that implies it solves the whole problem. Given that scope and the real risk of modifying
`IndexMetadata.java`/`Metadata.java` in OpenSearch core -- shared production code every
cluster depends on, not contained plugin or benchmark code -- that prototype is a decision
point for a human maintainer to sign off on before it starts, not something to begin
autonomously off the back of a benchmarks-module spike.
