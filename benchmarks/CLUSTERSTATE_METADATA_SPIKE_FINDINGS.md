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
```

Measured on this machine (OpenJDK 21, SerialGC, 200,000-object samples):

| | bytes retained per index | extrapolated to 100M tenants |
|---|---|---|
| Real `IndexMetadata` + `RoutingTable` | **~3,668 B** | **~341.6 GiB** |
| Compact DTO (theoretical floor) | **~161 B** | **~15.0 GiB** |

341.6 GiB for metadata alone, before counting `ClusterState`'s other fields, `DiscoveryNodes`,
per-node JVM/OpenSearch overhead, or any actual document data, is well beyond what a single
realistic cluster-manager node holds today (tens of GiB heap is already large for a
cluster-manager). This confirms the original problem statement with a real number rather
than architectural intuition alone: at 100M tenants, today's design does not fit on one node,
by roughly 5-10x even if the *entire* heap were dedicated to nothing else.

The compact floor -- ~15 GiB at 100M tenants -- comfortably fits a single reasonably-large
node. That's a ~22.8x reduction. If it were fully achievable, it would take the cluster-manager's
own resident-metadata cost from clearly infeasible to clearly tractable.

**Important caveat, and the honest limit of this spike**: the 161 B/index compact number is a
best-case floor from a bare DTO with no OpenSearch object graph behind it -- it is *not* a
proven achievable design. The previous review round found that OpenSearch's diff-apply
protocol requires each node to already hold a valid full diff-basis object to apply the next
incremental `ClusterState` diff (`DiffableUtils.JdkMapDiff`, `PublicationTransportHandler`),
and that `Metadata`'s index-abstraction lookup needs enough real data (aliases, hidden/system
flags, version numbers for diffing) to answer wildcard/alias queries correctly. A real,
working compact representation needs to satisfy both of those, so its true size sits
somewhere between 161 B and 3,668 B, not at the measured floor. This spike did not build or
measure that version, because doing so is itself the core-module prototype the previous round
recommended as the next step, not something a plugin-side benchmark can produce.

## 3. Revised bottom line

The heap payoff question that was previously "unproven, might be much smaller than hoped" now
has a concrete, favorable answer: even allowing generous headroom for the caveat in section 2,
a compact representation several times smaller than the current ~3,668 B/index is very
plausible, and the current design's ~341.6 GiB at 100M tenants is confirmed to be genuinely
infeasible on a single node, not just theoretically large. That's enough to say the underlying
idea is worth continuing to invest in.

What it does not change: this is still core-module work, not a plugin (section 1), and the
diff-protocol-compatible, `IndexAbstraction`-compatible compact type is still unbuilt and
unmeasured (section 2's caveat). The smallest next step is a genuine `server/`-local prototype
(not this plugin, not a new external plugin) of a diff-compatible compact `IndexMetadata`
representation, measured the same way this spike measured the floor and the current design,
to close the gap between the 161 B floor and the 3,668 B baseline with a real number instead
of a bracket. Only once that number exists is a formal core RFC for the full SPI worth writing.
