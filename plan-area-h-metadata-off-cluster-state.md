# Area H: take index metadata out of cluster state entirely

## H.0 Goal

Index creation costs the same at 100 million indices as at zero, and no node holds a structure with one
entry per index.

## H.1 Why, measured rather than argued

Three numbers from this session say the current design cannot reach the target, and that the areas
already built do not fix it.

**Creation is superlinear in the existing population.** G2a measured index creation at 7.4 ms/index with
200 indices present, 10.3 at 1,000, 34.6 at 3,000 and 98.8 at 6,000. Projected to a million that is 27
hours and still climbing. The plan's G2 assumed "per-index costs known to be flat so extrapolation is
defensible", but what C11 established as flat was metadata *heap* (698 B/index at both 3 and 30 shards).
Time is not flat, and the two were conflated.

**The cause is structural.** `Metadata.Builder.build()` takes a fast path only when
`indices.equals(previousMetadata.indices)`, and creating an index changes that map by definition. So every
create runs `buildMetadataWithRecomputedIndicesLookups()`, sweeping every index in the cluster to rebuild
six name arrays and the sorted `indicesLookup`. Creating the millionth index does a million units of work
that have nothing to do with it. Even the fast path is six `Arrays.copyOf` of N-length arrays.

**Deferral changed the constant, not the complexity.** `LazyIndexMetadata` reduces an unread index from
2,944 B to 698 B, which is what makes the population fit in memory at all. But a stub is still an entry in
the `indices` map, and `build()` iterates holders. 100M stubs is still a 100M-element sweep, and 70 GB
resident.

Areas A, E and F each reduce bytes per index. None removes the entry. That is the gap this area closes.

## H.2 The shape to copy

S3 serves on the order of 10^14 objects with no global map. Its key index is range-partitioned across many
nodes, sorted, disk-backed, with memory as cache. A `HEAD` routes by key to the single partition that could
hold it. A `LIST prefix` is a range scan over sorted keys in the partitions covering that range. Nothing
enumerates, nothing is replicated everywhere. The old per-prefix rate limits and adaptive prefix splitting
were this design leaking through the API.

The two operations map exactly onto what OpenSearch needs from index metadata:

| S3 | what OpenSearch asks | today |
|---|---|---|
| `HEAD key` | does `logs-2024` exist, what are its bones | lookup in a replicated `HashMap` |
| `LIST prefix` | resolve `logs-*` | scan of that map |

**An OpenSearch index already is that structure.** A term dictionary is sorted, sharded, disk-backed,
cached by block, with point lookup and prefix scan as its two native operations. So this area does not
build an S3-like index. It stores index metadata in the thing OpenSearch already is, instead of in a map
bolted onto the consensus layer.

## H.3 What cluster state keeps

Everything with a bounded count stays. Everything with one entry per index leaves.

Stays: nodes, computed placement membership, cluster settings, templates, security metadata, repository
definitions, and exactly one index entry for the name index itself.

Leaves: `indices` (the map), `IndexGraveyard` (per-index tombstones), and for serverless indices the
routing table entry, which Area C already removed.

## H.4 The descriptor, which is the irreducible piece

A coordinator that receives `(index-name, query)` cannot route without knowing the index UUID and shard
count, because those are the inputs to `RendezvousShardPlacement`. So something global must answer:

```
name -> { uuid, shardCount, createdVersion, state, blocks, aliases }
```

That is the descriptor, roughly 200 B. At 100M indices it is a 20 GB index: a large but ordinary
OpenSearch index, sharded, and itself placed by computed placement. It is not held in memory anywhere.

Mappings and settings do **not** belong in the descriptor. They live in the object store under a
convention, `indices/{uuid}/metadata/{generation}`, fetched on demand and cached on the coordinator. Area
B's affinity routing exists to keep that cache warm, which is what makes the fetch rare rather than
per-request.

## H.5 Index creation, redesigned

The mechanism for uniqueness already exists and needs no invention. Indexing a document with
`op_type=create` and `_id=<index name>` is put-if-absent, enforced by the single shard that owns that id.
That is the same guarantee S3 gives on a conditional put, and it is one indexing request.

```
create index "logs-2024":
  1. index descriptor into the name index, _id = "logs-2024", op_type = create
     -> conflict means the name is taken, which is the uniqueness check
  2. write mappings and settings object to the store at indices/{uuid}/metadata/0
  3. done
```

**No cluster state update at all.** No consensus round, no publication, no `Metadata.build()`, no sweep.
Cost is one indexing operation routed to one shard, independent of how many indices exist. That is the
falsifiable claim of this area, and G2a is the measurement that tests it.

Resolution and wildcards become the two S3 operations: a realtime GET by id, and a prefix search.

## H.6 Deletion, which is the subtle one

`IndexGraveyard` exists so a node partitioned during a delete does not resurrect the index's dangling data
on rejoin. Convention-based storage cannot answer this on its own: absence of an object is
indistinguishable from not having looked.

The descriptor answers it, and better. Deletion writes `state: DELETED` with a timestamp into the name
index rather than removing the document. A node adopting local shard data must resolve the descriptor
first and delete its data if the descriptor is missing or tombstoned. This is strictly stronger than the
graveyard, which keeps a bounded list (500 by default) and forgets older deletions.

## H.7 Dynamic mappings, and why no shard has to be informed

Today a document with a new field triggers a cluster state update through the elected manager: a global
serialisation point on the write path, plus the O(total indices) rebuild S20 measured. The same path
serves the explicit `PUT _mapping` API, so for a gated index **both** fail identically at
`Metadata#getIndexSafe`. The constraint is therefore not "no dynamic fields", it is "the mapping is
immutable after creation", which is a far larger commitment and not one worth accepting.

**The key realisation is that a broadcast is not required.** It exists because cluster state was the only
distribution mechanism available, not because every shard must be told. Look at who needs the mapping and
when.

**Writers converge without being told.** A shard receiving `age: 30` infers `long`, which is what every
other shard would infer from the same data. It does not need the answer pushed to it; it needs its
inference not to conflict. An optimistic loop on a generation counter gives exactly that:

```
shard A: read gen 5, add age:long,     CAS 5 -> 6  ok
shard B: read gen 5, add city:keyword, CAS 5 -> 6  conflict
         re-read 6, merge city onto it, CAS 6 -> 7  ok
```

One CAS per **new field**, not per document. Mappings stabilise, so this is rare traffic rather than
per-write traffic.

**Readers get the version for free.** The mapping generation goes in the descriptor. A coordinator already
fetches the descriptor to route, so that same fetch says whether its cached mapping is stale. No extra
round trip, and no invalidation protocol.

**Shards refresh lazily.** The coordinator stamps the request with the generation it planned against, and
a shard behind that generation reads the mapping object before executing. Pull on demand rather than push
on change.

### What it costs at 100M indices and 100 shards each

| | today | this design |
|---|---|---|
| a mapping change | publication to every node, plus the O(total indices) rebuild | one CAS, one descriptor write |
| informing 100 shards | broadcast | nothing, because nobody is informed |
| scaling in index count | O(N) | O(1) |
| scaling in shard count | O(shards) | O(1) |

Independent of both counts, because nothing fans out.

### Three honest consequences

**Type conflicts move rather than disappear.** `age:long` against `age:float` fails today at the manager
and here at the second shard's merge. Same error, different location, and the retry loop must merge rather
than last-writer-win or a field mapping is lost silently, which is the failure shape this project has hit
eight times.

**Cross-shard read-your-write is weakened slightly.** Writing a new field to shard A and immediately
querying it on shard B before B refreshes will miss it. Publication is asynchronous today so this is
comparable rather than worse, but it belongs in the API contract next to the wildcard freshness decision
rather than being discovered.

**The field limit needs a new home.** Nothing counts fields centrally any more, so `index.mapping.total_fields.limit`
has to be enforced inside the mapping object at CAS time.

## H.8 The enumeration surface, audited

The reason this is a quarter of work rather than a year:

| surface | count | shape |
|---|---|---|
| `IndexNameExpressionResolver` call sites | 95, of which 25 are inside the resolver | **one chokepoint** |
| direct `metadata().indices()` | 31 | individual |
| `for (IndexMetadata : metadata())` loops | 10 | individual |

Almost everything funnels through one class. Teaching `IndexNameExpressionResolver` to resolve through the
name index converts the bulk of the surface in one place. The ~41 direct enumerations are the residual,
comparable in size to C23's nine callers and needing the same treatment: probe each, convert what is
reachable, refuse what should be refused.

## H.9 Hard preconditions

- **Serverless indices only.** An index whose placement is computed, whose routing is not published, and
  whose data is in the object store. Ordinary indices keep the `indices` map and get none of this. The two
  coexist: the resolver consults the map first, then the name index.
- **Area A** (name index tier) is the substrate and must land first.
- **Area C** is done, which is what makes routing derivable from the descriptor alone.

## H.10 Detailed tasks

Numbered so they can be worked one at a time. Each says what would make it fail, because in this area a
task that cannot fail is a task that measures nothing.

### H1. Audit the enumeration surface

**H1a. Classify the 41 direct enumerations.** `metadata().indices()` (31 sites) and
`for (IndexMetadata : metadata())` (10 sites). Each is convertible (becomes a descriptor query),
refusable (rejects a computed index the way C14 and C28 reject resharding and scaling), or blocking
(genuinely needs every index and cannot become a query). Blocking is a legitimate outcome and caps what
this area can remove. F1's precedent: the plan assumed two fields were equally removable and one was
load-bearing on the write path.

**H1b. Classify the resolver's own paths.** `IndexNameExpressionResolver` has 25 internal sites. Separate
those that resolve one name from those that expand a pattern, because the first becomes a GET and the
second becomes a search, and only the second has the freshness problem.

**H1c. Find what enumerates outside `server/`.** The plugin, and anything reading metadata through a
different door. C23's sweep found nine callers where the plan expected fewer.

### H2. Resolution through the descriptor index

**H2a. Descriptor value type and its index.** Name, uuid, shard count, created version, state, blocks,
aliases. A system index with a fixed name and uuid, placed by computed placement. Fails if the descriptor
cannot answer everything `RendezvousShardPlacement` needs, which would mean routing still requires the
map.

**H2b. Write a descriptor on index creation, in addition to the cluster state entry.** Dual write,
deliberately redundant, so H2c can compare. Fails if creation cost regresses measurably for ordinary
indices.

**H2d. Resolve through the descriptor when the map misses.** Behind the gate. The map still wins when
present, so this changes nothing yet and proves the read path in isolation.

**H2c. Compare both answers under load.** For a population of indices, resolve every name and every
wildcard through both paths and assert they agree. This is the phase where correctness is established
cheaply, while the old structure is still there to be right. Fails if the two disagree on aliases,
hidden or system indices, or closed state, all of which the descriptor must carry.

### H3. Creation without a cluster state update

**H3a. Create writes only the descriptor**, behind the gate, for computed indices. No metadata entry, no
publication.

**H3b. Re-run G2a.** The kill criterion. Creation cost per index must be flat against population where
today it is 7.4, 10.3, 34.6, 98.8 ms at 200, 1k, 3k, 6k. Not flat means stop and rediagnose.

**H3c. Re-measure residency.** With no map entry, a manager node should hold O(nodes) plus the descriptor
cache, not O(indices). The number to beat is C11's 698 B/index.

### H4. Deletion and dynamic mappings

**H4a. Tombstone in the descriptor.** State DELETED with a timestamp, not document removal. Fails if a
node can adopt dangling shard data without consulting it, which is the dangling-index resurrection
`IndexGraveyard` exists to prevent.

**H4b. A node adopting local shard data must resolve the descriptor first** and delete its data when the
descriptor is absent or tombstoned.

**H4c. Mapping updates as a CAS on the mapping object generation**, replacing the cluster state update on
the write path. Fails if two concurrent dynamic mapping updates can lose one.

### H5. Remove the map for computed indices

Only after H2 to H4 have soaked. Fails if any H1a blocking enumeration is still reachable.

## H.10a What Area H does not fix, found during the audit

Two costs scale with the index population and survive everything above, because they do not go through
cluster state at all.

**The plugin's periodic enumerations.** `ShardSuspensionCoordinator`, `ReaderCacheAffinityRecorder` and
`InPlaceMergeTriggerCoordinator` each iterate every index on a scheduler tick. Emptying the map would make
them iterate nothing and silently do no work, which is this project's characteristic failure and worse
than the cost it replaces.

**`ShardSuspensionCoordinator.findByUuid` is a full scan**, called from six places including two that run
on the cluster manager's state update thread. Suspending one shard is O(total indices) and a tick
suspending N shards is O(N x total). The cheap fix is unavailable: the candidate entry and the node reports
it is built from carry only UUIDs, and `Metadata` has no UUID lookup. Adding one would build another O(N)
structure per `Metadata.build`, which is the ceiling this area exists to remove.

Both are tracked as H1d. They matter because they are the counter-example to "Area H clears ceiling 3":
it clears the part that flows through cluster state, and these two do not.

## H.10b The limit H3 and H5 ship with, until H4c lands

Gated indices have no cluster state entry, and every mapping update path resolves its target through
`Metadata#getIndexSafe`, which throws when the index is not in the map. So **a gated index cannot accept a
document carrying a new field**: dynamic mapping fails at the first unknown field rather than degrading.
Proven by `GatedIndexMappingUpdateTests` rather than reasoned about.

That inverts H4c's priority. Moving mappings to a compare-and-swap on an object-store generation was filed
as a way to remove a global serialisation point from the write path, which is true and secondary. The
reason to do it is that without it the gate is only safe for indices whose mapping is fully known at
creation, which is not most of them.

**The gate is therefore not ready for general use**, and that is a property of this design rather than a
bug in it. H3 and H5 are correct and measured: creation is flat and leaves nothing resident. They are
usable today for a fixed-mapping index and not for a dynamic one, and shipping them without saying so
would be the dishonest version of a real result.

## H.10c A gated index cannot scale to zero, which is the point of the design

Found by asking whether the plugin's suspension path works for an index Area H has removed from cluster
state. It does not, and the way it fails is worse than not working.

`ShardSuspensionCoordinator.findByUuid` resolves a shard's index by scanning `metadata.indices()`. H1d
recorded that as O(total indices) on a path that runs per candidate shard per tick. For a gated index the
scan also finds nothing, because the index is not in metadata at all.

**The prediction was wrong and the correction is the finding.** The guess was that the pre-check would
find nothing and skip the submission, making suspension a cheap no-op. What happens is that the pre-check
falls through on a null, a cluster state task *is* submitted, and the transform inside it then finds
nothing and returns the state unchanged. The publication slot is spent and the work is not done, so every
candidate shard of every gated index queues a task per tick that can only be a no-op.

So an index that uses Area H cannot scale to zero, and scaling to zero is what the serverless design is
for. Pinned by `GatedIndexSuspensionGapTests`, which asserts the no-op rather than an absence of
exceptions.

**One fix addresses both problems.** Resolving the index through the descriptor seam rather than by
scanning metadata makes suspension work for a gated index and removes the O(total indices) cost for every
index at the same time. That is H9b, and it is the first thing the next cycle should do, because a
serverless index that never sleeps is a worse outcome than one that cannot be created.

## H.10d The second wrong prediction in the same section, and what suspension actually needed

The paragraph above says one fix addresses both problems, and that resolving the index through the
descriptor seam would make suspension work. That is wrong, and it is wrong in the same way the first
prediction was: it assumes the only thing missing is a lookup.

Suspension is enforced by `SuspendedShardAllocationDecider`, which works by telling the allocator to
refuse the shard. A computed index never reaches the allocator. Area C derives its placement instead, so
a decider refusing the shard is a verdict delivered to something that is not running. Giving the state a
new home while leaving the enforcement where it was would have produced a shard marked asleep in one
place and placed on a node by another, which looks exactly like scale-to-zero silently not working.

So suspension changes shape rather than address. It stops being a verdict handed to an allocator and
becomes an input to placement: a shard that placement does not place is assigned nowhere, and that is the
only definition of asleep available to an index whose routing is computed.

**H9c** put the filter in `AbsentIndexRoutingSuppliers.supply`, the one method every computed routing read
funnels through, so a supplier cannot forget it and wake a sleeping shard. `IndexDescriptor` carries the
suspended set as the durable record; the registry carries the hot-path view, keyed by uuid so a
delete-and-recreate cannot inherit the previous index's sleeping shards.

**H9d** connected the two. The property worth stating is not that gated suspension works but that it
submits no cluster state task at all. Scale-to-zero suspends shards continuously, so at a hundred million
indices one publication per suspension is the entire cluster manager. The test asserts the absence of the
submission for that reason: a version that suspended correctly while still publishing would pass a
"does it work" test and fail at the scale the area exists for.

The registry is bounded by shards currently asleep, not by indices, and an index whose last shard wakes
loses its entry rather than keeping an empty set. A map with an entry per index that has ever slept is the
residency ceiling rebuilt in another data structure.

State is node-local, deliberately. Divergence is safe because of its direction: a node that has not
learned of a suspension places the shard and serves reads from it, which is stale but correct, while the
node that suspended it stops sending work there.

## H.11 Phasing, each phase falsifiable

**H1. Audit.** Classify all 41 direct enumerations as convertible, refusable, or blocking. F1's precedent:
the plan assumed two fields were equally removable and one was load-bearing on the write path. Expect at
least one of these to be the same.

**H2. Resolve through the name index, with indices still in cluster state.** Proves the read path against a
population that still exists in the old structure, so any divergence is visible by comparing the two
answers. This is the phase where correctness is established cheaply.

**H3. Create without touching cluster state**, behind the gate. **Re-run G2a. The claim is that the curve
goes flat.** If per-index creation cost still grows with population, this area has failed and should stop
here.

**H4. Tombstones and dynamic mappings** on the new path.

**H5. Remove the map for serverless indices**, only after H2 to H4 have soaked. Then re-measure resident
heap: the target is O(nodes), not O(indices).

## H.11 What would kill this

- **Wildcard freshness.** A newly created index is visible to a realtime GET immediately but not to a
  prefix search until refresh. `logs-*` may not include an index created a second ago. That is a semantic
  change to index resolution and it must be stated in the API contract rather than discovered.
- **The name index becoming the bottleneck.** 100M creates is 100M indexing operations. That is ordinary
  indexing throughput, but the name index's own shards are placed by computed placement, and a hot prefix
  concentrates on one shard exactly as it did for S3 before adaptive splitting. This needs measuring, not
  assuming.
- **An enumeration that cannot be converted.** Upgrade-time version checks and some snapshot paths
  genuinely want every index. If one of them is load-bearing and cannot become a query, it caps what this
  area can remove.
- **Bootstrapping.** The name index must be findable without itself. It gets a fixed name and UUID, its
  placement computed, and the one cluster state entry that remains.

## H.12 The single number that decides it

G2a, re-run after H3: creation cost per index against population size. Today it is 7.4, 10.3, 34.6, 98.8
ms at 200, 1k, 3k, 6k. Flat is the claim. Anything else and the architecture has not changed, only moved.

## H.13 Spike result: the premise holds

Measured before committing to the work, and recorded as S19.

The cause is confirmed rather than inferred. `Metadata.builder(existing).put(one).build()` costs 0.67 ms
at a thousand indices and 107.67 ms at a hundred thousand. The no-change fast path is O(N) as well, 31.5
ms at a hundred thousand, so every metadata change pays for the whole population and not only creation.

The replacement was measured in the same harness on the same cluster: descriptor writes with
`op_type=create` stay flat where index creation degrades fourteenfold, 0.16 ms against 91.66 ms at six
thousand. Projected to a million that is 2.7 minutes against 25.5 hours. The duplicate-id write throws
`VersionConflictEngineException`, so the put-if-absent guarantee this design rests on is real.

**The kill criterion was "if the curve is not flat, stop". It is flat, so H1 onwards is justified.** What
the spike does not prove is everything after creation: resolution, wildcards, deletion, and the
forty-one enumerations. It proves the foundation only.
