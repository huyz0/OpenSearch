# What this branch is, and what is left to deliver it

A step back from task-level work to ask three questions. What was this supposed to do, what does it
actually do now, and what stands between those two.

Written against `feature/serverless` at `b98d6e9a050`, measured rather than recalled. Every count below
comes from the diff, not from a document.

---

## 1. Requirements

Restating these because most of the gap analysis below is a judgement against them, and two of them have
never been written as testable statements.

**R1. Traditional OpenSearch is unaffected.** With no serverless plugin installed, the product behaves as
`main` does. Not approximately. This is invariant I4 in the metadata plane design, and it is the
requirement the whole approach is affordable under.

**R2. Core changes are extension points, not behaviour.** Where core must change, it gains a seam that
does nothing by default, and a plugin supplies the behaviour. The measure of success is the number of core
files that carry a decision rather than a hook.

**R3. Durable state lives only in the object store.** Descriptors, mappings, settings, manifests, segments,
the WAL, tombstones. Nothing on a node is a source of truth.

**R4. Every local structure is reconstructible.** Losing a node costs speed, never data. There is a path
from the object store alone back to any local structure.

**R5. Placement is computed, not stored.** Shard-to-node is a function of shard identity and a membership
epoch. No routing table is published for a serverless index.

**R6. The target population is 100M indices at up to 100 shards each**, index per tenant, so per-index
cluster state cost has to go to zero rather than get smaller.

**R7. Both kinds of index coexist.** A cluster runs gated and traditional indices at once, which is what
makes migration expressible.

**R8. Elasticity down to zero.** Nodes come and go; shards suspend when idle and reactivate on demand.

R1 and R2 are the user-facing constraints. R3 through R8 are what the design is for.

---

## 2. What the change actually is

Three layers, and conflating them makes every number meaningless.

| layer | commits | what it is |
|---|---|---|
| `main` to T39 base | 1,028 | the serverless storage plugin: object-store engine, WAL, GC, PITR, resharding, scale-to-zero, computed placement |
| T39 base to HEAD | 43 | the metadata plane: descriptors replacing cluster state entries |

Against `main`: 3,694 files, 368,520 insertions. The plugin is 403 production Java files across 24
subsystems. 673 test files were added.

The thesis is that an index's record becomes a descriptor in object storage instead of an entry in cluster
state. A gated index has no cluster state entry, so creation costs one conditional PUT rather than a
metadata rebuild plus a publication plus a cluster-wide acknowledgement. Uniqueness comes from the object
store's compare-and-swap rather than from the cluster manager serialising. Placement is computed.

Measured: 25 gated creations advance the cluster state version zero times, against 2.16 versions per
ordinary index.

---

## 3. The core footprint, measured

`server/src/main/java` against `main`: **303 files, 17,959 insertions, 1,605 deletions.**

| | count |
|---|---|
| added (new seams and mechanisms) | 51 |
| modified | 252 |
| of the modified, files whose diff references a seam | **54** |
| of the modified, files that do not | **198** |

That split is the whole R2 story and it is worth being blunt about.

The 54 are the metadata plane's real footprint. They follow one shape: a static registry holding a
nullable supplier, a call site that checks for null, and identical behaviour when nothing is registered.
That is a defensible extension-point design and it scales to the ten seams listed in section 4.

**The 198 are not that.** They belong to the pluggable engine work the branch was originally cut for, and
they were never audited against R2. Some are certainly benign. Nobody has established which. The metadata
plane design document says this explicitly and then sets it aside as "not this design's concern", which
was reasonable for that document and is not reasonable for delivery.

`IndexShard` gains 403 lines, `Metadata` 332, `MetadataCreateIndexService` 261, `StoreRecovery` 238,
`DataFormatAwareEngine` 862. Whether those are guarded by a plugin check or change the default path is
unknown, and it is the single largest open question against R1 and R2.

---

## 4. What is not delivered yet

### 4a. Mechanisms with no caller

The branch's own name for this failure is *correct and unreachable*: a mechanism that is right, tested,
and that nothing invokes. It has produced it repeatedly, and a no-op is indistinguishable from a fast path,
so only counting callers finds it. Counted now:

| | state | consequence |
|---|---|---|
| `DurableTombstones` | hook called from `MetadataDeleteIndexService`, **zero registrars** | a gated delete acknowledges before its tombstone is durable. A node partitioned during the delete can adopt the dangling shard data on rejoin. This is the resurrection the mechanism exists to prevent, and it is currently a no-op. |
| `BlobNameIndexCheckpointStore` | **zero production references** | the name index has no persisted checkpoint, so a node rebuilds it by LIST over the whole population on every start |
| `WarmCandidates` | **zero production references** | routing cannot prefer a node that already holds a shard's data, so reactivation ignores warmth |
| `MappingRefreshOnDemand` | **zero production references**, superseded by `StoreBackedFieldRefresher` | dead code. Its own javadoc says the other class "is what ships". Delete it. |

`DurableTombstones` is the one with a correctness consequence rather than a performance one.

### 4b. Cross-node propagation is missing

A descriptor change reaches other nodes only by cache expiry, currently a one second TTL. There is a change
log (`BlobDescriptorChangeLog`) and a feed, but no tailer, so:

- a deleted gated index's shards are reclaimed by a **60 second polling sweep** rather than an event. The
  sweep exists and works and is honest about being a stand-in; its own comment says the push version
  belongs on the change feed.
- the descriptor cache is never invalidated by a write on another node.

This is the highest-value remaining piece, because it turns three separate stand-ins into one mechanism.

### 4c. Never measured against a real object store

Everything runs against `FsBlobContainer`. A local read is microseconds; an S3 GET is 20 to 100 ms. The
entire design rests on caching hiding that difference, and the hit rate has been measured while the latency
it is hiding has not. The 1 second cache TTL means a hot tenant re-reads once per second per node, which is
free on a filesystem and may not be on S3.

No number in this branch about latency or throughput has an object store behind it.

### 4d. Residency assumptions unaudited

212 core files were never checked for code that assumes an index is in cluster state. The failure mode is
specific and has already happened more than once: a caller reads `Metadata.index(name)`, gets null for a
gated index, and reports absence rather than deferring to the descriptor. The last two instances were a
delete path that threw "no such index" and a wildcard path that silently matched nothing.

### 4e. R1 has no continuous guard

R1 is the requirement most likely to be broken silently and it is currently checked by running the core
suites and observing that they pass. That is evidence, not a guard: it proves tested paths are preserved,
not that core is inert when the plugin is absent.

The cheap version is a test asserting every seam registry is empty on a stock node and that the
corresponding call sites take their default branch. Section 1 of the design document says I4 "can be tested
cheaply and continuously, which is why it should be the one guarding the rest". It is not yet.

### 4f. Known semantic losses

Not bugs, but they are part of delivery and need a decision rather than a comment:

- `_all` and `*` deliberately do not expand over gated indices (T28). Documented, pinned by a test, and a
  real behaviour difference a user can hit.
- Wildcard expansion over gated indices is capped at 100.
- An alias filter, alias routing, or a write-index flag makes an index ineligible for gating, so it keeps
  its cluster state entry.

---

## 5. What would close it

Ordered by what unblocks the most, not by size.

1. **Register a `DurableTombstones.Writer`.** Smallest change with a correctness consequence. A gated
   delete currently acknowledges with no durable record behind it.
2. **Build the change log tailer.** Replaces the D2 sweep with a push, gives the descriptor cache real
   invalidation, and lets a node learn about another node's writes. One mechanism, three stand-ins retired.
3. **Wire the name index checkpoint** so a restart does not LIST the population.
4. **Audit the 198 non-seam core files against R2.** The output is a list: benign, needs a seam, or changes
   default behaviour. Until it exists, R1 and R2 are asserted rather than known.
5. **Add the R1 guard test** so the answer to 4 stays true.
6. **Measure against a real object store.** One bucket, the existing benchmarks, no new code. Every latency
   claim in this branch depends on it.
7. **Consume `WarmCandidates` on the routing path**, or delete it and stop implying reactivation is
   warmth-aware.
8. **Delete `MappingRefreshOnDemand`.**
9. **Finish the residency audit** of the remaining core files.

Items 1 through 3 are the metadata plane finishing what it started. Item 4 is the one that decides whether
this is upstreamable at all, and it has never been attempted.

---

## 6. What this analysis did not establish

Stated so it is not mistaken for coverage.

- Whether the 198 non-seam core files preserve default behaviour. Not sampled, not read. The largest open
  question here and the reason section 3 refuses to call R2 met.
- Whether the plugin's other 20 subsystems have their own unreachable mechanisms. The reachability count in
  4a covers the metadata plane only. The same grep over `wal`, `gc`, `retention`, `resharding` and the rest
  has not been run, and this branch's history says it would find something.
- Whether the numbers in `SCALABLE_METADATA_SPIKE_RESULTS.md` still hold. Some were re-measured recently
  and one was found to have been quoted against the wrong JVM configuration for months.
- Correctness under node failure, partition, or concurrent reshard. The tests build real clusters but do
  not fault-inject.

---

## 7. G4: the non-seam core audit, first pass

Section 3 said 198 modified core files had never been checked against R2 and that this was the largest open
question. This is the first pass over them. It does not clear them; it finds enough to say the answer is
"no" and to say where.

### Method

Every modified core file whose diff never mentions a seam, bucketed by whether it alters existing lines,
then scanned for removed public or protected members, then the largest read by hand. Deletions are the
signal that matters: adding a method is an extension, removing one is a change to what core is.

| | count |
|---|---|
| non-seam modified core files | 198 |
| pure additions, no existing line altered | 73 |
| alter existing lines | 125 |
| **remove public or protected members** | **21 files, 34 members** |
| core files deleted outright | 1 |

### Three findings that answer the R2 question

**`Plugin.nodeStats()` was removed, and it was an extension point.** Core also lost
`plugins/PluginNodeStats.java` entirely, the only core file this branch deletes. In its place
`NodeStats` gained a concrete `NativeAllocatorPoolStats` from `org.opensearch.plugin.stats`. So a generic
mechanism by which any plugin could contribute node statistics was replaced by one implementation named
directly in core. That is the exact inversion of the requirement: an extension point became a hardcoded
dependency, and any other plugin relying on `nodeStats()` no longer compiles.

**`ShardRouting` lost `recoveringChildShards` and `parentShardId`**, eleven references in `main`, none here,
along with a constructor overload. Whatever the merits, a routing primitive changing shape is not a seam and
cannot be inert when the plugin is absent, because there is no plugin involved.

**Twenty-one files remove public or protected members**, thirty-four in total, including `IndexShard`,
`KeywordFieldMapper`, `ClusterMetadataManifest`, `FsRepository` and `MultiBucketConsumerService`. These are
not additive and were not written as seams.

### What this means for the requirement

R2 holds for the metadata plane and does not hold for the branch. The 54 seam-touching files follow the
extension-point shape faithfully. The engine work alongside them changes core's shape directly, and in at
least one case by deleting a plugin extension point.

That is a statement about upstreamability rather than about correctness. Nothing above is a bug. It means
the branch cannot be offered to `main` as "a few hooks plus a plugin" until the engine work is either
separated from the metadata plane or reworked behind seams of its own.

### What is still unchecked

The 73 pure-addition files were not read. Adding a method to an existing class does not change default
behaviour, but adding a call inside an existing method does, and the two are indistinguishable from the
numbers alone.

Of the 125 that alter existing lines, 104 remove nothing public and were not read either. `IndexShard` at
403 added lines, `StoreRecovery` at 238 and `IndexingMemoryController` at 142 are the ones most likely to
carry a behaviour change, and none has been walked through.

The honest summary is that this pass proves R2 is violated without establishing how far. Closing it needs
the file-by-file read, which is a larger exercise than the rest of this list combined.
