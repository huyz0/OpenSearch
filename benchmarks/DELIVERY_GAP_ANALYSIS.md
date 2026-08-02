# What this branch is, and what is left to deliver it

A step back from task-level work to ask three questions. What was this supposed to do, what does it
actually do now, and what stands between those two.

Written against `feature/serverless`, measured rather than recalled. Every count below comes from the diff,
not from a document.

**Baseline, and a warning.** All current figures are measured against `b99229e3f36`, the merge base of this
branch and `upstream/main`. Sections 3, 7, 11 and 12 were originally measured against the local `main` ref,
which is six weeks stale, and so counted 332 upstream commits as this branch's work. Section 13 records the
correction and what it changed. Sections marked superseded keep their original text because the reasoning is
still worth reading; their numbers are not. If you add a measurement to this document, diff against the merge
base, `git merge-base HEAD upstream/main`, and say so.

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

**Baseline.** Every figure below is measured against `b99229e3f36` (Jul 6 2026), the merge base of this
branch and `upstream/main`. That matters: the local `main` ref is stale, sitting at May 25, and **332 of
the 1088 commits in `main..HEAD` are upstream commits, not this branch's work**. Earlier revisions of this
document measured against `main` and so attributed roughly a third of upstream's changes to this branch.
Section 3.1 records what that corrected. Measure against the merge base, never against `main`.

`server/src/main/java` against the fork point: **143 files, 11,341 insertions, 449 deletions.**

| | count |
|---|---|
| added (new seams and mechanisms) | 37 |
| modified | 106 |
| of the modified, files reaching a seam | **59** |
| of the modified, files that do not | **47** |
| of all modified, files changing six lines or fewer | 17 |
| core files deleted | **0** |

That split is the whole R2 story.

The 59 are the metadata plane's real footprint. They follow one shape: a static registry holding a
nullable supplier, a call site that checks for null, and identical behaviour when nothing is registered.
That is a defensible extension-point design and it scales to the ten seams listed in section 4.

The 47 are not that. Section 15 reads all of them, and in particular reads every one of the 18 that
alters an existing line. Two things came out of it: the default path is unchanged in all 18, by
construction rather than by luck, and the branch turns out to contain a third body of work rather than
two.

A note on the seam count, because it moved. An earlier version of this table said 49 and 57, matching
seam names as text in each diff. That undercounts, because seam reach is transitive:
`IndicesStore` and `IncrementalClusterStateWriter` mention no seam but call
`RoutingNodes.localRoutingNode`, which calls one. Resolve callees, do not grep the patch.

### 3.1 What the stale baseline had wrong

The corrected numbers are roughly half the previously reported ones, and two claims did not survive at all.

| claim | measured against stale `main` | measured against the fork point |
|---|---|---|
| core files changed | 303 | **143** |
| insertions | 17,959 | **11,341** |
| deletions | 1,605 | **449** |
| non-seam modified files | 198 | **47** |
| removed public/protected members | 34, across 21 files | **5, across 2 files** |
| core files deleted | reported as a concern | **0** |

The five genuine removals are `ShardRouting.splitting/isSplitTarget/getParentShardId/getRecoveringChildShards`
and `OperationRouting.shardWithRecoveringChild`, all removed deliberately by CC1 and all verifiably dead on
the baseline. Everything else counted as a "removal" was a modified signature whose name still exists in the
same file. See section 13.

One further correction, of a change this analysis itself prompted: an earlier pass concluded that the branch
had deleted the `PluginNodeStats` extension point and restored it. That was wrong. Upstream removed it, in
PR #21820, replacing an `@ExperimentalApi` SPI that had one consumer and an unreleased wire format with a
typed `NodeStats` field and a narrower `ArrowAllocatorPlugin` SPI in `libs/arrow-spi`. The restoration
reintroduced an interface upstream had deliberately retired, and added a `Plugin.nodeStats()` that nothing
in the repository called, the same *correct and unreachable* failure section 4a describes, committed while
fixing a different instance of it. Reverted.

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

> **Superseded in its conclusions by section 13.** This pass was run against the stale `main` baseline, so
> its counts include upstream's changes as though they were this branch's, and its three headline findings
> do not survive re-measurement against the fork point. The method below is sound and worth keeping; the
> numbers and the verdict are not. Read section 13 for what is actually true.

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

---

## 8. G6: what a descriptor read costs when the store behaves like a store

Section 4c said no number on this branch had an object store behind it, and that the design rests on a cache
hiding latency whose size had never been measured. Measuring it found the cache was not there.

### The finding

`BlobDescriptorBackend` had no cache. T7 extracted `DescriptorCache` from the system-index store so "the
blob backend reuses it rather than growing a second copy", and the blob backend never took it. So switching
`descriptor_backend` to the object store, which is the entire point of the design, sent every point read to
the store. Each of the eleven synchronous resolution sites became a network round trip.

Invisible against `FsBlobContainer`, where a read is microseconds. That is why every earlier measurement
missed it, and it is the same shape as the rest of this branch's defects: correct component, never
integrated, and the gap only visible under a condition nothing reproduced.

### Measured, `LatencyProfile.TYPICAL` (GET 20-40 ms, PUT 40-80, LIST 50-100)

| | uncached, as it was | with the cache wired |
|---|---|---|
| one cold read | 31 ms | 39 ms |
| twelve reads of one hot tenant | **362 ms** | **0 ms** |
| four distinct tenants, second pass | 138 ms | 0 ms |
| one miss | 63 ms | 48 ms |

Twelve resolutions of a single hot tenant cost 362 ms uncached and nothing cached. A bulk request touching
one tenant resolves that tenant at several of the eleven sites, so this is a per-request cost rather than a
per-tenant one.

The latency is simulated, and that is the honest limit: `LatencyProfile` is this branch's own approximation
of S3, not S3. It gets the order of magnitude right, which is what the design question needed, and it needs
no credentials. A run against a real bucket is still worth doing and is still not done.

### Two costs that are correct and now have numbers

**A miss costs two round trips**, about 48-63 ms, because absence is only absence once the tombstone prefix
agrees. That is the right behaviour: "deleted" and "never existed" have opposite safe responses. It is the
most expensive descriptor operation and nothing had costed it.

**A miss is never cached**, so an index created moments after a failed lookup resolves immediately. Caching
absence would make a just-created index unresolvable for a freshness window, which H18 refused.

### What the cache needed to be safe here

Writes invalidate. `put` invalidates after the write rather than before, since dropping the entry first
leaves a window where a concurrent read repopulates from the old value and outlives the write. Deletion
invalidates once the tombstone lands, because a deleted index that still resolves would accept a write
against a shard the cluster no longer believes in.

---

## 9. G9: the residency audit

Section 4d said 212 core files had never been checked for code that assumes an index is in cluster state.
This narrows that to the accessor that actually fails and walks the results.

### The signal

`Metadata.getIndexSafe` throws `IndexNotFoundException` for an index cluster state has no entry for, which
is a gated index by definition. It is the "assume residency, fail loudly" accessor, and it is where both
previously-found residency bugs lived. 58 call sites across 37 core files.

`metadata().index()` returning null is the quieter half of the same problem and is not covered here.

### Classification

| | files | reachable for a gated index? |
|---|---|---|
| allocation, deciders, routing, gateway | 20 | **no**. A gated index publishes no routing table and gets no allocator involvement, so none of this ever sees one. |
| the metadata plane's own files | 5 | already handled, and the reason they call it |
| request and admin paths | 12 | **yes**, and this is where the audit had to look |

### Found and fixed

**`TransportGetAction.getExecutor`** called `getIndexSafe` purely to choose a thread pool, so **every
single-document get on a gated index failed** with "no such index". Bulk and search worked throughout,
because this is the only read path that picks its executor from metadata, which is exactly why nothing
caught it: the class of test that would have found it is a get by id, and none existed.

Proven before fixing rather than after, by adding the get-by-id case to `BlobBackedDescriptorIT` and
watching it fail with `IndexNotFoundException[no such index [tenant-getbyid]]`.

### Found and left, with reasons

**`TransportBroadcastReplicationAction`** calls `getIndexSafe` only inside `onFailure`, to count replicas
for the failure report. A gated index reaching it would throw while handling another failure and mask the
original cause. Worth fixing; it degrades an error message rather than breaking a working path, and it needs
a shard failure on a gated index to reach, which no current test produces.

**`MetadataUpdateSettingsService`** (three sites) and **`MetadataIndexStateService`** (two) are unguarded.
Update-settings, open and close on a gated index would fail the same way the delete path did before D1.
These are real gaps and they are not fixed here because each needs the same treatment D1 got, which is a
gated branch plus a test that proves the branch is taken, and that is a task each rather than a line each.

**`TransportUpdateSettingsAction`**, tiering, and `TruncateTranslogAction` were not walked.

### What this pass establishes

That the residency problem is bounded and locatable rather than diffuse: two thirds of the call sites cannot
see a gated index at all, and the ones that can are a list of about a dozen. It does not establish that the
dozen are safe. Four are now known unsafe and three of those are still unsafe.

### G9b: triaging the server internalClusterTest failures

Two full runs of `:server:internalClusterTest`, 5,931 tests each. The first reported 37 failures. The
question was whether any belonged to this work.

**The failing sets are almost disjoint.** Run one: `RemoteCloseIndexIT`, `SnapshotStatusApisIT`,
`SearchFieldsIT`, `QueryStringIT`, `RestoreShallowSnapshotV2IT` and others. Run two, over the same code:
`RemoteMigrationIndexMetadataUpdateIT`, `RemoteStoreRepositoryRegistrationIT`, `LegacyGeoShapeIntegrationIT`,
`CreateRemoteIndexClusterDefaultDocRepIT`. One class, `AutoExpandSearchReplicasIT`, appears in both.

That is the attribution. A regression fails the same thing every time. A set that changes between runs of
identical code is the machine, and several of the failures say so directly: "Test abandoned because suite
timeout was reached", a translog transfer timeout, a temporary-file cleanup assertion.

**Passing in isolation was deliberately not treated as sufficient.** That exact reasoning would have cleared
the D2 sweep regression, which also failed only under whole-suite load and also passed alone, and was real.
What separated that case was that its failures were *stable* across runs and the suite's wall clock doubled.
Neither holds here.

Two descriptor suites did appear and were checked directly rather than waved through, because they are in
this work's own area: `DescriptorResolutionScaleIT` asserts a point lookup does not scale with population,
which is the property the whole design rests on, and `DescriptorScalingCurveIT` guards against warmup
contaminating its own baseline. Both pass in isolation, in 2m55s.

**What is not established.** The second run was killed at about 60 percent, so its total is unknown, and no
baseline arm was run. The disjointness answers the attribution question without one; it does not tell anyone
how many of these suites are chronically flaky on this hardware, which is a separate and useful thing to
know.

---

## 10. H1: the uniqueness primitive, checked against an independent implementation

Section 4c said no number here had an object store behind it. G6 answered the latency half by simulation.
This answers the half simulation cannot reach: whether a real store enforces the preconditions the design's
uniqueness depends on.

### Why the existing coverage was not enough

`S3BlobStoreRepositoryTests.testCreateRegisterIfAbsentIsEnforcedServerSide` already proves server-side
enforcement, and its javadoc is right about why that matters: a unit test asserting `ifNoneMatch` is set
"proves the request-construction code is right and nothing else... if the header were ignored, every
concurrent creator would win and the unit test would still pass".

But it proves it against `fixture.s3.S3HttpHandler`, which this project extended to enforce those headers. A
fixture enforces what its author believed the semantics to be, so passing against it shows the code agrees
with our reading of S3, not with S3. A wrong reading makes both sides wrong together and every test green.

### What was run

MinIO in Docker, an independent implementation of the same API, exercised through the AWS SDK on the two
behaviours `createRegisterIfAbsent` and `compareAndSwapRegister` are built on.

| | result |
|---|---|
| `If-None-Match: *` against an existing key | refused, **412**, first writer's value survives |
| `If-Match: <stale etag>` | refused, **412**, winner's value survives |
| eight concurrent creators of one name | **exactly one winner, seven 412s** |

412 specifically matters: that status is what the register code translates into a lost race rather than an
error, so a store answering 409 or 200 would break the design quietly.

The concurrent case is the one the design actually cares about, since a hundred million tenants provisioning
at once is the workload and there is no lock anywhere in the path.

### Limits, stated

MinIO is closer to S3 than a fixture and is not S3. It runs on localhost, so this says nothing about
round-trip cost and does not supersede G6's simulated latency. It covers the register preconditions only,
not the rest of the descriptor path.

The test skips unless `-Dtests.s3.endpoint` is set, so an ordinary build is untouched, and it runs against
any S3-compatible endpoint including real S3 when someone has credentials.

---

## 11. H2: finishing the core audit, and a correction to section 3

> **Counts superseded by section 13.** Like section 7, this pass measured against the stale `main`, so its
> denominator of 196 includes upstream files this branch never touched; the true figure is 57. What it found
> by *reading* files stands, since those findings are about content rather than which ref produced them.

Section 3 said 198 modified core files "belong to the pluggable engine work". That was an assumption stated
as fact. Measuring it changes the shape of the problem.

### The actual composition

Of 196 non-seam modified core files:

| | count | +/- lines |
|---|---|---|
| mention engine, tiering, resharding or ingest concepts | **73** | the pluggable-engine work, as assumed |
| touch metadata-plane primitives (register, descriptor) | **11** | this design's own footprint, missed by the seam grep |
| remote cluster state / remote store | 2 | |
| **none of the above** | **110** | +1,986 / -484, and never characterised by anyone |

So "the 198 are engine work" was wrong twice over. Only 73 are. Eleven are the metadata plane's own, which
the seam-name grep undercounted because `FsBlobContainer` and `BlobContainer` carry the register primitive
without naming a seam class. And 110 files, about two thousand added lines, belong to neither and had never
been looked at.

Of those 110, 48 are trivial (ten or fewer added lines, three or fewer removed). The remaining 62 are where
an unexamined behaviour change would live.

### What reading the largest of them found

**`RestController` carries a serverless mode in core.** It gains
`rest.serverless_mode.enabled`, a `serverlessModeEnabled` field, a `ServerlessScope` on `RestHandler`, and
a check that 410s any handler not marked `AVAILABLE` when the setting is on. That is core holding a
serverless-specific policy decision directly rather than consulting a seam a plugin fills. It defaults off,
so R1 holds; R2 does not, and this is the clearest instance found so far because the concept is named in
core rather than injected into it.

**`IndexingMemoryController`** adds a node setting for native off-heap indexing buffers. Additive, and
engine work by nature even though it names no engine class, so the 73 undercounts.

**`BalancedShardsAllocator`** and **`Security`** look like refactors: extracted validators, and threading
`Settings` through policy reading. Neither is obviously behaviour-changing, and neither was written as a
seam.

### The honest state of R2 after this

Three distinct kinds of core change now have names, which is what H3 needs:

1. **Seam-shaped and inert by default.** The 54 plus the 11 register files. Defensible as-is.
2. **Serverless policy named directly in core.** `RestController`, `Plugin.nodeStats()`s removal,
   `PluginNodeStats`'s deletion. These contradict R2 and are individually small.
3. **Engine work, unaudited.** 73 files, plus some of the 110. Large, and not this design's to justify.

### Still not established

The 62 non-trivial files in the "neither" bucket were not read individually. The four largest were, and one
of the four was an R2 violation, which is not a reassuring hit rate for the other 58.

---

## 12. H3: separating the metadata plane from the engine work

What an upstreamable metadata plane would consist of, and what stands in the way. This is a plan with file
lists, not the split itself; the mechanical work is large and the decisions below should be settled first.

### The metadata plane's actual core footprint

**18 new files**, all seams or the primitives they need:

`AbsentIndexDescriptorSuppliers`, `AbsentIndexRoutingSuppliers`, `DescriptorOnlyCreation`,
`DescriptorPrefetch`, `DescriptorRepresentable`, `DescriptorUnavailableException`, `DurableTombstones`,
`GatedIndexRelease`, `IndexDescriptor`, `IndexDescriptorPublisher`, `MappingGenerationStore`,
`GatedMappingStatsAggregator`, `UnknownFieldRefresh`, `ComputedPlacementMembership`,
`ComputedPlacementMembershipService`, `ComputedShardRouting`, `BlobRegister`, `BlobRegisterCasResult`.

**49 modified files**, each consulting one of those seams. They cluster into four groups rather than being
scattered: broadcast and stats actions that iterate indices (17), the metadata services for create, delete,
mapping, settings and state (7), resolution and routing (6), and the rest one-offs.

That is the whole of it. Roughly 67 core files, every one either a registry that is null by default or a
call site that checks for null first, plus `BlobRegister` which is a new blob-store primitive with no
serverless coupling at all and would stand on its own merits.

### What blocks it, in order of difficulty

> Item 1 as originally written, "`Plugin.nodeStats()` and `PluginNodeStats` were deleted, the one outright
> regression against `main`", was false, and the attempt to act on it is described in section 3.1. Upstream
> removed that SPI deliberately. There is no such regression. The list below is what remains.

**1. `RestController` holds a serverless mode.** ~~Resolved, see section 14.~~ Moved to the plugin. Core no
longer has the setting, the field, the extra constructor or the enforcement branch.

**2. Five public members were removed from `ShardRouting` and `OperationRouting`**, all by CC1, all
verifiably dead on the baseline. Not a blocker so much as something to disclose and offer upstream on its
own merits. Section 13.

**3. CC1 also changed core behaviour**, rejecting primary writes on the parent of an in-progress in-place
split. It applies with no plugin installed, so it is a real R1 deviation, and it fixes a genuine upstream
data-loss window. It is separable and should be proposed to `main` as its own bug fix.

**4. The engine work is most of the 47 non-seam modified files plus part of the 37 new ones.** Not this
design's to justify, and the reason the raw diff looks several times larger than the metadata plane is.

**5. Ten of the new core files are neither.** They complete in-place shard split and add in-place merge,
a core feature upstream had started and left without an API. Additive, so R1 is untouched, and the most
straightforwardly upstreamable part of the branch. Section 15.

### The sequence that would work

1. ~~Settle whether `RestController`'s serverless mode can move onto the existing `getRestHandlerWrapper`
   extension point.~~ Done, section 14. It could.
2. Offer CC1's write-rejection fix to `main` separately, with its dead-scaffolding removal attached.
3. Read the remaining unexamined non-trivial files, against the fork point this time.
4. Only then attempt the branch split, because until 3 is done nobody knows which side each file belongs on.

### Why this is a plan and not a branch

Splitting 756 branch commits across two intertwined bodies of work is not a mechanical `git` operation: the
metadata plane was built on top of the engine work and uses its plugin, so a branch containing only the 67
core files plus `serverless-storage` would not compile without deciding what to do with `DataFormatAwareEngine`
and everything under it. That decision is architectural and belongs to whoever owns both, not to a script.

---

## 13. H4: the baseline was wrong, and what changed when it was fixed

Every measurement in sections 3, 7, 11 and 12 was taken against the local `main` ref. That ref is stale. It
sits at `fae98a3a5f3` (May 25 2026) while the branch forked from `upstream/main` at `b99229e3f36`
(Jul 6 2026), so **332 of the 1088 commits in `main..HEAD` belong to upstream**, and every `git diff main`
in this document counted six weeks of other people's work as this branch's.

### How it surfaced

Not by re-reading the numbers. Section 12 listed "restore `Plugin.nodeStats()`" as step one, the single
outright regression against `main`. Acting on it meant restoring the interface, and restoring it meant
asking who had deleted it. That turned out to be upstream, in PR #21820, deliberately, with a commit
message explaining that the SPI was `@ExperimentalApi`, had one consumer, had an unreleased wire format,
and was being replaced by a typed field plus a narrower `ArrowAllocatorPlugin` SPI in `libs/arrow-spi`.

The restoration had already been committed by then. It reintroduced an interface upstream had retired and
added a `Plugin.nodeStats()` with no caller anywhere in the repository: the *correct and unreachable*
pattern section 4a is about, produced while trying to fix a different instance of it. Reverted in full;
`Plugin.java` is now byte-identical to the fork point.

The general lesson is narrow and worth stating plainly: a diff is only as trustworthy as the ref on the
left of it, and nothing about `git diff main` announces that `main` is six weeks behind.

### What re-measurement changed

See the table in section 3.1. The two claims that did not survive:

**"Twenty-one files remove 34 public or protected members."** Actually two files remove five, and the other
29 counted "removals" were modified signatures, a `-` line and a `+` line for the same member, counted as a
deletion by a grep that only looked at the `-` side. The five real ones are
`ShardRouting.splitting/isSplitTarget/getParentShardId/getRecoveringChildShards` and
`OperationRouting.shardWithRecoveringChild`.

**"One core file deleted outright."** Zero. That file was `PluginNodeStats`, deleted by upstream.

### The five removals, checked rather than assumed

CC1 removed them along with `ShardRoutingState.SPLITTING`, justifying it as dead scaffolding upstream left
behind for a dual-write path the in-place-split feature never wired up. That justification is load-bearing,
so it was verified rather than taken on trust. On the fork point:

- the five accessors have **zero callers** outside the two files declaring them, in `server/src/main/java`,
  `plugins/`, `modules/` and `libs/`;
- `SPLITTING` appears **only inside `ShardRouting.java` and its own enum declaration**. Nothing ever puts a
  shard into that state.

The second point also disposes of the backwards-compatibility worry that removing a wire enum value would
normally raise. `ShardRoutingState.fromValue(5)` now throws, but no node ever produced byte 5, because no
code path ever constructed a `SPLITTING` routing. The removal is wire-safe.

What remains is a public API removal from a routing primitive. It is defensible, it should be disclosed, and
it belongs upstream as its own change rather than buried in a serverless branch.

### The behaviour change that comes with it

CC1's actual fix, `IndexShard.ensureNotInProgressSplitParent`, which rejects primary writes on a split
parent, is core behaviour that applies with no plugin installed. That is a genuine R1 deviation, the only one this
document has found that is not default-off. It is also a real bug fix: without it, documents acknowledged by
the parent after a child cloned its manifest but before the split committed became permanently unreachable.
Both things are true, and the resolution is the same either way, which is to offer it to `main` on its own.

---

## 14. H5: the REST gate moves out of core

Section 12 listed `RestController`'s serverless mode as the clearest remaining strain on R2 and asked whether
the existing `ActionPlugin.getRestHandlerWrapper` hook could carry it instead of a new seam. It can, and it
now does.

### What core had

A `rest.serverless_mode.enabled` setting, a `serverlessModeEnabled` field, a sixth constructor parameter, a
registration in `ClusterSettings`, a resolution in `ActionModule`, and a branch at the top of
`dispatchRequest` returning 410 for any handler not declaring itself `AVAILABLE`. All default-off, so R1 was
never at risk. The problem was R2: that is a decision living in core, not a hook, and it was serverless's
decision specifically.

There was also a tell. `RestController` had to give `/favicon.ico` an explicit `AVAILABLE` override, because
the handler it registers for the favicon is a bare lambda and `serverlessScope()` defaults to `UNAVAILABLE`.
So core's own new setting would have started returning 410 for the favicon on any node that opted in. A
mechanism that has to defend core against itself is sited wrong.

### What core has now

`RestHandler.serverlessScope()` and the `ServerlessScope` enum, and nothing that reads them. The declaration
is inert by construction rather than by a default value, which is a stronger guarantee than the old one:
before, R1 held because a setting defaulted to false; now it holds because there is no code path.

Everything else is deleted. The setting, the field, the six-argument constructor, the `ClusterSettings`
entry, the `ActionModule` resolution, the `dispatchRequest` branch, and the favicon workaround.

### What the plugin has

`ServerlessRestGate`, a `UnaryOperator<RestHandler>` handed to core through `getRestHandlerWrapper`, switched
by `serverless_storage.rest_gating.enabled`. Three details that are not incidental:

- **It returns `null`, not an identity wrapper, when gating is off.** Core allows exactly one plugin to
  install a REST wrapper and throws if a second tries. Returning a passthrough would consume that slot for
  nothing and stop any other plugin from wrapping handlers.
- **An `AVAILABLE` handler is returned unwrapped**, so it keeps its own identity for anything that inspects
  it rather than being hidden behind a delegate for no reason.
- **A refused handler still delegates everything that describes it**, via `RestHandler.Wrapper`. Core
  registers a wrapped handler under the wrapper's routes, so a gate that dropped `routes()` would unregister
  the path instead of refusing it, and the request would 404 rather than 410.

The favicon case did not need reproducing. `RestController` registers it through `registerHandlerNoWrap`,
which wrappers never see, so the bug the core version had to patch cannot occur in this one.

### Tests

Six in `ServerlessRestGateTests`, all behaviour-based rather than timing-based: an `AVAILABLE` handler is
returned unwrapped and still runs; `UNAVAILABLE` and `INTERNAL_ONLY` are refused with 410 **and never
execute**, asserted by recording whether the handler ran rather than by reading the status alone; an
undeclared handler is refused, which is the case that shows why core cannot own this check; a refused
handler still reports its routes, content-stream support and scope; and the wrapper is absent unless gating
is switched on.

Verified by mutation. Replacing `return new Refused(handler)` with `return handler` fails three of the six.

Core keeps one test, rewritten to assert the opposite of what it used to. `RestControllerTests`
`testCoreDispatchesAnUnavailableHandlerNormally` pins that an `UNAVAILABLE`-declaring handler is dispatched
completely normally, because with no plugin installed there is no serverless mode to be unavailable under.
The four tests that covered the old core enforcement are gone with the thing they covered.

### One incidental fix

`ServerlessStoragePlugin` needed node settings to read its own switch, so its no-arg constructor became
`ServerlessStoragePlugin(Settings)`. It had to *replace* the no-arg one rather than sit beside it:
`PluginsService.loadPlugin` refuses any plugin class with more than one public constructor, so keeping both
would have loaded fine in every unit test, which calls `new` directly, and failed every real node.

---

## 15. H8: the non-seam core files, read against the corrected baseline

Sections 7 and 11 audited these against the stale `main`, so their denominators counted upstream files
this branch never touched. Redone against the fork point, and this time every file that alters an
existing line was read rather than sampled.

### The population

47 non-seam modified core files, not 57. The difference is that the earlier count matched seam names as
plain text in the diff, and several seams were missing from that list. 29 are pure additions, adding
methods or fields without touching an existing line. 18 alter existing lines, and those 18 are where the
R1 question lives.

### The finding on R1

**All 18 leave the default path unchanged**, and each does so by one of five mechanisms rather than by
luck:

| mechanism | files | why it is inert |
|---|---|---|
| default-`false` hook on `EngineFactory` | `StoreRecovery`, `IndexShard` | `recoverMissingLocalStore`, `recoverInPlaceSplitLocalStore`, `recoverInPlaceMergeLocalStore`, `supportsEngineNativeSnapshots`, `ownsRemoteSegmentDurability` all default false, so the new branch falls through to the original code |
| default-method overload on a plugin interface | `IndexStorePlugin`, `EnginePlugin`, `IndicesService`, `IndexService` | the new signature delegates to the old one, so existing implementations are unaffected |
| null-guard for metadata without routing | `SnapshotsService`, `DiskThresholdDecider`, `TieringServiceValidator`, `IndexRoutingTable` | a traditional index always has a routing entry, so the guard cannot fire |
| behaviour-equivalent optimisation | `IndicesStore`, `IncrementalClusterStateWriter` | `RoutingNodes.localRoutingNode` selects the same shards for a data node as the full constructor |
| deliberate removal, verified dead | `ShardRouting`, `ShardRoutingState`, `SplitShardsMetadata` | CC1, section 13 |

Two are worth singling out because they are the two most likely to have been wrong.

`IndexShard` now routes snapshot recovery through `restoreFromEngineNativeSnapshot` instead of
`restoreFromRepository`. That reads like an extra remote probe on every restore. It is not:
`recoverFromEngineNativeSnapshot` checks `supportsEngineNativeSnapshots()` **locally first**, and when it
is false, which is the default, it delegates without issuing any blob request at all. The probe only
happens for an engine that has said it produces such snapshots.

`BufferedAsyncIOProcessor` grew a byte-threshold early drain. `getBufferByteThreshold()` defaults to
`-1` and `itemSizeInBytes()` to `0`, so the threshold can never be crossed and the new
`scheduleProcess(immediate)` is always called with `false`, which is the original code path.

### A flaw in the audit method itself

`IndicesStore` and `IncrementalClusterStateWriter` were classified as non-seam because neither diff
mentions a seam. Both call `RoutingNodes.localRoutingNode`, which is new on this branch and calls
`AbsentIndexRoutingSuppliers.localShards`. So both reach a seam one level down.

That does not change the R1 answer for either, since the seam returns nothing when unregistered. It does
mean **seam reach is transitive and a text match on the diff undercounts it**. Any future pass should
resolve callees rather than grep the patch.

### A third body of work, not two

The branch is not "metadata plane plus engine work". It also completes and extends a core feature that
has nothing to do with either.

Upstream shipped `MetadataInPlaceSplitShardService` and `InPlaceSplitShardClusterStateUpdateRequest` and
no API surface at all. This branch adds ten new core files: the action, transport action and REST
handler for in-place split, the whole of in-place merge (action, transport, request, two metadata
services, REST handler), plus registrations in `ActionModule`, a `ClusterManagerTask` entry, an
`IN_PLACE_MERGE_SHARD` recovery source, and new wire fields on `SplitShardsMetadata` correctly gated on
`Version.V_3_8_0`.

This is additive, so R1 holds: nothing existing behaves differently. But it is core feature work sitting
outside any plugin, which is neither a seam nor engine work, and it is a third thing to decide about
when splitting the branch. It is also the most straightforwardly upstreamable part of the whole branch,
since it finishes something upstream started.

### Where R1 and R2 actually stand

R1 has exactly one deviation, and it is the one section 13 already named: CC1's write rejection on an
in-progress split parent. Everything else in core is inert without a plugin, by construction rather than
by a setting default.

R2 is a judgement rather than a measurement. The metadata plane's 49 files follow the seam shape
faithfully. The engine work reaches core mostly through default methods on `EnginePlugin` and
`IndexStorePlugin`, which is the right shape, plus visibility widening on `InternalEngine` so an
alternative engine can subclass rather than reimplement. Widening `private` to `protected` is a
maintenance commitment rather than a behaviour change, and it is worth being explicit that this is the
mechanism, because "no behaviour changed" and "core's API surface did not grow" are different claims and
only the first is true.

---

## 16. H9: CC1 extracted as a standalone patch

Sections 12 and 13 concluded that CC1 should be offered to `main` on its own, for two reasons that point
the same way: it fixes a real upstream data-loss window, and it is the only R1 deviation in the branch
that is not default-off. Leaving it buried in a serverless branch means the bug stays unfixed upstream
for as long as the branch takes.

The patch is `benchmarks/cc1-in-place-split-write-rejection.patch`.

### What it took to separate

CC1 as committed does not apply to the fork point. It touches
`MetadataInPlaceMergeShardServiceTests`, which is part of the in-place merge feature this branch adds
and upstream does not have, and one hunk of `SplitShardsMetadataTests` covering a merge case that
likewise does not exist there.

Both are additions this branch made, not part of the fix, so the extracted patch drops them. One hunk in
`SplitShardsMetadata` then needed applying by hand, because the surrounding code has moved on this branch
since. Nothing else required judgement: the guard, its test, and the dead-scaffolding removal all apply
unchanged.

### Verified, not assumed

Against a worktree checked out at the fork point and nothing else:

- applies cleanly to a pristine checkout;
- `:server:compileJava`, `:server:compileTestJava` and `:test:framework:compileJava` all pass, which
  matters because the patch removes public members and changes a method signature, so a missed caller
  would show up here;
- `IndexShardTests`, `SplitShardsMetadataTests`, `OperationRoutingTests` and `ShardRoutingTests` pass;
- removing the guard fails `testRejectsPrimaryWriteWhileInPlaceSplitInProgress`, so the test is
  load-bearing there and not only on this branch.

### What it does not resolve

Applying it upstream would still leave this branch carrying the same change, so the eventual merge has to
account for it. And it removes public members from `ShardRouting`, which is a compatibility decision for
whoever reviews it rather than something this branch can settle. The dead-code evidence is in section 13
and reproducible with a single `git grep` against the fork point.

---

## 17. S1: population-scale evidence, restored in count form

Section 13's cleanup deleted every test that built a large population, on the grounds that they asserted on
elapsed time and were the branch's largest source of flaky failures. That was right about the assertions and
wrong about the coverage. What went with them was the only evidence above a few dozen indices, leaving R6,
the claim the whole design exists to serve, resting on 25 creations.

This restores it in the form those tests should have taken.

### The two new tests

**`DescriptorResolutionIsFlatAtScaleTests`** builds two populations differing twentyfold and asserts that a
cold point read costs one round trip at both, a miss costs two at both, and a creation costs the same at
both. Reads go through a second backend over the same container so the cache is cold, which is the case that
matters: a node joining a cluster that already holds a large population must not find its first read of a
tenant more expensive because other tenants exist.

**`GatedCreationClusterStateFootprintIT`** serialises the whole cluster state before and after 25 creations
of each kind. Measured:

```
  ordinary      20,625 bytes        825 per index
  gated              0 bytes
```

This is the first time the central claim has been measured in bytes rather than in publications. It matters
because the two are different claims: a design could publish nothing per creation and still accumulate
state, if entries were added to a map that rode along in some later unrelated publication. That would cost
zero versions and still put 100M entries in every node's heap and every full state transfer. At 825 bytes
per index, 100M traditional indices is roughly 82 GB of cluster state. Gated is zero.

### One design mistake worth recording

The flatness test originally compared total request counts at the two populations, which is weaker than it
looks against a filesystem fixture. `FsBlobContainer` answers any listing in a single call however many
blobs it returns, so a stray enumeration on the read path costs exactly one extra request at *both*
populations, the totals still match, and the assertion passes while the thing it exists to catch has
happened. Confirmed by mutation: injecting a `listBlobsByPrefix` into the read path left both totals equal
at 2.

An attempt to fix it by charging per thousand-key page, the way S3 bills, ran into its own problem worth
noting: `FilterBlobContainer` does not forward the register operations, because `BlobContainer` declares
them as defaults that throw, so a filter silently inherits the throwing default rather than the delegate's
implementation. Anything wrapping a container for the descriptor path has to forward them explicitly.

The simpler expression turned out to be the better one. A listing is the only operation whose cost grows
with the population, so **"the read path issues zero listings" is the honest statement of flatness**, and
the counter already tracks listings separately. That assertion does catch the injected regression, and it
is the one that would still hold against a real object store rather than against this fixture.

### What this does not cover

The populations are thousands, not millions. `-Dtests.descriptor.population` raises them and the assertions
need no retuning, since they are equalities and zeroes rather than thresholds, but nothing runs at 100M and
nothing here says the object store behind it would keep up. That is the request-budget question, still open.
