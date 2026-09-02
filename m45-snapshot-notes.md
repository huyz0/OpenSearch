# M45 (finished) — snapshot and restore, on this deployment's own object store

R7 recorded snapshot/restore as out of scope, enforced in code by `ServerlessPlugins#createComponents`
handing every plugin a `RepositoriesService` supplier that yields `null`. That stays true — this milestone
does not implement classic OpenSearch's repository-plugin API surface, and a plugin that registers a
repository *type* still finds nothing there. What closes is the gap a caller actually feels: this shell had
no way to take a durable, point-in-time copy of an index and get it back later. It does now, as a
shell-native REST surface with no dependency on `RepositoriesService`/`SnapshotsService` at all — the same
relationship `_pit` has to core's own PIT machinery.

## The scope decision: a repository is a namespace, not a storage backend

Classic OpenSearch's repository abstraction exists to let a snapshot land on storage distinct from the
cluster's own data — a different bucket, provider, or account. This shell has exactly one configured
object store (D3: one register implementation, not a pluggable set of backends), and a snapshot's data
already lives durably in it, under the same R11 conformance question every other write already answers to.
Registering a repository (`PUT /_snapshot/{repo}`) records a name and nothing else — no `type`, no
`settings`, no bucket — because there is nothing this deployment's own `serverless.store.*` configuration
does not already fix. A caller wanting genuine cross-store durability wants object-store replication, which
is an operations question this deployment's provider already answers, not a second repository type here.

## Taking a snapshot costs no data movement

`PUT /_snapshot/{repo}/{name}` (body: `{"indices": "a,b,c"}`, required and explicit — this surface does not
enumerate a deployment's population, the same rule multi-index search and `/_serverless/indices`'s refusal
both already follow) reads each named index's *currently published* manifest per shard and writes one
record referencing the blobs those manifests already name. Every file was already durable before the
record existed; this is one metadata write regardless of index size, the same shallow shape a
`PointInTime` already has. A shard that has published nothing refuses with 409 `nothing_published` — the
identical refusal `_pit` gives, for the identical reason: a partial capture would silently cover only part
of an index, which the whole feature exists to make impossible rather than explain after the fact.

## Restoring is not free, and the reason is a real invariant

This shell's shard storage is keyed by an index's own uuid (`RegisterMap#shardData`), and a manifest's blob
paths are relative to that uuid's own container (`SegmentPublisher#restoreInto` resolves every file against
its own `shardBase`, never an absolute or foreign path). A restore creates a genuinely new index — a fresh
uuid, always, because reusing one is the exact bug M32 closed for index deletion, and doing it here would
reopen it for restore instead. So the blobs a snapshot named have to be copied into the new index's own
storage before its manifest can name them.

**The snapshot stays shallow; restoring is the one operation that pays for it.** `POST
/_snapshot/{repo}/{name}/_restore` copies each captured shard's referenced blobs (read-then-write, scoped
exactly to the bytes a shard's manifest names — never a listing over anything else) into the new index's
storage, under one fresh term container (term 1 — see the canary below for why not 0), and writes the
manifest register directly rather than through `SegmentPublisher#publish`, which needs a live Lucene
`Store` this operation never opens. Every captured index restores independently — one bad target name
(already taken) reports an error for that index and does not cost the others, the same per-item-outcome
shape `_bulk` and `_mget` already have.

### The term-0 defect this found

The first version of `copyShardBlobs` wrote the restored manifest at term `0`. Every restore failed:
`shard-head term must be positive, got 0 (see s0-findings.md F4)`. F4 recorded this once already, for the
write path — "a primary refuses to activate at term 0" is load-bearing in the data plane, not an
optimisation. It turned out to bind the *read* path too: opening a restored shard as a reader goes through
the same engine machinery, and refused the same way. Fixed by starting a restored shard's term at `1`
instead of `0` — an arbitrary choice past that point, since nothing downstream reads meaning into the
specific number, only that it is positive.

## Pinning is by uuid, tighter than a point in time's own check

`GarbageCollector.sweepShard` now also treats a live snapshot's referenced blobs as protected, the same
rule §M37 built for a point in time — read once per sweep of a shard, before the listing, for the same
reason: a snapshot taken mid-sweep must be seen whole or not at all. But `PointInTime#referencedBlobs`
matches by **index name**, and `SnapshotRecord#referencedBlobs` matches by **uuid**. That difference is
deliberate and tighter, not merely different: a name can be reused by an unrelated later index — M32 is the
entire reason storage is keyed by uuid rather than name — and a snapshot durable enough to outlive that
reuse must never have a later, unrelated index's shard mistaken for the one it actually captured. The uuid
is resolved via one extra descriptor read, and only when at least one snapshot exists anywhere in the
deployment (`plane.liveSnapshots()` checked first) — a deployment that has never taken one pays nothing
extra for this at all.

**An unreadable snapshot record does not pin, unlike an unreadable point in time.** `PointInTime`'s
conservative fallback — "a record we cannot read must be assumed to be holding something" — exists because
a paging caller loses only latency if the sweep is briefly too cautious, and a view names exactly one
shard, so "assume it holds this one" is a bounded, affordable guess. A snapshot can name an unbounded
number of indices and shards; the equivalent guess would be "assume it holds everything in the deployment,"
which turns one corrupted record into garbage collection switched off everywhere, silently, forever. This
milestone does not solve that — `MetadataPlane#liveSnapshots` skips an unreadable record and logs a
warning, which is a real, disclosed gap rather than a silently absent guarantee. See "What this does NOT
establish" below.

## Canaries

- **133 — snapshot pinning removed from the sweep.** `plane.liveSnapshots()` replaced with an empty list
  inside `sweepShard`. Caught by `testASweepDoesNotCollectWhatASnapshotIsHolding` — and caught loudly
  rather than silently: the collected blobs were gone, so the *restore* itself failed with "snapshot
  references a blob that no longer exists" rather than quietly returning corrupt data. A defense found by
  accident is still a defense.
- **134 — the uuid check in `SnapshotRecord#referencedBlobs` disabled**, matching any captured index
  regardless of uuid. Caught by `testASnapshotDoesNotPinAnUnrelatedIndexThatReusedTheName`: a snapshot of a
  deleted index's shard 0 claimed to reference the unrelated second index's own live blobs.
- **135 — the repository-in-use refusal removed** from `MetadataPlane#deleteRepository`. Caught by
  `testARepositoryInUseRefusesDeletion`: a repository still holding a snapshot deleted cleanly instead of
  refusing with 409.

Three planted, three caught, one (133) caught by a louder failure than the one it was aimed at.

## What this does NOT establish

- **An unreadable snapshot record does not pin its blobs**, unlike a point in time's conservative fallback
  — disclosed above, not fixed. The gap is real: a corrupted or half-written snapshot record could have its
  files collected out from under it before anyone reads it again.
- **No incremental snapshots.** Every snapshot is a full, independent capture of each named index's current
  published commits — nothing here deduplicates unchanged segments *across* snapshots the way classic
  OpenSearch's repository format does (each snapshot's manifest independently references whatever blobs
  were current when it was taken; two snapshots of an unchanged index reference the same blobs by
  coincidence of immutability, not by any bookkeeping that tracks it).
- **No restore into an existing index.** Every restore creates a new index; there is no equivalent of
  restoring a snapshot's data back onto an index that is still there.
- **No repository listing.** `GET /_snapshot` with no name, and `GET /_snapshot/_all`, are both refused
  with 501 — the same inventory-operation refusal `/_serverless/indices` has — rather than silently
  returning nothing or 404ing like a typo. `GET /_snapshot/{repo}` describes one repository by name;
  `GET /_snapshot/{repo}/_all` lists that repository's own snapshots, which is bounded by what one
  repository holds rather than by the deployment's population.
- **No progress polling.** Every operation here is synchronous within its request, matching every other
  handler in this shell — there is no `_status` endpoint to ask "how far along is this," because nothing
  here runs long enough in the background to need one. A restore's blob copy happens inline, on the
  dispatched thread, before the response is sent.
- **Nothing about S3 or GCS specifically** (D5/R11) beyond what every other write on this surface already
  answers to — this milestone adds no new object-store operation shape, only new callers of ones already
  exercised (`readBlob`, `writeBlob`, `listBlobs`, `compareAndSwapRegister`).

M45 is done.
