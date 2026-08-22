# C2: how the cluster metadata manifest gets sharded

Design output for Phase C of `rfc-scalable-index-metadata-tasks.md`. C3 through C6 implement what this
decides.

> **Status (2026-08-22): shipped, default-off.** This is no longer a forward-looking design — the write
> path, read path, codec and cleanup described below are implemented (`IndexMetadataManifestSharder`,
> `ManifestShardFunction`, `UploadedManifestShard`, `RemoteManifestShard`, and the sharded branches of
> `RemoteManifestManager` / `RemoteClusterStateService` / `RemoteClusterStateCleanupManager`), with
> `ManifestShardingIT` covering live-data safety. Two things below differ from what shipped and are
> corrected in place: the **shard-count default is 0 (sharding off)**, not 256 — 256 remains the
> recommended value once an operator turns it on; and the measured win is **4.9x at N=1,000**, the only
> scale actually re-measured on the shipped code. The 125x figure is a projection from the original
> spike, not a validated result. Treat the rest of this document as the design record it is.

## Why this exists, restated from the measurements

The manifest is the one file rewritten in full on **every** cluster state version, and MS.1 confirmed
it genuinely lists all N indices every time rather than only the changed ones. `ManifestDescriptorSizeEstimate`
puts that at about 0.5 MB compressed at 100k indices, going to 0.8 MB once C6's descriptor is on.

Both numbers scale linearly with index count and are paid per version. At a cluster state version rate
of a few per second, 100k indices is a few MB/s of pure rewrite for a cluster where almost nothing
changed. That is what sharding removes, and it is why C5 and C6 are not usable at that scale without
it.

**The win is bounded by how many indices change per version, not by index count.** With S shards and
one index changed, the write goes from the whole manifest to one shard plus the small top-level file.
For a bulk operation touching every index there is no win at all, because every shard changes. The
expected case is a handful of indices per version, so the expected win is close to the full factor S.

## Partition function

`murmurhash3(index UUID) mod shardCount`.

**UUID rather than index name.** An index cannot be renamed, so the two are equally stable in
practice, but the UUID is fixed-width and uniformly distributed where names are neither. Tenant index
names in this system share long prefixes by construction, and a hash over a shared prefix is fine for
murmur but the UUID removes the question entirely. It is also what `IndexId` already keys on
elsewhere.

**No sub-sharding for hot shards.** Distribution is uniform by construction; there is no equivalent of
a hot key here because every index contributes about the same bytes.

## Shard count

**Fixed, declared in the top-level manifest, never derived from index count.**

This is the one design decision with a scar behind it. During C6 I made the codec version depend on
whether a descriptor was present, and reverted it: a cluster would flap between codec versions as
indices came and went. Deriving shard count from index count is the same mistake with a worse failure
mode -- crossing the threshold rewrites every shard, so a cluster hovering near a boundary would
rewrite the entire manifest repeatedly, which is precisely the cost sharding exists to remove.

So:

- `cluster.remote_store.state.manifest.shard_count`, node-scoped, **shipped default 0 (sharding off)**;
  **256 is the recommended value when an operator enables it**. (This design originally specified 256 as
  the default and 64 before that; C3a measured 64 saturating at 100 changed indices and turning into a
  9.5% *loss*. See `benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md`. Shipping off-by-default was chosen
  because enabling sharding moves the repository to a codec stock OpenSearch cannot read — see
  "Codec (C5)" below — so it must be an explicit operator decision, not a silent upgrade.)
- The count in force is written into the top-level manifest. Readers use the value they find there,
  not the value they are configured with.
- Changing it is an explicit operator action that costs one full rewrite, and takes effect on the next
  published version.

256 at 100k indices is about 390 entries per shard. C3a measured that at 125x less written for a
single changed index and 22x at ten, against 55x and 5.8x for 64.

**The curve is not monotonic in shard count**, which is the part worth knowing. Raising the count
further trades the common case away, because the top-level manifest listing S references becomes the
floor: at 4,096 shards a single changed index still costs 55 KB. 256 sits near the optimum for a steady
state of single-digit changed indices per version.

## Blob layout and naming

```
manifest/<term>__<version>__<...>__manifest.dat        top-level, as today
manifest/shards/<term>__<version>__<shardId>__<uuid>   one per shard, new
```

The top-level manifest keeps every field it has today except the index list, which is replaced by a
list of shard references: shard id, blob name, and the count of entries it holds. Entry count is not
needed to read but makes a partially written manifest detectable, and makes the "did we lose a shard"
question answerable without fetching every shard.

Shard blobs are immutable and content-addressed by the term/version that produced them. An unchanged
shard is referenced by its **existing** name from the previous version rather than rewritten -- that
reference-carrying is where the entire saving comes from, and it is the same shape `Metadata`'s holder
map already uses in memory for unchanged indices.

## Write path (C3)

1. Compute each changed index's shard from its UUID.
2. Rewrite only those shards, at the new term/version.
3. Carry every other shard reference forward verbatim.
4. Write the top-level manifest referencing the mix of new and carried-forward shard blobs.

The write amplification reduction is the deliverable and must be measured as part of C3 rather than
assumed, per the plan. The measurement is bytes written per cluster state version against changed-index
count, sharded versus not, at 10k / 100k / 1M indices with distinct per-index data. The first
descriptor measurement flattered itself 4x by reusing one descriptor across 100k entries; do not repeat
that.

## Read path (C4)

A full-state read fetches the top-level manifest, then all shards, in parallel. Same total bytes as
today, more round trips, and the C6 descriptor means those bytes are the whole story rather than a
prelude to N index-blob fetches.

**The open question C4 must answer is whether anything wants a single index without reading all of
them.** C6 made a single-index fetch reachable; if that path is used, sharding lets it fetch one shard
rather than everything, which is a second and separate win. If nothing uses it, the read path is
strictly a small regression in round trips and the whole justification rests on the write path.

## Codec (C5)

`CODEC_V6`, its own parser, an entry in `VERSION_TO_CODEC_MAPPING`, a
`CLUSTER_METADATA_MANIFEST_FORMAT_V5` for reading what is already in repositories, and
`MANIFEST_CURRENT_CODEC_VERSION` moved **unconditionally**. Do not make the codec depend on whether
the manifest happens to be sharded; that was tried during C6 and reverted for the flapping reason
above.

## Cleanup, which is the dangerous part

C1 landed the half that could land first: a node refuses to sweep at all when it sees a manifest whose
codec it does not understand. That converts the rolling-upgrade failure from a repository-wide delete
into a paused sweep.

**C3 still has to add transitive resolution**, and it is not optional. `deleteClusterMetadata` computes
`filesToKeep` as a union over retained manifests. Once index names live behind shard blobs it must add
the shard blob names *and* fetch each shard to add the index blob names it references. Getting this
wrong deletes index metadata that is still referenced.

Two properties make that tractable:

- Shard blobs are immutable, so a shard reference that appears in any retained manifest keeps both the
  shard and everything it names.
- The entry count in the top-level reference lets the sweep assert it read what it expected, rather
  than silently treating a truncated shard as "references nothing" -- the same failure class C1 guards
  against at the codec level.

## Go / no-go

**Go, with one condition.** The cost being removed is real, measured, and grows linearly with a number
this project exists to make large. The design has no novel mechanism in it: hash partitioning, an
immutable blob per partition, and carry-forward by reference are all shapes already present in this
codebase.

The condition is that **C3 measures the write amplification reduction before C4 and C5 are written**.
If the realistic changed-indices-per-version count turns out to be high enough that most shards change
anyway, the saving collapses and the remaining work is not worth its cleanup risk. That measurement is
cheap and it is the last place this can be stopped cheaply.
