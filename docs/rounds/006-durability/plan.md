# Round 006 — Durability for the gated fleet

Written 2026-08-15. The question that started it: *will snapshot work?* It does not, and answering why
turned up a set of gaps that are smaller and better-shaped than they look, because most of the machinery
already exists and is pointed at the wrong authority.

## Where this starts

Measured, not assumed (each of these has a test on the branch):

- **Snapshot by name of a gated index** is refused with an honest error. Deliberate, already tested.
- **Snapshot of everything succeeds and silently omits the gated fleet.** All four spellings — no indices
  set, `*`, `_all`, a matching prefix wildcard — report `SUCCESS` with only ordinary indices captured.
- **A shallow snapshot already exists**: `_snapshot_pin` writes a durable pin naming a manifest generation
  and copies nothing; `_snapshot_restore` compare-and-swaps the shard head back to a pinned generation;
  `_snapshot_release` drops the pin. GC honours durable pins.
- **It cannot name a gated index.** The three index-level actions resolve through
  `clusterService.state().metadata().index(name)`, which is null for a gated index, so a pin fails
  `IndexNotFoundException` for an index that is serving traffic and has manifests to pin. The shard-level
  actions underneath take a uuid and a shard id and never consult cluster state.
- **Core `_snapshot` against a serverless index is pointer-based unconditionally**, and already writes a
  durable pin underneath (`EngineNativeSnapshotPayload` carries a `pinId`). The two shallow front doors are
  one mechanism.
- **Nothing resolves a timestamp.** `CommitManifest.createdAtMillis` is read only by retention policies
  deciding what to keep. `serverless_storage.pitr_window` defaults to `-1`, disabled.

So: two front doors onto one shallow mechanism, zero deep ones, and no way to ask for a point in time.

## What decides the shape

**Three tiers, insuring against three different failures.** Conflating them is how a backup story becomes a
false one.

| tier | protects against | cost | belongs at |
|---|---|---|---|
| pin (shallow) | your own GC; logical error, bad write, tenant rollback | one blob write per shard | per index, always on |
| pointer snapshot | same as pin, plus cataloguing and standard tooling | a few hundred bytes | per index, on request |
| independent copy | loss of the source bucket, account, or region | O(bytes) | **per index only on request; per fleet at the store** |

The third row is the one that must not be built the obvious way. A per-index deep snapshot across a hundred
million indices is O(N) cluster work and an unbounded copy, for a fleet whose entire premise is that
per-index work was removed. Fleet-wide physical DR belongs to the object store — bucket versioning, a
deny-delete policy, cross-region replication — where the provider does it asynchronously at storage cost
with no per-index work in the cluster. Per-index deep copy earns its keep only for tenant export, legal
hold, and a "protect these specific indices properly" policy.

**The pin is protection against our own GC and nothing else.** Stated here because it is the sentence that
is easiest to forget: a pin stops `ManifestRetentionPolicy` from reclaiming a generation. It does not stop a
lifecycle rule, an operator, or a compromised credential. Versioning and deny-delete are what stop those,
and they are out of band by design — the two enforcements should not share a failure domain.

## The work

Ordered so each item is usable on its own and each unblocks the next.

### 1. The pin surface reaches gated indices — DONE, and it found something bigger

`TransportIndexSnapshotPinAction`, `...RestoreAction` and `...ReleaseAction` resolve the index name through
`AbsentIndexDescriptorSuppliers.metadataOrDescriptor(metadata, name)` instead of `metadata.index(name)`.
That call already synthesises an `IndexMetadata` carrying the uuid and shard count for a gated index, and
these actions run on transport/GENERIC threads, so a descriptor read is safe here — `blockingIsUnsafeHere`
forbids it only on cluster state threads.

*Verification*: `GatedShallowSnapshotIT` already pins the failure; it becomes the success. Extend it to the
full round trip — pin, write more, restore, assert the later write is gone — because a pin that can be
taken and not restored from is not a snapshot.

*Risk assessed as none. That was right about the resolution and wrong about the round trip.*

**What landed**: all three actions resolve through `metadataOrDescriptor`. Pin and release now work on a
gated index, asserted.

**What the round trip found — a defect two layers down.** Restore-in-place refuses while a writer lease is
held. The ordinary-index restore test releases that lease by closing the index. Closing a *gated* index did
not release it, and pulling that thread found that closing a gated index did not do anything at all:

- `IndexDescriptor.toIndexMetadata()` never set the state, so every reader that synthesised metadata saw
  `OPEN` whatever the descriptor said.
- `IndexNameExpressionResolver`'s gated branch returns a concrete index and `continue`s **before**
  `shouldTrackConcreteIndex`, the one place that refuses a closed index.

So `MetadataIndexStateService.closeGatedIndices` wrote `descriptor.withState(CLOSE)`, answered
acknowledged, and the index went on accepting writes and answering searches. Measured against an ordinary
index, which refuses both with `IndexClosedException`. The existing test for gated close asserted the
descriptor's own state — the label it had just written — which is exactly why a label was all it was.

**Both fixed**, and a closed gated index now refuses precisely what a closed ordinary one refuses, compared
against it rather than against a written-down expectation.

**Still open, and it is what blocks the restore round trip**: the *shard* is not released on close, so the
writer lease survives. The only channel that tells other nodes about a gated index's change is the
descriptor change log, and `DescriptorChange` carries `(name, uuid, kind, atMillis)` with no state — so the
tailer cannot distinguish a close from a mapping update and releases shards only for deletions. Fixing it
means either a persisted change-log format carrying state, or a descriptor read per update change. That is
a decision about a format that lives on the object store, and it is the next thing to do in this round.
The round-trip test is `AwaitsFix` against it rather than weakened to assert the refusal, which would turn
a defect into a specification.

### 2. Restore to a time

Add `restoreToMillis` to the shard-level restore request and `restore_to` to the REST surface. When set,
the target generation is resolved by time rather than by pin id: **the newest manifest whose
`createdAtMillis` is at or before the requested instant**. `BlobContainerManifestStore.listManifests()`
supplies the input; the resolution itself is a pure function and gets a unit test of its own, in the shape
`PitrRetentionPolicy` already established.

Two refusals matter more than the happy path, and both get tests:

- **Nothing at or before the instant** — the requested time predates the shard's oldest surviving manifest.
  Refuse naming the oldest available instant, rather than silently restoring to the oldest.
- **The resolved generation is no longer pinned** — it fell out of the PITR window between resolution and
  restore, or PITR was never enabled. Refuse naming the generation, rather than restoring to something that
  GC may be about to reclaim.

*Granularity, stated so nobody expects otherwise*: this lands on the last commit at or before the instant,
not on the instant. Sub-commit precision needs WAL replay to a timestamp, and `WalRecord` carries
`(indexUuid, shardId, primaryTerm, seqNo)` and no time. That is a separate piece of work with a separate
decision behind it (add a timestamp per record, or keep a periodic time-to-seqNo index) and is out of scope
here.

### 3. The silent omission becomes audible

A wildcard snapshot on a cluster with the gate installed logs a warning naming the omission. Deliberately
**not** a count: counting means a descriptor prefix search on the snapshot path, which is O(fleet) work to
produce a number nobody can act on differently. A fixed, honest warning is what the operator needs, and it
costs a boolean check.

Also **not** a new field on `SnapshotInfo`: the engine-native design explicitly kept that surface unchanged,
and this is not the change that should break it.

### 4. The durability posture, written down

A design page saying, in one place: what each of the three tiers protects against; that neither snapshot
surface is an independent copy; that fleet-wide physical DR is the object store's job and requires
versioning, deny-delete and replication to be configured; and that the pin protects only against our own
GC. This is the piece that prevents the next person from reading "snapshot" and believing they have a
backup — which is exactly what the current documentation would let them believe.

### 5. An independent copy, with OpenSearch as the compute

The one that changes what is possible rather than fixing what is misrouted. The design doc previously
declined this on the grounds that `BlobContainer` has no `copyBlob` and no repository plugin uses
server-side copy — a valid objection to the *optimization*, not to the operation. If the node moves the
bytes, server-side copy was never on the table and the repack-versus-bulk-copy tension it created
disappears.

**The shape**, and why it is much smaller than the previous estimate:

1. Pin the generation being copied, so GC cannot reclaim it mid-copy.
2. Build a `LazyBundleDirectory` over that manifest. It already exists, and it needs only a manifest, a
   cache directory and a transfer manager — not a live shard, not the writer.
3. Wrap it in a `Store`, open its `IndexCommit`.
4. Hand both to core's existing `Repository#snapshotShard`, which takes the `Store` and the `IndexCommit`
   **as parameters** rather than reading them off a shard.
5. Release the pin, on both the success and failure paths.

What this buys that the pointer snapshot does not: a genuine independent copy, in the standard repository
format, restorable by a cluster that has never heard of this plugin, incremental for free (the repository
dedupes by file name, length and checksum against earlier snapshots), with no new `BlobContainer` SPI, no
repack format, no new restore path, and no temporary disk. Lucene-file granularity is what the repository
format already wants, so the copy is correctly sized without repacking anything.

It also runs without the writer: the lazy directory needs only the object store and a manifest, so this can
execute on a node holding no shard of the index — off the serving path entirely, which classic snapshot
cannot do.

**The three things that are real work rather than wiring**, and the order to find out about them:

- *Does a `Store` over a lazy directory yield a commit whose files can be read end to end?* This is the
  load-bearing assumption and it is cheap to test on its own. **Do this first, as a spike, before building
  anything on top of it.** If it fails, the rest of item 5 does not exist in this form.
- *Orchestration.* `snapshotShard` is normally driven by `SnapshotsService`, which also creates the
  repository-side bookkeeping and calls `finalizeSnapshot`. Driving it from a plugin action means owning
  that sequence. This is the part most likely to be larger than it looks.
- *The opt-in.* `attemptEngineNativeSnapshot` returns a pointer unconditionally today; deep versus shallow
  has to become a choice, mirroring `remote_store_index_shallow_copy`'s shape.

## Sequencing, and what "done" means for each

| # | item | done when | size |
|---|---|---|---|
| 1 | pin surface reaches gated indices | pin and release: **done**. Restore: blocked on close releasing the shard | small → medium |
| 1b | **close releases a gated shard** (new, found by 1) | closing a gated index releases its writer lease | medium — persisted format decision |
| 2 | restore to a time | resolution unit-tested, both refusals tested, round trip in an IT | small |
| 3 | audible omission | a wildcard snapshot on a gated cluster warns; asserted | small |
| 4 | posture written down | one design page, linked from the snapshot page | small |
| 5 | independent copy | spike proves the commit is readable; then orchestration | spike small, rest medium |

Items 1–4 are each independently shippable and none depends on 5. Item 5's spike is the gate on whether the
rest of it is a days-long piece or a different design entirely, so the spike is the deliverable that
matters, not the feature.

## What this round deliberately does not do

- **Sub-commit precision.** Needs a decision about WAL record timestamps first.
- **Fleet-wide deep snapshot.** The wrong layer, per the table above.
- **A new `Repository` implementation.** Investigated and deferred twice already; nothing here changes
  that reasoning.
- **Cross-region replication itself.** That is object-store configuration, not code. What this round owes
  it is documentation saying it is required and why.
