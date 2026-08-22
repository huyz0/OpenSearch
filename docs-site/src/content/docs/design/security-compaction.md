---
title: Security & Compaction
description: At-rest encryption, credential scoping via restricted blob containers, and background segment compaction.
---

## Encryption

Every byte this plugin writes to the object store needs to be unreadable without the right key, but every existing store (bundle, manifest, shard-state) was written against a plain `BlobContainer` interface with no idea encryption exists underneath it. `EncryptionKeyProvider` is the abstraction (`currentKey()`, and `currentKey(indexUuid)` for per-index differentiation); `StaticEncryptionKeyProvider` (one key for everything) and `PerIndexEncryptionKeyProvider` (keyed by index) are the two implementations. The single integration seam is `ServerlessStoragePlugin.resolveBlobContainer`, which wraps the raw per-shard `BlobContainer` in `EncryptingBlobContainer` whenever a key provider is configured — every downstream store (bundle store, manifest store, shard-state store, WAL chunk writer) depends only on the plain `BlobContainer` interface and is unaware encryption is happening underneath it.

### Wire format: independently-authenticated blocks, not one big envelope

`BlockLayout` defines a fixed 20-byte header (`magic 'EBC1'`, format version, block size, total plaintext length), followed by independently-encrypted, independently-authenticated fixed-size blocks (64 KiB default). Each block is its own `AesGcmCipher` envelope — a 12-byte random IV followed by `AES/GCM/NoPadding` ciphertext with a 16-byte tag.

The reason for chunking into independent blocks rather than one whole-blob AEAD envelope: **ranged reads**. Fetching one file's bytes out of a multi-file bundle only needs to decrypt the specific blocks that file's byte range touches — `readBlob(blobName, position, length)` computes `firstBlock`/`lastBlock` via `BlockLayout.blockIndexFor`, fetches only the disk-contiguous ciphertext span covering exactly those blocks, and decrypts only those blocks. A single whole-blob AEAD envelope would force decrypting the entire bundle for even a one-file, one-byte-range read.

### Registers are never encrypted

`readRegister`/`compareAndSwapRegister` pass straight through `EncryptingBlobContainer` unencrypted. Shard-head and pin registers hold no document data — only node ids, terms, and generation numbers — so there's nothing to protect, and leaving them in plaintext keeps CAS conditional-write semantics simple (no risk of an encryption-layer bug corrupting the coordination primitive everything else depends on).

## Credential scoping: least-privilege blob containers

`RestrictingBlobContainer` wraps a container to allow only a specific subset of operations, throwing (unchecked) `SecurityException` on anything outside that subset. The plugin applies this asymmetrically by role, on the principle that **only GC needs delete**:

- Writer and reader engines both get a GET+PUT-only container for the shard-state store, manifest store, and bundle store — neither role should ever be able to delete a manifest or bundle, even by accident or bug.
- `DurablePinRegistry` gets the same GET+PUT-only container — pin add/overwrite only, never delete, which is safe because pins are never individually deleted by anything in this plugin outside GC's own retention-window expiry logic.
- `GcSchedulerTask` alone is built against the **unrestricted** container, since deleting superseded manifests and orphaned bundles is its entire purpose.
- `CompactionSchedulerTask`/`PartitionRewriteSchedulerTask` are also built against the unrestricted container — compaction's rebase-publish path doesn't itself delete, but `PartitionRewritePublisher.clearDescriptor()` is a real delete, so that scheduler needs delete capability too.

`CompactionSchedulerTask.maybeCompactSafely` deliberately catches every `Exception`, not just `IOException` — a denied operation against a restricted container surfaces as an unchecked `SecurityException`, and a scheduled background task must never let one failed tick kill its own recurring schedule.

## Compaction

Segments accumulate small files and deleted-but-not-reclaimed documents over time; compaction periodically folds them back down without waiting for an operator to ask. `CompactionSchedulerTask` runs from both writer and reader engines, on its own jittered interval. `CompactionPolicy.shouldCompact` is a pure two-condition trigger evaluated purely from manifest metadata — no bundle is opened just to decide whether to compact:

```
estimatedDeleteRatio >= minDeleteRatioToCompact (default 0.2)
    OR
(segmentCount >= minSegmentCountToCompact (default 10)
    AND largestSingleSegmentBytes < maxTargetBundleSizeBytes (default 5 GiB))
```

### No lease gating — proven safe under contention, not just assumed

Compaction deliberately does **not** skip when a writer's lease is currently held. Correctness instead rests entirely on the same [ShardHead CAS](/design/coordination/) publish protocol every other publisher uses: the target generation is always recomputed live from the current head inside a retry loop, so a compactor and an active writer racing to publish simply resolve through ordinary CAS contention, with the loser retrying against the winner's new head. This was validated both by a dedicated concurrency test (`CompactionRebaseExecutorTests#testConcurrentRebaseExecutorsNeverLoseAnUpdate`) and a formally-verified TLA+ spec (`ShardHeadDecoupled.cfg`) — not merely assumed safe by analogy to the publish path.

### The merge itself, and bundle-name-collision handling

Once compaction decides to run, this is the actual mechanism: a real Lucene segment merge, not a metadata-only trick. `LuceneMergeCompactionPublisher` materializes the source manifest into a `ByteBuffersDirectory`, runs a real Lucene `IndexWriter.forceMerge(targetSegmentCount)` (segment files are combined directly — no re-parsing of documents), and publishes the merged result as a new manifest generation.

`publishWithBundleNameCollisionRetry` handles one specific edge case: if the deterministic bundle name this compaction would publish under already exists with genuinely different content (a real name collision, detected by walking the cause chain for `writeBundle`'s collision signature), it retries publishing the **same already-merged bytes** under a fresh random suffix, up to 5 attempts — it never redoes the Lucene merge itself, since a bundle-name collision is orthogonal to whether the shard head has changed underneath it.

`CompactionRebaseExecutor.publish` wraps this in the standard CAS-retry shape (up to 5 outer attempts): re-read the live head, ask the publisher to compute a new head from it, attempt the CAS. A version conflict retries against the fresh head; an absent shard or empty computed head is treated as abandonment; exhausting the attempt budget is `RebaseResult.exhausted` — in every failure case, the worst outcome is wasted compute, never corruption.
