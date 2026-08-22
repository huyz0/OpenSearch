---
title: Snapshot & Restore Sequence
description: The engine-native snapshot creation, restore, and delete/release call chains.
---

See [Snapshot & Restore](/design/snapshot-restore-proposal/) for why each step exists.

## Snapshot creation

Pinning happens before anything durable is written, and every failure path after the pin lands has to release it or the pin outlives whatever was supposed to reference it.

```mermaid
sequenceDiagram
    participant SSS as SnapshotShardsService.snapshot()
    participant IS as IndexShard
    participant Eng as ObjectStoreWriterEngine
    participant HP as ObjectStoreCommitHeadPublisher
    participant Pin as DurablePinRegistry
    participant Repo as BlobStoreRepository

    SSS->>IS: attemptEngineNativeSnapshot(snapshotId)
    IS->>Eng: applyOnEngine(...) -> attemptEngineNativeSnapshot(snapshotId)
    Eng->>HP: readLatestManifestWithPin(indexUuid, shardId, pinRegistry, snapshotId.getUUID())
    HP->>HP: read current ShardHead: (primaryTerm, generation)
    HP->>Pin: addPin(pinId = snapshot UUID, primaryTerm, generation)
    Note over HP,Pin: pinned BEFORE the read below, not after --<br/>closes a TOCTOU race with a concurrent GC sweep
    HP->>HP: readManifest(primaryTerm, generation)
    Note over HP,Pin: read failure here removes the pin just added, then rethrows
    HP-->>Eng: CommitManifest
    Eng->>Eng: EngineNativeSnapshotPayload(pinId, manifest).toBytes()
    Note over Eng,Pin: serialization failure here also removes the pin, then rethrows
    Eng-->>IS: EngineNativeSnapshotPointer(engineId, payload)
    IS-->>SSS: Optional.of(pointer)
    SSS->>Repo: snapshotEngineNative(store, snapshotId, indexId, ..., pointer.engineId(), pointer.payload(), listener)
    Repo->>Repo: write engine-native-snap-&lt;uuid&gt;.dat
    Repo-->>SSS: onResponse(generation)
    Note over SSS,Pin: if this write fails instead, SSS releases the pin via<br/>EngineNativeSnapshotReleasers before propagating the failure
```

A shard whose `attemptEngineNativeSnapshot` throws `IllegalIndexShardStateException` (neither `STARTED` nor `CLOSED`, a brief `POST_RECOVERY` window) is treated as if no pointer were available: `SnapshotShardsService` falls through to the classic/remote-store shallow-copy branching below it, unchanged.

## Restore

A cheap probe decides which path to take, classic or engine-native, so restore doesn't need to know ahead of time which format a given snapshot used.

```mermaid
sequenceDiagram
    participant IS as IndexShard.startRecovery(SNAPSHOT)
    participant SR as StoreRecovery
    participant Repo as BlobStoreRepository
    participant EF as WriterEngineFactory
    participant Sup as EngineNativeSnapshotSupport
    participant Mat as ObjectStoreCommitMaterializer

    IS->>SR: recoverFromEngineNativeSnapshot(indexShard, repository, listener)
    SR->>Repo: getEngineNativeShardSnapshotMetadata(snapshotId, indexId, shardId)
    Repo->>Repo: blobExists(engine-native-snap-&lt;uuid&gt;.dat)?
    alt blob absent — classic or remote-store shallow-copy snapshot
        Repo-->>SR: Optional.empty()
        SR->>SR: recoverFromRepository(...) — original, unmodified classic path
    else blob present
        Repo-->>SR: EngineNativeShardSnapshot(engineId, payload)
        SR->>SR: preRecovery(), prepareForIndexRecovery()
        SR->>EF: recoverFromEngineNativeSnapshot(indexShard, store, payload)
        EF->>Sup: recoverFromEngineNativeSnapshot(indexShard, store, payload)
        Sup->>Sup: EngineNativeSnapshotPayload.fromBytes(payload) -> manifest
        Sup->>Sup: containerResolver.resolve(manifest.indexUuid(), manifest.shardId())
        Note over Sup: the ORIGINAL shard's container, not the restore target's —<br/>restoring into a new index means a different indexUuid
        Sup->>Mat: materialize(manifest, store.directory())
        Sup->>Sup: Translog.createEmptyTranslog(manifest.localCheckpoint())
        Sup->>Sup: store.associateIndexWithNewTranslog(...)
        Sup-->>SR: true
        SR->>SR: openEngineAndRecoverFromTranslog(), finalizeRecovery(), postRecovery()
    end
```

Restoring into an existing index of the same name still requires it closed first, the same core precondition every restore has always had. Restoring after delete works too: core always assigns the restored index a fresh UUID in that case, so the container resolution step above is doing real cross-index work, not a same-shard no-op.

## Delete / release

Whatever pinned a generation during creation has to be the thing that unpins it on delete too, or the pin outlives the snapshot that justified it.

```mermaid
sequenceDiagram
    participant Del as BlobStoreRepository.deleteFromShardSnapshotMeta()
    participant Reg as EngineNativeSnapshotReleasers
    participant Sup as EngineNativeSnapshotSupport
    participant Pin as DurablePinRegistry (original shard)

    loop each removed snapshotId, per shard
        Del->>Del: blobExists(engine-native-snap-&lt;uuid&gt;.dat)?
        alt absent — the common case, every classic/shallow-copy shard
            Note over Del: skip, nothing to release
        else present
            Del->>Del: read EngineNativeShardSnapshot -> engineId, payload
            Del->>Reg: find(engineId)
            alt releaser registered
                Reg-->>Del: EngineNativeSnapshotSupport
                Del->>Sup: releaseEngineNativeSnapshot(payload)
                Sup->>Sup: EngineNativeSnapshotPayload.fromBytes(payload) -> manifest, pinId
                Sup->>Sup: containerResolver.resolve(manifest.indexUuid(), manifest.shardId())
                Sup->>Pin: removePin(indexUuid, shardId, pinId)
            else no releaser registered — plugin uninstalled, or not yet initialized
                Note over Del: logged, not fatal
            end
        end
    end
    Note over Del: any exception anywhere in this loop is caught and logged,<br/>never blocks the snapshot delete itself
```

This is the same registry-lookup, best-effort, log-and-continue shape as the snapshot-creation-failure release path above: both ultimately call the same `EngineNativeSnapshotSupport.releaseEngineNativeSnapshot`, just from two different trigger points (a failed write vs. a later explicit delete).
