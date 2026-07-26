---
title: Core Sequences
description: Write path, read path, and GC/retention as full sequence diagrams with real method names.
---

These three sequences are the everyday-operation backbone of the system. Each mechanism referenced here (CAS fencing, manifest polling, pin-before-read) has its own dedicated explanation on the linked Design pages.

## Write path

A single `index`/`delete` call, from the moment the client sends it to the moment it's either durably published or the shard gives up and fails itself out.

```mermaid
sequenceDiagram
    participant C as Client
    participant E as ObjectStoreWriterEngine
    participant T as WalMirroringTranslog
    participant WCS as WalChunkService
    participant CHP as ObjectStoreCommitHeadPublisher
    participant CP as ObjectStoreCommitPublisher
    participant SS as ShardStateStore (CAS)

    C->>E: index(Index) / delete
    E->>E: super.index() — local Lucene write + local translog append
    E->>T: Translog.add(operation)
    T->>WCS: mirror as WalRecord (buffered/batched, non-blocking)
    Note over T,WCS: ensureSynced() blocks on this shard's most recent<br/>group-commit future, per index.translog.durability

    Note over E: periodic flush / refresh("api"|"schedule")
    E->>E: commitIndexWriter() — LOCAL Lucene commit happens first, unconditionally
    E->>CHP: publishCommitAsHead(term, maxSeqNo, walPosition, ...)
    loop CAS retry
        CHP->>SS: get current head
        alt currentHead.leaseTerm() > term
            CHP-->>E: false — FENCED OUT
        else not fenced
            CHP->>CP: publishCommit(..., generation = currentGeneration+1)
            CP->>CP: pack referenced files into one bundle blob
            CP->>CP: write manifest blob
            CHP->>SS: compareAndSet(expectedVersion, newHead)
        end
    end
    alt published
        E->>E: translogDeletionPolicy.recordDurablePublication(maxSeqNo)
        E-->>C: dispatch async reader notification (optimization only)
    else fenced out
        E->>E: failEngine(...) — shard stops serving as writer
    end
```

Full explanation: [Writer Engine & WAL](/design/writer-engine/). Fencing mechanism: [Coordination & Consistency](/design/coordination/).

## Read path

How a reader shard notices new data exists and pulls it in, without ever accepting a write of its own.

```mermaid
sequenceDiagram
    participant Sched as manifestPollTask (5s) / PollNow (forced)
    participant R as ObjectStoreReaderEngine
    participant SS as ShardStateStore
    participant MS as BlobContainerManifestStore
    participant Dir as Store Directory

    Sched->>R: pollForNewerManifest() [synchronized]
    R->>SS: get current head
    alt latestManifestGeneration <= currentManifestGeneration
        R-->>Sched: already caught up, return
    else admission controller over budget
        R-->>Sched: skip, retry next tick
    else newer generation available
        R->>MS: readManifest(primaryTerm, latestGeneration)
        R->>Dir: applyManifestToDirectory(manifest)
        alt LazyBundleDirectory
            Dir->>Dir: advanceToManifest — pure in-memory merge, zero I/O now
        else eager
            Dir->>Dir: ObjectStoreCommitMaterializer.materialize — fetch NEW files only
        end
        R->>R: maybeRefresh("manifest-generation-advance")
        R->>R: currentManifestGeneration.set(manifest.generation())
    end
```

Full explanation, including the push-notification path and `waitForGeneration`: [Reader Engine & Materialization](/design/reader-engine/).

## GC / retention sweep

One sweep, deciding what's safe to delete and then deleting it, with a second pin check squeezed in immediately before the delete call to shrink the race window against a concurrent pin as far as it'll go.

```mermaid
sequenceDiagram
    participant GC as GcSchedulerTask
    participant Pin as DurablePinRegistry
    participant MS as ManifestStore
    participant BS as BundleStore

    loop gc_interval
        GC->>MS: listManifests()
        GC->>Pin: getPinnedManifestIds() [1st read]
        GC->>GC: ManifestRetentionPolicy.computeDeletableManifests<br/>(lease pins never consulted — only durable pins)
        GC->>GC: computeRetainedManifests (always run, even if nothing deletable)
        GC->>GC: BundleReferenceCounter.computeLiveBundles(retained)
        GC->>BS: listBundleNames()
        GC->>GC: computeDeletableBundles(all, live)
        GC->>GC: sustained-orphan check — must be orphaned for full retentionWindowMillis
        GC->>Pin: getPinnedManifestIds() [2nd read, immediately before delete]
        GC->>BS: deleteBundles(deletable) — BEFORE manifests, for crash safety
        GC->>MS: deleteManifests(deletable)
    end
```

PITR retention runs on its own separate schedule from **both** the writer and reader engine — full explanation: [GC, Retention & PITR](/design/gc-retention/).
