---
title: Flows
description: The main request/lifecycle flows through the serverless storage engine.
---

## Write path

```mermaid
sequenceDiagram
    participant C as Client
    participant W as Writer Engine
    participant WAL as WAL
    participant L as Local Lucene index
    participant O as Object store

    C->>W: index/update/delete
    W->>WAL: append (durability before commit)
    W->>L: apply to local index
    Note over W,L: periodic commit
    W->>L: commit
    W->>O: write segment bundle(s)
    W->>O: publish commit manifest (CAS on ShardHead)
    O-->>W: ack
```

A write is durable once the WAL append succeeds, not once the manifest is published — the WAL is what lets a writer recover uncommitted writes after a restart without waiting for the next commit.

## Read path

```mermaid
sequenceDiagram
    participant C as Client
    participant R as Reader Engine
    participant O as Object store

    R->>O: poll ShardHead / latest manifest
    O-->>R: manifest (new generation)
    alt lazy directory enabled
        R->>O: fetch only files touched by query
    else eager
        R->>O: download full bundle(s)
    end
    C->>R: search
    R-->>C: results from materialized reader
```

Reader materialization is independent per reader replica — each one polls and fetches on its own, which is what lets `scaleup/` add reader replicas without coordinating with the writer.

## Scale-to-zero lifecycle

```mermaid
sequenceDiagram
    participant Sched as ScaleToZeroCandidatesSchedulerTask
    participant Coord as ShardSuspensionCoordinator
    participant SH as ShardHead
    participant Node as Writer/Reader node

    loop eval_interval
        Sched->>Sched: evaluate idle_threshold, lag_threshold
    end
    Sched->>Coord: candidate shard(s)
    Coord->>Node: best-effort final flush + commit
    Coord->>SH: mark suspended (CAS)
    Coord->>Node: release compute (writer + reader)
    Note over SH: durable state stays in object store

    par reactivation
        Node->>SH: incoming request detects suspended
        SH-->>Node: latest manifest generation
        Node->>Node: materialize from manifest
    end
```

The final flush before suspension is best-effort, not a guarantee — this is safe by design because the durability watermark only advances after a successful publish, so a flush that fails just means the next reactivation replays from one commit earlier via the WAL, not that data is lost.

## Resharding (split)

```mermaid
sequenceDiagram
    participant Trig as Split trigger (manual or InPlaceSplitTriggerCoordinator)
    participant Src as Source shard
    participant Cloner as ShardCloner
    participant Tgt as Target shard(s)

    Trig->>Src: candidate detected (wpm / size threshold)
    Trig->>Cloner: clone(source, target, hash-range)
    Cloner->>Cloner: pin source generation (DurablePinRegistry)
    Cloner->>Tgt: write manifest referencing filtered file set (no byte copy)
    Trig->>Src: fence writes (write-block)
    Trig->>Tgt: cutover — accept writes for its range
    Note over Tgt: background physical rewrite compacts to a<br/>partition-native bundle over time
```

Splitting is zero-copy at cutover time: a target shard's first manifest references the same underlying bundle files as the source, filtered to its hash range, via `PartitionFilteringDirectoryReader`. The background rewrite that actually removes the now-irrelevant file ranges from disk happens later, off the write-fencing critical path.

Merge (shrink) is the inverse for exactly a split's own two children — their filtered readers fold together via `IndexWriter#addIndexes`, which is why it's cheap; a general N-way merge across unrelated shards is out of scope.

## GC / retention

```mermaid
sequenceDiagram
    participant GC as GcSchedulerTask
    participant Pin as DurablePinRegistry
    participant O as Object store

    loop gc_interval
        GC->>O: list manifests for shard
        GC->>GC: supersession check (generation < latest, past retention window)
        GC->>Pin: any pin (snapshot/PITR) on this generation?
        alt pinned
            GC->>GC: skip
        else unpinned + superseded
            GC->>O: delete manifest
            GC->>GC: reference-count bundle files
            GC->>O: delete orphaned bundle files
        end
    end
```

PITR retention (`retention/`) and GC both run from the writer engine **and** the reader engine — a shard that has scaled to zero on the writer side may still have an active reader, and pins/reconciliation must keep working even when no writer is present. This dual-scheduling was itself a fix made during the broad-scope review; see [Plugin Fixes](/plugin-fixes/).
