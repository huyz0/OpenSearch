---
title: Resharding Sequences
description: The full split and merge call chains, from candidate trigger through core actions to the write path.
---

See [Resharding](/design/resharding/) for why each stage exists.

## Split, end-to-end

From the moment a candidate sustains long enough to trigger, through core's own split machinery, to the write path finally routing traffic at the new partitions.

```mermaid
sequenceDiagram
    participant Trig as InPlaceSplitTriggerCoordinator
    participant Core as TransportInPlaceSplitShardAction (core)
    participant Meta as MetadataInPlaceSplitShardService (core)
    participant Orch as TransportOrchestrateShardSplitAction
    participant Cloner as ShardCloner / ShardSplitter
    participant Filter as WritePartitionRoutingActionFilter
    participant Rewrite as PartitionRewritePublisher

    Trig->>Trig: sustained-candidate check (N consecutive ticks)
    Trig->>Core: client.execute(InPlaceSplitShardAction)
    Core->>Core: whole-index FlushRequest
    Core->>Meta: split(...) — records split-in-progress in SplitShardsMetadata

    Note over Orch: operator/orchestrator drives the rest, resumable at each stage
    Orch->>Orch: provisionStage — create target index shells (skip if exist)
    Orch->>Cloner: splitStage — ShardSplitter.split per target/partition
    Cloner->>Cloner: pin source generation BEFORE reading its manifest
    Cloner->>Cloner: write target manifest referencing source's files (zero-copy)
    Cloner->>Cloner: beforeActivation hook — write ShardPartitionDescriptor
    Cloner->>Cloner: ShardStateStore CAS — put-if-absent target head
    Orch->>Orch: cutoverStage — add targets to routing alias
    Orch->>Orch: fenceSourceStage — write SourceSplitFenceMetadata on source
    Orch->>Orch: writeRoutingStage — assign each target a partition

    Note over Filter: runtime write path, from here on
    Filter->>Filter: write against alias — resolve real target via hash(id), rewrite
    Filter->>Filter: write against direct target/fenced source — reject

    loop background, per target
        Rewrite->>Rewrite: materialize + filter + IndexWriter.addIndexes
        Rewrite->>Rewrite: publish new manifest generation via ordinary CAS
        Rewrite->>Rewrite: clearDescriptor() only AFTER publish succeeds
    end
```

## Merge, end-to-end

The reverse of split: folding two live children's data back into their revived parent, triggered far more conservatively than a split ever is.

```mermaid
sequenceDiagram
    participant Trig as InPlaceMergeTriggerCoordinator
    participant Core as TransportInPlaceMergeShardAction (core)
    participant Meta as MetadataInPlaceMergeShardService (core)
    participant Merger as InPlaceSiblingMerger

    Trig->>Trig: cooldown floor check (BEFORE sustained-tick tracking)
    Trig->>Trig: sustained-candidate check (higher tick count than split)
    Trig->>Core: client.execute(InPlaceMergeShardAction, parentShardId)
    Core->>Core: whole-index FlushRequest
    Core->>Meta: merge(...) — validates parent is a committed split parent,<br/>no further-split children, children live+started
    Meta->>Merger: engine hook rebuilds revived parent's local store
    Merger->>Merger: materialize + SoftDeletesDirectoryReaderWrapper +<br/>InPlaceSplitFilteringDirectoryReader, per child
    Merger->>Merger: IndexWriter.addIndexes — fold BOTH children's<br/>disjoint range-filtered slices, no double-counting
    Merger->>Merger: fresh HISTORY_UUID, max(LOCAL_CHECKPOINT/MAX_SEQ_NO) across children
```

Note the manual, non-automatic `ShardShrinker` primitive (arbitrary sources, full documents, transient pin) is a distinct code path from this automatic sibling-merge flow — see [Resharding](/design/resharding/#a-distinct-manual-primitive-shardshrinker).
