---
title: Scale-to-Zero Sequence
description: The suspend and reactivate sequences, including the search-vs-write asymmetry.
---

See [Scale-to-Zero & Scale-Up](/design/scale-to-zero-scale-up/) for why each step exists.

## Suspend

Idle candidates get evicted from the cluster, not just marked read-only, since core's balancer won't unassign a shard with nowhere to move it on its own.

```mermaid
sequenceDiagram
    participant Sched as ScaleToZeroCandidatesSchedulerTask (cluster-manager only)
    participant Coord as ShardSuspensionCoordinator
    participant CS as ClusterStateUpdateTask
    participant Alloc as reroute

    loop eval_interval
        Sched->>Sched: isLocalNodeElectedClusterManager()? (checked fresh, not cached)
        Sched->>Sched: evaluate writer idle time + reader manifest-generation lag
    end
    Sched->>Coord: suspendCandidates() / suspendReaderCandidates()
    Coord->>CS: submit(Priority.NORMAL)
    CS->>CS: already suspended? idempotent no-op if so
    CS->>CS: isSuspensionAllowed(cooldownMillis)? skip if reactivated too recently
    CS->>CS: SuspendedShardsMetadata.withShardSuspended / withReaderShardSuspended
    CS-->>Coord: clusterStateProcessed
    Coord->>Alloc: evict() — explicit CancelAllocationCommand per assigned copy
    Note over Alloc: NOT just canRemain=NO — the balancer only moves,<br/>never force-unassigns with no valid target
```

Durability note: the final flush attempted before suspension is best-effort. Because the durability watermark only advances on a successful publish, a failed flush just means the next reactivation replays one commit further back via the WAL — eviction proceeds unconditionally either way, it never waits on flush success.

## Reactivate

An incoming request against a suspended shard triggers reactivation on the way in, but only search actually waits for it to finish before proceeding.

```mermaid
sequenceDiagram
    participant Req as Incoming request
    participant RF as ShardReactivationActionFilter (order = MIN_VALUE, runs first)
    participant TA as TransportReactivateShardsAction
    participant Obs as ClusterStateObserver

    Req->>RF: apply() — index has a suspended shard (writer or reader)
    RF->>TA: client.execute(ReactivateShardsAction) — fire-and-forget,<br/>via TransportClusterManagerNodeAction (filter may run on non-manager node)
    TA->>TA: clear SuspendedShardsMetadata, stamp reactivation timestamp

    alt request is SearchAction
        RF->>Obs: waitForNextChange(...) — BLOCKS
        Note over Obs: resolves only when BOTH the suspension marker is<br/>cleared AND routing reaches STARTED for the SPECIFIC<br/>role that was suspended (not "any copy started")
        Obs-->>RF: resolved, or timed out after searchReactivationWait (default 30s) — fail-open
    else write or get
        Note over RF: no wait — these paths already retry via their<br/>own existing "no active copy" handling
    end
    RF->>Req: chain.proceed()
```

The `order = Integer.MIN_VALUE` placement matters: this filter runs one slot ahead of `WritePartitionRoutingActionFilter` (`Integer.MIN_VALUE + 1`, see [Resharding](/design/resharding/)) — reactivation must resolve before any write-partition routing/fencing decision is made on the same request.
