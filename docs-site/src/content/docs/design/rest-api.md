---
title: REST API Surface
description: The full REST endpoint table this plugin registers, grouped by subsystem.
---

Every endpoint lives under the base path `/_plugins/_serverless/storage/...`. Each `Rest*Action` pairs 1:1 with a `Transport*Action` of the same feature name, in the same subpackage — the REST/transport layer is deliberately not centralized; every functional area owns its own `action/` subpackage (`writerengine/action`, `readerengine/action`, `retention/action`, `deepsnapshot/action`, `compaction/action`, `resharding/action`, `scaleup/action`, `scaletozero/action`, `clone/action`, `migration/action`, `wal/action`, `format/action`, `security/action`, `nodecapacity/action`).

New handlers here should also override `RestHandler.apiAvailabilityScope()` — see [Core Changes](/core-changes/) for what that is for. (Earlier versions of this page called it `serverlessScope()`; the method was renamed when the product-named REST vocabulary was removed from core.)

## Writer engine

| Method | Path | Purpose |
|---|---|---|
| GET | `_idle_shards` | Node-level list of currently idle writer shards |
| GET | `{index_uuid}/{shard_id}/_idle_time` | How long a specific shard has been idle |
| GET | `{index_uuid}/{shard_id}/_realtime_get/{id}` | Realtime get bypassing normal search |

## Reader engine

| Method | Path | Purpose |
|---|---|---|
| GET | `{index_uuid}/{shard_id}/_wait_for_generation` | Block (bounded) until a reader has materialized a given manifest generation — see [Reader Engine](/design/reader-engine/) |
| POST | `{index_uuid}/{shard_id}/_poll_now` | Force an off-schedule manifest poll |
| GET | `_manifest_lag` | Node-level reader manifest-generation lag report |

## Format / caching

| Method | Path | Purpose |
|---|---|---|
| GET | `_cache_stats` | Node-level bundle-cache hit/miss statistics |

## WAL

| Method | Path | Purpose |
|---|---|---|
| GET | `_wal_backlog` | Node-level WAL batching backlog report |

## Compaction

| Method | Path | Purpose |
|---|---|---|
| POST | `_compact` | On-demand compaction trigger, sharing the same `maybeCompact` logic the scheduled task uses |

## Clone

| Method | Path | Purpose |
|---|---|---|
| POST | `_clone` | Zero-copy clone of a shard into a new target |

## Migration

| Method | Path | Purpose |
|---|---|---|
| POST | `{index_uuid}/{shard_id}/_migrate` | Adopt a classic-engine shard's current commit into serverless storage — see [Overview](/overview/) |

## Security

| Method | Path | Purpose |
|---|---|---|
| GET | `_object_store_request_stats` | Node-level object-store request counters (via `RequestCountingBlobContainer`) |

## Retention (durable pins, PITR, snapshot pin/restore)

| Method | Path | Purpose |
|---|---|---|
| GET | `{index_uuid}/{shard_id}/_retention_stats` | Per-shard retention/pin diagnostics |
| POST | `_snapshot_pin` | Pin the current manifest generation under a name (plugin's own mechanism — **not** core's repository-based snapshot API; see [Interoperability](/interoperability/)) |
| POST | `_snapshot_release` | Release a previously-created pin |
| POST | `_snapshot_restore` | CAS the shard head back to a pinned generation |
| POST | `index/{index}/_snapshot_pin` | Index-wide variant of pin |
| POST | `index/{index}/_snapshot_release` | Index-wide variant of release |
| POST | `index/{index}/_snapshot_restore` | Index-wide variant of restore |

## Deep snapshot (`deepsnapshot/action`)

Unlike the pointer-based pins above, these copy the bytes into a real repository, producing an ordinary snapshot restorable by a cluster that has never heard of this plugin. See [Snapshot/Restore](/design/snapshot-restore-proposal/) and [Durability Posture](/design/durability-posture/).

| Method | Path | Purpose |
|---|---|---|
| POST | `index/{index}/_snapshot_deep/{repository}/{snapshot}` | Deep-copy every shard of an index into the repository, finalizing on the cluster manager |
| POST | `_snapshot_deep` | Per-shard variant (body-addressed), used internally by the index-wide action |

## Scale-up

| Method | Path | Purpose |
|---|---|---|
| GET | `_scale_up/candidates` | Current reader-replica-expansion candidates |

## Scale-to-zero

| Method | Path | Purpose |
|---|---|---|
| GET | `_scale_to_zero/candidates` | Current suspension candidates |
| POST | `_reactivate` | Manually trigger reactivation, bypassing the request-triggered path |

## Node autoscaling ([design doc](/design/node-autoscaling/))

The in-cluster half of node autoscaling — the signal, drain, and warmup endpoints below — is built; the external control plane that calls them is not part of this repo.

| Method | Path | Purpose |
|---|---|---|
| GET | `node_capacity` | The cluster-wide node-autoscaling signal an external control plane polls |
| POST/DELETE | `nodes/{node_id}/drain` | Start or cancel a node drain |
| POST/DELETE | `nodes/{node_id}/warming` | Mark or clear a node as warming, withholding reader shard placement while set |

## Resharding

| Method | Path | Purpose |
|---|---|---|
| GET | `_resharding/split_candidates` | Current split (and, by the same signal, merge) candidates |
| POST | `_split` | Low-level: clone one target/partition (used internally by orchestration) |
| POST | `_shrink` | Manual `ShardShrinker` invocation — arbitrary sources, not necessarily split siblings |
| POST | `_shrink/{source_index}/_retire` | Retire a shrink source after confirming it's safely superseded |
| POST | `_resharding/_orchestrate_split/{source}` | The composed, resumable end-to-end split flow |
| POST | `_resharding/_fence_split_source/{source}` | Write `SourceSplitFenceMetadata` on the source |
| POST | `_resharding/_provision_split_targets/{source}` | Create target index shells |
| POST | `_resharding/_cutover/{alias}` | Add split targets to the routing alias |
| POST | `_resharding/_enable_write_routing/{alias}` | Assign each target a partition under `WritePartitionRoutingMetadata` |
| POST | `_resharding/_disable_write_routing` | Remove write-routing assignment |
| POST | `_partition_rewrite` | Manually trigger the background physical rewrite for one target |

See [Resharding](/design/resharding/) for how these compose into the full split/merge flows.
