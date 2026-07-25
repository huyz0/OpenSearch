# A4: how would an index become absent from the routing table?

Spike output for Phase A of `rfc-scalable-index-metadata-tasks.md`. The question was whether the
serverless plugin's scale-to-zero already produces this state, so core only has to tolerate it.

**It does not, and the reason is the interesting part.** Scale-to-zero produces the *opposite*: it
deliberately keeps the shard in the routing table so that a decider can keep saying no to it.

## What scale-to-zero actually does

`ShardSuspensionCoordinator.suspend()` marks a shard suspended, which is a custom-data mutation on
`IndexMetadata`, then evicts the assigned copies with an explicit `CancelAllocationCommand`. Eviction
makes the shards `UNASSIGNED`. Nothing anywhere removes the `IndexRoutingTable` entry -- there is no
call that does it.

`SuspendedShardAllocationDecider` then holds the shard down: `canAllocate` returns `NO` on every node
so an unassigned suspended shard is never assigned, and `canRemain` returns `NO` so a started one is
evicted on the next pass.

So a fully scaled-to-zero tenant is not free to the allocator. Its shards are present, unassigned, and
re-evaluated on every reroute, once per candidate node. That is the cost cold-absence would remove,
and it scales with suspended shards times nodes.

**This is worth measuring before designing anything.** A6 was written as "confirm reroute scales with
active rather than absent indices". It should measure the *current* curve first -- reroute time
against suspended-shard count with the plugin's own mechanism -- because that number is the prize, and
nobody has it yet. S6 measured 40k *active* shards; nothing has measured 40k suspended ones.

## What blocks simply removing the entry

Reactivation reads the routing table to decide whether it has finished.
`ShardReactivationActionFilter` iterates `state.routingTable().index(indexName)` in three places
(around lines 226 and 306-312) to check whether the primary is `STARTED` and whether search replicas
are back. With no routing entry there is nothing to iterate, and those three sites would throw
`NullPointerException` in the same shape as the sixteen already fixed in core under A7.

That is not merely another guard to add. It is the design problem: **the routing table is currently
the completion signal for reactivation.** Take it away and reactivation needs a different one.

## Options for A5

1. **Recreate the entry as part of clearing the suspended flag.** Reactivation is already a
   cluster-state update (`TransportReactivateShardsAction`); it could add the `IndexRoutingTable` in
   the same update that clears suspension, so the entry exists before anything looks for it. The
   completion check then works unchanged. This keeps the change inside the plugin plus the core
   tolerance work already done.
2. **Move the completion signal into metadata.** Reactivation reports done based on the suspended
   flag and shard states recorded in `IndexMetadata` rather than the routing table. Larger, and it
   duplicates state that the routing table already holds for active indices.
3. **Synthesize the entry lazily in `RoutingTable.Builder`.** Build an entry on demand for any index
   whose metadata says not-suspended. Avoids a plugin change but puts tenant-lifecycle knowledge into
   core's routing construction, which is the wrong place for it.

Option 1 is the recommendation. It is the smallest, it keeps the mechanism where the rest of
scale-to-zero already lives, and it leaves core's role as tolerating absence rather than managing it.

## What A5 has to cover, concretely

- The suspension path stops writing an `IndexRoutingTable` entry, or removes it after eviction.
- `TransportReactivateShardsAction` adds the entry back in the same cluster-state update that clears
  the suspended flag.
- The three `ShardReactivationActionFilter` sites tolerate absence, since they run before the
  reactivation update completes.
- `SuspendedShardAllocationDecider` becomes mostly dead for suspended shards, because the allocator no
  longer sees them. Check whether it is still needed for the window between marking and eviction.
- Whatever else in the plugin reads the routing table for a suspended index. This audit covered
  `server/src/main` only; the plugin has not been swept the way A1 swept core.

That last point is its own task: **A8, sweep the plugin for the same assumption.** A1 found sixteen
sites in core. The plugin is smaller but it is the component that would actually create this state, so
its own dereferences matter more, not less.
