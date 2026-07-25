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

**Corrected after the A8 sweep.** The first version of this section said
`ShardReactivationActionFilter` would throw `NullPointerException` at three sites. That was wrong:
both of its routing reads are already guarded by `state.routingTable().hasIndex(...)`. The sweep
found exactly one genuinely unguarded dereference in the plugin,
`ShardSuspensionCoordinator`'s eviction path, now fixed.

The real problem is worse than an NPE, because it is silent. Those guards exist, but they give the
**wrong answer** for a routing-absent index:

- `readerCopyNotYetStarted` returns `false` when the index has no routing entry, meaning "no
  reactivation needed". A cold index needs reactivation more than any other.
- `allFullyReactivated` skips such an entry with `continue`, meaning "this one is done". It would
  report reactivation complete for an index that has no shards at all.

So the design problem stands, sharpened: **the routing table is the completion signal for
reactivation, and absence currently reads as "nothing to do" rather than "not started".** Removing the
entry without changing that would make reactivation silently succeed while serving nothing. An
exception would at least have been loud.

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
- `readerCopyNotYetStarted` and `allFullyReactivated` distinguish "no routing entry because cold" from
  "no routing entry because gone". Today both read as done.
- `SuspendedShardAllocationDecider` becomes mostly dead for suspended shards, because the allocator no
  longer sees them. Check whether it is still needed for the window between marking and eviction.
- Whatever else in the plugin reads the routing table for a suspended index. This audit covered
  `server/src/main` only; the plugin has not been swept the way A1 swept core.

That last point was its own task, **A8, and it is done.** The plugin has four routing-entry reads:
`WriterPublicationNotifier` (already guarded), `ShardSuspensionCoordinator` (was not, now is), and the
two `ShardReactivationActionFilter` sites (guarded, but semantically wrong for a cold index, as above).
Far fewer than core's sixteen, and the interesting ones are semantic rather than null-safety.
