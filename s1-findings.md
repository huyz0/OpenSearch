# S1 — the two-node probe

- Spike: `server/src/test/java/org/opensearch/s0/` (`S1TwoNodeTests`, `S1Node`)
- Closes: [`rfc-serverless-shell.md`](rfc-serverless-shell.md) §11 Q4, and resolves R1
- Run: `./gradlew :server:test --tests "org.opensearch.s0.*" -Dbuild.docker=false`
- Result: **5 tests, 0 failures** (2 from S0, 3 from S1)

## Verdict

**Q4 is answered. R1 is resolved — as a contract, not as a defect.**

Two nodes holding genuinely different node-local views both serve. A projected view that omits a
hosted index does not disturb the shard. And the §5.3 version hazard reproduces exactly, with the
proposed fix verified in the same test.

## S1-A — two nodes, disjoint views, both serving

Node `s1-a` hosts `alpha`; node `s1-b` hosts `beta`. Each node's projected `ClusterState` contains
**only its own index**, and each is asserted not to know the other's:

```
assertFalse(a.clusterService.state().metadata().hasIndex("beta"));
assertFalse(b.clusterService.state().metadata().hasIndex("alpha"));
```

Both serve: 2 hits on A, 1 hit on B. This is the §5 claim at N>1, which S0 could not show — nothing
in the system agrees on a global state, and nothing needs to.

## S1-B — R1, measured rather than assumed

A node hosting `alpha` applies a view containing **no indices at all**. Measured:

| Observation | Result |
|---|---|
| Shard state after the partial view | `STARTED` — undisturbed |
| Shard still serving | 1 hit, unchanged |
| Close-set a **diff-based** reconciler would compute | `[[alpha][0]]` — **non-empty** |

Both halves matter, and they say different things:

1. **Applying a state does not close shards.** The applier is not the thing that removes shards, so
   §5.2's fear was mis-located: projection alone is harmless.
2. **The hazard is real, and it lives in the reconciler.** A reconciler that closes what is absent
   from the applied view would have closed a live, serving shard. The close-set is exactly the set of
   locally-open shards.

Since §10.5 already established that `IndicesClusterStateService` is replaced rather than reused,
**R1 stops being a risk and becomes a contract on the shell's reconciler**:

> **Absence from a projected view is never a removal signal.** A shard is closed when *truth* says so
> — the shard-head register says this node no longer owns it — and never because a locally-computed
> view failed to mention it.

That rule is cheap to state, cheap to test, and it is the single most important invariant the phase-2
reconciler must hold.

**Scope note:** the close-set is computed by test code modelling the diff rule, not by running
`IndicesClusterStateService`. It demonstrates the rule's consequence, not that class's behaviour.

## S1-C — Q4 answered, and the fix verified

**The hazard, reproduced:**

```
appliedClusterStateVersion = 500      (a long-lived node's projection counter)
updateShardState(..., version = 4)    (a fresh node's counter — lower)
  -> silently ignored. appliedClusterStateVersion still 500. Nothing threw.
updateShardState(..., version = 501)
  -> applied.
```

This is §5.3 executing. `ReplicationTracker.updateFromClusterManager` gates on
`applyingClusterStateVersion > appliedClusterStateVersion`, and a per-node projection counter makes
that comparison meaningless across nodes. **It fails silently** — no exception, no log, just an update
that did not happen. Another instance of the failure mode this project already knows it is bad at
seeing.

**The fix, verified in the same test:** a single monotonic source per shard — the shard-head
register's CAS generation — shared by every node that touches that shard. Six consecutive generations
applied, **none ignored**, including one representing a post-handoff takeover. A relocation target
reads the *same* register, so it structurally cannot produce a lower number. That is the whole fix:
not "use bigger numbers" but "have only one counter."

**Scope note:** S1-C exercises the version-comparison semantics directly through `updateShardState`.
It does not perform a real primary relocation through `PeerRecoveryTargetService`, so the full handoff
path — including `PrimaryContext` serialization over transport — remains untested. What is established
is the *mechanism* and its fix, not the end-to-end relocation.

## Risk register changes

| Risk | Before | After |
|---|---|---|
| R1 — projected partial state closes live shards | **Critical, open** | **Resolved into a contract.** Projection is harmless; the reconciler carries the invariant above. Phase 2 must test it. |
| R12 — cross-node version comparisons | High | **Confirmed real and fixed in design.** Remaining exposure is any *other* cross-node number, still unenumerated |

## What S1 still does not establish

- A real primary relocation (transport, `PeerRecoveryTargetService`, `PrimaryContext` over the wire).
- Shard removal, failure handling, or replica promotion — the parts of
  `IndicesClusterStateService` that are most of its 1,705 lines.
- Anything about CAS against a real object store, membership, gossip, or fencing. R9 and R11 remain
  fully open and are independent of everything measured here.
- Anything about scale. Two nodes, two indices, three documents.

## Recommendation

**Phase 1 is unblocked with no Critical risk outstanding in the shell thesis.** The two remaining
Criticals — R9 (zombie fencing) and R11 (provider CAS linearizability) — are storage-layer risks that
need no shell and can be closed in parallel, and R11 gates phase 3 regardless.

Carry into phase 2 as an explicit test, not a comment: *a reconciler must never close a shard because
a view omitted it.*
