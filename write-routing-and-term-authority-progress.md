# Progress: Write-Side Partition Routing + Metadata-Plane Term-Authority Migration

Tracking doc for the two large follow-on efforts scoped out of `rfc-serverless-opensearch.md`.
Not part of that RFC's own task list (which is fully closed) -- this is new work, tracked here
until/unless it's folded back into the RFC proper.

Status legend: `[ ]` not started, `[~]` in progress, `[x]` done (reviewed before being marked),
`[!]` blocked.

## Effort A: Real write-side partition routing + auto-split controller

### Design
- [x] A1. Decide where the doc->partition routing map lives. **Resolved: no new stored map at
      all.** Found that `RoutingPartitionFilter`/`Murmur3HashFunction` already gives a
      deterministic, pure function of a document's `_id` -> partition index -- the exact same
      function a reader shard already uses read-side. The only thing that needs to live anywhere
      is "partition index N -> which real target index," and that's small, static per assignment,
      and fits the same `IndexMetadata` custom-data extension point `SuspendedShardsMetadata`
      already uses (see `WritePartitionRoutingMetadata`).
- [x] A2. Decide how a coordinating node looks up the owning partition cheaply. **Resolved: reads
      already-local cluster state, zero remote calls.** `WritePartitionRoutingActionFilter` reads
      `ClusterService#state()` (already held by every node) and computes the hash locally --
      exactly the same cost profile as any other cluster-state-based request classification
      already done in this plugin (e.g. `ShardReactivationActionFilter`).
- [ ] A3. Decide the consistency window between "rewrite finished" and "routing map updated" for
      writes (search-only cutover doesn't have this problem; writes do). Not yet addressed: this
      first increment's `EnableWritePartitionRoutingAction` is a single atomic cluster-state
      update across all targets at once, so there's no *partial* assignment window within one
      call, but the window between "physical rewrite finishes" and "operator calls this action"
      is still manual/operator-timed, not automated. Real answer needs the auto-split controller
      (A14-A17).
- [ ] A4. Decide the re-split story: how the routing map versions/migrates if a target splits again.
      Not yet addressed -- `WritePartitionRoutingMetadata.withAssignment` would simply overwrite,
      but nothing yet enforces in-flight writes drain before a reassignment; flagged as a real gap
      for a re-split scenario, not silently unsafe today since re-split isn't wired to anything yet.
- [ ] A5. Decide the rollback story: how misrouted writes are reconciled if a split is aborted
      after write-cutover. Not yet addressed.

### Implementation
- [x] A6. Model and expose the partition-routing function/map as first-class cluster-state metadata.
      **Done**: `WritePartitionRoutingMetadata` (new file, `resharding` package) -- `IndexMetadata`
      custom data recording `(writeRoutingAlias, partitionIndex, numPartitions)` per target index.
- [x] A7. Wire indexing-path routing resolution on the coordinating node. **Done, scoped to
      explicit-id requests**: `WritePartitionRoutingActionFilter` (new file) intercepts
      `IndexRequest`/`DeleteRequest`/`UpdateRequest`/`BulkRequest` items naming a write-routing
      alias and rewrites the target to the correct real partition index before `chain.proceed`.
      Auto-generated ids (no explicit `_id` on create) are explicitly out of scope for this
      increment -- documented in the filter's own javadoc, not silently mishandled: such a
      request passes through unmodified and core's own multi-index-alias guard rejects it outright
      (safe failure, never misroutes).
- [x] A8. Extend cutover to a write-routing phase, distinct from the existing search-only
      `CutoverSplitRoutingAction`. **Done**: new `EnableWritePartitionRoutingAction`/
      `TransportEnableWritePartitionRoutingAction`/`RestEnableWritePartitionRoutingAction`,
      REST-exposed as `POST .../_resharding/_enable_write_routing/{alias}?target_indices=a,b,c`,
      deliberately a separate explicit opt-in step, not a flag on the search-only action.
- [ ] A9. Fence writes arriving at the source index after write-cutover. **Not done, documented as
      a known gap**: distinguishing this filter's own already-rewritten request from a client
      writing to the same target index name directly needs information not available at this
      point in the filter chain -- real follow-up design work (see the filter's own javadoc).
- [ ] A10. Fence writes arriving at a target before write-cutover completes. Same gap as A9 --
      not done, documented, not silently unsafe (a target accepting a direct write before
      write-routing is enabled is no different from writing to any ordinary index today).

### Testing
- [x] A11 (narrowed to this increment's scope). **Done**:
      `ServerlessStorageWritePartitionRoutingActionIT#testWritesAgainstTheAliasLandInTheirOwnCorrectRealPartitionOnly`
      -- 20 documents with distinct explicit ids indexed against a real 2-node cluster's alias,
      each independently predicted (by replaying the exact same Murmur3 hash) to land in one of
      two real target indices, and verified via `GET` against both targets that it landed in
      exactly the predicted one and nowhere else. Confirmed meaningful: disabled the filter's
      rewrite call, reran, watched the test fail with core's own "no write index is defined for
      alias" error (proving the filter -- not some other mechanism -- is what makes this work),
      restored, reran clean. Full plugin quality gate and the entire `internalClusterTest` suite
      (every other test in the plugin) pass clean with this change in place.
- [ ] A12. Concurrency IT: writes racing the exact cutover moment. Not yet done.
- [ ] A13. Chaos test: node death mid-cutover, routing map state survives intact. Not yet done --
      likely already covered in spirit by cluster-state's own general replication/failover
      guarantees (this metadata rides the same mechanism as `SuspendedShardsMetadata`, which chaos
      tests already exercise), but not proven directly for this specific metadata yet.

### Auto-split controller
- [ ] A14. Design trigger policy off `writesPerMinute()` (thresholds + hysteresis).
- [ ] A15. Wire controller: rewrite -> write-cutover as one coordinated, resumable state machine.
- [ ] A16. Observability: REST stats for auto-split decisions/progress.
- [ ] A17. RFC update: close out §16 Phase 4's routing-cutover gap note.

## Effort B: Metadata-plane term-authority migration

### Design
- [ ] B1. Specify the atomic primitive: single CAS granting a new primary term *and* recording
      the WAL position of that grant.
- [ ] B2. Decide where the combined (term, WAL position) record lives in cluster state.
- [ ] B3. Determine the failure-mode story if the CAS's two halves (term grant, WAL-position
      write) partially fail or race.
- [ ] B4. Assess blast radius -- this changes primary-term grant for every shard, not just
      serverless-storage ones; needs core cluster-coordination review.
- [ ] B5. Decide backward compatibility for classic (non-serverless) shards.

### Formal verification
- [ ] B6. Extend/write a TLA+ model of the proposed atomic grant; check it closes the residual
      race `WalReplayFencing.tla`'s `FixedReplay` left open.
- [ ] B7. Verify no new race at the CAS boundary itself (two nodes racing the same grant).

### Implementation
- [ ] B8. Implement the atomic grant primitive in core cluster-coordination.
- [ ] B9. Replace `ObjectStoreWriterEngine`'s constructor-time `activationWalPosition` snapshot
      with a read of the atomically-recorded position.
- [ ] B10. Update/remove the "honest limitation" javadoc once the gap is closed.

### Testing
- [ ] B11. Regression test reproducing the original race under the old approach, confirming the
      new grant closes it.
- [ ] B12. Extend dual-writer fencing tests (§17) to exercise the new grant path under real failover.
- [ ] B13. Chaos test: node death exactly at grant time.

### Documentation
- [ ] B14. RFC update: close out §6.4/§7.1's "still future work" notes.

## Log

- Progress file created. Starting with Effort A design tasks (A1-A5), since Effort B's
  implementation phase (B8) is a cross-cutting core cluster-coordination change with a much
  larger blast radius (affects primary-term grant for every shard in OpenSearch, not just this
  plugin) -- expect that to need explicit human sign-off before implementation, similar to the
  REST API surface widening earlier in this project. Design and formal-verification work for
  both efforts can proceed; B8's actual implementation is the item most likely to get flagged.
- A1/A2 resolved by direct code investigation (not just design-on-paper): `RoutingPartitionFilter`'s
  existing Murmur3 hash and the `IndexMetadata` custom-data extension point `SuspendedShardsMetadata`
  already established were both directly reusable, closing what looked like the two hardest design
  questions without inventing new mechanism. Implemented A6-A8, A11 as a first real, tested,
  entirely plugin-local increment (`WritePartitionRoutingMetadata`, `EnableWritePartitionRoutingAction`,
  `WritePartitionRoutingActionFilter`, `ServerlessStorageWritePartitionRoutingActionIT`) -- no core
  changes, so no security-classifier-style review needed for this piece. A9/A10 (fencing) and A3-A5
  (consistency/re-split/rollback stories) remain open, documented in code and here rather than
  rushed. Full plugin quality gate + entire internalClusterTest suite pass clean. RFC §16 Phase 4
  updated with a status note. Committing and pushing this increment next, then continuing to A14-A17
  (auto-split controller) or Effort B's design/formal-verification tasks (B1-B7).
