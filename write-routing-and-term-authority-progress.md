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
- [x] A3. Decide the consistency window between "rewrite finished" and "routing map updated" for
      writes. **Resolved: deliberately operator-timed, no automation yet, and that's fine for this
      increment.** `EnableWritePartitionRoutingAction` is a single atomic cluster-state update
      across all targets at once, so there's no *partial* assignment window within one call. The
      window between "physical rewrite finishes" and "operator calls this action" is real but
      identical in shape to the window `CutoverSplitRoutingAction` already has for search-only
      cutover -- both are explicit, sequenced, operator-triggered steps by design (rewrite -> search
      cutover -> write cutover), not automated. Automating that sequencing is exactly what the
      auto-split controller (A14-A17) is for; this increment correctly leaves it manual.
- [x] A4. Decide the re-split story. **Resolved: not reachable today, so not unsafely handled --
      genuinely out of scope until re-split itself exists.** Nothing in this plugin currently
      re-splits an already-split target (`ShardSplitter`/`PartitionRewriteSchedulerTask` only
      operate against a fresh source, and a split target's own `ShardPartitionDescriptor` is
      documented as write-once). `WritePartitionRoutingMetadata.withAssignment` would silently
      overwrite an existing assignment if called twice for the same target, with no drain-in-flight
      step -- a real gap, but one with no current caller, so it cannot fire today. If re-split is
      ever built, the fix is a two-step reassignment (block new writes against the old assignment,
      wait for in-flight completion, then reassign) analogous to `ShardSuspensionCoordinator`'s
      existing drain-before-mutate pattern elsewhere in this plugin -- deferred until re-split
      itself is designed, not implemented speculatively against a mechanism that doesn't exist.
- [x] A5. Decide the rollback story. **Resolved by finding and closing a real, smaller gap than
      originally framed: there was no way to *disable* write-routing once enabled at all.** A full
      "reconcile already-written documents back to a different target" is a genuine data-migration
      problem (documents already correctly landed in their assigned partition per the routing
      function in effect at write time -- there is nothing to "fix" about where they landed, only
      about whether new writes should keep going through routing). The actual missing piece was
      simpler and more urgent: no symmetric action existed to turn write-routing back off, so an
      operator who enabled it had no supported way to revert to ordinary single-index writes.
      Closed by adding `DisableWritePartitionRoutingAction` (see A6/A9 update below) -- clears the
      write-routing assignment from every named target, after which `WritePartitionRoutingActionFilter`
      naturally stops rewriting or fencing against them (no assignment left to match). This is the
      right-sized rollback primitive for this increment; broader reconciliation tooling (if writes
      need to move between partitions after the fact) remains explicitly out of scope, same
      category as A4's re-split gap.

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
- [x] A9/A10. Fence a target index's direct (alias-bypassing) writes. **Done, combined into one
      check**: at the point `WritePartitionRoutingActionFilter#apply` first reads a request's
      `index()`, that value is still the client's own original target -- if it directly names an
      index carrying a write-routing assignment, the write is rejected outright with
      `IllegalArgumentException` before any rewriting happens later in the same call. This turned
      out to subsume both A9 (fencing the source after cutover isn't applicable in this design --
      the source keeps its own name and was never itself assigned a partition) and A10 (fencing a
      target before/after write-routing is enabled): any direct write to an *assigned* target index
      is refused, unconditionally, regardless of timing relative to cutover.
      **A real re-entrancy bug found and fixed while building this**: a single-item
      `client().prepareIndex(alias)` call doesn't traverse this filter chain once -- core's
      `TransportSingleItemBulkWriteAction` wraps it into a `BulkRequest` and dispatches that
      through the *same* filter chain a second time, synchronously, on the same thread. By the
      second pass, this filter's own first-pass rewrite had already replaced the alias name with
      the real target index name, which the naive fencing check then misidentified as a client
      writing to that target directly -- confirmed by the new fencing IT genuinely failing the
      *legitimate* alias-write test until fixed. Fixed with a `ThreadContext` transient marker: an
      identity-based `Set` of requests this filter has itself rewritten, stashed once per
      thread-context, consulted (not re-created) on re-entry. Tested end-to-end in
      `testDirectWritesToAnAssignedTargetIndexAreRefused`; confirmed meaningful by disabling the
      fencing check and watching the test fail with "no exception was thrown," then restoring.
      Full plugin quality gate + entire `internalClusterTest` suite pass clean.

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
- [x] A12. Concurrency IT: writes racing the exact cutover moment. **Done**:
      `testConcurrentWritesRacingEnableAreNeverMisroutedOrDuplicated` -- 8 threads x 25 writes each
      hammer the alias with unique explicit ids concurrently with a single
      `EnableWritePartitionRoutingAction` call landing partway through. Writes issued before the
      enable takes effect are expected (and allowed) to fail -- core's own multi-index-alias guard
      rejects them, the same safe failure mode already proven elsewhere -- but every write that
      *succeeds* is checked: routed to exactly the one real partition its id's hash predicts, never
      present in both targets, and the total successful-write count matches exactly what's found
      across both targets (no loss, no duplication). Re-ran 3 additional times with different
      random seeds to rule out flakiness before trusting the result -- all passed. Full plugin
      quality gate and the entire `internalClusterTest` suite pass clean.
- [x] A13. Chaos test: node death, routing map state survives intact. **Done**:
      `testWriteRoutingAssignmentSurvivesATargetsNodeBeingKilled` -- enables write routing, kills
      the data node holding one target's primary shard, starts a replacement node, waits for green,
      then asserts (a) the write-routing assignment read directly off cluster-manager-held
      `IndexMetadata` is exactly what it was before the kill, and (b) a write against the alias
      afterward still routes correctly end-to-end, not just that the metadata field happens to be
      present. **A real test-design bug found and fixed while building this**: the first version
      used 0-replica targets, so killing the node holding a target's *only* copy left that shard
      permanently unrecoverable (an ordinary, expected outcome for any 0-replica index on node
      death, unrelated to what this test is meant to exercise) -- `ensureGreen` timed out for real,
      not due to a routing bug. Fixed by giving both targets 1 replica, matching the fault model
      `ServerlessStorageWriterFailoverIT`'s own node-kill tests already use elsewhere in this
      plugin, so a live copy is promoted and the cluster genuinely returns to green. Full plugin
      quality gate and the entire `internalClusterTest` suite pass clean.

      **This closes out all of Effort A's design, implementation, and testing work (A1-A13).**
      Only the auto-split controller itself (A14-A17) remains.

### Auto-split controller
- [!] A14-A17 investigated together -- **genuinely blocked, not by authorization but by a missing
      foundational mechanism that doesn't exist anywhere in this plugin yet.** Read
      `ShardSplitter#split`'s real signature directly: it takes a `targetIndexUuid` documented as
      "the brand-new index this split target creates" -- meaning the real OpenSearch index (with
      its own settings, shard count, mapping) must already have been created via ordinary
      `_create_index` *before* this plugin's object-store-level split can run against it. Nothing
      in this plugin creates that index automatically; `ShardSplitCandidatesAction`'s own
      `ShardSplitCandidateEntry` javadoc says this explicitly and un-defensively: "`ShardSplitter#split`
      only re-points an already-provisioned target shard identity; it does not create new
      indices/shards, allocate them, or cut over routing from the source shard to the split
      targets. None of that orchestration exists in this plugin yet, so there is no automated
      action this signal could safely drive today." That sentence, written when the candidate
      signal itself was built, is still accurate after this session's write-routing work: A15's
      "wire rewrite -> write-cutover as one resumable state machine" implicitly assumed the target
      indices already exist, which was the wrong assumption -- the real missing piece is target
      *index* auto-provisioning, not orchestration of already-existing mechanism.

      **Why this isn't safely buildable without new product/design decisions, not just more
      code**: auto-creating a target index means deciding, with no operator in the loop, a naming
      convention (collision-safe, discoverable, reversible), how many partitions to split into
      (itself a policy question -- 2? scaled to current write rate?), what settings/mapping the new
      index inherits from the source, and what happens if creation succeeds but the rest of the
      pipeline (rewrite, cutover) later fails, leaving an orphan index behind. Every one of those
      is a product decision this RFC's own scoping notes have repeatedly deferred rather than
      guessed at (see A4/A5's own "not reachable, don't build against a mechanism that doesn't
      exist" reasoning, and the REST-scope-widening precedent for "needs explicit sign-off, not
      inference"). Building A14-A17 today would mean inventing that policy unilaterally.

      **What's actually done and doesn't need redoing**: `ShardSplitCandidatesAction` (fully built,
      tested, and shipped in an earlier session) already is the correct-shaped "detection" half --
      cluster-wide `writesPerMinute()` aggregation, threshold + sustained-duration hysteresis,
      surfaced via REST for an operator (or, once target-provisioning policy exists, a future
      controller) to consult. A14's "design trigger policy" is therefore already resolved by that
      existing signal; A16's "observability" is already `ShardSplitCandidatesAction`'s own REST
      surface. What remains open is specifically A15 (the orchestration across rewrite + both
      cutover steps) and, as a hard precondition for it, target-index auto-provisioning policy --
      neither attempted here.

- [x] **The target-index auto-provisioning blocker above is now closed, the safe way.** An earlier
      attempt (`AutoSplitPlanner`, algorithmic naming) was blocked by the security classifier as
      unilateral policy invention, correctly -- see the log entry documenting that denial. Grounded
      the redo in core's own real, existing `_split` API (researched directly against
      `TransportResizeAction`/`RestResizeHandler`, not assumed) before rebuilding: core's own
      `_split` always requires the caller to supply the target index name in the URL path, and
      core's own code explicitly *refuses* to auto-calculate a split's shard count (`assert
      resizeRequest.getResizeType() != ResizeType.SPLIT : "split must specify the number of shards
      explicitly"`, `TransportResizeAction`) even though it does auto-calculate for shrink -- a
      real signal from core's own maintainers that naming and partition count for a split are
      always the caller's decision. `ProvisionSplitTargetsAction` (new files:
      `ProvisionSplitTargetsRequest`/`Action`/`TransportProvisionSplitTargetsAction`/
      `RestProvisionSplitTargetsAction`, REST-exposed as `POST
      .../_resharding/_provision_split_targets/{source}?target_indices=a,b`) follows that same
      contract exactly: every target name is caller-supplied, every call, zero naming algorithm.
      `number_of_shards` fixed at 1 per target (this plugin's own per-shard split model, distinct
      from core's single-multi-shard-index resize); `number_of_replicas` and (if set)
      serverless-storage opt-in inherited from the source; mapping inherited from the source.
      Refuses cleanly (no partial creation) if a named target already exists or if the source is
      itself already a split target. Tested in `ServerlessStorageProvisionSplitTargetsActionIT`
      (2 tests): settings/mapping inheritance proven correct and targets proven independently
      writable/searchable; a name-collision refusal proven atomic. Confirmed meaningful by
      disabling mapping inheritance and watching the test fail on the missing field, then
      restoring. Full plugin quality gate and the entire `internalClusterTest` suite pass clean.
      Still deliberately a separate, explicit, operator-triggered action -- not wired to
      `ShardSplitCandidatesAction`'s threshold/hysteresis signal, same "signal exists, no auto
      action" boundary this plugin already draws elsewhere.

      **What A15 (real end-to-end orchestration) still needs, now that the precondition is
      closed**: chaining `ProvisionSplitTargetsAction` -> the existing per-partition
      `ShardSplitAction` calls -> `CutoverSplitRoutingAction` -> `EnableWritePartitionRoutingAction`
      into one resumable sequence, still explicitly operator-triggered (this action deliberately
      does not do that chaining itself). Not attempted here.

- [x] **Elasticsearch-Serverless-style write-load autosharding -- built and verified.** Elastic
      Cloud Serverless's own "autosharding" (researched directly, not assumed) never live-splits an
      existing shard: it tracks a `write_load` metric per data stream and uses it only to decide
      the shard count for the data stream's *next rollover generation* -- the old backing index is
      never touched again once rollover happens, so there is no live-split consistency window to
      solve at all. Built on OpenSearch core's own existing machinery, confirmed by tracing the
      real code: `MetadataRolloverService.rolloverDataStream` creates every new data-stream backing
      index via the exact same `MetadataCreateIndexService.applyCreateIndexRequest` path any index
      creation uses, which invokes every registered `IndexSettingProvider.getAdditionalIndexSettings`
      (`MetadataCreateIndexService.java:1159-1161`), *including* on data-stream rollover -- and this
      plugin already implements that exact interface (`ServerlessStorageIndexSettingProvider`).

      **The real, precise blocker this design works around**: `IndexSettingProvider
      .getAdditionalIndexSettings(String indexName, boolean isDataStreamIndex, Settings
      templateAndRequestSettings)` has **no `ClusterState` parameter** (confirmed at the real call
      site, `MetadataCreateIndexService.java:1159`) -- it cannot synchronously read the cluster-wide
      `writesPerMinute()` aggregation `ShardSplitCandidatesAction` computes, since that aggregation
      requires a network fan-out to every data node, which this hook cannot perform (it runs
      inline during cluster-state processing, no I/O).

      **What was built**: `DataStreamShardCountAdvisorSchedulerTask` -- a new periodic background
      task matching this plugin's own existing scheduled-task shape (mirrors
      `ScaleUpCandidatesSchedulerTask`), gated behind a disabled-by-default node setting
      (`serverless_storage.resharding.shard_count_advisor.eval_interval`, `TimeValue.MINUS_ONE` by
      default). On each cluster-manager-only evaluation it calls `ShardSplitCandidatesAction` once
      (the existing `writesPerMinute()` aggregation) and, for each serverless-storage-enabled data
      stream whose current write index is a sustained-high candidate, records a recommended
      next-generation shard count (current count doubled, capped at 32) into
      `DataStreamShardCountAdvisorCache` -- a small thread-safe in-memory map keyed by data stream
      name. `ServerlessStorageIndexSettingProvider.getAdditionalIndexSettings` now consults that
      cache synchronously (plain in-memory read, no I/O) whenever `isDataStreamIndex` is true and
      the template/request left `number_of_shards` unset, injecting the cached recommendation --
      unconditional on serverless-storage being enabled on the index itself, since the signal is
      purely about data-stream shard topology, not the storage engine. Data stream name is
      recovered from the about-to-be-created backing index's own name via the new
      `DataStreamBackingIndexNames.parseDataStreamName`, reversing core's
      `.ds-<streamName>-<generation:%06d>` format.

      **A real bug found and fixed by the plugin's own unit test, before any integration testing**:
      the first `DataStreamBackingIndexNames` regex assumed the generation suffix was always
      exactly 6 digits; core's `%06d` is a *minimum* width, not exact, so a generation >= 1,000,000
      produces a wider, still-all-digit suffix that the exact-6-digit pattern would parse
      incorrectly. Caught by `testHandlesAHighGenerationNumberBeyondSixDigits`; fixed by widening
      the pattern to `-\d{6,}$`.

      **Tests**: `DataStreamBackingIndexNamesTests` (6 unit tests) and
      `DataStreamShardCountAdvisorSchedulerTaskTests` (4 unit tests, package-private
      `evaluateDataStream` invoked directly to avoid the network round-trip `evaluate()` itself
      requires) cover the parsing and decision logic in isolation. The new
      `ServerlessStorageDataStreamShardCountAdvisorIT` (2 tests, using the plugin's own test-only
      `dataStreamShardCountAdvisorCacheForTesting()` accessor to seed a recommendation directly
      rather than wait on a real sustained-write-rate evaluation) proves the one thing the unit
      tests can't: a real core data-stream rollover genuinely picks up the cached recommendation
      for its new backing index, and behaves as an untouched no-op when nothing is cached. Verified
      meaningful by temporarily disabling the cache-consultation branch in
      `ServerlessStorageIndexSettingProvider` and confirming the seeded-recommendation test fails
      (`expected:<5> but was:<1>`), then restoring. Full plugin quality gate and the entire
      `internalClusterTest` suite pass clean (incidentally also fixed a pre-existing
      `missingJavadoc` gap in `WritePartitionRoutingMetadata`'s four accessor methods, unrelated to
      this feature, hit while running the same gate).

      Same safety property as Part 1 and as Elastic's own real design: this only ever influences a
      **not-yet-created** index -- it never live-splits or otherwise touches an index that already
      exists, so there is no write-loss/consistency window to solve, unlike a live-split controller
      would have.

      **What A15 (real end-to-end orchestration for operator-triggered splits) still needs**:
      chaining `ProvisionSplitTargetsAction` -> the existing per-partition `ShardSplitAction` calls
      -> `CutoverSplitRoutingAction` -> `EnableWritePartitionRoutingAction` into one resumable
      sequence, still explicitly operator-triggered. Not attempted here -- deliberately out of
      scope for this increment.

## Effort B: Metadata-plane term-authority migration

### Design
- [x] B1/B4 investigated together -- **the term this effort must fence is core's own
      `IndexShard`/`Engine` primary term, not this plugin's separate `ShardHead.primaryTerm`.**
      Traced `ObjectStoreWriterEngine#replayWalOperations`'s `ReplayFloor = currentTerm - 1`
      (`WalReplayFencing.tla`'s own floor) directly to `engineConfig.getPrimaryTermSupplier().getAsLong()`
      -- core's `EngineConfig`, the same primary term core's own shard-allocation/failover
      machinery grants. `ShardHead.primaryTerm` (this plugin's own `shardstate` package) is a
      distinct, unrelated counter that only advances on `withPublishedGeneration` (a real manifest
      publish), explicitly *not* on lease acquisition/activation (`withRenewedLease`'s own javadoc
      is explicit: "term advancement is deliberately not this method's job"). This resolves B1's
      original framing: there is no plugin-level term-grant CAS to design, because the term this
      effort needs to fence is core's, not this plugin's own. B4's blast-radius concern is
      therefore not hypothetical -- traced core's own term-bump entry point to
      `IndexShard#bumpPrimaryTerm` (`server/src/main/java/org/opensearch/index/shard/IndexShard.java`,
      called from at least two sites, including primary-relocation/promotion), deep, shared,
      every-shard-in-the-cluster machinery, not something reachable from a plugin-level engine
      seam (the term bump happens during cluster-state application, before any `Engine` --
      serverless or classic -- is even swapped in). **Conclusion: the atomic (term, WAL-position)
      CAS this effort wants would have to be built inside or immediately alongside
      `bumpPrimaryTerm` itself** -- e.g. a pluggable "primary-activation hook" core seam that fires
      synchronously as part of the same state transition that bumps the term, letting a
      serverless-storage shard snapshot its WAL position atomically with the term bump rather than
      afterward in the engine constructor as today. No such seam exists in `IndexShard` today.
- [x] B2. Decide where the combined (term, WAL position) record lives. **Resolved: nowhere new --
      no cluster-state record needed at all.** Read `IndexShard#bumpPrimaryTerm`'s real call site
      (`updateShardState`, the primary-promotion path, `IndexShard.java` around line 880): the term
      bump already takes an `onBlocked` callback that runs *after* operations are blocked but
      *before* they're unblocked -- and that exact callback is already where core resets/promotes
      the shard's engine on promotion today ("Resetting engine on promotion of shard... to
      primary"). This is a real, already-existing seam, atomic with respect to the term bump by
      construction (no operation can observe the new term until this callback finishes). A
      serverless-storage hook piggybacking on this same callback could snapshot
      `activationWalPosition` in-memory, at exactly the right atomic instant, with zero new
      cluster-state schema.
- [x] B3. Determine the failure-mode story. **Resolved, reusing existing machinery, no new failure
      surface needed.** `bumpPrimaryTerm`'s existing `onFailure`/`innerFail`/`failShard` path
      already exists for exactly this callback's own failures -- a hook's WAL-position-snapshot
      call throwing would flow through the same already-tested shard-failure path, not a new one.
      The one real residual risk, by direct analogy to &sect;12's own "self-review pass caught a
      real leak risk" lesson (deferred `DedicatedWalGcConfig` construction to avoid a resource leak
      if construction fails after partial setup): if the hook's snapshot succeeds but engine
      construction fails for an unrelated reason afterward, the snapshotted position must not be
      treated as durably valid until the engine it belongs to actually exists -- the same
      "construct late, at the exact moment the resource is needed" discipline already applied
      there should apply here too.
- [x] B5. Decide backward compatibility. **Resolved: confirmed genuinely free for classic shards.**
      The `onBlocked` callback `bumpPrimaryTerm` already takes is a plain `CheckedRunnable`
      supplied by the caller (`IndexShard` itself, not the engine/plugin layer) -- it already runs
      unconditionally for every shard, classic or serverless, doing whatever that specific
      promotion path needs (segment-replication engine reset, translog resync, etc.). Adding one
      more optional step to that existing callback -- gated behind "is this a
      serverless-storage-backed shard," the same kind of check `EnginePlugin#getEngineFactory`
      already makes today -- costs a classic shard nothing extra: the check itself is cheap and the
      new step is skipped entirely. No default-no-op interface needs inventing; the existing
      callback already is that seam.

### Formal verification
- [x] B6/B7. **Resolved by code-correspondence tracing, not new TLC-checked states.**
      `WalReplayFencing.tla`'s `AcquireLease` action already models the (term bump,
      `leaseTransferWalPos` snapshot) pair as one atomic step -- both change together, nothing else
      can interleave. `FixedReplayNeverIncludesAPostFencingWrite`'s exhaustive HOLD result
      (603,722 states) is only a faithful guarantee for the real system if core's real
      primary-activation path can actually provide that same atomicity. Traced directly against
      `IndexShard#bumpPrimaryTerm`/`#updateShardState`: **B7** (no new race at the CAS boundary) --
      `bumpPrimaryTerm` asserts `Thread.holdsLock(mutex)` on entry, so only one term bump per shard
      can be in flight at a time; two nodes/threads cannot race the same shard's grant
      concurrently, exactly matching this model's own single-`term`-variable assumption. **B6**
      (does the real design close the gap) -- the term bump and the `onBlocked` callback (the real
      candidate hook site identified in B1/B2) both run inside the same `asyncBlockOperations`-guarded
      window: operations are blocked from before the term increments until after the callback
      returns, so nothing can append to the WAL between the two halves of what the model treats as
      one atomic step. The real design's atomicity is not weaker than what `AcquireLease` assumes,
      so the existing exhaustive proof applies to the real design as specified, not just to the
      abstraction. Recorded directly in `WalReplayFencing.tla`'s own STATUS section. No new
      violation found; the Java implementation itself remains not yet done (still withheld pending
      authorization, per B4/B8).

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
  updated with a status note. Committed and pushed as e6a8ba72130.
- Effort A increment pushed; moved to Effort B design work (B1-B5) since it's pure investigation --
  no code changes, so no security-classifier review needed yet. Real finding: traced the term this
  effort must fence to core's own `EngineConfig#getPrimaryTermSupplier()` / `IndexShard#bumpPrimaryTerm`,
  NOT this plugin's own `ShardHead.primaryTerm` (a separate, manifest-generation-scoped counter that
  doesn't advance on activation at all). This reframes B1 from "design a plugin-level CAS" to "core's
  own bumpPrimaryTerm is where a fix would need to live" -- confirms B4's blast-radius concern is real
  and precisely locates it, and suggests the actual fix may be a smaller, more surgical synchronous
  hook rather than a new cluster-state-replicated record (B2 potentially simplifies). RFC §7.1 updated
  with this finding. B8 (implementing the hook inside `bumpPrimaryTerm`, private/every-shard/mutex-held
  core machinery) remains explicitly not attempted without explicit human authorization -- same bar
  the REST-scope-widening precedent set earlier in this project, and for the same reason: this is a
  cross-cutting core change to shared primary-term-grant machinery, not a plugin-local decision.
  B6/B7 (TLA+ model extension) not yet started -- next candidate task.
