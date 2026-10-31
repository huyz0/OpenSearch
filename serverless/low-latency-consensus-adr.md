# ADR: consensus feasibility and physical log groups

Status: accepted direction for implementation; production activation remains gated.
This records M2 findings, not completion of the low-latency runtime.

## Decisions

1. Use Apache Ratis 3.3.1 as the candidate consensus backend. Keep it test-scoped
   until the runtime adapter, security, storage, licensing, and failure gates pass.
   Reuse its election, durable replication, membership, and snapshot protocols.
2. Explicitly disable unsafe log flushing. The initial implementation also disables
   asynchronous flushing, establishing a simple synchronous persistence baseline.
3. Bound physical consensus groups per node. Multiplex logical shard streams within
   those groups; do not allocate a Raft division for every index shard by default.
4. Start with a private preparation engine and a committed-serving replica engine.
   Measure the duplicate indexing cost before accepting it for production. Retain
   the prepare/commit core-hook alternative if it misses the performance gates.
5. Keep conditional preparation behind dependent commit barriers. Never evaluate
   CAS, create/upsert existence, or scripts against speculative state whose log
   append could fail without committing.

## Evidence

### Existing-path regression

After the timing and append/checkpoint foundation commits, the full existing suite
ran 1,432 cases with zero failures/errors and two skips in 8m26s. This establishes
regression coverage for those commits, not quorum correctness.

### Persistence and replay probe

`RatisPersistenceSpikeTests`, run by `:serverless:testkit:consensusTest`, starts
three loopback replicas with persistent local files, commits 100 operations,
closes the leader, commits 100 more, closes all replicas, recreates all state
machines from disk, and commits operation 201. Reply counts verify that replay
restored the preceding history. Three randomized iterations passed.

This uses graceful server closes on one machine. It does not prove process-kill,
power-loss, partition, corruption, reconfiguration, or actual AZ-loss behavior.
Those are required M3 and qualification tests. It also does not yet integrate
document operations, S3 archival, authorization, or index lifecycle transitions.

Ratis 3.3.1 source inspection identified:

- `SegmentedRaftLogWorker` can advance flushed progress before synchronization
  completes when unsafe flushing is enabled. That mode is prohibited.
- The synchronous path reaches `BufferedWriteChannel.flush`, which calls
  `FileChannel.force(false)` before completing the corresponding log work.
- Segment creation/rename, parent-directory durability, metadata/vote persistence,
  failure propagation, and filesystem/storage assumptions still require a dedicated
  production audit. A source-level `force` call is not a complete crash proof.
- The follow-up metadata audit confirmed that the upstream atomic metadata stream
  syncs its temporary file but not the renamed parent directory. The curated
  [ratis-common module](ratis-common/README.md) fixes that filesystem boundary;
  it preserves the protocol and every other upstream class/resource byte. Actual
  directory-sync failure is covered by a regression test. Segment-directory and
  bootstrap persistence still need their runtime-specific integration.
- Eight JVM-global daemon timeout Timers have no close API. The probe filters only
  those named timers in its Ratis peer thread groups, with TimerThread stacks;
  server/appender/executor leak checks remain enabled.

The dependency closure is enumerated explicitly because OpenSearch disables
transitive dependencies. Shaded Ratis third-party code remains separate from
OpenSearch Netty/protobuf packages. The probe uses the repository's existing
SLF4J API; full binary compatibility and runtime dependency/license validation
remain gates before moving anything from test scope to production scope.

### Committed-serving engine probe

`CommittedServingEngineTests` uses native primary and replica `IndexShard`
instances. The primary prepares insert/update/delete operations while the replica
applies only the manually committed prefix. Refreshing the primary and replica
before application leaves serving GET/search state unchanged. After committed
application, original `_seq_no`, `_primary_term`, and `_version` are preserved.
Three randomized iterations passed.

This proves a useful engine boundary. It does not establish routing isolation,
automatic commit admission, crash recovery, query token semantics, or the cost of
running two engines. The actual shell must also isolate private index services:
one `IndexService` cannot host two copies of the same `ShardId`. Evaluate an
unadvertised preparation service with separate local identity and preserved public
index semantics, or a separately owned preparation service. Test real shell
lifecycle, mappings, plugins, and cleanup before committing to that integration.

The serving copy must use writable replica semantics for committed replay. The
existing S3 read-only engine cannot simply accept streamed operations. Likewise,
replica-origin operations cannot be applied to a primary-mode shard by bypassing
core assertions. No assertion disabling is part of the design.

### Physical group footprint

Reproduce the bounded probe with:

```sh
./gradlew :serverless:testkit:consensusBenchmark \
  -Dtests.consensus.groups=10
```

It runs three local replicas in one test JVM, adds logical consensus groups to
the same servers, and commits one operation in each group. The JSON artifact is
`serverless/testkit/build/benchmarks/consensus-groups.json`. Use
`-Dtests.heap.size=1g` to change the repository test JVM heap; the report records
the actual maximum heap. The feasibility probe deliberately caps group count at 16.

Observed on JDK 25.0.3:

| Measurement | Result |
| --- | --- |
| Test JVM thread count with one group | 66 |
| Test JVM thread count with ten groups | 241 |
| Increment across nine additional groups / three replicas | 175 threads |
| Actual maximum heap in that run | 512 MiB |
| Heap used at first / last sample | About 237.5 / 78.5 MiB |

Heap samples are affected by allocation and GC and cannot be interpreted as
retained per-group memory. An isolated 256 MiB run stalled while adding its tenth
group; an isolated 1 GiB run completed with roughly 453 MiB process RSS. These are
resource warnings, not a universal minimum-memory claim. Measure retained heap,
direct buffers, queue occupancy, file cache, and native thread stacks under load.

The thread slope alone makes a default one-group-per-logical-shard deployment
unattractive at thousands of shards. There is no claim that a 10,000-group cluster
was tested. Instead, qualify thousands of logical shard streams over a bounded
physical group count in the next implementation stage.

## Shared physical group requirements

- One physical group carries multiple logical shards, potentially from multiple
  indices in the same authorized deployment. Do not share a group across deployments.
- A committed envelope identifies the shard incarnation/activation, original
  operation identities, schema dependency, and operation kind. Group log indexes
  are not document sequence numbers.
- The group leader coordinates write authority. Any separate shard preparation
  owner is bound by a committed ownership epoch; S3 discovery leases cannot create
  an independent competing authority. Prefer colocating preparation actors with
  the group leader initially to avoid another foreground network hop.
- Bounded fair admission prevents a hot shard from consuming every log slot,
  queue, fsync batch, or catch-up byte. Group assignment must support isolating
  hot shards without silently reordering history.
- A search replica receives payloads only for assigned shards. Because filtering
  omits other group entries, trusted progress barriers must account for skipped
  intervals. Seeing a later shard operation alone does not prove a contiguous
  group prefix was consumed.
- Group snapshots contain configuration/ownership state and a per-shard recovery
  catalog cut. Never truncate group history based on only one shard's newest base.
- Move a shard between groups using a committed source closing barrier, durable
  transfer proof, and destination activation. Define token lineage and restart
  handling; a changed hash assignment alone cannot transfer authority.
- Bound group count, active preparation engines, subscriptions, and disk/IO queues
  separately. Physical groups, shard count, search copies, and voter count are
  independent configuration dimensions.
- Verify cold activation, group leadership movement, and hot-shard isolation at
  1,000 and 10,000 logical shards before claiming scalable operation.

## Remaining M2/M3 gates

The persistence/engine probes justify proceeding, but are insufficient to enable
a new storage mode. Next deliver the versioned committed envelope and shared-group
adapter, with no speculative serving and a bounded disk/memory model. Complete the
shell preparation-service integration and measure its overhead. Before real
traffic, require mutual authentication, activation binding, membership validation,
disk-full/corruption/kill tests, safe state snapshots, and explicit legacy-mode
exclusion. Keep all production behavior on the existing path until those gates pass.
