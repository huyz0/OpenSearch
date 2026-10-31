# ADR: consensus feasibility and physical log groups

Status: accepted direction for implementation; production activation remains gated.
This records M2 findings, not completion of the low-latency runtime.

## Decisions

1. Use Apache Ratis 3.3.1 as the candidate consensus backend. Introduce it through
   the inactive runtime adapter in `serverless/log`; enable shell traffic only after
   the security, storage, licensing, and failure gates pass. Reuse its election,
   durable replication, membership, and snapshot protocols.
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

## Initial group activation and voter lifetime

The static runtime gates initial logical writes on a seed entry committed on all
three initial voters, followed by a majority-committed activation marker. This
protects failover from Ratis's empty-log vote exclusion. Watching index zero does
not establish nonempty durable logs. Once activated, normal writes use majority
persistence; replay restores activation after restart. This is a local protocol
barrier, not the unfinished durable controller reservation/activation state machine.

A journal volume is leased until its JVM terminates, including after failed
startup or successful SDK close. A real-process stalled-I/O test showed native
close returning while the log worker still held unfinished disk work. Holding the
OS lock until process exit prevents a replacement voter overlapping that work.
In-process hot reopening is unsupported. Membership changes within a running
journal remain separate work; this decision does not change Raft election rules.

## Permanent bootstrap reservations

The dedicated journal catalog stores an immutable deployment UUID and one
conditional register per physical group, separate from legacy descriptors,
shard-head leases, and node discovery. A reservation permanently binds the unique
group UUID and its canonical initial voter identities, addresses, and verified AZ
labels. It cannot be overwritten to create an independent group after quorum
loss. Initial reservation and activation are lifecycle writes; they add no S3
request to normal consensus appends.

The controller validates reservations before opening local journal volumes. After
the all-initial-voter seed barrier and majority activation marker commit, it
publishes that first marker with conditional replacement. Retries reconcile the
same durable marker after lost replies. Logical-shard assignment must wait for the
active catalog state. Catalog storage I/O runs on a bounded controller executor,
never on Raft completion threads.

On recovery, catalog activation evidence is passed into the journal. Missing
replay cannot trigger fresh activation, and a different committed marker fences
the local journal before advancing its applied prefix. This is historical lineage
evidence, not authority for a current read, and it does not prove completeness of
later log tails. Full local corruption-boundary and S3 recovery-catalog proofs
remain required.

The first catalog implements reservation and activation only. It does not yet run
from the node lifecycle or authorize voter replacement. Safe learner catch-up and
membership changes need separate durable transitions; editing genesis placement
is not a membership protocol. Local conditional-register tests and real local
journal integration do not substitute for cloud or independent-AZ qualification.

## Native learner and atomic replacement probe

A local SDK probe with four server instances in one JVM verifies
non-voting learner catch-up, followed by a direct old-three to new-three voter
replacement using native joint consensus. It preserves the existing 201-operation
restart probe. The added history catches up operations 1–20, stalls the learner's
persistence for operation 21, then confirms voter `ALL_COMMITTED` succeeds while
the learner remains behind. After resuming persistence, it requires that learner's
own committed prefix to cover the cut, replaces one non-leader voter, rejects a
stale configuration CAS, writes 22–25, closes the leader, and commits operation 26
using the remaining original voter plus its replacement. Both probes passed three
iterations. This is native API feasibility, not production membership integration,
independent voter processes, authentication, or AZ-failure qualification.

Important Ratis 3.3.1 contracts for runtime integration:

- `ALL_COMMITTED` covers voters. It cannot certify that a listener has caught up.
  The local leader's public `Division.getCommitInfos()` includes listener progress
  keyed by peer identity. Require the intended learner's committed prefix to cover
  a committed transition barrier, with matching group, controller intent, and
  leadership/configuration checks. This historical cut is not a current-read API.
- Native staging uses `matchIndex + gap > committedIndex`. A gap of one requires
  coverage of the committed index; zero incorrectly requires being ahead. The
  probe uses one. Existing listeners promoted to voters skip fresh staging, so
  explicit learner evidence is still necessary.
- Use `SetConfigurationRequest.Mode.COMPARE_AND_SET` with expected voters and
  listeners. Native CAS compares peer identities; it does not bind full addresses
  or a controller revision. A durable pending transition must bind those fields
  and serialize configuration changes before a local request is issued.
- Replace old three with new three atomically after catch-up. A stable four-voter
  intermediate configuration needs three votes and can lose availability when
  two voters temporarily share the AZ being replaced.

The runtime still needs durable transition records, authorized learner startup,
restart reconciliation, and fencing of retired identities. Genesis reservations
remain immutable. No reconfiguration API is enabled by this probe.

## Local persisted-prefix evidence

A process test removed a follower's newest acknowledged segment while preserving
its identity, initial activation, native term/vote/configuration files, and earlier
closed segment. The prior runtime accepted startup. A stable identity and CRC on
remaining bytes do not prove that a previously persisted suffix still exists.

Each physical group now has a fixed two-slot, checksummed local boundary file,
scoped to deployment, voter-volume UUID, and group UUID. An opt-in native worker
hook forces that evidence after log data force and before the flush index or
replication acknowledgement advances. The boundary is local durability evidence,
not a commit index, read authority, or archive/purge permission. It costs an extra
local metadata force per native flush batch; workload latency/IOPS qualification
remains required.

Recovery checks both slots before SDK startup; it refuses corrupt/torn slots
rather than falling back and hiding a newer acknowledged boundary. It verifies a
contiguous unpurged prefix covering the required position using the pinned native
CRC reader, with entry/file/directory bounds. Only the required segment and newest
closed/open tails need additional reading. Valid existing metadata is re-forced;
valid unacknowledged open-tail data is verified and forced before its observed end
is adopted as durable and voting begins. Log contents are not repaired or removed.

A worker-ordered pre-truncation hook can lower the boundary for a legitimate
uncommitted suffix; generations still increase. The upstream state-machine
truncate callback occurs earlier under the Raft write lock, so its timing cannot
provide this evidence. Segment finalization and truncation propagate output errors
and force directory changes before persistence progress advances. Safe synchronous
flush is mandatory. Consensus selection, ordering, and voting algorithms remain
unchanged.

The missing-tail test now refuses startup. A second process history persists a
long uncommitted suffix, elects a different quorum, discards that suffix, and
restarts all voters with the committed history intact. Binding format 3 rejects
older experimental volumes without this evidence. Snapshot/purged-prefix recovery,
arbitrary storage rollback/old-backup restoration, and production recovery/replacement
remain separate gates. The feature remains disabled.
