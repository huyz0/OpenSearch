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
4. Start with a private preparation engine and a committed-serving engine using
   native non-primary operation planning.
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
running two engines. One `IndexService` cannot host two copies of the same
`ShardId`. The integration direction below uses a private engine directly rather
than registering a second copy of the shard in another public index service.

The serving copy must use writable replica semantics for committed replay. The
existing S3 read-only engine cannot simply accept streamed operations. Likewise,
replica-origin operations cannot be applied to a primary-mode shard by bypassing
core assertions. No assertion disabling is part of the design.

### Primary-serving adapter for M4

The core `EngineConfig` already supports `PrimaryOperationPolicy`. The shell's
`CommittedServingEngine` uses this extension to accept resolved primary-origin
operations with upstream sequence numbers, terms and versions. Its policy plans
those operations using native non-primary conflict handling. Planning receives a
replica-origin copy; the actual applied operation retains its primary origin and
identity. CAS and version allocation belong to preparation and are not repeated
during application. Ordinary primary requests without assigned identities fail
before changing the serving engine. Gap filling cannot invent operations absent
from the committed log.

This permits the public primary `IndexShard` to apply committed records through
`applyTranslogOperation(..., PRIMARY)`, retaining standard parsing, GET/search,
routing, and indexing listeners. It avoids applying replica-origin writes to a
primary shard. The engine advances the update/delete boundary conservatively for
every assigned index/delete, so replay and out-of-order delivery cannot use an
unsafe append-only optimization. This trades some indexing efficiency for a
simpler initial correctness boundary.

Keep one public `IndexService` and one public serving shard. A bare private
`InternalEngine` now has a shared-mapper local journal/native-shard integration
probe. It uses `PrivatePreparationEngineConfig` to require separate canonical
filesystem index/translog paths, restore default primary operation planning, and
create independently owned merge policies through separate index settings. Its
initial Lucene buffer receives an explicit byte allowance; a core system-property
override cannot exceed that allowance. Public refresh listeners, warmers, query
cache access and failure callbacks are removed. Private failures notify only the
private context. Custom committers, data formats and document lookup/metadata
providers fail closed in this prototype.

This configuration builder neither opens an engine nor owns its lifetime. The
host still needs resident-engine admission, private version-map/buffer flushing,
SSD watermarks and cleanup before runtime integration. A Lucene buffer allowance
does not cap total heap, native memory, merge IO or retained translog history.
Share mapper/analyzers without another plugin/index registration, and close the
private engine before their owner closes. Direct preparation must also be checked
against plugins that depend on shard hooks; custom engines cannot be silently
enabled. Conditional operations in the probe wait for the prior journal commit
and serving application. It does not yet implement scripts, retry sequencing or
private-engine rebuild after an ambiguous append.

`PreparationEngineAdmission` supplies the initial resident-context gate. It keeps
at most 256 active records and checks both declared resident bytes and context
count before resource opening. Cold descriptors consume no entry. Assigned groups
retain one context and the maximum context-byte allowance while idle; another
group can borrow spare capacity without consuming those protected reservations.
Duplicate shard contexts, unknown groups and invalid budgets fail before native
work. Ticket release is idempotent. Uncertain cleanup can quarantine its native
resource references and retain the reservation for this admission instance's
lifetime; close cannot silently free that capacity. The host must retain the
admission instance while such resources may remain alive. This is accounting
infrastructure for the owned context, not an enabled resident-engine controller
or a measured 256-engine memory qualification.

`PrivatePreparationResources` now owns an admitted fresh generation. It acquires
the ticket before filesystem/native opening, refuses a nonempty serving engine
or an existing generation, and atomically claims the directory before creating
its private filesystem store/translog. Its private shard lock cannot release the
public shard's node-environment lock. Normal close finishes native IO, closes the
store, deletes only the claimed generation, then releases admission. Once native
initialization has started, a failed constructor with unpublished resources is
quarantined rather than treated as a clean rollback. Subsequent native work can
still use the retained store, and another opener cannot adopt the generation.
Concurrent/idempotent close cannot release capacity before cleanup finishes.

The host must independently establish empty committed history and ownership;
an empty local engine is not proof of an empty durable stream. Rebuild from a
verified prefix, mutation sequencing, version-map/SSD governors and parent index
lifecycle integration remain pending. Keep private generations outside journal
binding directories, retain quarantine owners until process-level recovery, and
do not close their shared mapper/analyzer owner while native IO may remain alive.
The real local journal probe now uses this resource owner rather than a second
preparation shard or a test-only native-store helper. It still enables no client
storage mode.

Private mutation application now goes through the resource owner's index/delete
boundary. A bare native engine accepted a parsed document with an unresolved
dynamic mapping as `SUCCESS`. The owner checks that update before invoking native
indexing and returns the same `MAPPING_UPDATE_REQUIRED` result as `IndexShard`,
without consuming a sequence number. A retry reparses against the installed
mapping. Assigned/replayed operations, replica origins and operations in a
different fixed term are rejected before private history changes.

The boundary preserves native results, including conditional failures and missing
deletes. Only successful resolved operations can proceed to append; callers must
inspect the result type. An assigned-sequence failure or ambiguous IO still needs
to fence/rebuild the private context. The tests' quiescent local mapping update
is not a durable schema barrier, current ownership proof, a script adapter or
public lifecycle integration. The real journal probes use this boundary too.

The owned private engine also samples native writer, complete version-map
(including delete tombstones), and in-flight refresh/flush byte estimates. It uses
the native writer-statistics hook with a reused accumulator, without walking
segment readers on the mutation path. The ordinary indexing-buffer counter omits
tombstones: a delete/refresh loop reproduced continued admission beyond the
configured tracked-buffer allowance.

Before and after each guarded mutation, excess buffers trigger an internal refresh
to spill recoverable data to local SSD. If retained estimates still exceed the
allowance, or native buffer work fails, the owner fences the context and refuses
further preparation. Recent delete versions are preserved for conditional and
external-version semantics. Admission stays charged until successful close.
Tests cover tombstones that survive every refresh, actual local segment output,
continued bounded live indexing, conditional deletion after spilling, and an
unchanged public serving reader.

This is an operation-boundary governor for known native buffer estimates. One
operation can overshoot before refusal; parsing/request admission, analysis
amplification, merges, mapping/index overhead, mmap residency, total host memory
and local disk occupancy still need their own bounds. The setting is not a hard
heap/RSS ceiling, and this does not enable client quorum mode or private rebuild.

`PreparationCommitSequencer` adds a dependency gate around preparation, append
and verified application. A stream has one pending preparation until the prior
record is durably appended and applied. Failure fences that context; caller
cancellation cannot cancel accepted persistence or admit another preparation.
Native work, SDK submission, application and result callbacks run outside its
state monitor. Asynchronous close waits for preparation/application before the
host releases private resources, but it does not wait indefinitely for an
accepted quorum reply. A reply arriving after close cannot apply to a closed
serving lifecycle. Original operation/checkpoint correlation is checked before
success is reported.

This initial gate conservatively fences every preparation exception and does
not pipeline independent keys. The native adapter still needs proven no-change
rejection handling, mapping barriers, request-byte admission and bounded runtime
dispatch and host-level admission. Preparation/application now require an
explicit executor: runtime callers must supply bounded group workers with room
for already-admitted application handoffs, without inline/caller-runs fallback.
The real local probe uses a one-worker, one-slot executor and asserts worker
ownership for native preparation/application. A real journal test holds a committed reply, cancels the client view, verifies
that a dependent preparation is rejected, then applies the insert and performs
a conditional delete. No client storage mode or throughput qualification is
claimed.

The sequencer distinguishes queued jobs from currently running native work.
Close can skip queued work safely; running preparation/application must finish
before resources can close. Each job balances its work count in its own finally
block, even if dispatch or callback registration reports a failure after work
starts. Rejection fences the private state and never runs native application on
the journal callback as a fallback. Closed/fenced queued attempts clear retained
preparer/payload references; closed gates also detach their resource callbacks.
An accepted journal append still runs to its real outcome. Per-host request-byte
admission and group worker ownership are integration requirements, not supplied
by the executor parameter alone.

`PreparationWorkDispatcher` now owns one bounded worker per assigned physical
group, using the host's thread context. It admits bounded active bindings and
pending request count/declared bytes, with one binding and a maximum-size request
reserved per group. Reuse the tested constant-time reservation accounting; cold
descriptors do not allocate a binding. A stream admits one pending preparation.
Workers have enough queue slots for admitted application handoffs and finishing
runners, without caller-thread fallback.

A request keeps its admission until its outcome is known and all queued/running
wrappers drain. Closing a queued gate cannot prematurely free that byte charge.
Binding close waits for retained wrappers and running work, then releases its
binding reservation; accepted journal persistence is not canceled or awaited
indefinitely. An unknown scheduling failure retains capacity until its wrapper
drains, or until process recovery if it never runs. No native work/result callback
runs under the host state monitor. The real journal probe uses this dispatcher,
including cancellation and a dependent conditional delete.

These are declared request limits and active binding bounds, not measured heap,
resident engine ownership, a durable retry cache or logical ownership/schema
controllers. The trusted adapter still computes request weights, opens/closes
private/serving resources in the correct order and supplies verified replay.
The capacity model charges both request-stage and journal budgets independently.

The adapter itself is not a commit verifier and is not enabled by the launcher.
Assigned operation identities are not evidence of commitment. The host must
validate the committed stream, ownership epoch, schema and shard incarnation
before invoking application. Quorum replay after local data loss, term changes,
private-engine rebuild after ambiguity, mapping/lifecycle barriers and bounded
engine admission remain M4 integration work. No duplicate-indexing cost or
millisecond latency qualification is claimed by these engine tests.

`ResolvedMutation` supplies the shell payload for a future committed application
pipeline. Its version-one format uses explicit result codes and an internal UUID,
original sequence/term/version, document ID, nullable routing, raw source and
auto-ID timestamp, or a no-op reason. It preserves a missing-delete result as
well as created/updated/deleted results; replay cannot reconstruct the original
response from current engine state. All variable fields have checked lengths,
text is strict UTF-8, and encoded size is checked before output allocation. Source
buffers cannot escape the immutable record. The envelope supplies the checksum,
shard activation, ownership and schema binding. A stable UUID field alone does
not implement retry deduplication or its retention policy. The legacy `WalRecord`
format and launcher behavior remain unchanged.

`CommittedMutationReplayer` connects a retained journal to a native serving shard
for one fixed, already installed ownership/term/mapping binding. Each poll obtains
its own bounded committed scan from `RatisJournal`; callers cannot supply a
deserialized checkpoint as commit proof. The target must be a primary with the
exact store owned by the supplied committed-serving engine. Replay validates the
binding, payload kind/term, and consecutive resolved sequence positions before
native application, including explicit no-ops. It checks the returned identity
and advances selected-stream progress only after successful application. Physical
scan progress also covers skipped streams. A failure permanently stops that
instance, preserving its successfully applied prefix.

The host must exclude concurrent shard lifecycle/mapping/term changes while
polling. This adapter starts at the beginning of retained history; it does not
accept an unverified resume cursor. Ownership/schema transitions, snapshot/archive
bootstrap and internal retry records need their controllers before runtime
activation. Its applied checkpoint is neither a refreshed-reader proof nor
linearizable latest-read authority. Run the local TLS/native-shard integration
with `./gradlew :serverless:testkit:journalReplayTest`; it is also part of the
testkit's checks. The launcher still does not enable quorum-mode index writes.

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

The probe established the native mechanics. The explicit controller integration
below adds durable intent, authorized learner startup and completion publication;
genesis reservations remain immutable. The probe alone enabled no runtime API.

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

## Reported local I/O failure

Independent voter JVM tests inject a persistent I/O exception on the native log
worker before log force. The healthy two voters can elect a replacement after a
leader's storage fails. If a follower fails storage and the other follower is
subsequently lost, another append cannot be acknowledged or exposed in the local
committed scan. The failed process retains its volume lease until termination.
Both histories passed three iterations without additional runtime changes.

An unsuccessful append remains ambiguous: it can have committed on another
quorum member. These tests do not establish availability under uninterruptible
I/O stalls or production hardware faults.

## Durable replacement serialization

Keep genesis immutable and serialize controller intent in a separate per-group
conditional register. `JournalMembershipRecord` binds activated genesis, completed
logical revision, current exact placement, and at most one pending operation nonce
and target. Replace one voter at a time while retaining two unchanged voters;
address changes require a new storage identity. A permanent per-group reservation
binds each replacement voter to its original intent, including an abandoned
contender. Previously retired identities cannot be reused in later revisions.

Publish the initial membership head before the ACTIVE genesis record. A missing
head after activation is corruption or incomplete recovery, not permission to
reconstruct revision zero. Lost replies reconcile the original values; competing
controllers cannot supersede unresolved intent. These lifecycle requests add no
per-document object-store operation and scale with physical groups, not shards.

Static startup rejects pending intent before volume access. The explicit assigned
startup and native replacement path described below handles authorized learners
and pending reconciliation. Intent alone is neither membership commitment nor
permission to start an independent history.

## Completion reentry and bounded dispatch

A repeated majority-loss test exposed a deadlock: the leader completed a watch
under its leader/watch locks, the activation continuation synchronously reentered
native async submission, and that method joined another client-executor thread
waiting for the held leader lock. Shutdown then waited indefinitely as well.
A separate focused history reproduced activation failing at the valid one-request
limit because its previous permit remained held while a continuation ran.

The initial fix used four bounded workers shared by physical groups. The M3a
isolation regression below replaces that sharing with one bounded worker per
active physical group. A permit stays charged through submission,
persistence and queued notification, and releases before public completion.
At most one queued task belongs to each charged permit, so the pending-request
limit bounds the queue. Workers close only after outstanding notifications drain;
late native results never fall back to running user continuations under SDK locks.
The exact failing seed and the one-permit history passed three iterations.
Cancellation, synchronous submission failure, slow callbacks at a full window,
and late completion during close have deterministic dispatcher coverage.

## Retirement of colocated assignments

Completed controller membership can retire one assignment while the same volume
continues hosting other groups. Under its exclusive process lease and before SDK
startup, recovery atomically moves each retired UUID directory beneath a
non-UUID `retired` parent, then forces both parent directories. Native discovery
never loads that old group. Recovery also re-forces an already-completed rename;
duplicates, foreign histories, corrupt bindings, or stale retirement authorization
are refused. Old files remain intact and occupy disk until authorized reclamation.
This uses filesystem operations and needs no additional native consensus patch.

A child-JVM history models completed controller authorization, then verifies that
one group stays excluded across two restarts while its colocated group continues
serving committed data. Its retired boundary bytes remain unchanged. This test
isolates retirement; it does not prove learner promotion or configuration commit.
The controller must establish those proofs before publishing completed membership.

Completed membership loads additionally verify every new voter's permanent
reservation and exact placement. Validation performs at most three direct reads
per group and never recursively walks transition history. Missing or rebound
identities fail closed, with no repair to an earlier membership revision.

## Retained metadata capacity at persistence and startup

A real-process recovery history accepted a log larger than its newly configured
retained-entry budget. Bounding file count and frame bytes did not bound the
number of log-entry metadata objects loaded by the native SDK. Ordinary proposal
admission also cannot cover automatic native election/configuration entries.

A cached ceiling now permits at most the configured retained watermark plus
`4 * maximumPendingRequests + 32` physical positions. The post-force/pre-ACK hook
checks it without acquiring a native Raft/log lock. Recovery checks both the saved
boundary and the verified observed tail before SDK startup and boundary adoption.
The history verifies refusal for a high saved boundary and for a synthetic low
boundary with a valid larger suffix, then successfully restores the original
capacity with acknowledged data intact. No bytes are deleted or rewritten.
This assumes unpurged history starting at zero; snapshot/purge integration must
switch to a retained-prefix count before reclamation is enabled.

## Inbound peer RPC execution

A real mTLS request-vote burst under a held consensus monitor created 64 blocked
RPC handler threads. The native async-request pool setting only configures its
client-write service, which this runtime denies; peer callbacks still used gRPC's
unbounded cached executor.

The first execution bound installed a separate four-worker node pool and
a bounded queue of `max(128, maximumPendingRequests)`. The protocol reserve keeps
one-slot proposal admission usable. Dispatch and peer execution remain separate,
so blocked peer handlers cannot consume the SDK submission/notification workers.
Shutdown closes native services before the peer executor, and preserves it across
interrupted cleanup. No consensus-library or third-party artifact patch is needed.

The original overload regression verified four blocked handlers, queue rejection rather
than new threads, and successful quorum append/replay after releasing the monitor.
The pinned transport reports executor rejection as INTERNAL, a failed peer RPC
that follows normal retry/backoff. The per-group dispatch below further confines
native callbacks to one worker per group. This qualification covers inbound execution;
outbound callback behavior and full memory/throughput sizing remain workload and
membership qualification gates.

## Authorized native replacement and completion

Assigned startup validates the winning durable intent before provisioning a
nonvoting learner. Original genesis and the learner's original transport binding
remain immutable through subsequent roles. Empty initialized learners can restart
before native listener addition without inventing missing vote or floor metadata.
Pending-aware recovery can reconcile native completion even when the controller
has not yet published a completed head; native consensus excludes retired voters.

Replacement adds one listener, commits a nonce-bound barrier and checks that exact
learner's committed prefix by UUID before atomic old-three-to-new-three native CAS.
The first barrier per logical revision/nonce survives duplicate proposals/replay;
a separate bounded tracker requests a fresh cut in each leader term. Controls use
the finite metadata headroom, preserving the hard native persistence ceiling.
Submission and completion use the existing bounded dispatcher outside native locks.

The controller obtains a historical receipt directly from committed/applied native
target state, publishes it before the completed head and never accepts caller-built
proof. Receipts retain the original intent and first barrier. A permanent exact-intent
nonce reservation precedes pending-head CAS, preventing a later revision or orphan
contender from reusing an operation identity and colliding with an older receipt. Elections may append
another identical stable configuration; the first published receipt wins while
later valid configuration coordinates remain acceptable proof of the same change.
Retries after later replacements use the permanent original receipt. Missing heads
or missing historical receipts cannot reset or reconstruct controller history.

Failed-learner recovery/abort, automatic provisioning, sustained replacement
resource qualification, command-line assignment/TLS loading and three-AZ qualification remain outstanding.

## Retired peer connection lifetime

A real mTLS peer RPC confirmed that a removed peer's established channel remained
open after native replacement. Completion publication and retries now use the
supported SDK reset to close the local unused transport client. Cleanup inspects
all local groups' current/previous voters/listeners before acting, so another
colocated group retains its connection. It runs on the controller executor, never
a consensus callback. A two-group history verifies retention until both changes
finish, closure afterwards and successful subsequent append/replay. No additional
vendor patch or object-store request is required. Lightweight SDK descriptors
remain subject to the current retained-history limit; archive integration must
bound/reclaim them before allowing unbounded history.

## Bounded node controller lifecycle

Explicit programmatic bootstrap now owns the authenticated journal and its catalog
controller before publishing metadata-plane availability. Native opening finishes
before startup returns; activation continues asynchronously so hosts can start
sequentially. Close and failed startup retain ownership of a late native open,
including the process-lifetime volume quarantine after SDK startup.

One dedicated worker and one queued task isolate controller I/O from application
and consensus work. Per-group pending controls and wakeups coalesce. Local 200 ms
reconciliation uses cached durable intent; catalog metadata polls every 30 seconds
or on an explicit placement-change signal, with five-second failure backoff.
These counts scale with physical groups. Current-read authority, searchable
checkpoints and the opt-in index write path remain separate work. Command-line
assignment/TLS loading and cloud qualification are not enabled by this host API.

## Leader removal with concurrent proposals

A process regression exposed an SDK shutdown cycle: a removed leader held the
consensus monitor while closing its client executor, whose worker needed that
monitor to finish an append. The host dispatcher was waiting for the SDK's extra
client-executor handoff. The curated proxy now recognizes an internal marker for
state machines whose host already bounds submission and detaches completions.
Those submissions use the host worker directly; other SDK users retain upstream
behavior. Consensus and durable acknowledgement rules are unchanged.

Local admission also checks division lifecycle and current/previous voter
membership alongside the native readiness hint. Removed or closing leaders
therefore refuse new proposals. The native-pool isolation regression fails on
the old path, and replacement tests drain all accepted concurrent proposals
before killing the old leader, then verify acknowledged history on the new quorum.
Connection cleanup remains local: a controller publishing or retrying completion
reconciles its own transport after checking every colocated group's membership.

## Restart before initial quorum

An original voter can persist its bootstrap configuration and term/vote without
receiving any log entry. Before the initial all-voter activation, a restart then
has no normal `raft-meta.conf`. Requiring that normal configuration stranded the
voter even though its original authority and vote were intact.

Assigned startup grants a narrowly scoped empty-original permit only while the
catalog has no activated genesis. Recovery still requires the immutable initialized
binding, voting metadata, native bootstrap configuration, empty durable boundary,
and verified empty log. Known activated heads, static startup without catalog
authority, missing vote metadata, and nonempty histories do not get this permit.
A child-JVM test grants a real vote, restarts the original voter, verifies the
term survives and another candidate is refused in that term, then forms the quorum
and verifies writes survive another leader loss.

## Term and vote corruption

A real process history demonstrated a second same-term vote after a one-bit change
in the separate native term/vote properties file. That file was atomic and forced,
but its contents had no checksum. Persisted log-entry checks cannot detect this.
The curated native writer now seals version, term and voted-for identity in the
same atomic publication; a bounded reader verifies the seal. A host preflight
requires this verified metadata before native startup and never uses the SDK's
initial-format missing-file default for established recovery.

Binding format four enforces this boundary in both upgrade and downgrade
attempts. Earlier unsealed experimental voters require authorized fresh-identity
replacement from a healthy quorum, not automatic checksum creation. Recovery
leaves refused files intact. This integrity guard does not replace the existing
restriction on copying live voter directories or resurrecting old disk snapshots.

## Proposal isolation between physical groups

A native-lock stall with four accepted proposals reproduced every shared host
submission worker blocking on one group. A healthy group led on the same host
then timed out. Separate histories filled the node-wide request or byte budget
with that stalled group and reproduced the healthy group's immediate rejection.

Use one bounded submission/completion worker per active physical group, with the
existing node-wide count and conservative byte ceilings. Reserve one maximum-size
proposal per group; borrowing may use other spare capacity but never another
group's reservation. Constant-time accounting tracks covered reservations and
restores them on release, including idempotent close and cancellation semantics.
Preflight rejects impossible reservations before creating the volume binding.

These histories prove the tested proposal boundary. The following peer RPC
history adds a separate execution boundary; shared-device failures and broader
cell routing/recovery isolation still need their own qualification.

## Peer RPC isolation between physical groups

Four request-vote calls blocked on one division's monitor occupied every worker
in the bounded node pool. A request for a healthy colocated group then exceeded
its three-second deadline. Keep the four-worker pool for gRPC decoding and native
handler initialization, and dispatch the parsed peer listener callbacks to one
bounded worker per assigned physical group. Preserve each call's callback order
and gRPC context without running native code under the dispatcher locks.

The node admits at most 128 calls. Its calculated ceiling reserves four calls per
group and may borrow additional capacity from a conservative pending-byte-derived
allowance. A classified group cannot consume another group's reservation. Each
call has at most eight queued callbacks and two queued messages; ready events
coalesce. Refusal, deadline, cancellation and shutdown retain the call's charge
until any running native callback finishes. The queue runner processes one event
at a time so one stream cannot drain an unlimited backlog ahead of other calls.

Calls without a first message expire after five seconds, with canceled timers
removed immediately. Before a body identifies its group, a call still occupies
the bounded shared admission/decoding path; this does not promise isolation from
an arbitrary header-only flood. Serialized wire-size checks and the capacity
model's message allowances are not a complete heap/RSS guarantee. Native async
work, transport buffering and sustained throughput need separate qualification.

Snapshot chunks are refused before the native callback because this journal has
no archive-backed snapshot recovery proof. Ratis's snapshot notification remains
allowed: with transfer disabled, an empty learner needs that notification to fall
back to full retained-log catch-up. Rejecting the whole RPC instead reproduced
membership staging timeouts. No consensus-library artifact patch is needed.

The focused three-iteration run passed 18 dispatcher unit cases and nine native
cases covering the original stall, leader replacement and colocated retirement.
Tests cover callback context/order, group reservations, overflow while a native
callback remains active, idle-call expiry, unknown/group-changing streams, and
snapshot-data refusal with notification acceptance. Broader validation is recorded
in the implementation progress log. The broad fault suite also exposed a fixture
that removed a follower's persistence before the third voter had caught up with
the initial write. The surviving leader exceeded its 300-ms election timeout
while that voter was still behind. This fault history now verifies the initial
committed prefix on every voter before injecting the disk failure; it still
requires a healthy majority and refuses a second ACK from the failed disk.


## Logical control payload groundwork

`LogicalCommand` defines an engine-independent, versioned payload for ownership
claims, schema definitions, resolved mutations, outcome-only records and stream
retirement. Identity includes an internal request UUID, its original observed
physical-log index and a digest of the original input. Those fields stay stable
across possibly executed retries, including after owner changes. The digest is
not a substitute for authentication or the enclosing envelope's checksum.

The codec uses explicit action codes, fixed-width identity fields, strict
per-action arguments and payload presence, bounded decoding and complete-size
admission before encoding allocation. It owns its arrays and exposes read-only
payload streams. This is a protocol building block: no journal runtime decision,
authoritative owner transition, retry cache or client storage mode is enabled by
serializing one of these commands. Committed semantic validation and receipts,
bounded outcome retention and controlled replay remain the next integration work.

In particular, an evicted retry UUID must not become an apparently new mutation.
The committed controller must use the original observation/retention fence to
report an explicit expired/unknown outcome when it cannot safely establish the
original result. A coordinator must not renew an ambiguous request's identity or
observation and then blindly execute it again.


`LogicalStreamState` supplies the deterministic semantic fold for a single
verified ordered stream. Accepted ownership claims compare the preceding epoch
and advance the engine primary term independently of the Raft term. Schema
transitions compare the prior version and preserve the exact definition's digest
and checkpoint. Mutations extend one contiguous engine sequence prefix; outcome
records consume no engine sequence. Retirement permanently closes the activation.
Legacy history cannot silently switch protocols.

The bounded accepted-outcome table keeps request action, original observation,
input digest and first accepted checkpoint. A retained retry returns that first
checkpoint even after owner changes; changed identity fields are refused. Eviction
advances an observation floor, so a cache miss older than the floor is explicitly
expired rather than re-executed. Recovery with the same ordered prefix and limits
reproduces the decisions, floor and state projection. Payloads remain in the log.

The fold requires caller serialization and authenticated, verified committed
input; it does not provide either by itself. Its retention and frame limits must
be fixed by the eventual durable protocol configuration before runtime use:
changing retention independently on replicas could change semantic acceptance.
It does not select a cold-state cache policy, authorize unbounded resident shard
state, migrate a legacy stream or enable client quorum writes. Distinct logical
protocol framing and committed receipts/replay remain required integration work.


Logical command frames now have explicit envelope kind 6; the existing codes
1 through 5 are unchanged. The logical fold refuses other kinds before changing
state. Legacy payloads are not guessed from magic bytes or reinterpreted as
logical controls.

`LogicalStreamReplay` binds one stream to a real local journal scan and begins
at physical index -1. It tracks bounded scan progress through other streams and
configuration entries, folds selected logical commands, and returns two distinct
lists: newly accepted effects and receipts for all selected physical entries.
Duplicate receipts point to the first accepted checkpoint. Semantically rejected
or duplicate entries have no emitted effect despite their physical commitment.
Legacy records are refused and prevent silent activation over existing history.

The cursor has no externally supplied recovery position. Missing history or
partial evaluation failure stops it permanently; recovery needs the verified
complete prefix, or a future authenticated state-base protocol. Application must
finish the emitted batch before another poll, and application failure requires
rebuilding the consumer and cursor. Returned receipts describe historical local
replay, not latest-read authority or a complete client append protocol. Fixed
protocol limits, authenticated proposal admission and aggregate resident-state
budgets remain runtime integration requirements. No client storage mode changes.

A real three-voter TLS history commits schema and data, removes the leader,
commits an owner takeover on its replacement, then commits a retry and stale
producer frame. Small bounded scans emit the four original control/data effects,
return the original data checkpoint for the retry and refuse the stale write.
Reconstruction in a single larger scan reproduces the same semantic state and
receipts. This is local fault-history coverage, not three-AZ latency qualification.


Logical proposal admission now explicitly binds the command actor to the local
journal's persistent voter UUID. Only the ready local leader may append; a
prepared operation is never redirected after an election. Ordinary append refuses
logical frames, and the native transaction entry point revalidates their decoded
actor. Trusted host orchestration must still establish the logical owner epoch,
exact schema and retry outcome from committed history: a physically acknowledged
proposal is not a logical success.

Complete command size, including its header, is checked without allocating an
encoding. Logical serialization and envelope construction run only after the
normal journal request/byte permit is acquired. Failure releases the permit; an
accepted proposal retains the existing cancellation and persistence behavior.
This uses the existing transport and admission queues and adds no per-write S3 IO.

Native tests now use separate suite JVMs because volume leases intentionally
survive journal close until process exit. Adding repeated fixtures had reached
the fixed 128-volume process cap. A shared fixture and per-suite forks keep that
safeguard intact rather than increasing it or resetting retained identities.


`LogicalStreamRecord` permanently binds an exact deployment/index/shard/activation
to one physical group, its original committed activation checkpoint, and fixed
semantic retention and frame limits. Its bounded versioned encoding checks all
fields with CRC32C; the trusted catalog, not deserialization alone, supplies
provenance. Changing physical history or semantic limits requires a new explicitly
fenced activation, not an in-place record overwrite.

The group catalog creates one `stream-<activation UUID>` conditional register after
the physical activation is durably published. Reusing a nonce for another shard,
group or limit is a conflict. Reads use that key directly and verify both logical
identity and durable group lineage. They neither enumerate the descriptor
population nor allocate resident rows for cold streams. Lost creation replies
reconcile the identical immutable assignment. Group membership replacement leaves
the logical assignment and original activation unchanged.

Public logical proposal and replay construction now take this catalog-loaded
record, verify local physical lineage and frame capability, and use its fixed
semantic limits. Proposal encoding also respects the record's complete payload
limit even when the local journal admits larger frames. These checks perform no
S3 IO per mutation. The caller is trusted to load an authorized record; a record
received from a client is not a proof. Assignment alone does not activate an index
storage mode, fence legacy writers or provide current logical ownership. Those
lifecycle and runtime controllers remain required before client quorum writes.

Local frame-capability validation is not proof that every existing or replacement
voter has the same capability. Before enabling client writes, bootstrap and
membership startup must enforce a durable group-wide protocol/frame capability
without enumerating every logical stream. A smaller-capability follower must be
refused before native startup rather than fence while applying a larger frame.


A client retry's mutation versus outcome-only classification is a resolved
result, not its original request identity. A script/update may resolve as a no-op
or error after its original mutation committed, or mutate after an original
no-op committed. Both classifications therefore share a retry identity domain.
The accepted UUID, original observation and original-input digest return the first
accepted checkpoint regardless of that later classification, actor or resolution.
Ownership, schema and retirement control actions remain distinct identity domains.
This changes no accepted data effect for those historical retries: a previously
rejected retry becomes a duplicate, and neither consumes another engine sequence.


`JournalGroupProtocol` now fixes the wire and unpurged-history contract in one
61-byte versioned, checksummed register per physical group. It captures maximum
payload, queue/appender batch bytes, native pending-request count and retained log
positions. Payload/queue agreement prevents a voter from persisting or receiving
traffic that its decoder or peer RPC limit cannot handle; count/history agreement
preserves the same retained-prefix safety margin. The current host configuration
has one shared profile, so all assigned groups must match it. Node-wide proposal
bytes, caches, free-space reserves and local segment settings remain local,
subject to their existing validation and recovery constraints.

The controller explicitly reserves the protocol with the genesis placement before
native bootstrap. Both static and membership-aware startup read and compare the
contract before volume access, including replacement learners. Activation
publication also verifies the local journal's configured contract. Startup itself
cannot create a missing contract. A missing contract on an active group cannot be
inferred from a new assignment or overwritten on retry; older experimental groups
need a separately verified upgrade or fenced migration. Logical assignment creation
and reads verify their payload bound against this same permanent group contract.
These are lifecycle/cold-activation S3 accesses, with no per-document S3 request.

Fault checks cover lost reservation replies, immutable conflicts, corruption,
foreign keys/namespaces, missing active contracts and incompatible original or
replacement startup. A native learner with the wrong frame limit is refused
before `journal-binding` creation. The file-based bootstrap test also exposed an
actual configuration mismatch: its default wire limits differed from its fixture
companions. It now explicitly declares the common protocol while retaining local
resource allowances. No client quorum storage mode is enabled by these checks.


The folded stream now supports a bounded request-identity lookup before expensive
payload preparation. It returns a retained original checkpoint, explicit refusal
or unknown without advancing the physical cursor or refreshing FIFO retention.
`LogicalStreamReplay` serializes this lookup and returns its inspected physical
prefix; a partially failed cursor refuses further lookup. Unknown is not permission
to execute: the host still needs its committed ownership barrier, a caught-up
consumer and exclusion of ambiguous in-flight proposals.

`RatisJournal.readCommitted` retrieves one exact historical physical frame using
the permanent assignment and expected entry term. It checks the locally applied
committed prefix, history availability, logical identity, frame kind, checksum and
payload admission. Original results can therefore be fetched on demand without
retaining opaque result bodies in the identity cache or rescanning the prefix.
The caller must reserve a frame's result budget and use a bounded worker. Reading
a physical coordinate alone does not establish semantic acceptance or latest-read
authority; use the canonical checkpoint from verified logical outcome evaluation.
Missing history never authorizes executing the ambiguous request again.


`LogicalOwnershipSession` is the caller-serialized runtime ownership driver for
one admitted activation. It applies verified bounded history on the host's worker,
then proposes a fresh internal ownership CAS with an advancing engine term. A
physical reply alone cannot expose ownership: subsequent bounded replay must find
the same accepted claim, apply every preceding effect and still observe the
claimed owner/epoch/term with local leader readiness. No engine sequence may be
allocated until the host also has a verified rebuilt native context at that cut.
The caller supplies an engine term compatible with native serving/migration floors.

The driver does not block waiting for an unresolved claim or register a callback
that retains the resident fold. Callers schedule further work from detached physical
completion. Ambiguous submission/reply, rejected/superseded claims, lost readiness
and partial application fence the session permanently. A fresh session must rebuild
its consumer and commit another barrier, so late old proposals cannot authorize
new speculative preparation. Close after host work drains releases references
without waiting for or canceling accepted physical persistence.

Schema/data effects apply before ownership is exposed; a successful schema update
can retain the same owner and term. The ordered consumer must quiesce/install native
schema and preparation state accordingly. Progress contains only small immutable
metadata, not opaque frame batches. Ownership is hidden during application, and
callbacks cannot reenter lifecycle work. These cuts are historical evidence, not
continuing leases, latest-read proofs or completed native engine integration.
A real three-voter probe verifies an applied initial claim and epoch/term advancement
on the replacement after leader loss. No client storage-mode activation changes.


`CommittedLogicalApplier` connects newly accepted logical replay effects to the
native committed-serving primary. It independently verifies ordered identity,
producer transitions, the pinned schema definition/dependency, resolved request
UUID, sequence and engine term before native mutation. Native success must retain
the original assigned sequence/term/version. Controls and outcome-only records do
not allocate document sequences; an explicit resolved translog no-op does.
Failures fence the consumer rather than continuing an uncertain native prefix.

The adapter pins the mapper object and mapping version, so an unversioned mapper
replacement cannot silently change parsing. Definitions are hashed in bounded
chunks. It retains only small progress metadata; original mutation results are
returned to the caller under its result-byte reservation. Inputs must be newly
accepted effects from logical replay, not physically committed raw frames or
client-supplied assertions. Producer adapters must validate the opaque native
payload before irreversible proposal admission as well.

The current native adapter supports an empty verified serving state, one exact
already installed schema/analysis context, and operation terms at or below its
installed native term. Dynamic schema/native-term lifecycle and verified recovery
bases require their controllers; it does not reindex history with a newer schema.
A real private native preparation -> TLS quorum -> logical ownership/schema ->
serving history verifies visibility only after accepted application, original
identity, a changed-result retry and conditional delete. No frontend storage mode
is activated, and these tests do not qualify all plugin or update-script behavior.
