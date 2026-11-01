# Shared persistent journal

This library implements the local persistent Raft journal used by the planned
low-latency mode. It is not yet connected to shell writes or enabled by an index
setting. The shell continues to use its object-store WAL.

`RatisJournal` hosts a bounded number of physical groups, each carrying many
`OperationEnvelope` logical shard streams. It requires three storage identities
and three distinct AZ labels per group. The controller must verify those labels
against infrastructure; a string label is not proof of AZ placement.

## Admission and persistence

- A proposal goes only to the local ready leader. There is no client-side redirect
  of an operation prepared by a previous leader.
- Ratis 3.3.1 persists the replicated log with both unsafe and asynchronous flush
  disabled. The state-machine flush hook forces the segment directory before
  completing disk persistence. The curated `ratis-common` artifact also forces
  directories after atomic term/vote/configuration metadata publication.
- Each accepted proposal holds a node-wide count and conservative byte permit
  until its underlying consensus future finishes. Cancelling a caller's derived
  future does not cancel accepted persistence. A failed or timed-out append can
  have an ambiguous outcome; the mutation layer must reconcile operation identity.
- Per-group log, write, and replication buffers, transport frame size, segment
  size, segment cache, and physical group count have finite limits. The write
  buffer includes the native frame overhead and the gRPC limit includes Ratis's
  required 1 MiB framing allowance. Ratis requires a write queue of at least 1 MiB.
- A host-supplied probe checks the actual provisioned volume and refuses writes
  at its free-space reserve. An entry-count watermark additionally bounds retained
  history metadata, including entries whose payloads were evicted from cache.
  In-flight proposals can consume the bounded queue allowance beyond a watermark.
- Snapshots, snapshot installation, and automatic truncation remain disabled.
  There is no permission to discard unarchived committed history. Archival and
  recovery-catalog integration must reclaim history before a watermark is reached.

The pending-proposal budget is not the entire process memory budget. Heap and
native memory sizing must also include per-group buffers/caches, native Raft
metadata and retry caches, bounded replay batches, TLS, and transport overhead.
The retry-cache budget must be revisited before archival permits sustained history
reclamation; a time-based native retry cache alone is not a hard count limit.

The ordinary retained-entry watermark is also backed by a native persistence and
recovery ceiling: at most `maximumRetainedEntries + 4 * maximumPendingRequests + 32`
physical positions. The finite allowance covers in-flight and control metadata.
This includes automatic native election/configuration entries that bypass customer
admission. The worker checks a cached bound before advancing its durable boundary;
it never acquires a native Raft/log lock to calculate it.

Before SDK startup, recovery checks both the saved boundary and the observed
on-disk end. Lowering the configured budget cannot silently load an older, larger
history into native heap metadata. A refusal preserves its bytes; restoring the
original capacity remains possible. The bound currently uses a zero-based unpurged
prefix. Snapshot/reclamation integration must change it to count retained positions
before enabling log purge. This metadata count is one component of process sizing,
not a guarantee that arbitrary heap, payload and buffer settings fit a host.

## Submission and completion workers

Native async submission can join the SDK's client executor, and native futures
can complete while holding leader/watch locks. Calling back into submission from
those locks deadlocks activation and shutdown. Local proposals and activation
watches therefore dispatch both submission and notification through four bounded
node workers, shared across physical groups. Their queue holds at most the node's
pending-request limit; each charged proposal owns at most one queued task.

Permits remain charged until notification is dequeued, then release before the
public future completes. Dependent appends and activation handoffs consequently
work with a single pending slot. Caller cancellation still cannot release accepted
persistence. Close refuses new dispatch and queued proposals not yet submitted;
it preserves the dispatcher for outstanding native results, including late I/O
completion after SDK close. Once notifications drain, its workers terminate.
Managed state machines bypass the redundant SDK client-executor hop. Otherwise
a removed leader can close its division while holding a monitor needed by a
queued client worker, then wait for that worker to terminate. Admission also
requires a running division whose current or previous voter configuration still
contains the local member; the native leader-ready hint alone can outlive removal.

The extra worker threads and queue hops require workload qualification; they add
no object-store request and do not change the quorum persistence rule.

Peer RPC handlers use a separate pool of four node workers, with a queue bounded
by `max(128, maximumPendingRequests)`. Protocol progress needs queue capacity even
when local proposal admission permits only one request. The SDK's async-request
pool setting covers its client-write service; it does not bound peer handlers.
The supported server customizer installs the actual peer executor. Native close
precedes executor shutdown; interrupted close preserves it until cleanup is retried.
The volume lease still lasts until JVM exit.

A real mTLS test reproduced 64 blocked peer-handler threads on the prior default
executor. Its regression overloads the queue under a held consensus monitor,
verifies at most four blocked handlers, then releases the monitor and confirms
quorum writes and replay resume. The pinned gRPC transport reports queue rejection
as an RPC `INTERNAL` failure, which follows normal peer retry/backoff. This bounds
inbound handler execution; full throughput, CPU and outbound callback behavior
still require workload and membership qualification.

## Storage identity and startup

The host must first provision and durably mount a dedicated journal directory.
`JournalDiskIdentity.discover` acquires its lock briefly and returns the persistent
storage-lineage UUID. The controller binds this UUID to voter membership. An empty
replacement SSD receives a different UUID and cannot restart under cached old
membership.

`RatisJournal.open` reacquires and holds the exclusive lease. It verifies the
expected UUID before starting replication. Initial bootstrap must be explicitly
authorized after the deployment and physical group UUIDs have been reserved in
the durable control plane. Bootstrap binds the volume to the deployment, group
identities, and exact static voter placement. Reusing a group UUID for independent
bootstrap after quorum loss is forbidden.

Recovery checks every assigned group's initialized marker, current directory,
vote metadata, and persisted configuration before calling Ratis. Missing or
inconsistent storage fails closed; native `RECOVER` is never allowed to silently
format a wiped established voter. An assigned original voter whose genesis is not
activated may recover its existing native bootstrap configuration only when both
the durable boundary and verified log are empty. The initialized volume binding
and recorded term/vote remain mandatory. This resumes a process stopped before
its first quorum formed; it cannot reset a voter or erase an established prefix.
Known activated membership never grants this exception. Initialization synchronizes newly created group
directories up to the provisioned volume root before writing its marker.

Before admitting logical operations, the local leader must complete
`activateGroup`: append a seed, wait for its `ALL_COMMITTED` watch across all three
initial voters, then commit an activation record through the majority path. A
watch for index zero is insufficient because native initial commit hints can
already report zero on an otherwise empty voter. Ratis rejects votes from empty
logs after a nonempty commit; omitting this initial barrier can strand the two
remaining voters after the first leader dies. Activation survives replay; normal
writes and subsequent leader changes require a majority, not all three voters.
Control identities are reserved and activation records are excluded from replay
subscriptions while still consuming scan budgets. Binding format version 3
rejects earlier experimental volumes without persisted local boundary evidence.

Once native startup begins, the volume lease lasts for the **entire JVM lifetime**,
even after `close` or failed startup. Native close can return while stalled disk
work remains alive. A bounded process registry retains the lock's channel, and
only process termination releases it for a replacement voter. The host must wait
for the old process to exit before reopening its volume. Pre-interrupted close
refuses SDK cleanup, closes admission, and preserves the interrupt; cleanup can be
retried from a non-interrupted thread. The host normally opens one volume; the
registry caps successful and failed-startup volumes at 128 per JVM.

A crash during incomplete initial bootstrap requires controller intervention.
This runtime supports static membership. Learner catch-up, safe membership
changes, group movement, and bootstrap recovery still need their controller state
machines. Operators must not edit the binding or copy a live voter directory to
another live voter. The curated `ratis-server` artifact closes failed groups and
unpublished logs on startup errors. Other I/O-error cleanup paths still require
qualification; the lifetime lease prevents their old tasks overlapping a new voter.

## Local durability boundaries and corruption

Each group keeps a fixed 152-byte, two-slot checksummed boundary file, bound to its
deployment, volume UUID, and group UUID. The native worker forces this evidence
**after log data persistence and before advancing its flush index or acknowledging
replication**. This is one additional local metadata force per native flush batch;
its latency and IOPS cost still require workload qualification. It adds no S3 call.
The position proves local persistence only, not commitment, searchability, or
permission to discard history.

Recovery validates both slots and their generations and re-forces the valid file
and its directory before SDK startup. A corrupt
or torn slot quarantines the voter even when the other slot is valid: silently
falling back could hide an acknowledged newer boundary. Missing segments, gaps,
or a tail shorter than that boundary refuse startup. A valid open tail is verified
and forced, then its actual end is recorded before voting can begin. This lets
normal process restarts retain valid unacknowledged entries without treating dirty
page-cache data as already durable.

Authorized Raft truncation can reduce the index. A worker-ordered hook records
that lower bound before deleting an uncommitted suffix; generations still increase.
This preserves recovery after legitimate leader changes. Segment roll/truncate
also force their directory changes, and output-close failures propagate before
persistence progress can be reported. The floor is not a snapshot or archive cut.

A missing-tail process test removed the newest acknowledged segments while
retaining the identity, activation and native metadata; the earlier runtime
accepted that voter. It now refuses before consensus startup. Another real-process
history discards a long uncommitted suffix and then restarts all voters, preserving
only committed operations. CRC/omission checks do not authorize restoring old
backups under a live voter identity or protect against arbitrary stable-storage
rollback. A quarantined volume must follow authorized recovery/replacement.

## Durable bootstrap catalog

`JournalGroupCatalog` in the shell metadata package stores one permanent deployment
identity and one conditional register per physical group under
`RegisterMap.journalGroups(base)`. A bounded, checksummed `JournalGroupRecord`
binds the deployment, unique group UUID, and canonical initial voter UUIDs,
addresses, and AZ labels. Retrying the same reservation is idempotent; changing its
placement is refused. No catalog deletion or identity reuse API is exposed.

The controller must reserve the complete initial placement before calling
`catalog.openJournal` on the three hosts. This entry point checks every reservation
before touching local storage or starting SDK services. After `activateGroup`
completes, the controller calls `publishActivation` on its own bounded executor.
Publication records the first committed activation coordinate with conditional
replacement. A lost storage reply is reconciled by retrying against the same
reservation and marker. Only the durable active record may authorize assigning
logical shards. These are lifecycle operations; normal appends perform no catalog
read or write, and catalog I/O must never run on a Raft completion callback.

Recovery uses `bootstrap=false` and the original placement. The catalog passes its
active marker to the journal: until that marker replays, the journal cannot append
or seed a fresh activation. A different committed activation fences the local
journal. The marker is historical evidence, not current-read authority. It does
not by itself prove completeness of all later data; omitted-tail detection and
archive/recovery-boundary qualification remain separate work.

An explicit `ServerlessBootstrap.JournalAssignment` now attaches the catalog and
native journal to the node startup/close lifecycle. The host controller described
below drives activation and native replacement. Command-line assignment/credential
loading, index-mode activation, logical-shard ownership and migration remain pending. Initial placement is immutable; operators must never edit
it to simulate reconfiguration or bootstrap an active group after quorum loss.
The protocol tests use local filesystem conditional registers; they do not
establish a cloud provider's failure or durability behavior.

## Serialized membership intent

The separate bounded `JournalMembershipRecord` keeps the activated genesis,
completed logical controller revision, exact current placement and one pending
single-voter replacement. The revision is independent of native configuration
log positions, which can change during election. Source and target retain two
identical voters and bind every UUID, address and AZ label. An existing identity
cannot be rebound to another endpoint.

Activation publication first creates the permanent membership head, then marks
the genesis reservation active. A lost reply at either step is retryable. Once
active, a missing or corrupt head refuses recovery; it never recreates revision
zero. Earlier experimental catalog activations without this head are refused.
They must not be repaired by blindly reconstructing the genesis membership.

`JournalGroupCatalog.prepareReplacement` conditionally persists a unique operation
nonce and exact target. The same nonce and placement can retry after a lost reply;
a competing operation, stale revision or different target cannot overwrite an
unresolved intent. Each new voter identity also receives a permanent per-group
reservation before intent publication. Failed competing intents can leave unused
reservations, but those identities cannot be rebound or reused in later changes.
Retired genesis identities are reserved implicitly by the immutable genesis.
All these are bounded per-group lookups on lifecycle paths, with no per-write S3
calls and no listing or global membership lock.

The internal assigned-startup and completion APIs below now drive native learner
catch-up, configuration CAS, restart reconciliation and retirement through the
explicit host controller. An operator endpoint is not exposed. Static startup
continues refusing pending replacement before opening a volume; assigned startup
validates its winning permanent reservation. Intent alone never promotes a voter
or authorizes independent bootstrap.

Completed membership also checks the permanent reservations for its non-genesis
voters, including their original revision and exact address/AZ placement. Missing
or rebound identities refuse recovery. These are at most three direct register
reads per physical group on lifecycle paths; validation never recursively walks
transition history. Genesis addresses cannot be rebound in the membership codec.

## Retired colocated assignments

`recoverMembership` accepts completed controller heads for the volume's original
assignments, with the original activation guards. Pending intent, foreign lineage
and bootstrap are refused. A retired group is excluded independently of other
groups on the same volume; retirement does not change that immutable binding.
The catalog recovery entry point supplies the heads read from durable registers.
The separate `openAssignedJournal` entry point supports pending reconciliation and
permanent learner reservations; `publishReplacement` publishes native completion.

Under the exclusive volume lease, before any native thread starts, retirement
atomically moves each authorized group's directory into `retired/<group-uuid>`.
Both parent directories are synchronized, including after recovery finds an
already-completed rename. Native group discovery ignores this non-UUID parent,
so it never loads the retired history or starts its voter. Original files are
preserved for diagnosis and later authorized reclamation; they still consume disk
space. No rename is attempted while another process holds the volume.

An interrupted rename can resume from either location. Duplicates, unknown retired
histories, corrupt ownership bindings and missing paths refuse startup. Stale
configuration cannot move history back into service. This uses normal filesystem
recovery and requires no additional consensus-library patch. It does not certify
that the supplied controller completion was backed by a native membership change;
the explicit learner/promotion path below establishes that proof before
publishing a completed head.

## Authentication and replay

Peer consensus RPCs require mutual TLS. Network client-write and admin services
are explicitly denied, including for a client with a trusted certificate; proposals
and initial group management use local server calls. This prevents network callers
from bypassing the local admission and preparation boundary. The supplied trust manager
must trust only authorized journal members in this deployment. The controller is
responsible for credential issuance, rotation, peer identity, and logical-shard
placement/ownership authorization. This library does not expose an unauthenticated
fallback or an end-user endpoint.

`append` returns a committed `LogCheckpoint`, not a searchable-reader guarantee.
`scan` reads only the locally applied committed prefix, with finite physical-entry
and byte budgets. Unselected streams consume the scan budget too. Returned
through-index includes skipped configuration/metadata and unselected streams;
reuse it only for the same physical group and exact subscription selection.
Missing history and cursors ahead of the committed prefix fail explicitly.

A local leader hint or replay prefix is not authority for a current linearizable
read. The future read path must obtain quorum read authority and pin a reader
covering its required prefix. Preparation engines, serving engines, schema/owner
epochs, S3 archival, and mode migration remain separate integration work.

## Local verification

Run unit tests and the socket/disk integration task:

```
./gradlew :serverless:log:test :serverless:log:journalTest
```

The integration task disables the test security manager and system-call filter
for real gRPC sockets and child voter JVMs. It is part of this module's `check`. Its certificates
are public test fixtures, copied from testkit, with password `testonly`.

Local tests cover shared-stream isolation, two physical groups, leader close,
SIGKILL followed by failover, full independent-JVM restart/replay with payload
verification, stalled shutdown retaining the volume lock until process exit,
activation/control isolation, majority loss, absent client
credentials, disk-reserve refusal, namespace rejection, and strict storage
binding/identity checks. Independent processes on one machine do not establish power-loss behavior,
independent-AZ corruption recovery, or three-AZ durability/latency.

## Authorized learners and native replacement

`openAssignedJournal` checks activated genesis, the current membership head and
any permanent original learner reservation before acquiring the volume. A blank
learner volume requires the exact winning pending intent; an orphan reservation
cannot authorize startup. Its original transport identity becomes part of the
immutable volume binding and remains unchanged through promotion and retirement.
The initial native group contains the source three voters and a nonvoting listener.
An initialized empty learner can restart from its native bootstrap configuration
before listener addition, but missing vote metadata, floor or binding still refuses
startup. A nonempty history requires its normal native configuration metadata.

On a bounded controller executor, call `advanceReplacement` with the exact durable
pending head. Each attempt observes progress or issues one bounded native proposal:
add the listener with native CAS; commit a nonce-bound membership barrier; require
the learner's UUID-keyed committed prefix to cover a fresh cut in this leader term;
then atomically replace old three plus listener with new three. Voter
`ALL_COMMITTED` is never treated as learner catch-up evidence. The native staging
gap is one, so the zero-gap comparison cannot prevent completed catch-up.

The first committed barrier for each controller revision and nonce is preserved
through duplicate proposals and replay. A bounded per-group tracker coalesces
fresh barrier requests within a leader term. Internal controls may consume the
finite metadata allowance beyond ordinary customer admission; the hard native
persistence ceiling remains enforced. Customer appends cannot use the reserved
membership namespace, and scans exclude these controls while charging their
entry and byte budgets.

`replacementReceipt` requires a locally committed/applied stable exact target
configuration following that barrier. `publishReplacement` obtains proof directly
from the authenticated journal, creates a permanent operation receipt first, then
conditionally advances the pending head. A lost receipt reply leaves the head
pending; a lost head reply reconciles from the same receipt. Later elections can
write a newer native configuration with identical membership: the first published
receipt wins, and a later valid proof of the same intent/barrier accepts it. An old
retry can return that receipt after later replacements; a missing old receipt is
never synthesized from a later controller head. Coordinates are historical proof,
not current-read authority or a guarantee of present learner health.

Each replacement nonce also has a permanent exact-intent reservation before the
pending-head CAS. Completed or abandoned nonces cannot be rebound to a later
revision or target and collide with an earlier receipt. Missing reservations on a
pending head refuse recovery; they are never silently recreated. This adds one
conditional metadata creation per lifecycle transition and no per-document S3 I/O.

Pending intent is not canceled or overwritten. Failed-learner recovery, automatic
provisioning and command-line configuration still need integration. The explicit
host lifecycle below is qualified separately. These APIs do not enable quorum-mode shell writes.

## Retired peer transport resources

After completion publication, including an idempotent retry, the catalog asks the
local journal to release the removed peer's cached transport client. The public
`releaseUnusedPeer` hook also supports other local controllers. It first checks
all active assignments' current and previous voters and listeners; a peer still
used by any colocated group keeps its connection. Run cleanup on the bounded
controller executor, outside SDK callbacks. Closing a transport client does not
change membership or durable history, and a concurrent use can reconnect.

A real peer RPC reproduced the old channel remaining open after replacement.
The regression now verifies closure, while a two-group test keeps it open until
both groups replace that peer and confirms subsequent append/replay. The supported
SDK reset closes its heavy channels/stub pool but preserves a lightweight peer
descriptor. Current unpurged history limits bound that descriptor population;
archive/reclamation work must also address descriptor count before permitting
unbounded membership history. This is resource cleanup, not log reclamation.

## Explicit node host lifecycle

`ServerlessBootstrap.start(settings, JournalAssignment)` accepts a provisioned
`JournalConfiguration`, member key manager, deployment-scoped trust manager and
actual-volume probe. It attaches `JournalController` before publishing metadata-plane
availability. Native opening must finish before startup returns. All-voter
activation proceeds asynchronously, allowing hosts to start sequentially. The
one-argument bootstrap keeps its legacy behavior. This explicit host API does not
enable quorum-mode index writes or provide command-line credential loading.

The node supplies exactly one controller worker and one queued task; operator
settings cannot silently enlarge these bounds. Wakeups coalesce, and each physical
group retains at most one pending native control operation. A 200 ms local ticker
drives activation and membership reconciliation; ordinary appends bypass it.
Stable catalog metadata is cached for 30 seconds, with an explicit refresh signal
for known controller changes and a five-second error backoff. Native completion
callbacks only enqueue work. Catalog I/O never runs on consensus callbacks, and
control state grows with the bounded physical group count, not logical shards.

`state`, a bounded failure diagnostic and the reconciliation counter support host
observability. States describe locally observed control progress; they do not
establish current read authority or a searchable checkpoint. The controller checks
native availability before treating a missing marker as catch-up. A retired local
assignment stops native mutation attempts while other colocated groups continue.

Node close stops controller admission/timers and owns native cleanup before its
thread pool terminates. A close racing catalog startup cannot subsequently start
native storage. If native opening already began, a late server closes itself and
its volume remains quarantined until JVM exit. Failed bootstrap authorization
releases ordinary node resources without touching the journal volume.

Real socket tests cover automatic two-group activation, learner replacement,
replacement-quorum survival, colocated availability, metadata-outage backoff,
10,000 coalesced wakeups without extra catalog reads, startup/close races and
rejection of an enlarged controller queue. The host API uses local filesystem
conditional metadata in these tests; cloud/three-AZ qualification remains pending.

## Normal launcher integration

The shell's one-argument bootstrap loads an optional versioned assignment file and
dedicated PKCS12 credentials from secure settings. The one-shot disk identity
command does not start a node. See [journal host configuration](../journal-host-configuration.md)
for the schema, bounds, provisioning command and explicit bootstrap/recovery policy.
File loading configures the journal host only; index durability remains unchanged.
