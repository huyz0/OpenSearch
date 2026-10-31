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
format a wiped established voter. Initialization synchronizes newly created group
directories up to the provisioned volume root before writing its marker.

A crash during incomplete initial bootstrap requires controller intervention.
This first runtime supports static membership. Learner catch-up, safe membership
changes, group movement, and bootstrap recovery still need their controller state
machines. Operators must not edit the binding or copy a live voter directory to
another live voter. A failed replication-service close retains the volume lease. The curated
`ratis-server` artifact closes a failed group before unregistering it, fixing a
reproduced native storage-lock leak on initialization failure. Other I/O-error
and interrupted-shutdown cleanup paths still require qualification.

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
only for real gRPC sockets. It is part of this module's `check`. Its certificates
are public test fixtures, copied from testkit, with password `testonly`.

Local tests cover shared-stream isolation, two physical groups, leader close,
full restart/replay with payload verification, majority loss, absent client
credentials, disk-reserve refusal, namespace rejection, and strict storage
binding/identity checks. Graceful close on one machine is not evidence of power
loss, independent-process corruption recovery, or three-AZ durability/latency.
