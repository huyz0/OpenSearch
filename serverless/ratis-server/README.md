# Curated Apache Ratis server artifact

Pinned upstream: Apache Ratis 3.3.1. The merged artifact replaces only
`org.apache.ratis.server.impl.RaftServerProxy`, `ServerState`,
`org.apache.ratis.server.raftlog.segmented.SegmentedRaftLogWorker`,
`org.apache.ratis.server.storage.RaftStorageMetadataFileImpl`, and their nested classes.
Other upstream classes and resources are checked byte-for-byte against the
original JAR by `verifyUpstreamContents`.

## Resource cleanup fixes

When group initialization throws, upstream `groupAddAsync` removes the failed
division from its map without closing it. A later server close cannot find it,
leaving its storage lock and other resources held. The replacement closes the
failed division before propagating the original failure. Cleanup failures are
attached as suppressed exceptions. Election, quorum, log, and membership
algorithms are unchanged.

The source retains the upstream Apache license. Wildcard imports are expanded,
the repository formatter is applied, and legacy file/socket API usage is narrowly
exempted on the upstream class. The upstream public SDK API is unchanged; the
host integration uses its own internal extension types.

Binary SHA-256: `a635a8c69b5c59089b026992178ecea95ead7d709c5ff4f94834e06eaa0c911d`.
Original source entry SHA-256: `b3a61cc9a1053d1319ec88d939c7360e2e779bf88b5fa44d62c6fcb319597e5e`.
Sources: https://repo.maven.apache.org/maven2/org/apache/ratis/ratis-server/3.3.1/ratis-server-3.3.1-sources.jar

The merged JAR is used instead of the original server JAR, never alongside it.
All other Ratis components remain pinned to 3.3.1 and use the curated common
artifact's metadata directory synchronization fix.

The normal third-party dependency audit skips project artifacts and compile-only
inputs. `patchedArtifactAudit` separately audits the complete merged artifact;
it uses the same forbidden-API and JDK-jar-hell checks without blanket exclusions.
The runtime regression checks the actual artifact origin and refuses duplicate
copies of the replacement class.

When `ServerState` constructs a log whose `open` fails, the memoized supplier does
not publish that object. Later division cleanup cannot reach it. The replacement
closes the unpublished log on failed open, preserving the original exception and
suppressing secondary close failures. A corrupt-header recovery test reproduced
the leaked cache-eviction thread before this fix and checks that it terminates.

Original `ServerState.java` source entry SHA-256: `87ea1923a11060963d3e2f5150418431d10f3c85ebbba81acec3b0238653a5e2`.

## Stalled shutdown I/O resources

Upstream log-worker close waits three seconds, then closes the output stream and
frees its direct write buffer even if the worker remains active in disk I/O. A
real-process test reproduced the log channel closing while its worker was stalled
inside the persistence hook. Closing or freeing live resources can race the old
worker when that I/O resumes.

The replacement cleans up the stream and direct buffer only after both worker
and optional flush executors have terminated. Otherwise it leaves them alive for
process teardown. The journal's process-lifetime volume lease prevents another
voter opening that volume in the meantime. This intentionally trades immediate
resource reclamation for safe process replacement after a stuck I/O shutdown.
The consensus, log format, and acknowledgement rules are unchanged.

Original `SegmentedRaftLogWorker.java` source entry SHA-256:
`f9899dc64d71bd33194f220eb389ec9be3be8d6d7763b5874c98a1d8d41eac4e`.

## Persisted log boundaries

Two internal extension types are added beside the pinned reader classes:
`OpenSearchLogPersistence` supplies opt-in hooks on the serialized log worker,
and `OpenSearchLogRecovery` uses the native CRC reader to verify a required prefix
before any server/voter is started. The hooks require safe synchronous log flush.
Normal acknowledgement evidence is recorded after the file force and before the
native flush index advances. A separate worker-ordered truncation hook runs before
suffix deletion; the upstream state-machine truncate callback runs earlier, under
the Raft write lock, and cannot provide that ordering.

Segment finalization now propagates output close/flush errors, completes pending
state-machine data persistence, and forces the directory after rename. Truncation
also forces its directory changes before advancing the flush index. The original
state-machine callback ordering and consensus algorithms remain intact. Shutdown's
best-effort cleanup still retains live resources when workers have not terminated.

Recovery verification checks unpurged filename ranges from index zero and reads
at most the required segment and the newest closed/open tails, with native CRC,
entry-size, file-size and directory-entry bounds. It forces a valid existing open
tail before the host adopts its end as durable. It never repairs, rewrites, or
deletes log contents. Snapshot/purged-prefix recovery needs a separate trusted
catalog proof and remains unsupported here.

## Host-managed client dispatch

`OpenSearchClientRequestDispatch` opts a state machine into direct proxy
submission on its embedding host's bounded executor. Other state machines keep
the upstream client-executor path. The journal already detaches both submission
and application completion from consensus locks.

Upstream asynchronous submission queues a native client worker and joins it.
During leader removal, the leader closes its division while holding the same
monitor needed by that worker, then waits for the client executor to terminate.
A real replacement run reproduced the resulting deadlock. Avoiding the redundant
handoff for managed state machines removes that cycle without changing log,
commit, vote, or membership rules. A deterministic regression holds every native
client worker and verifies a managed quorum append still completes. Process tests
also require accepted concurrent writes to finish before killing the removed
leader and verify all acknowledged records survive in the replacement quorum.

## Voting metadata integrity

The upstream term/vote file uses unchecksummed properties. A child-process
regression granted one candidate a vote in term seven, stopped the voter, flipped
one persisted bit to change its term to six, then restarted it. The voter granted
a different candidate a vote in term seven. Log-entry CRCs cannot detect this
separate voting-state corruption.

The curated metadata writer adds a format version and CRC32C over the term and
exact voted-for identity in the same atomic, forced file publication. Its reader
bounds the file to 4 KiB and the UTF-8 voter identity to 1 KiB. It refuses missing
integrity fields, unsupported versions, invalid terms, changed checksums and
oversized files instead of interpreting them as an empty vote. The native
missing-file default remains restricted to initial formatting; the host's internal
`OpenSearchVotingMetadata` verifier always requires the existing file before any
native service starts.

Journal binding format four prevents an older host silently opening a directory
that requires these checks. Existing unsealed experimental voter directories are
not rewritten in place. Preserve them and use authorized replacement with a fresh
storage identity and a healthy quorum; do not manufacture a checksum over possibly
corrupt voting state. Whole-quorum disaster recovery remains a separate milestone.
The consensus voting and commit algorithms and upstream public SDK API are unchanged.
Checksums detect corruption; they do not authorize restoring old backups under a
live voter identity or provide protection against malicious stable-storage rollback.

Original `RaftStorageMetadataFileImpl.java` source entry SHA-256:
`ebe98a2f6c9900e64ded2e83d8e330a723e011802848bd65bcd8d5751ab060df`.
