# Low-latency implementation progress

Branch: `low-latency-serverless`. The delivery plan is
[low-latency-serverless-plan.md](low-latency-serverless-plan.md).

The current runtime still uses object-store WAL durability. Quorum-mode activation,
replicated SSD logs, committed-only serving engines, streaming search replicas, and
asynchronous S3 archival are not enabled by these foundation commits.

| Milestone | Delivered | Remaining |
| --- | --- | --- |
| M0 | Node timing histograms, WAL group counters, explicit open-loop benchmark, initial real-process filesystem baseline | Additional pipeline stages during extraction; actual S3 and sustained-load qualification |
| M1 | Append and streaming recovery interfaces, optional async dispatch with cancellation/rejection semantics, existing S3 path integration, bounded versioned log/checkpoint identity codecs | Subscription/token formats and shell adapters for consensus |
| M2 | Three-replica persisted-log/restart probe, committed-serving engine boundary probe, group footprint benchmark, [consensus ADR](low-latency-consensus-adr.md) | Shell preparation-service integration, duplicate-indexing overhead, failure and security gates |
| M3 | Engine-independent log module, checksummed frames, durable metadata/volume identity, static shared-group runtime with peer-only mTLS, bounded admission/history, committed replay | I/O-error cleanup qualification, hard-kill/corruption/partition histories, learner/reconfiguration lifecycle, controller activation binding |
| M4–M9 | Not implemented | Quorum writes, fresh readers, selective replication, archival, lifecycle, migration |
| M10–M12 | Not implemented | Scale/cost/failure qualification, controlled rollout, optional attribute/QUIC extensions |

## Validation so far

- Timing changes: 60 focused iterations, zero failures; root precommit passed.
- Append/recovery and checkpoint foundations: 90 focused iterations, zero failures;
  root precommit passed.
- Baseline task: real shell process, explicit single-shard and multi-index/shard
  overload runs. Results and limitations are in [low-latency-baseline.md](low-latency-baseline.md).
- Async append tests cover executor rejection, preserved exception identity, fatal
  failures, and cancellation without discarding already accepted persistence work.
- Checkpoint tests cover empty/max coordinates, activation/incarnation separation,
  preallocation length limits, malformed UTF-8, and unknown format versions.
- Full foundation regression: 1,432 cases, zero failures/errors, two skips, 8m26s.
- Consensus persistence and engine-boundary probes each passed three randomized
  iterations. Group-count benchmark and root precommit passed. These probes originally
  used test-scoped Ratis; the M3 log library now has an inactive runtime dependency.
- Framing/checkpoint module: 24 randomized iterations passed. Corruption of every
  individual frame byte, every truncated prefix, oversized declared lengths, and
  mutation of caller buffers are covered. Module check and root precommit passed.
- Metadata durability: actual post-rename directory-sync failure test, artifact
  origin/uniqueness tests, and patched-Ratis replay probe passed. The log module's
  33 randomized cases, both module checks, and root precommit passed. Other upstream
  class/resource bytes and the replacement's public API remain unchanged.
- Storage identity: 18 randomized cases passed, including real lock contention,
  every-byte corruption, bounded oversized reads, lost identity beside existing
  data, and a directory-sync failure on discovery of an existing identity. The
  host must provision a durable directory, and the backend must compare its identity
  with authorized membership; this primitive alone does not activate a voter.

- Failed group initialization: a native storage-lock leak was reproduced, then
  fixed in the curated `ratis-server` artifact. Its two regression/artifact tests
  and the runtime suite passed three iterations (18 journal cases total), alongside
  75 unit cases, both vendor module checks, the log module check, and root precommit.
  The earlier 201-operation persistence probe and shell process smoke test also
  passed three iterations. Both merged vendor artifacts now receive a complete
  third-party API/JDK-jar-hell audit in addition to dependency audits.
- Corrupt log open: recovery refused the invalid segment header, but the unpublished
  log leaked its cache-eviction thread. The curated server artifact now closes logs
  whose open fails. Three iterations passed (75 unit and 21 journal cases), with
  vendor/log checks, full artifact audits, byte preservation, and root precommit.
  The corrupted segment remains intact for diagnosis.
- Static journal runtime: persistent majority appends, two shared physical groups,
  selected logical-stream replay with payload verification, leader close/full restart,
  majority-loss refusal, namespace/disk-reserve refusal, required client certificates,
  trusted-client admin RPC rejection, and bind failure without JVM exit. The runtime
  holds a storage lease and validates exact static placement before recovery. Local
  log-entry and byte/count watermarks refuse further admission rather than delete
  unarchived history. The final three-iteration run passed 75 unit and 12 socket
  cases with zero failures/errors/skips; module check and root precommit passed.
  Details are in [log/README.md](log/README.md).

- Initial activation and voter lifetime: real child JVM tests reproduced failover
  becoming unavailable when the third initial voter still had an empty log, and
  SDK close releasing a volume while its disk worker remained stalled. The runtime
  now gates initial writes on a seed committed by all initial voters plus a
  majority-committed activation marker; subsequent writes still require a majority.
  Volumes remain exclusively leased until their JVM exits, including after failed
  startup. Independent process tests cover leader SIGKILL, two shared groups,
  selective replay, full JVM restart, and a stalled shutdown retaining the volume
  lock until process exit. In-process tests cover reserved control identities,
  repeated activation, pre-interrupted close, and retrying cleanup on a clean thread.
  Three iterations passed 75 unit and 30 journal cases, with module checks and
  root precommit. These are local process tests, not independent-AZ qualification.

These checks validate the listed foundations. They are not evidence of completed
three-AZ durability, millisecond quorum latency, or production readiness.
