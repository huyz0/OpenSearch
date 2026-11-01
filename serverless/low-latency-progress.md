# Low-latency implementation progress

Branch: `low-latency-serverless`. The delivery plan is
[low-latency-serverless-plan.md](low-latency-serverless-plan.md).

Index writes still use object-store WAL durability. An explicit journal host now
drives replicated-log activation and membership control. Quorum-mode index writes,
committed-only serving engines, streaming search replicas and asynchronous S3
archival remain pending.

| Milestone | Delivered | Remaining |
| --- | --- | --- |
| M0 | Node timing histograms, WAL group counters, explicit open-loop benchmark, initial real-process filesystem baseline | Additional pipeline stages during extraction; actual S3 and sustained-load qualification |
| M1 | Append and streaming recovery interfaces, optional async dispatch with cancellation/rejection semantics, existing S3 path integration, bounded versioned log/checkpoint identity codecs | Subscription/token formats and shell adapters for consensus |
| M2 | Three-replica persisted-log/restart probe, committed-serving engine boundary probe, group footprint benchmark, [consensus ADR](low-latency-consensus-adr.md) | Shell preparation-service integration, duplicate-indexing overhead, failure and security gates |
| M3 | Engine-independent checksummed journal, durable metadata/volume identity and activation catalog, bounded peer-only mTLS runtime, committed replay, crash/storage-error tests, authorized learner replacement and bounded host controller | Command-line assignment/TLS loading, remaining partition/hardware histories and sustained resource qualification |
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

- Stalled shutdown resources: the native log worker closed its output stream and
  freed its direct write buffer after its shutdown timeout even if disk work was
  still active. A child-JVM regression reproduced the channel close. The curated
  server artifact now leaves both resources alive until worker and flush executors
  terminate, or process teardown reclaims them. The targeted test passed three
  iterations; 75 unit and 30 journal cases, vendor/log module checks, full artifact
  audits, upstream byte verification, and root precommit also passed.

- Bootstrap catalog: permanent deployment/group reservation registers, canonical
  genesis placement, pre-volume startup authorization, and conditional publication
  of the first committed activation marker. Lost reservation/activation replies
  reconcile against the original identity; active groups cannot bootstrap another
  history. Recovery requires replay of the catalog's original activation marker.
  Three iterations passed 90 log unit cases, 33 journal cases, 15 catalog protocol
  cases, 45 existing metadata cases, and three real-journal/catalog integration
  cases. Log module check and root precommit passed. These counts exclude Gradle's
  filtered-class descriptors. Node lifecycle wiring, safe learner replacement, and
  logical-shard authorization remain pending; catalog tests use local FS registers.

- Learner/replacement feasibility: a native SDK probe confirmed voter-only
  `ALL_COMMITTED` can succeed while a learner is stalled. After requiring that
  learner's committed prefix to cover the cut, native CAS replaced one voter
  atomically, rejected stale membership, and survived subsequent leader loss using
  the replacement quorum. Both this 26-operation probe and the original
  201-operation restart probe passed three iterations, with root precommit.
  Production durable transitions and authorized learner startup remain pending.

- Persisted-prefix recovery: a real-process test reproduced startup accepting a
  voter after its newest acknowledged segment was removed. Per-group fixed-size
  boundary evidence now persists after log force and before native progress/ACK,
  and recovery validates it before SDK startup. Corrupt floors and missing tails
  refuse recovery; valid unacknowledged tails are forced before adoption. Another
  process history verifies legitimate uncommitted-tail truncation and subsequent
  full restart. Three iterations passed 108 unit, 42 journal and three catalog
  integration cases; both consensus probes (six cases), vendor/log checks, complete
  artifact audits and root precommit also passed. SDK global timer checks now match
  exact owned thread identities, with a same-name foreign timer regression.
  Additional local metadata force cost still needs workload qualification.

- Live storage failure: independent child-JVM tests inject an I/O exception on
  the native log worker before force. A failed leader permits healthy-quorum
  takeover while retaining its volume lease, and a failed follower cannot supply
  a second persistent acknowledgement after the other follower is lost. Failed
  requests remain ambiguous and may have committed elsewhere. Both histories
  passed three iterations (six cases), with root precommit. These tests cover
  reported local I/O errors; they do not qualify arbitrary hardware failure or
  stalled I/O availability.

- Membership serialization: separate bounded controller revision and pending
  replacement records bind exact placements and idempotency nonces. A permanent
  per-group voter reservation prevents identity reuse. Activation creates its
  membership head first; missing heads never reset to genesis. Competing controller,
  lost reply, deleted head, stale revision and retired identity histories are
  covered. Static startup refuses pending intent before touching a volume. Native
  learner startup, catch-up proof, configuration changes and completion publication
  remain pending; no replacement endpoint is enabled. Three iterations passed
  126 log unit, 36 catalog protocol and three catalog integration cases. The broad
  journal/consensus checks passed 48 journal and six native probe cases, with log
  module check and root precommit. These counts exclude filtered-class descriptors.

- Callback reentry: a majority-loss history exposed activation reentering native
  async submission under leader/watch locks, deadlocking both progress and close.
  Bounded node workers now handle SDK submission and notification, with permits
  released before public completion. This also fixes activation and dependent
  appends at the valid one-request limit. Exact failing seed and one-permit
  regressions passed three iterations. Dispatcher tests cover saturation with
  slow callbacks, monitor separation, cancellation, synchronous failure and late
  completion during close. The broad run passed 144 unit, 51 journal, three catalog
  integration and six native probe cases across three iterations, with log module
  check and root precommit. No additional SDK patch or per-write S3 call is needed.

- Colocated retirement recovery: completed controller membership can exclude one
  group while its volume's other groups recover normally. Before native startup,
  exclusive-lease recovery atomically moves authorized retired directories out of
  native discovery and synchronizes both parents. Old bytes remain intact; partial
  rename, duplicates, stale authorization and corrupt bindings have coverage.
  Completed membership also checks permanent exact-placement reservations for new
  voters with bounded direct reads. The process history models completed controller
  authority and does not qualify native promotion. Three iterations passed 156
  unit, 57 journal, 39 catalog and three catalog integration cases; log module check
  and root precommit passed. No additional native-library patch was needed.

- Native metadata capacity: a child-JVM history reproduced recovery accepting a
  log larger than its configured retained-entry budget. The persistence hook and
  pre-SDK recovery now enforce a cached ceiling including finite in-flight/control
  allowance, covering automatic native metadata as well as customer proposals.
  Both saved boundaries and valid larger observed tails are checked. Restoring
  the original capacity recovers acknowledged data without deleting history.
  Three iterations passed 159 unit, 60 journal and three catalog integration cases,
  with log module check and root precommit. The current absolute-index ceiling
  must become a retained-prefix count before snapshot/purge is enabled.

- Inbound peer RPC limits: a real mTLS burst reproduced 64 blocked handlers while
  holding the consensus monitor, despite the SDK's async-request pool setting.
  A supported server customizer now installs four dedicated node workers and a
  finite protocol queue, independent of submission/notification workers. The
  regression overloads that queue, verifies the thread bound and restores quorum
  append/replay after releasing the monitor. Three iterations passed 159 unit,
  63 journal and three catalog integration cases, with log module check and root
  precommit. Outbound callback and sustained-load qualification remain pending.

- Authorized native replacement: explicit assigned startup requires the winning
  pending intent and permanent original learner placement. A nonce-bound committed
  barrier and UUID-keyed learner commit proof precede atomic three-to-three native
  CAS. Permanent receipts precede completed controller heads; lost replies, newer
  election configuration entries and later transition retries preserve the first
  receipt. Operation nonces are permanently bound before pending-head CAS, preventing
  reuse across revisions or orphan contenders. Empty initialized learners can resume
  before listener addition; missing vote/floor metadata still refuses startup.
  Stalled learners, leader replacement, replacement-quorum survival, pending-head
  full restart, successive replacements and metadata reply loss have real socket/JVM
  coverage. Three iterations passed 183 unit, 69 journal, 42 catalog protocol and
  three catalog integration cases, with log module check. The RPC overload fixture
  now establishes TLS before holding the consensus monitor, excluding cold setup
  from its overload window. Node lifecycle, failed-learner recovery and outbound
  connection retirement remain separate work; shell quorum writes remain disabled.

- Retired transport lifetime: a real peer RPC reproduced an established channel
  surviving native removal. Completion publication/retry now resets unused local
  SDK clients through its supported API. All current/previous voters and listeners
  across colocated groups protect still-needed connections. Three iterations passed
  the two-group retention/closure/append/replay history and the complete catalog
  integration history (six cases). Lightweight descriptors remain bounded by the
  current history ceiling and need reclamation before unbounded archival history.

- Node host lifecycle: an explicit bootstrap assignment attaches and opens the
  authenticated journal before metadata-plane availability. All-voter activation
  and membership changes reconcile asynchronously on exactly one worker with one
  queued task. Per-group pending native work and wakeups coalesce; a 30-second
  metadata cache and five-second failure backoff keep progress edges off S3.
  Readiness remains historical controller state. Close handles blocked catalog
  startup and late native opening, preserving process-lifetime volume quarantine.
  Native unavailability is distinguished from activation catch-up. Three iterations
  passed 15 real controller cases and 84 existing bootstrap/scheduler cases, with
  zero failures/errors/skips. Tests include 10,000 coalesced wakeups without extra
  catalog reads, automatic two-group activation/replacement, new-quorum survival,
  colocated availability, failure diagnostics, startup/close races and queue-bound
  enforcement, including simultaneous learner/genesis roles in different groups.
  Command-line loading and quorum-mode index writes remain pending.

- Leader-removal dispatch: concurrent proposals exposed an SDK close/client-worker
  monitor deadlock. Managed state machines now submit directly from the existing
  bounded host dispatcher; other SDK state machines retain upstream dispatch.
  A deterministic native-client-pool isolation test fails before the fix, and
  process replacement now drains accepted proposals before killing the old leader
  and checks acknowledged writes on the new quorum. Admission rejects closing or
  removed leaders even if the native readiness hint remains true. Three iterations
  passed 183 unit, 75 journal, six native probe and 15 controller cases. The catalog
  integration's local-publisher cleanup assertion was corrected and its three
  iterations passed. Curated artifact audits and unchanged-upstream checks passed.

- Early original restart: an initialized original voter stopped before its first
  quorum can resume from its native bootstrap configuration under unactivated
  catalog authority. Term/vote metadata, binding and verified empty history remain
  mandatory; activated membership and established prefixes refuse this exception.
  A real child restart preserves its recorded term, refuses another candidate in
  that term, forms the initial quorum and retains writes after leader loss.
  Three iterations passed 192 unit and 78 journal cases, with log module check.

These checks validate the listed foundations. They are not evidence of completed
three-AZ durability, millisecond quorum latency, or production readiness.
