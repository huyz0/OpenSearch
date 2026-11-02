# Low-latency implementation progress

Branch: `low-latency-serverless`. The delivery plan is
[low-latency-serverless-plan.md](low-latency-serverless-plan.md).

Index writes still use object-store WAL durability. An explicit journal host now
drives replicated-log activation and membership control. A committed-serving
primary adapter has engine and shard-level tests but is not enabled by the
launcher. Quorum-mode index writes, streaming search replicas and asynchronous
S3 archival remain pending.

| Milestone | Delivered | Remaining |
| --- | --- | --- |
| M0 | Node timing histograms, WAL group counters, explicit open-loop benchmark, initial real-process filesystem baseline | Additional pipeline stages during extraction; actual S3 and sustained-load qualification |
| M1 | Append and streaming recovery interfaces, optional async dispatch with cancellation/rejection semantics, existing S3 path integration, bounded versioned log/checkpoint identity codecs | Subscription/token formats and shell adapters for consensus |
| M2 | Three-replica persisted-log/restart probe, committed-serving engine boundary probe, group footprint benchmark, [consensus ADR](low-latency-consensus-adr.md) | Shell preparation-service integration, duplicate-indexing overhead, failure and security gates |
| M3 | Engine-independent checksummed journal, durable metadata/volume identity and activation catalog, bounded peer-only mTLS runtime, committed replay, crash/storage-error tests, authorized learner replacement, bounded host controller and file-based launch configuration | Remaining partition/hardware histories and sustained resource qualification |
| M3a | [Cell/active-state design](low-latency-capacity-adr.md), runnable models/raw results, per-group proposal/peer isolation and resident-context admission primitive | Metadata routing, dynamic group admission, owned bounded engines and broader isolation pending |
| M4 | Committed-serving adapter, resolved payload, fixed-binding replay, admitted private resources, mapping/identity guard, private native-buffer spill/fencing, admitted group-worker dependency gate and shared-mapper TLS/native-shard probe | Native mutation compatibility and shell admission/dispatch integration, serving/total-memory and SSD governors, ownership/schema/lifecycle controllers and shell integration |
| M5–M9 | Not implemented | Fresh readers, selective replication, archival, lifecycle, migration |
| M4a | Planned; no runtime completion claimed | Healthy-cell write continuity, durable internal retry outcomes, bounded failover queues and route generations |
| M6a | Planned; no runtime completion claimed | Missing-chunk/resumable bulk shard copies, peer/S3 source selection and live-versus-recovery scheduling |
| M9a | Planned; no runtime completion claimed | Fenced hot-shard movement, cell expansion and post-failure capacity headroom |
| M10–M12 | Not implemented | Scale/cost/failure qualification, controlled rollout, optional attribute/QUIC extensions |

## Revised delivery order (1 November 2026)

The plan now treats local benchmarks and models as exploratory evidence for design
selection, not as a requirement for a production-like benchmark environment.
M3a closes the cell/metadata/capacity design before M4 integration is finalized;
its runtime bounds develop alongside M4. M4a adds failure isolation and explicit
retry outcomes. M6a makes actual cross-node shard data copying a first-class
milestone; its design can proceed before consensus tuning finishes. M9a covers
safe hotspot relocation and cell expansion. M10 retains production qualification.

The initial local compact-array probe suggests active-only scheduling avoids
43–46x work at 1% activity. The fluid model shows that more equally provisioned
groups can worsen a 20%-hot-stream overload. These are design estimates, not
OpenSearch throughput, JVM heap, multi-AZ availability or completed implementation
evidence. Millions of logical descriptors do not imply millions of open engines.
Avoiding a global write stop does not promise uninterrupted synchronous commits
for a stream whose leader fails; minority writes remain forbidden. The original
delivered work and validation history below are preserved.

The committed [capacity experiments](benchmarks/capacity/README.md) extend the
initial research with runnable sources, raw trials, a compact active representation,
per-host limits and cost inputs. The historical speedup estimates above are not
universal: a sparse gather can lose to a dense scan, while the compact active set
was faster in this run. The ADR selects prototype profiles and records that
existing whole-volume bindings still require a per-group admission protocol before
dynamic host assignment. This closes a design decision, not runtime M3a delivery.

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

- Launcher assignment: the normal bootstrap reads a bounded versioned placement
  file before node construction, with strict field/type/UUID validation and
  dedicated PKCS12 credentials loaded from secure settings. A one-shot identity
  command provisions or discovers an SSD lineage without starting a node or
  contacting object storage. Passwords in ordinary settings and ambiguous assignment
  sources are refused. The real launcher integration loads an OpenSearch keystore,
  activates a three-host authenticated quorum and appends/replays committed data.
  Three iterations passed 27 configuration, 21 bootstrap and 21 catalog/controller
  cases, including the corrected real-filesystem disk provisioning fixture.
  The full shell module check passed after restoring missing API documentation.
  The final filesystem-wrapper update passed 27 configuration, 63 scheduler and
  three launcher-quorum cases over three iterations, plus repository precommit.

- Voting-state integrity: a child restart reproduced a second vote in the same
  term after a one-bit change in the separate native term/vote file. The curated
  writer now includes a version and checksum in the same forced atomic write;
  bounded preflight verification refuses corrupted, missing, oversized or unsealed
  voting state before native services start. Binding format four prevents a silent
  downgrade. Earlier experimental voter directories are preserved on refusal and
  require authorized fresh-identity replacement from a healthy quorum. Three
  iterations passed 204 unit, 81 journal, 21 catalog/controller and six consensus
  cases, including successful vote preservation and corrupt-vote refusal after
  real process restarts. Vendor audits and unchanged-upstream checks passed.

- Physical-group proposal isolation: real lock-stall histories reproduced a
  healthy colocated group's timeout when another group occupied all shared workers,
  and immediate rejection when that group filled the shared request/byte budget.
  Each active group now owns one bounded proposal/completion worker and a reserved
  maximum-size pending proposal; borrowing cannot consume another group's reserve.
  Node-wide ceilings and cancellation semantics remain intact. Impossible budgets
  refuse before volume binding. Three iterations passed 213 unit, 93 journal and
  21 catalog/controller cases, including request/byte borrowing and release,
  blocked-group progress and pre-binding refusal. The capacity model now includes
  the minimum per-group pending-byte requirement. Shared peer-RPC and broader cell
  failure isolation remain separate gates.

These checks validate the listed foundations. They are not evidence of completed
three-AZ durability, millisecond quorum latency, or production readiness.

- Peer RPC group isolation: a real held-division monitor reproduced a healthy
  colocated group's three-second RPC timeout. Parsed callbacks now use one worker
  per physical group with bounded active calls/messages, group reservations and
  first-message expiry. Context/order, overload, cancellation charging, unknown
  groups and snapshot notification versus data are covered. The complete run
  passed 231 unit, 96 journal process/runtime, 21 catalog/controller and 15 committed
  replay cases across three iterations, with root precommit (8m45s). The follower
  persistence history also passed ten repetitions with its failing seed after
  explicitly waiting for initial catch-up before disk failure. Native snapshot
  notifications remain available for full-log learner catch-up. Capacity estimates
  now charge the added peer message/worker allowances; these are not measured RSS.

- Private primary application now stops unresolved dynamic mappings before native
  indexing can assign sequence numbers. The regression first reproduced a bare
  engine returning success for that document, then passed through the resource
  owner's guarded path. Tests cover reparse after a quiescent mapping update,
  routing, conditional conflicts, missing deletes and rejection of assigned,
  replica or wrong-fixed-term operations. The shared engine/resource suite passed
  132 cases, including external-version/delete/recreation coverage, and real
  journal replay passed 15 cases over three iterations. Shell check and root
  precommit passed. A local mapping update
  in this test is not a committed schema/ownership barrier or enabled client mode.

- Private buffer governance now includes delete tombstones and in-flight version
  maps. A delete/refresh loop first reproduced the old counter's continued
  admission beyond the limit. Guarded mutations spill reclaimable buffers to SSD;
  persistent excess or native buffer work failure fences the context and retains
  its admission until cleanup. The broader run passed 138 unit and 15 real-journal
  replay cases over three iterations, shell check and root precommit. The final
  review avoids double-counting writer flush bytes already in native writer RAM.
  This bounds known buffer estimates at operation boundaries, with one-operation
  overshoot; it does not establish total heap/RSS, parsing, mmap or SSD limits.

- Preparation sequencing now requires an explicit executor for preparation and
  committed application. The real journal probe uses one owned worker and one
  queue slot; native application no longer runs inline on its journal callback.
  Tests cover close before queued work starts, rejection without inline fallback,
  synchronous journal replies, and dispatch failure after native work starts.
  Running work keeps close pending until its own finally completes; canceled
  queued attempts clear their preparer/payload references. The full focused run
  passed 153 unit and 15 journal replay cases over three iterations, with shell
  check and root precommit. Host-wide request bytes, queued-wrapper ownership and
  group worker lifecycle/admission still need runtime integration.

- Host preparation dispatch now bounds active bindings, pending request count and
  declared bytes, using one worker per assigned group. Both binding and request
  admission reserve progress for every group. Queued/running wrappers retain
  their byte charge through close and uncertain scheduling failure until they
  drain. Tests cover healthy-group continuity during a stall, queued close,
  accepted-append cancellation isolation and post-queue failure. The real journal
  probe now uses this host dispatcher. The full run passed 171 unit and 15 journal
  replay cases across three iterations, shell check and root precommit (3m7s).
  Capacity estimates now include the separate preparation budget and worker stacks;
  they still do not qualify parsed documents, plugins, real JVM residency or cloud
  throughput. Engine/resource lifecycle and durable logical controls remain pending.

- Logical control payload groundwork: a bounded versioned codec for ownership,
  schema, mutation, outcome-only and retirement actions. Retry identity retains
  the original UUID, physical-log observation and input digest. Golden encoding,
  malformed/truncated/oversized inputs, action-specific fields, mutable aliases
  and envelope integrity are covered. The log suite passed 249 cases over three
  iterations, with root precommit. This codec does not yet implement committed
  logical authority, outcome retention or controlled replay in the runtime.

- Logical state validation now folds one verified ordered stream with producer
  epoch/engine term, exact schema dependency, contiguous mutation sequence and
  permanent retirement. Bounded retry identities return the original checkpoint;
  an evicted identity below the retention fence is refused instead of executed
  again. Tests cover competing owners, stale producers, schema/sequence refusal,
  malformed and legacy histories, outcome-only records and deterministic recovery.
  The log suite passed 270 cases across three iterations, with root precommit.
  This is a caller-serialized semantic fold, not runtime commit authority: distinct
  protocol framing, authenticated admission, durable retention configuration,
  bounded resident replay state and journal receipts remain integration work.

- Controlled replay now uses a distinct logical frame code, preserving the five
  existing wire codes. A bounded per-stream cursor starts from the complete local
  committed history and emits only newly accepted effects, alongside receipts
  that distinguish physical position from logical acceptance and original retry
  outcome. Raw legacy records cannot silently become logical writes. Partial
  replay failure permanently stops the cursor. The log unit suite passed 294
  cases over three iterations; a real TLS three-voter leader-loss history passed
  three iterations, confirming takeover, duplicate suppression, stale-write refusal
  and identical reconstruction across small versus large scan batches. Root
  precommit passed. Producer authentication, durable protocol configuration,
  host-wide residency admission, serving application and client integration remain.

- Logical proposals now require the ready local journal leader's disk-bound voter
  identity. Raw append cannot bypass explicit logical admission, and native
  transaction admission rechecks the actor. Full command size is validated without
  encoding, then serialization/envelope allocation occurs only after the existing
  request/byte permit is acquired. Native tests cover impersonation, direct native
  admission, follower submission, reserved/foreign identities, oversized commands
  and takeover/retry replay. The final run passed 294 unit and 57 native cases over
  three iterations plus root precommit (5m43s). The broader run also passed 45
  process/recovery, 21 catalog and 15 serving-replay cases. Its repeated in-process
  suite exposed the deliberate 128-volume lifetime cap; native suites now fork
  separately with a shared fixture, preserving that production safeguard. This
  admission API returns physical commitment; logical acceptance still requires
  verified semantic replay, and client quorum mode remains disabled.

- Permanent logical stream assignments now bind each activation UUID to one
  activated physical history and fixed outcome-retention/payload limits. Creation
  uses a dedicated per-activation conditional register; direct reads validate key,
  deployment, checksum and original group activation. Conflicting group, shard or
  protocol changes are refused, including after lost replies and concurrent
  reservation. Public proposal and replay APIs validate this catalog-loaded
  binding without per-write S3 IO. The run passed 309 log and 54 catalog unit cases
  across three iterations, plus three native catalog histories, shell check and
  root precommit (1m12s). The native history verifies refusal before durable group
  publication, exact payload admission, wrong-history/capability refusal and stable
  replay through leader loss and two voter replacements. Storage-mode activation,
  host-wide state budgets, group-wide voter frame-capability checks and the
  ownership/schema/application controller remain.

- Fixed retry identity when an ambiguous client operation resolves differently on
  a new owner. A reproduced regression rejected an original mutation retried as
  an outcome-only result. Mutation and outcome-only records now share the client
  request identity domain; control actions, original observation and input digest
  still must match. The original checkpoint is returned in both directions, with
  no extra sequence allocation. The run passed 312 unit and six native logical
  cases across three iterations, plus root precommit (1m17s). The native history
  covers both classifications after leader loss and deterministic reconstruction.

- Voter wire/history compatibility now has a permanent per-group protocol record.
  The controller reserves it explicitly before bootstrap; original, recovering and
  replacement voter startup compares payload, queue/batch, request-count and
  retained-history limits before native volume access. Missing/corrupt active
  contracts cannot be recreated, and logical streams cannot exceed the group
  payload bound. Node-wide memory/cache/disk allowances remain local. Tests cover
  conflicting profiles on both startup paths, lost replies, corruption, key
  mismatch, stream bounds and an actual incompatible learner. The file-bootstrap
  fixture exposed previously hidden mixed voter limits and now declares its shared
  profile. The run passed 324 log and 69 metadata unit cases plus 21 native
  catalog/controller cases across three iterations, shell check and root precommit
  (1m59s). Existing experimental groups without this contract are refused; no
  automatic metadata upgrade or client quorum storage mode is enabled.

- Retry recovery now exposes a bounded identity lookup before payload preparation
  and a direct verified read of the original physical result frame. Lookups do not
  scan, advance state or extend outcome retention, and partial replay failure
  fences them. Unknown means only unknown in the inspected prefix, requiring the
  ownership barrier and exclusive sequencer before execution. Historical reads
  verify assignment, committed/applied prefix, stream, entry term, frame integrity
  and payload bounds; they do not assert logical acceptance or latest authority.
  Tests cover expiry, retirement, identity conflicts, cursor progress and original
  mutation/result-only bytes after leader loss, plus invalid coordinates. The run
  passed 333 unit and six native logical cases across three iterations with root
  precommit (1m24s). Opaque outcome payloads remain in the log rather than the cache.
