# Fleet scale test: results and method

What happens to acknowledged writes when a fleet of serverless shell nodes is run hard against a real object
store and then broken on purpose. The harness is `:serverless:testkit:fleetTest`, and it is kept runnable:

```
./gradlew :serverless:testkit:fleetTest -Dtests.fleet.endpoint=http://127.0.0.1:9200 \
    -Dtests.fleet.nodes=6 -Dtests.fleet.heap=3g -Dtests.fleet.population=1000000 \
    -Dtests.fleet.max_shards=400 -Dtests.fleet.active=40 -Dtests.fleet.writes_per_second=200 \
    -Dtests.fleet.scenario_seconds=600
```

Every `tests.fleet.*` property is listed in the javadoc of `ServerlessFleetScaleTests`. A run writes
`build/testrun/fleetTest/build/fleet-results/<run>/results.md`, the full `ledger.csv`, and each node's log.

## The headline

**No acknowledged write was lost, and no refused write became visible, in any of the seven scenarios at full
scale** -- six nodes, a million indices, 200 writes a second, ten minutes a scenario: about 540,000
acknowledged writes read back one by one. Getting there took fixing five defects the runs found, two of which
could make acknowledged writes unreadable and one of which made the fleet stop acknowledging almost entirely
(below, "What the runs found").

| | |
| --- | --- |
| fleet | 6 shell JVMs, 3 GB heap each, shard cap 400, roles ingest+search |
| population | 1,000,000 indices on RustFS 1.0.0, single drive |
| load | 200 writes/s open-loop; 240 active indices, 5% of writes to dormant ones, 5% to churned names |
| lease TTL | 30 s |
| kill -9 takeover | 267 of 267 shards writable again; **p50 59.4 s, p99 120.6 s** (2.0 and 4.0 TTLs) |
| pause past the lease | 209 of 250 re-taken while frozen; p50 44.0 s, p99 61.0 s; the thawed zombie acknowledged nothing it no longer owned |
| partition | 2,120 writes sent to the cut-off node after its lease ran out: **0 acknowledged** |
| 503 SlowDown, half of all requests for 60 s | writes stalled (p50 30 s), recovered 20 s after the burst, no divergence |
| idle background | ~77,000 store requests per node-hour holding ~400 shards: 190 per held shard per hour |
| wide `logs-*` search | **fails at this population**: the store cannot list a million names under one prefix |

## Goal 9: the five open items, and what a starved pool had been hiding

Goal 9 took the five items the first full-scale runs left open, correctness first, and re-ran the fleet at a million
indices after each change. The final run is `run1790863291492`: the same fleet, population and load as above, five
scenarios of five minutes each after a two-minute warm-up, on the bucket the earlier runs left behind.

**Correctness held throughout: 0 acknowledged writes lost and 0 refused writes visible in every scenario of every
run, and 0 searches silently answering from a commit behind the log.** The new stale-read check searched up to 5,991
indices after each scenario; every one with no live writer answered with all of its acknowledged documents or
reported itself as not answering.

| | before (Goal 8) | after (Goal 9) | target |
| --- | --- | --- | --- |
| lost / refused but visible | 0 / 0 | **0 / 0**, all five scenarios | 0 / 0 |
| stale reads of an unowned shard | possible, unmeasured | **0 silently behind**; up to 3,894 per scenario reported as not answering | 0 |
| kill -9 takeover, p50 / p99 | 59.4 s / 120.6 s | **35.3 s / 43.2 s** (109 of 109 shards) | ≤ 45 s / ≤ 60 s |
| idle store requests, per held shard-hour | 190 | 86-106 on the nodes that kept their shards; 227 across the fleet while it released shards | 5× lower, flat |
| SlowDown burst | writes stalled to the client timeout, p50 30 s; back in 20 s | 939 refused early with 429 + `Retry-After`; accepted p50 16.9 s, p99 39.5 s; back to baseline in 116 s | early 429s, quick recovery |
| wide `logs-*` at 1M | listing failed server-side | resolved from rollups without listing in **0.17-0.23 s**, cold or warm, then refused at the 10,000-index cap | resolve without listing |

### Part 1: a shard nobody owns cannot be read behind its log

A node whose lease lapses gives back its heads unpublished, and a get of such a shard answered from the published
commit. Each published `CommitManifest` now records how far into the shard's log it reaches (`wal_ordinal`). A reader
whose shard has no live writer checks whether any record landed past that point; if one did, a get takes the shard
and answers from its writer, and a search reports the shard as not answering (`shard_behind_log`) rather than
answering short. Manifests written before this carry no ordinal and are judged by comparing sequence numbers.

Validating it found a durability-adjacent bug: the shell never advanced the global checkpoint, so the engine's safe
commit stayed the one it opened from, which named files the collector had since deleted; the engine failed, and a
failed engine kept its head. Publishing now advances the global checkpoint, and a failed writer is closed and its
shard re-taken from the published commit and log (`ServerlessCommitRetentionTests`, `ServerlessFailedEngineTests`).

### Part 2: wide searches resolve without listing

A pattern whose prefix is covered by a rollup group is resolved from the group's rollups instead of listing the
store. A name created after a crash before its rollup entry, or an alias, can never be ruled out: an alias entry
rules nothing out, a restore widens the rollup before it publishes, and a missing entry costs one descriptor read
rather than a wrong answer (`ServerlessPruningTests`). At a million indices `logs-*` now resolves in about 0.2 s; it is
then refused, correctly, because 40,000 to 48,000 indices could match -- see "Still open".

### Part 3: takeover after kill -9

Before, every shard of a dead node waited for a request to find it unowned. Now each survivor hears the departure
and takes the shards that hash to it, once the departed node's lease, read from the store, has run out. Renewal moved
to its own thread so a burst of activations cannot make a busy node look dead. In the final run the lease ran out 26 s
after the kill, survivors held all 109 heads by p50 35.3 s, and the first write was acknowledged a median 7.9 s after
that.

### Part 4: idle cost

Every open index's descriptor was re-read every 30 s, 120 reads an hour per index and most of an idle node's
traffic. Every operation now passes the incarnation fence before as well as after it runs, and the fence applies the
descriptor it reads, so a mapping or settings change made through another node is in force for the first operation
after it; the periodic re-read is a five-minute backstop (`ServerlessDescriptorPropagationTests`, which also found
that such a change used to fail the owner's next write with a 500).

Measured over 180 s with the load stopped, after 75 s for in-flight writes to drain, the two nodes that kept their
~390 shards cost 33,000 and 41,000 requests an hour, **86 and 106 per held shard-hour against 190 before**: about
2× lower, not the 5× asked for. The fleet-wide figure, 227, is dominated by the other four nodes releasing 80 to 160
shards each during the window -- the publishes and sweeps of going idle, not steady background. What remains per
held shard is head verification, reader staleness checks and the descriptor backstop; separating those from the
release traffic is the next measurement.

### Part 5: SlowDown is shed early

A node now limits the writes waiting on the store with additive increase and multiplicative decrease on log-append
time, and refuses past the limit with `429` and a `Retry-After`, before applying anything, so every refusal is
honest (`ServerlessWriteBackpressureTests`). During a minute of SlowDown on half of all requests (13,828 injected),
939 writes were refused at once instead of timing out; the limiter was back to baseline 116 s after the burst.
Without any injected throttling it also sheds load when this single-drive store is simply slow: about 26,000 of
steady's refusals said "the object store is slow to take writes". That is the store being the bottleneck, reported
honestly; whether the one-second target suits this store is open.

### What the runs found along the way

- **Shard activations starved the GENERIC pool they ran on.** Writes wait on GENERIC for their shard's activation,
  and activation passes wait there for their own. Under load all 128 threads were such waiters and the activations
  sat in the queue behind them, each starting only when a waiter timed out: nodes showed 300 activations queued
  with eight slots "running" and nothing running, and shards closed locally went unserved for 10 to 20 minutes,
  every forwarded write answered "not open here". This was most of every earlier run's steady-state errors (22% to
  75% of writes). Activations now run on `serverless_activation` (`ServerlessActivationStarvationTests`): the queue
  wait fell from 3 minutes to under 20 s, "not open here" from 14,734 to 3 per steady scenario, and acknowledged
  steady writes rose from 14,960 to 35,450 in the steady-only rerun.
- **A dynamic mapping addition that conflicted with the mapping poisoned the node's growth queue** for that index,
  failing every later write that added a field. Fixed with the descriptor propagation above.
- **Proactive takeover trusted the membership view**, which loses a member whose lease read failed. Live peers'
  shards were never taken -- the head's own liveness check refused -- but each attempt cost store reads. It now
  reads the lease first.
- **The harness misreported twice.** A delete whose outcome the client never learned, applied 160 s later, was
  counted as 23 lost writes; and the read-back retried every unread shard through one node, which had to take 1,200
  shards against a cap of 400 and reported 16 writes as lost that forensics found on every node. Both fixed; neither
  was data loss.

### Still open

- **Rollup "owned" marks outlive crashed owners.** A shard marked owned is never ruled out of a wide search, and the
  mark is cleared only on a clean release, so every crashed run leaves its shards' marks behind. A `logs-*` search
  for a range thirty days in the future still found about 47,700 candidates. *(Goal 10: the marks were not the
  cause -- see below.)*
- **Survivors at their cap take a frozen node's shards slowly.** With every node near 400 shards, the pause scenario
  re-took 44 of 376 while the owner was frozen (p50 55.8 s); taking needs evicting first. *(Goal 10: 141 of 147.)*
- **The idle measurement includes going idle.** See part 4.
- **The write limiter's target** against this store, and SlowDown recovery in 116 s rather than 20 s; see part 5.

## Goal 10: cleaning up after a crash

A crashed node leaves everything it held named to it: shard heads, its claims, and rollup entries. Goal 10 made the
fleet clean that up itself, and found on the way that the biggest leftover was not a crash's at all. Final run
`run1790905591572`, the same fleet and load, five scenarios on the bucket every earlier run -- several of them crashed
-- had left behind.

**Correctness held in every run of the milestone: 0 acknowledged writes lost, 0 refused writes visible, 0 searches
answering from a commit behind the log.** Two runs' ledgers reported losses (16 and 399 writes); in both, every one of
them was found -- by forensics reading through every node, or by taking the shard fresh and replaying its commit and log
-- and the cause was the read-back failing to reach shards that were changing hands.

| | before (Goal 9) | after (Goal 10) |
| --- | --- | --- |
| a dead node's shards, on a heard departure | all taken: opened, replayed, held | given back when fully published; only those with unpublished writes taken |
| pause: shards re-taken while the owner was frozen | 44 of 376 | **141 of 147** |
| kill -9 takeover p50 / p99 | 35.3 s / 43.2 s | **28.9 s / 36.7 s** |
| `logs-*` for next month: indices that cannot be ruled out | ~47,700, rising ~1,000 a scenario | **63,020 → 34,647 over the run**, falling every scenario |
| janitor, one hour | none | 32,495 cleaned up, 29 replays, 252 stale claims forgotten |

### Giving a dead node's shards back instead of taking them

A survivor that hears a departure used to take every shard the dead node claimed: open it, replay its log, and hold
it -- a few hundred mostly idle shards onto nodes already near their cap. Now each is first given back on the dead
owner's behalf, with a takeover's own guarantees: the owner must not be live, its lease is revoked, the head moves to
the next term with no owner, and the older terms are sealed so a writer that was not as dead as it looked can add
nothing. Then the log is compared with the published commit; if everything is published, the rollup mark and the
claim go and the shard is left for whoever needs it next. Only a shard whose log is ahead is taken and replayed
(`MetadataPlane#settleAbandoned`, `ServerlessCrashCleanupTests`). In the final run a departure typically read "taking 2
of its 309 claimed shards and giving back 55 fully published", and the frozen node's shards were re-taken almost all
within the window.

### The janitor

Each node runs a janitor every minute (`serverless.janitor.interval_millis`), splitting the work by hash between live
nodes, a bounded number of shards a pass, and following a full pass after five seconds so a backlog is worked through:

- **Claims of nodes long dead** -- a fleet that crashed as a whole, a departure nobody heard -- settled as above; a claim
  whose head has moved on is forgotten. Claims get at most half a pass, after a run where thousands of a crashed
  fleet's stale claims took every pass.
- **Rollup entries**, one bucket a pass in rotation: an owned mark under a head with a dead owner or none is settled as
  above, and a shard left behind its log is replayed, published and given straight back so it holds a cap slot for
  seconds. A commit published before manifests recorded their reach is judged from its commit point -- its segments
  file's highest sequence number against the log's -- rather than by a replay, which cost a full activation each.

### What the leftover actually was

A sample of the `logs-` rollups showed the owned marks crashes leave were 27 of 14,612 entries, 24 of them owned
legitimately. More than half, 7,758, were entries that recorded nothing: dormant indices opened only to be read -- a
get, a stale-read check, a verify retry -- and given back with nothing written. Activation enters an index before it
opens; release cleared the mark but left an entry with no digest, which can never rule its index out. Those indices
hold nothing, and the rollups' own invariant -- a name with no entry has nothing searchable -- means they need no
entry: an index whose every shard has no owner, no published commit and an empty log now has its entry removed, by a
swap that refuses if a writer has marked it since, and a writer enters it again before it opens. Releases do this for
what they give back empty, and the janitor for the 60,000 already there.

### Also fixed

- `clearOwned` skipped the register when the node's remembered copy said nothing would change; for a node clearing
  another's mark that copy could be stale. It now reads the register for those clears.
- Proactive takeover acted on the membership view, which loses a member whose lease read failed or came back late; it
  reads the departed node's lease first.
- The harness reports how far the active shards' terms move in each scenario and its read-back, and takes any node
  setting as `-Dtests.fleet.setting.<name>`.

### Still open

- **Write latency.** Steady write p50 in this milestone's runs was 4 to 8 s, against 0.4 to 1 s in Goal 9's, with the
  janitor working through the backlog on the same single-drive store; it should be measured again once the backlog is
  gone.
- **Idle cost** in this milestone's runs includes the janitor's backlog and is not comparable to part 4's figures.
- **About 35,000 indices** still could not be ruled out at the end of the run, falling by about 13,000 a scenario.

## Goal 11: a verdict that needs no forensics, measured clean

Goal 11 had three parts: make the ledger's verdict trustworthy on its own, re-measure what Goal 10 measured while the
janitor drained, and stabilise the one flaky test. The old bucket was lost when Docker was reset mid-goal, so the clean
measurements come from a fresh 1M-index bucket, `fleet-g11fresh` (`run1790942570739`): drained first -- of what an
interrupted run on it had left -- then steady, kill and slowdown at the same fleet and load as before.

**0 acknowledged writes lost, 0 refused writes visible and 0 unreached in every scenario.**

| | before | Goal 11 |
| --- | --- | --- |
| ledger verdict | lost or found; two runs reported 16 and 399 losses that forensics found | **verified, lost with evidence, or unreached**; all 415 earlier false losses verify; in steady, 4 indices the fleet could not hand over in time were decided from the store and verified |
| steady writes, nothing injected | 46% errors, 21,810 refused by the limiter (old bucket) | **6.5-6.8% errors, 0 refused**; p50 3.2-4.8 s, p99 11-19 s |
| write limiter, nothing injected | limits 18-27 on appends of 300-600 ms | **limits 777-1,024 on appends of 78-138 ms** |
| idle cost, nodes that kept their shards | 190 per held shard-hour (Goal 8) | **141**, 137 without the janitor -- 1.35x lower, not the 5x asked for |
| janitor once drained | -- | ~1,750 store requests per node-hour; drained a killed run's leftovers in 6 min |
| `logs-*` for next month at 1M | refused at the 10,000 cap (22,000-63,000 candidates) | **answered under the cap**: 966-1,110 indices not ruled out, ~17,500 shards skipped, ~2-2.5 s warm |
| kill -9 takeover p50 / p99 | 28.9 s / 36.7 s (Goal 10) | 38.0 s / 63.3 s |
| SlowDown burst | 939 refused early, back in 116 s (Goal 9) | 448 refused early, accepted p50 17.4 s, **not back within the window** |

### Part 1: three verdicts

What the read-back cannot reach through the fleet is decided from the store: the published commit, and the log a
successor would replay, read in that order -- a publish writes its manifest before it truncates the log, so a record is
in one or the other whatever happens in between, and a takeover mid-check cannot hide it (`DurabilityCheck`). A write is
**verified** if it is in either; **lost** only if both were read and it is in neither, reported with the head, the
commit's term and log ordinal and the log range read; **unreached** if the store could not be read in time, which fails
a run as inconclusive, never as a loss. Canaries: a record removed after its write was acknowledged is lost; a write
truncated from the log is found in the commit; a takeover during the check reads verified every time; an unreadable log
is unreached (`ServerlessLedgerVerdictTests`). Run against the kept bucket, the 16 and 399 writes Goal 10's runs had
reported lost all verified.

### Part 2: what the drained numbers say

- **The limiter was mistuned, not at the store's ceiling.** It cut by a third on any single append past its one-second
  target; the odd slow append on a store answering in 300-600 ms held limits at 18-27. Cutting on the smoothed time
  instead (`WriteBackpressure`), limits settle at 777-1,024 with appends of 78-138 ms and nothing refused. The cost:
  writes that were refused at once now wait, and steady p50 is 3-5 s. Appends alone do not explain that -- the
  slowdown scenario's unthrottled baseline in the same run was p50 246 ms -- and where the rest goes is open.
- **SlowDown recovery got worse.** With the limiter reacting to the smoothed time, a burst of throttling is backed off
  from later and released later; the run did not return to baseline within the window. A limiter that refuses early
  under a burst without cutting on outliers in steady state needs both signals, and is open.
- **Idle cost is 1.35x below Goal 8, not 5x.** Measured only on nodes that held the same shards through the window and
  net of the janitor's own requests (attributed at the store), the remainder is per-shard background: head
  verification, the descriptor backstop and reader checks. Reaching 5x needs those to be per node, not per shard.
- **A one-month `logs-*` search now fits under the 10,000-index cap.** Two changes got there: an index that holds
  nothing keeps no rollup entry (Goal 10), and an entry with data but no digest gets one -- from its commit, or a replay
  whose publish computes it. What is left, about a thousand indices, is mostly the shards held at that moment -- a
  writer's shard is never ruled out -- which sits right at the per-query activation budget of 1,024: two of the four
  samples were refused by that budget rather than the index cap.
- **The janitor is cheap once drained**: about 1,750 store requests per node-hour, and it found nothing more to do
  within six minutes of the start.

### Part 3: the flaky test

`ServerlessBlockReadTests` failed only under full-suite load: background refreshes during its indexing loop cut more
segments the slower the machine ran, and each costs a fixed amount to open. It force-merges to one segment now and
asserts so; the full suite ran green three times but for the known Windows jar lock in `ServerlessInstalledPluginTests`.

### Still open

- Steady write latency of 3-5 s at p50 with the limiter no longer refusing, against a 246 ms unthrottled baseline.
- SlowDown recovery: not within the window.
- Kill -9 p99 63 s, over the 60 s target.
- Idle cost: 5x needs per-node rather than per-shard background.
- The unprunable remainder sits at the 1,024-shard activation budget.

## After Goal 11: latency, SlowDown, two stale-read defects, and one loss not yet explained

This round took Goal 11's two open performance items, and the runs that measured them found three correctness
problems: two ways a reader could answer from a commit behind an acknowledged write, and one loss of three
acknowledged writes. The stale reads are fixed, each with a test that failed first. **The loss is not explained.** It
happened once, has not recurred in three hours of runs with every writer open and replay logged, and a shard now
refuses to open rather than drop a write if the leading theory is ever right. Same fleet, load and bucket
(`fleet-g11fresh`) as Goal 11, five-minute scenarios.

| | Goal 11 | now |
| --- | --- | --- |
| steady writes, p50 / p99 | 3.2-4.8 s / 11-19 s | **0.27-0.37 s** / 2.6-7.6 s |
| write limiter, nothing injected | 777-1,024, 0 refused | 352-1,024, 0-78 refused per node |
| SlowDown burst | not back within the window | **still not back within the window** |
| silent stale reads | 0 | 2, then 1, both fixed; **0** in the last 10 scenarios |
| acknowledged writes lost | 0 | **3 once** (`run1790974395481`); 0 in 15 scenarios since |
| kill -9 takeover p50 / p99 | 38.0 s / 63.3 s | **67-99 s / 84-288 s** (worse; not investigated) |
| idle cost, per held shard-hour | 141 | 127-141 |

### Latency: the local fsync

Each write's commit path fsynced the local segment cache under the shard's exclusive guard. That cache is
rebuilt from the object store on every open -- local commits and every file a manifest names are deleted first -- so
the fsync protected nothing, and it held every write to the shard behind a disk flush. `BlockCacheDirectory`'s
`sync` is now a no-op. Steady p50 went from 3-5 s to about 0.3 s. The limiter was also made to regrow geometrically
once appends are fast, and to refuse while an admitted write has waited past its budget, so one stuck append no
longer holds the shard's queue indefinitely.

### SlowDown recovery: partly

A node whose lease lapsed under throttling stopped taking writes until its next head verification, up to a full
interval away; it now rereads as soon as the lease is back. One run then recovered in 146 s, but the runs since did
not recover within the window. Still open.

### Stale read 1: a reader that remembered "nobody owns it"

`run1790953109586`, slowdown: 2 searches returned one of two acknowledged documents with no failure. A reader that
had validated a shard with no owner trusted that until a node departed. Meanwhile a writer took the shard,
acknowledged a write, and gave it back unpublished (a lease lapse). The check is now bound to the head's term and
owner, and the head is reread at most once a second (`ServerlessStaleReadTests.testAReaderNoticesAWriterThatCameAndWentUnpublished`).

### Stale read 2: a reader on a commit a successor replaced

`run1791010969808`, restart: one search returned one of two documents with no failure. The store showed the whole
sequence:

1. A reader opened on the term-1 commit.
2. A term-2 writer acknowledged a second write and went without publishing.
3. A term-3 writer replayed the write, published it, and its publish trimmed the log behind its own commit
   (`wal/t=2` holds only fences).
4. With nobody writing the shard, the reader compared its commit with the log, found nothing past it, and served it
   as complete.

The write was never at risk; the reader did not know its commit had been replaced. Readers were moved onto a new
commit only by a background pass. A reader of a shard with no live writer now compares its commit with the published
manifest first. A replaced one is let go and refused as retryable, without asking anyone to take the shard, and the
search path reopens it once onto the current commit
(`ServerlessStaleReadTests.testAReaderNoticesItsCommitWasSuperseded`, which returned the fleet's exact wrong answer
before the fix).

### The loss: three writes, not reproduced

`run1790974395481`, steady: three acknowledged writes in neither the commit nor the log. Example: `logs-0000088`,
seqNo 9706 at term 29, where `wal/t=29` holds fences 1 and 3 and record 2 is gone, and the current owner's
realtime get does not find the document.

**Ruled out from logs and store:**
- fencing;
- lease lapses;
- failed appends and failed engines;
- a stale local open;
- legacy seals;
- the writer's own truncation;
- overlapping publishes of one shard (serialised per node);
- truncation state carried across a term change (reset by each new term).

**The leading theory:** two operations sharing a sequence number across terms. Core's recovery skips a replayed
operation whose number the restored commit has already processed, which would drop one silently, and the next
publish trims the record. A canary shows that this loses a write. `ServerlessWriterEngine` now checks every replayed
record at or below the commit's checkpoint against the commit, logs any that is absent, and refuses to open on a
proven collision (`ServerlessReplayCollisionTests`).

**Since then:** three reproduction runs over 15 scenarios, with every writer open and replay logged
(`-Dtests.fleet.debug_loggers=...`):
- 27,000+ opens, and more than 14,000 replayed records at or below their commit's checkpoint;
- every one of those records was in its commit;
- not one skip, not one refusal, and 0 lost.

The other untested suspect is the janitor's replay-and-release, which these runs did not log.

### Still open

- **The three-write loss:** cause unknown. Next: log the janitor's replays and log deletions, and run kill and
  slowdown again.
- **SlowDown recovery:** not within the window.
- **Kill -9 takeover:** p50 67-99 s and p99 84-288 s, against Goal 11's 38 s and 63 s.
- **Readers refusing:** many shards with no live writer are reported as not answering, 1,000-4,000 per scenario,
  before and after the second fix. One cause seen in a test: a writer that only replays the log publishes nothing,
  so its readers keep refusing until someone does.
- **A superseded reader in use:** one held by a running query refuses until the background pass lets go of it.


## Method

### The fleet

- **Nodes.** Real shell JVMs (`NodeProcess`, the `processTest` machinery), each started from the shell's runtime
  classpath with roles `ingest,search`, demand-driven activation on, the production lease TTL of 30 s, and a
  per-node shard cap (`tests.fleet.max_shards`). Each node logs to its own file at INFO.
- **The store.** RustFS 1.0.0, `rustfs/rustfs@sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff`,
  **single-drive**, in Docker on WSL2, reached over path-style S3 on port 9200. Not erasure-coded: on one machine
  the four "drives" of an erasure set share one physical disk, so erasure would add coding overhead without
  independent failures -- the property erasure exists for -- and roughly quadruple the object count of a million-
  index population on a VM disk.
- **A proxy per node.** Each node reaches the store through its own `FaultProxy`, an HTTP/1.1 proxy that forwards
  bytes exactly as they are (so SigV4 signatures stay valid) and can add latency, answer a fraction of requests
  `503 SlowDown` itself, or partition the node from the store by refusing and closing connections. It also counts
  every store request by type, which is where the request figures below come from.
- **The population.** Index descriptors written straight to the store before the fleet starts, 32 at a time.
  They hold no documents until something writes to them, which is the point: most of a serverless population is
  dormant.

### The load

Open-loop: writes are sent on a schedule whatever the fleet is doing, from virtual threads, with at most 1,024
outstanding. A fixed pool of threads each waiting for its last answer was the first version, and it measured
the harness: one sick node holding requests for a client timeout stalled every writer. Writes the client could
not send because 1,024 were already outstanding are counted as shed; none were in any run reported here.

- **Writes** at `writes_per_second`: 90% to the active set (`active` indices per node), 5% to a random dormant
  index of the population, 5% to 50 names being created and deleted.
- **Churn**: those 50 names are created and deleted at `churn_per_second` (5, or 10 in the storm).
- **Wide searches**: `logs-*` over the last minute, every two seconds, `allow_partial_activation=true`.
- **Gets** of a random acknowledged document every 100 ms.
- **Routing** is a load balancer's: a random node per request. A paused node is taken out of rotation five
  seconds after it stops answering, as a health check would; a partitioned node still answers, so it stays in.

### The ledger, and what counts as lost

Every write attempt has an id of its own and is recorded, as it ends, in `ledger.csv`:

- **acked** -- a 2xx. It must be readable afterwards, unless a delete of its index had not returned before the
  write began (then it may have gone to the incarnation that delete removed, and it is counted as excused).
- **refused** -- a 4xx other than 408, or the request never left the client. It must never be readable.
- **unknown** -- a 5xx, a timeout, a connection broken mid-request. Either outcome is honest.

After each scenario the load stops and every acknowledged write since the scenario began is read back with
`_mget`, index by index. An item the node could not answer (an error, as opposed to `found: false`) is not
counted as absent; it is retried. An index with anything missing is retried on its own budget of four lease TTLs
and thirty seconds, with a write sent to it first, which makes some node take its shard and replay its log. A
write still missing after that is **lost**, and the run stops there: a later scenario would only bury it.

### The scenarios

Each runs for `scenario_seconds` under the load and is checked on its own.

| scenario | what is done |
| --- | --- |
| steady | nothing; then the load is stopped for half the scenario to measure the idle background |
| kill | `kill -9` of the node owning the most shards, restarted afterwards |
| pause | the busiest node frozen for twice the lease (SIGSTOP; `NtSuspendProcess` on Windows), then thawed |
| partition | the busiest node cut off from the store for twice the lease, still reachable by clients and peers |
| slowdown | 60 s of `503 SlowDown` on half of every node's store requests |
| restart | every node killed and restarted in turn (Windows has no graceful signal, so each stop is a crash) |
| storm | half the writes go to 10 names being deleted and recreated at 10/s |

**Takeover** is measured from the fault to the first acknowledged write on each shard the victim owned, by
probe writes sent every 250 ms through the nodes in rotation.

### The machine

Intel i5-13600KF (14 cores, 20 threads), 63.8 GB RAM, Windows 11, the store's Docker volume on the WSL2 VM disk,
about 200 GB free. Everything -- the nodes, the store, the proxies and the load -- runs on it at once, so the
figures below are about behaviour under failure, not about throughput a real deployment would see.

## What the runs found

The first runs found defects, and the harness stopped on them as it was built to. In the order found:

1. **A shard wedged with a head and no writer.** An acknowledged write became unreadable indefinitely: the shard's
   head named a live node with nothing open, so every peer forwarded to it, and it answered "acquiring, retry" for
   as long as it lived. The write was durable -- it was in the log -- but nothing would replay it. A reader and the
   writer that replaces it share a `ShardId`, and the pass that drops stale readers decided a shard was a reader,
   read the manifest, then closed by id; a write in between had made it the writer. Fixed by making every reader
   release check-and-close in one step, and by making a node that finds itself named with nothing open re-take the
   shard at a higher term (which replays its log) rather than refuse it at the shard cap. Canaries:
   `ServerlessReaderBecomesWriterTests`, `ServerlessEvictionTests#testAFullNodeServesAHeadThatAlreadyNamesIt`.
2. **Items of one `_mget` raced to open the same reader** on a shard nobody owns; all but one failed with "failed
   to obtain in-memory shard lock". Reader opens are single-flight now
   (`ServerlessMultiGetTests#testAMultiGetOverAReleasedShardReadsEveryItem`).
3. **A filesystem store showed half-written blobs**, which the log's fencing read as fences: a takeover could skip
   fencing a term and let the fenced writer append after it. `FsLogFencingTests` had been failing about one run in
   ten on this machine. Not reachable on S3, whose objects appear whole; fixed in `FsBlobContainer`
   (`FsBlobContainerTests#testAnExclusiveWriteIsNeverListedIncomplete`).
4. **Harness defects**, each of which made a scenario measure the harness rather than the fleet: a closed-loop load,
   a paused node left in rotation, a restart that read the previous process's readiness file, a Windows suspend
   script mangled by argument quoting, and an `_mget` error counted as a missing document.

5. **Forwarded writes deadlocked across nodes.** The first full-scale run acknowledged 0.4% of its writes.
   Every node's WRITE threads were blocked waiting on a write they had forwarded, and the owners applied forwarded
   writes on their own WRITE pools -- so with all of them waiting on each other, none was free to apply what the
   others waited for, until the forwards timed out. Gets had the same cycle on GET. Three nodes at 50 writes a
   second never filled the pools; six at 200 did at once. Owners now apply forwarded work on pools of their own,
   which forward nothing further, so no pool a forward waits on depends on itself (searches were already built
   this way). Canary: `ServerlessForwardingTests#testNodesForwardingWritesToEachOtherDoNotDeadlock`, two nodes with
   one WRITE thread each forwarding to one another.

## Full scale: six nodes, a million indices

Run `run1790813920607` (steady to slowdown) and `run1790820946471` (restart and storm, rerun after the first run's
restart outlived the harness's start timeout -- see below), the same configuration and the same bucket. The
population was created once, in 796 s, and reused; a reused population starts with heads still naming the
previous run's nodes, whose leases have long lapsed, so the first writes to those shards are takeovers.

| scenario | acked | refused | unknown | lost | refused but visible | write p50 / p99 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| steady | 102,331 | 2,975 | 14,686 | **0** | **0** | 286 ms / 6.9 s |
| kill | 99,719 | 2,978 | 28,224 | **0** | **0** | 336 ms / 11.2 s |
| pause | 93,573 | 2,823 | 27,649 | **0** | **0** | 498 ms / 19.7 s |
| partition | 95,407 | 3,206 | 25,909 | **0** | **0** | 343 ms / 18.3 s |
| slowdown | 5,033 | 530 | 16,478 | **0** | **0** | 330 ms / 58.3 s |
| restart | 66,102 | 2,017 | 13,329 | **0** | **0** | 377 ms / 17.5 s |
| storm | 77,807 | 34,741 | 5,263 | **0** | **0** | 223 ms / 10.5 s |

In the storm, half the writes went to ten names being deleted and recreated ten times a second: 32,335 of them
arrived at a name that did not exist and were refused 404, 469 met a recreation in flight and were answered 503,
and none of the refused ones was ever readable -- a write to a deleted index is refused, not acknowledged and
lost. 18,993 acknowledged writes were excused, each by a delete of its index that had not returned before it began.

"Acked" counts only writes whose index was not deleted under them; writes a delete may have removed are counted
separately in each run's report as excused (1,452 to 2,080 per scenario from the churn, 18,993 in the storm). Every
scenario's first read-back but the slowdown's found some indices with acknowledged writes not yet readable -- 1,069
to 3,472 of them -- and every one was readable once some node had taken its shard and replayed its log. Those are shards nobody owned at the
moment of the read, whose writes were in the log and not yet in a published commit; see "Open" below.

**Refusals** are the honest kind throughout: 404 for a churned name that did not exist when the write arrived,
and 421 when a node had no room to take a shard. **Unknowns** are mostly the client's: writes to a node that was
being killed or frozen, 503 `activation_in_progress` while a cold shard was opened, and -- in the first five
scenarios -- connection resets of the client's own making, fixed for the last two (the client now speaks HTTP/1.1
rather than asking every new connection to upgrade).

### Takeover against the lease

| fault | shards | taken over | p50 | p99 | max |
| --- | ---: | ---: | ---: | ---: | ---: |
| kill -9 (window 3 min) | 267 | 267 | 59.4 s | 120.6 s | 146.7 s |
| pause, while frozen (window 60 s) | 250 | 209 | 44.0 s | 61.0 s | 61.7 s |
| partition (window 60 s) | 190 | 29 | 44.7 s | 59.5 s | 59.5 s |

A shard can be taken only once its owner's lease has lapsed -- 30 s, plus the skew margin -- so nothing is faster
than about one TTL. The rest is the survivors making room: each already held up to its cap of 400 shards, so taking
the dead node's means evicting something idle first, publishing it, and only then activating the new one. The
partition window is twice the lease, too short at this scale to see most takeovers complete; the kill shows where
they land.

### What a frozen and a partitioned owner did

The paused node was thawed after twice its lease, still believing it owned 250 shards. It acknowledged nothing it
no longer owned: the ledger shows no acknowledged write missing and none refused-but-visible. The partitioned node
stayed reachable to clients and peers: of the 2,120 writes sent to it after its lease ran out, it acknowledged
none -- each was answered 5xx or timed out, which the client counts as unknown.

### SlowDown

A minute of `503 SlowDown` on half of every node's store requests (6,309 injected) stalled writes rather than
failing them fast: p50 30 s, p99 47 s, and nearly all of them ended in a client timeout. Every store call retries
with backoff, so a write that needs several of them inherits the backoff several times, and lease renewals were
slowed by the same throttling. Writes were back to the baseline 20 s after the burst ended. Nothing diverged.

### The idle background

With the load stopped, each node made about **77,000 store requests an hour** (one node, which had served more of
the verification reads, 198,000), holding close to its 400-shard cap. That is about 190 requests per held shard
per hour, and most of it is one line item: every open index's descriptor is re-read every 30 s
(`serverless.descriptors.refresh_interval_millis`), 120 reads an hour per open index -- about 48,000 of the 77,000.
The rest is head verification, lease renewal and reader staleness checks. The cost is per held shard, not per
index in the population: a million indices cost nothing while they are dormant, and a node costs what it holds.
The three-node shakedown, holding about 70 shards a node, measured 17,000 to 24,000 an hour on the same basis.

### Under load

Store requests a second, fleet-wide, during the steady scenario: PUT 801, GET 608, DELETE_BATCH 66, LIST 42 --
about 4 PUTs per acknowledged write, which is the log append plus the publishes, digests and rollups that follow.

### Wide searches at a million indices

Every `logs-*` search failed at this population (the shakedown's 500 indices served them in under a second).
Resolving a pattern lists the names under its prefix, and RustFS on one drive could not list a million entries
under `indices/logs-`: the listing failed server-side with `500 timeout` after four SDK attempts, before its first
page. This is the cost STATUS.md already names as the remaining one of the wide-search path -- listing the
pattern's names -- now shown to be a hard failure rather than a slow second at this scale. The fix it describes,
keeping each group's names in its rollups so resolution need not list, is the next step this result argues for.

### The restart that did not start

In the first full run, the rolling restart's first node did not answer within the harness's 90 s. The machine was
by then near the end of its memory: six 3 GB nodes, the harness, and the store's VM holding 23 GB of page cache.
A node starting with its whole heap committed paged for longer than the harness waited. The harness now starts
nodes with a small initial heap and waits five minutes; on the rerun every node restarted in 5 to 12 s.

### Open (closed by Goal 9; see above)

- **A shard nobody owns can be read from a commit behind its log.** A node whose lease lapses gives back the heads
  it held without publishing (it cannot: it has lost the right to), and a release at a failed open or a failed
  append does the same. A get of such a shard answers from the published commit until some node takes the shard
  and replays the log -- which the first write does. Every first read-back above that found something missing
  found this, and every one resolved. It is a read-visibility gap, not a durability one, and closing it needs the
  commit to record how far into the log it reaches, so a reader can tell when it is behind.
- **The wide-search listing** above.
- **SlowDown handling**: a fleet-wide throttle stalls writes for the client's whole timeout rather than shedding
  them early with a retryable 503.


## The three-node shakedown

Three nodes (2 GB heap, cap 100), 500 indices, 60 active, 50 writes/s, 60 s scenarios -- the configuration the
defects above were found and fixed in, run once more with every fix in place:

| scenario | acked | refused | unknown | lost | refused but visible | takeover |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| steady | 2,782 | 220 | 1 | 0 | 0 | |
| kill | 4,164 | 341 | 2,585 | 0 | 0 | 62 of 62, p50 28.4 s, p99 91.2 s |
| pause | 1,571 | 149 | 3,009 | 0 | 0 | 59 of 88 within 60 s, p50 34.9 s |
| partition | 520 | 60 | 3,301 | 0 | 0 | 2 of 82 within 60 s |
| slowdown | 1,657 | 104 | 2,740 | 0 | 0 | recovered 15 s after the burst |
| restart | 3,353 | 293 | 1,516 | 0 | 0 | |
| storm | 1,624 | 1,350 | 27 | 0 | 0 | |

The slow takeovers in pause and partition are the shard cap, not failover: two survivors each holding close to
their hundred shards, all in recent use, can take a shard only as fast as something else goes idle long enough to
evict. The full run is sized so the survivors have room.
