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

## After Goal 11: latency, two stale reads, and a loss traced to writes past a fence

This round took Goal 11's two open performance items. The runs that measured them found:
- two ways a reader could answer from a commit behind an acknowledged write;
- one loss of three acknowledged writes.

All are fixed, each with a test that failed first. The loss came from a writer whose records landed after its
successor had fenced the log, which happened three ways. A shard now refuses to open, rather than drop a write, if
its log ever holds one sequence number twice. The final run (`run1791031482814`) was 0 lost, 0 refused-but-visible,
0 unreached and 0 silently behind in all six scenarios. Same fleet, load and bucket (`fleet-g11fresh`) as Goal 11,
five-minute scenarios.

| | Goal 11 | now |
| --- | --- | --- |
| steady writes, p50 / p99 | 3.2-4.8 s / 11-19 s | **0.19-0.53 s** / 0.5-8.4 s |
| write limiter, nothing injected | 777-1,024, 0 refused | 352-1,024, 0-78 refused per node |
| SlowDown burst | not back within the window | **back to baseline 71 s after the burst** (ranked takeover) |
| silent stale reads | 0 | 2, then 1, both fixed; **0** in every run since |
| acknowledged writes lost | 0 | **3 once** (`run1790974395481`); cause found and fixed; **0** since |
| kill -9 takeover p50 / p99 | 38.0 s / 63.3 s | **39 s / 58 s** for 205 shards with room; 75 s / 119 s onto survivors at the cap (see below) |
| idle cost, per held shard-hour | 141 | 127-151 |

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

### The loss: records that landed past a fence

`run1790974395481`, steady: three acknowledged writes in neither the commit nor the log. Example: `logs-0000088`,
seqNo 9706 at term 29, where `wal/t=29` holds fences 1 and 3, record 2 is gone, and the current owner's realtime get
does not find the document.

**What the ledger showed.** A collision can make replay skip a record but cannot delete one, so something judged
record 2 covered. The ledger records each acknowledged write's seqNo and term, and **every lost seqNo had been
acknowledged twice**, within the same eight seconds:
- `logs-0000088` 9706: at term 29, then at term 28 six seconds later;
- `logs-0000095` 9760: at term 20 and at term 19;
- `logs-0000101` 9908: twice at term 20, for different documents.

A superseded writer's record had landed after its successor fenced and replayed the term. The next successor
replayed term 28 -- including that record -- before term 29. Core skipped term 29's 9706 as already processed, and
that successor's publish dropped the older terms, deleting it.

**Three ways a record could land past a fence**, each closed, with a test that failed first:

1. **Appends were pipelined.** With several PUTs in flight, a successor's fence took the slot of one not yet landed
   while the next, past it, landed and was acknowledged. A log writer now has one append in flight at a time
   (`FsLogFencingTests.testASecondAppendInFlightCannotLandPastAFence`). Steady latency did not suffer: p50
   193-531 ms, p99 0.5-8.4 s.
2. **A takeover skipped terms that only looked fenced.** It skipped any older term whose last blob was a fence, but
   a writer's own fence from beginning its term looks the same, so a writer that had written nothing yet was never
   fenced. A successor now leaves a marker when it fences a term, and skips only marked terms
   (`testAWriterThatHasWrittenNothingIsStillFenced`).
3. **A write could reach a reader's engine.** Found by the first run after 1 and 2 (`run1791029314869`). There,
   `logs-0000120` held seqNo 14752 twice at term 41: after the writer's records, a second term-41 writer placed a
   fence and logged it again.
   - On an ingest node a reader runs a writable engine, at its commit's term and numbered from its commit.
   - A reader was added to the open set before being recorded as a reader, and left the reader set first on
     release. A write on that node in either gap was applied to the reader.
   - Its first append began the term implicitly, with no ownership check, and the write was acknowledged.

   Readers are now recorded before they open and leave the open set first. A write is acknowledged only if the
   shard's log writer began that exact term, which only a writer's open does, after checking the head
   (`ServerlessReplayCollisionTests.testAWriteToAReaderIsNeverLogged`).

**And loudly, if it ever happens again.** An open refuses, naming both operations, when the log holds one seqNo
twice for different operations, or a record collides with a different document in the restored commit
(`ServerlessReplayCollisionTests`). That is how path 3 was found: the shard refused to open rather than drop one,
and `logs-0000120` still refuses. Repairing it needs someone to decide which write to keep.

**The confirming run, `run1791031482814`:** steady, slowdown, kill, pause, restart and steady, with every writer open
and replay logged:
- 14,414 writer opens;
- no new collision;
- 0 lost, 0 refused-but-visible, 0 unreached and 0 silently behind in every scenario.

### Takeover, readers refusing, and SlowDown: one cause, the shard cap

Three items were open after the loss: slower kill -9 takeover, 2,000-4,000 shards per scenario "not answering", and
SlowDown recovery. The runs that followed (`run1791036555877`, `run1791041810675`) show one cause under all three:
**the survivors are at their shard cap.** Six nodes hold about 395-400 shards each, against a cap of 400.

**Takeover.** The harness splits the time into lease expiry, a survivor taking the head, and the first acknowledged
write.
- One part was the log, and is fixed. Every takeover listed every term directory the shard had ever had, three times
  over: to fence, to replay, and to drop older terms. Those directories are never removed, and the active shards had
  44 on average (up to 271). Once a publish has dropped every older term, and each was fenced, the log now records
  that at its root, and later takeovers skip those terms. That is safe because nothing lands past a fence. Head to
  first write went from **8 s to 2.7-5.3 s** at p50 (`FsLogFencingTests.testACompactedTermIsNotListedAgain`).
- The rest is room. Kill to first write was p50 30 s for 68 shards, and 45-76 s for 200-300. Almost all of it is
  waiting for a survivor to take the head: survivors at the cap must evict a writer, which needs a minute unused, to
  make room.

**"Not answering" is the cap refusing reader opens, not a shard behind its log.** The stale check now says why each
search did not answer. Nearly every one was `this node holds 400 shards, the cap; ... was not opened`. The check
searches up to 6,000 indices in a burst against 2,400 slots fleet-wide, and a full node whose shards are all in use
refuses rather than thrash. That refusal is honest, by design.

**SlowDown recovery.** Write errors return to about baseline (3.5%) within 90 s of the burst ending. Write p99 stays
at the client's 30 s timeout for minutes. Those writes are to shards nobody holds, `503 activation_in_progress`
after waiting for an activation with no room, across almost 500 distinct indices. The harness's recovery test asks for
p99 back within 2x baseline, so it reports "not within the window".

**Tried and reverted: evicting idle readers first.** Giving readers a 5 s grace ahead of writers' minute did not
help, because the nodes were full of writers in use. Steady p99 rose to 22-25 s, against about 10 s, and the cap
refusals remained, so it was dropped.

### The cap was not the whole story: survivors raced each other

A run with the cap raised to 600 (`run1791077875798`) tested the diagnosis above, and it did not hold.
- **Kill:** 300 shards took p50 72 s, as at 400, while two survivors held under 300 shards each.
- **Slowdown:** still did not recover.

The survivors' activation queues show why. Each jumped to about 300 together -- the dead node's shard count -- because
writes for every one of its shards reach every coordinator. All five queued the same activations in arrival order
and raced on each shard at once: one won its head and four spent 5-13 s on nothing, so the fleet took a dead node's
shards at one node's pace.

**Queued activations are now ordered by rendezvous rank:** each node takes the shards it is the preferred taker of
first, then the rest in arrival order. By the time it reaches the rest, they are usually taken, and finding that out
costs a head read. No node is ever kept from a shard; a stale membership view only reorders the work
(`ServerlessTakeoverRankTests`).

`run1791081940875`, cap 400 again, showed:
- **Kill with room on the survivors (205 shards): p50 39 s, p99 58 s,** against 45-76 s / 70-150 s before. That is
  back to Goal 11's figures and under the 60 s target. A survivor holds each head 9 s after the lease runs out, and
  the first write follows 1.6 s later at p50.
- **Kill with every survivor at the cap (279 shards):** p50 75 s, p99 119 s, against 76 s / 150 s. Each takeover
  there must evict a writer first.
- **SlowDown recovered to baseline 70.8 s after the burst**, the first recovery within the window since Goal 9. A
  burst leaves many shards to take again, and survivors had been racing for those too.
- **Correctness:** 0 lost, 0 refused-but-visible, 0 unreached and 0 silently behind in all six scenarios.

**`logs-0000120` is repaired, keeping both writes.** Both had been acknowledged, so dropping either would have been
a loss. A one-off tool (`ServerlessLogRepairTests`, run under `s3Test` only when given its properties) rewrote the
stray record without its sequence number, keeping the original beside it. Replay applied it as a fresh operation.
The shard has opened since, with both documents: 85177 at seqNo 14752, and 86131 re-applied as 14760.

### The last two: full survivors, and reader opens refused at the cap

Both were addressed; one change is kept and one was measured and dropped (`run1791090809192`).

**Searches now spill to any node with room (kept).** A search for a shard tried its two placement-preferred nodes and
its owner. If both preferred nodes were full and nobody owned the shard, it failed, even with slots free elsewhere.
When a candidate refuses at the cap, the search now asks the other search nodes in placement order until one answers
(`ServerlessSearchSpillTests`, which failed without it). In this fleet every node runs near its cap, so there is
rarely room to spill to. The stale check's refusals continued, and they are the fleet's real capacity: its bursts of
thousands of indices exceed 2,400 slots. The change matters wherever load is uneven, such as dedicated search nodes.

**A standing reserve on every node (dropped).** Each 30 s pass kept a fifth of the cap free by evicting shards idle
past the one-minute grace, so a takeover would not have to evict inline.
- **What it did:** it held every node at 320 after each pass, and demand refilled them to about 390 by the next one.
- **Kill onto full survivors (259 shards):** a survivor held each head sooner (p50 39 s, against 61 s). But the first
  write after that slowed (p50 25 s, against 5 s), so the total moved only from 75 s / 119 s to 68 s / 99 s.
- **Side effects:** the evictions kept moving shards, so `421 not_the_writer` roughly doubled, and SlowDown recovery
  fell back outside the window (about 160 s, against 71 s).

A gain within the noise of single runs, with churn that showed up elsewhere, so it was taken out.

**Room is made outside the cap lock (kept).** A shard taken on demand by a full node evicted another first, inside
the lock every activation takes to reserve its slot. An eviction is a publish and a head release, so the survivors
took a dead node's shards one eviction at a time. The slot is still reserved under the lock; the eviction now runs
outside it. In `run1791095872059` the second kill landed on survivors holding 379-399 shards each:
- **211 shards: p50 54 s, p99 76 s,** against 75 s / 119 s with ranking alone and 68 s / 99 s with the dropped
  reserve;
- SlowDown recovered within the window again, 102 s after the burst;
- 0 lost, 0 refused-but-visible, 0 unreached and 0 silently behind in every scenario.

### Scaling out: the signals, the hand-off, and what kept every shard busy

The next question was whether the fleet says when to scale, and whether a node added at the cap relieves latency soon
enough. It measured as follows (`run1791104708788`, `run1791107747703`, `run1791148967324`, the last on a fresh
bucket).

**What to scale on.** `GET /_serverless/stats` gained a `capacity` section: the cap, shards held, reader opens and
writer activations refused at the cap, shards evicted for room or handed off, and writes steered elsewhere. It also
gained `activation_phases`, an activation's time by phase. Each node's lease carries shards held and the cap, so peers
see each other's load without a request. The refusals and evictions are the scale signal: a node can sit at its cap
harmlessly, as a cache, and these move only when it turns work away.

**Handing work to a node with room.** A node at 90% of its cap, when a live writer is at 75% of its own or less, lets go
of idle shards down to 80%, a tenth of its cap a pass. It also sends a write for a shard nobody owns to that member,
marked to be taken there. With nobody to take them it sheds nothing.

**Three things the measurements found:**
1. **Every search counted as use.** The wide search every two seconds touched every held writer, so nothing was ever old
   enough to evict, full nodes refused new shards outright, and a node added at the cap filled within two minutes. A
   search now counts as use of a shard it was already holding only if it matched something there. Cap refusals during
   scale-out halved.
2. **Full nodes evicted for shards someone else had taken.** Survivors of a dead node all queue all of its shards. A
   survivor at its cap evicted a shard for each, before finding that another survivor already held it. With 321 shards
   to take, takeover ran p50 131 s and p99 200 s. A full node now reads the head first.
3. **A slow store stopped every activation.** An activation renews the node's lease. After a lapse that meant re-reading
   every held head, under a lock the lease thread holds during its own re-read. With the store slowed to seconds a
   request -- Docker degraded after two days -- every activation on every node waited over an hour behind it. An
   activation now declines while another thread is re-reading. A shard taken then could not be written to.

**The fresh-bucket run** was 0 lost, 0 refused-but-visible, 0 unreached and 0 silently behind in all five scenarios.

| | |
| --- | --- |
| steady write p99, after the first minute | 2.1 s (5.5 s with it) |
| an activation, average | 1.4 s: describe 13 ms, acquire 534, mark owned 309, open 552 |
| kill -9, 233 shards | p50 45 s, p99 66 s |
| kill -9, 220 shards, survivors at the cap after scale-out | p50 64 s, p99 91 s; activations 7.2 s each |
| a node added at the cap | serving in 4.5 s; half the others' average in 37-50 s |
| write p99 after the join | 9-23 s the first minutes, then 3-8 s |

**Why p99 read 9 s.** Hot shards' writes run at p99 1-2 s in steady minutes. Between scenarios the harness stops the
load to read everything back. That gap releases the working set: at one resumption 221 of the 240 hot shards had
changed owner, so the first minute re-takes them all at once, with p99 near 20 s. The harness now reports each
scenario without its first minute too. That burst is real behaviour for any index that wakes after five idle minutes,
and the per-phase timings say where it goes: no single step dominates.

**Taking more shards at once.** A node activated eight shards at a time, a fixed number. That is now
`serverless.activation.concurrency`, and the activation pool is sized from it. At 32, against 8 on the same fresh
bucket (`run1791165104451` against `run1791148967324`):

| | 8 at a time | 32 at a time |
| --- | --- | --- |
| kill -9, survivors with room | p50 45 s, p99 66 s | **p50 37.5 s, p99 52.5 s** |
| kill -9 onto full survivors after scale-out | p50 64 s, p99 91 s | p50 52 s, p99 92 s |
| a new node at half the others' average | 37 s | 52 s |
| steady write p99, after the first minute | 2.1-7.1 s | 6.6-7.6 s |
| an activation, average | 1.4 s | 2.3 s, and up to 9.6 s in the second kill |

The single-drive store is the limit, not the node: more activations at once only slow each one down. Only the first
kill gained, and steady writes paid for it. The default stays at eight; the setting is for a store that takes more
concurrent requests. What remains of a takeover with room is mostly the 30 s lease.

### What a store like S3 would need: adapting by itself, and a fleet size to scale on

**Activation concurrency that follows the store: opt-in.** An AIMD limit over activations
(`serverless.activation.adaptive`, off by default):
- It starts at eight.
- It is cut by a quarter while activations that open a shard average over `serverless.activation.target_millis`
  (2 s), and grows while they average under half of it.
- It moves between four and `serverless.activation.concurrency`.

On this store (`run1791197021281`) it spent most of the run at its floor of four, and was no faster than a fixed eight:
- kills at p50 47 s / p99 62 s, and 66 s / 91 s onto full survivors;
- steady write p99 no better.

Concurrency is not this store's lever. It is there for a store that takes many requests at once, where a fixed eight
leaves throughput unused. It has not been measured on one.

**A fleet size to scale on.** Each node renews into its lease the shards it used in the last minute and the distinct
shards it turned away at its cap. `GET /_serverless/stats` reports, under `fleet`, the writer members and the nodes
that demand wants at 80% of the average cap. Shards merely held do not count, since a cap is a cache that fills
whatever the load. Two things had to be fixed before the figure could be used:
- **Refusals were counted as events.** A search burst asking for the same shards repeatedly read as 43 nodes wanted.
  Demand now counts each refused shard once.
- **Nodes left out members whose renewal they had not read yet.** Asked at the same moment, nodes answered 1 and 7. A
  member now counts until a TTL past its lease's expiry.

In `run1791257830497` every node then saw every member and agreed within one node:

| Phase | Nodes present | Nodes wanted |
| --- | --- | --- |
| Steady load | 6-7 | 5-6 |
| The harness's heavy measurement searches | 6 | 10-14 |
| The stale check's burst of thousands of indices | 7 | up to 55, for one sample |
| Load stopped for the read-back | 6-7 | 1-2 |

That is the shape an autoscaler needs. It should smooth the figure itself: scale up on a short sustained window,
scale in only after low demand has held. Nothing in the fleet starts or stops machines.

### Shards per node: not memory, but publishing

**What a held shard costs.** Measured in one node, with batches of 100 shards and the heap read after GC, a held
shard costs 119-140 KB as a writer (50 documents, published) and 109-192 KB as a reader. A node at a cap of 400 spends
about 55 MB of a 3 GB heap on its shards. Memory is not what bounds the cap. Stats now report heap used and max.

**What actually broke at a higher cap.** Raised to 1,200, steady writes failed at 36%:
- The write limiter refused 15,000 writes while appends took 10 ms.
- Opening a shard averaged 5 s.

Thread dumps of a loaded node found 288 GENERIC threads blocked on publish locks:
- A publish pass worked through every dirty shard in turn, which took minutes at that many shards.
- Every write meanwhile scheduled another pass.
- The passes piled up on the same shards and starved everything else waiting on GENERIC, including writes waiting for
  an activation or a forward.

A pass now runs alone, going round again while shards are dirty, and publishes eight shards at once on a pool of its
own. Batching the local view projection that each activation rebuilt, which also grows with shards held, was done too
and made little difference.

**Cap 1,200, after the fix** (`run1791285770133`). Clean in all three scenarios.

| | |
| --- | --- |
| steady write errors, after the first minute | 2.9% and 7.9%, against 31-39% before the fix and 6-9% at cap 400 |
| steady write p99, after the first minute | 4.1-4.5 s |
| stale-check searches refused for want of room | **0**, against thousands per scenario at cap 400 |
| kill -9, 134 shards | p50 55 s, p99 87 s |
| heap | at most 1.7 GB of 3 GB |
| background store requests | 236-334 per held shard-hour |

The default cap stays at 1,000 -- reasoned before, measured now. What a higher cap costs is the store's bill, per held
shard-hour, and a longer takeover when a full node dies.

### The lease: 30 s stays

A shorter lease notices a dead node sooner. Run at 15 s against 30 s, same fleet, cap 1,000, steady, kill and pause
(`run1791289306089`, `run1791298414962`), both clean:

| | TTL 15 s | TTL 30 s |
| --- | --- | --- |
| steady write errors, after the first minute | 17.7% | 2.7% |
| steady write p99, after the first minute | 5.8 s | 1.2 s |
| ownership changes on the active set, steady, nobody failing | 148 | 0 |
| kill: lease ran out after | 11.4 s | 28.3 s |
| kill: kill to first acknowledged write, p50 / p99 (~280 shards) | 61 s / 110 s | 50 s / 69 s |
| kill: write errors | 24.9% | 8.0% |
| background requests per held shard-hour | 289 | 187 |
| append latency at the end of steady | 180-310 ms | 55-77 ms |
| average open | 2.6 s | 0.3 s |

The lease is not only a lease. Each renewal also re-reads one head per held shard, and reader refreshes are paced by
renewals, so halving the TTL roughly doubles that traffic. On one drive the store slowed under it: writes waited and
were refused, and every open got slower -- the takeover with them. Detection was 17 s faster and recovery slower. (The
store was shared with another project's build during the 15 s run, which may have added to it; the extra traffic is
structural either way.)

Recovery after a kill is bounded by how fast survivors open shards, not by the lease: 40 s of the 50 s p50 is after
the lease ran out. That is where to spend.

**Overlapping the takeover's steps: tried, reverted.** Marking the rollup entry owned (a compare-and-swap, ~130 ms) was
run alongside projecting the local view, on GENERIC, the open waiting for both (`run1791301905865`). Steady was
unchanged; the kill stalled -- no node opened a writer for the whole scenario, 80 of 337 shards writable after five
minutes, 48% of writes failing, nothing lost. The likely cause: during a takeover burst GENERIC fills with writes waiting
on activations, and the activations waited on a mark queued behind them. That run put the local view at 14 ms of a 461 ms open;
the runs below found it at 650-1,350 ms under ordinary load, which is what made it worth fixing properly.

### Where a takeover's time goes, and the local view

**Measured by step.** Stats now break a writer's open into its steps, and the local view into projecting and applying.
In the fleet, an open averaged 1-2.7 s, of which:
- recover was 0.6-1.2 s, of which reading the log was only 30-90 ms; the rest is the engine;
- create_shard was 0.2-0.4 s;
- the local view was 0.65-1.35 s.

Applying a view takes under a millisecond. Projecting it is what costs: every activation rebuilt the metadata of every
index the node served or hosted, mapping parse included, one projection at a time under the view lock. A single-node
test (`ServerlessActivationCostTests`, one-shard indices opened in batches of 100) put projection at 4.5 ms a shard
taken at 100 held and 44 ms at 600, and growing. In the fleet, with activations queued on the lock, it was a second.

**The fix:** an index whose descriptor (the same object) and shard terms are unchanged reuses its last projection. In
the single-node test projection is now 1-2.5 ms at any size, and taking a shard a flat ~170 ms. The setting
`serverless.view.reuse_projections=false` turns it off, for measuring.

**The fleet, both ways, twice each** (cap 1,000, the kill aimed at the node holding the median of held shards; on:
`run1791369896819`, `run1791375826545`; off: `run1791372730518`, `run1791378899406`). All four clean.

| | node killed held | local view, steady / kill | open, kill | throughput to half / all writable | kill p50 / p99 |
| --- | --- | --- | --- | --- | --- |
| reuse on | 216 | 66 / 191 ms | 1,480 ms | 3.4 / 3.2 shards/s | 50 / 82 s |
| reuse on | 448 | 53 / 114 ms | 1,429 ms | 6.4 / 6.6 shards/s | 64 / 96 s |
| reuse off | 103 | 693 / 1,348 ms | 2,724 ms | 2.7 / 1.8 shards/s | 44 / 73 s |
| reuse off | 396 | 664 / 1,302 ms | 2,280 ms | 5.2 / 4.3 shards/s | 61 / 95 s |

The view is a tenth of what it was and the open during a kill 40% shorter, in every run. Recovery after a kill does not
show it cleanly: the median node held 103-448 shards across runs, and throughput roughly doubles with that -- more
shards spread over the same survivors' slots -- so the spread within one setting (3.4 against 6.4) is larger than any
difference between the two. The closest pair, 448 and 396 shards, favours reuse by a quarter to a half. A change has to
be about 2x to show in one pair of runs here.

**Overcommitting on takeover.** A survivor at its cap evicted a shard for nearly every one it took from a dead node --
a publish and a head release in front of each take. Now a shard whose head names a member with no live lease may be
taken past the cap, up to `serverless.takeover.overcommit` of it (0.2 by default, about 40 MB of heap at a cap of a
thousand), never past that; ordinary demand still evicts first, and each pass trims the node back to its cap, a tenth of
the cap a pass, never touching shards inside their eviction grace. Both ways, twice each (on: `run1791382924093`,
`run1791388393528`; off: `run1791385609045`, `run1791391145730`), all clean:

| | node killed held | throughput to half / all writable | kill p50 / p99 | writes failing, kill until all back |
| --- | --- | --- | --- | --- |
| overcommit 0.2 | 313 | 4.8 / 4.7 shards/s | 55 / 78 s | 27% |
| overcommit 0.2 | 360 | 6.3 / 5.0 shards/s | 51 / 87 s | 36% |
| off | 310 | 4.7 / 5.8 shards/s | 61 / 78 s | 37% |
| off | 338 | 4.9 / 4.3 shards/s | 58 / 99 s | 37% |

About 6 s off the kill p50 in both pairs: the right direction, inside the noise. It was barely needed: held peaked at
1,008-1,023. Survivors own 250-500 shards and hold about a thousand -- half their cap is readers, which cost nothing to
evict -- so evicting on the take path was not the bottleneck it looked like. What is: during a kill each survivor's
activation queue held ~300 of the dead node's shards, every survivor queueing every shard, because every write
through any coordinator for an unheld shard asks for it.

These four runs also ran two changes made while they were queued: hand-off on deviation (below) and write-paced
activations, both in all four. Paced, a node whose write limiter fell to 4 ran one activation at a time with 300
queued. Pacing is off by default until a run measures it.

**One survivor per dead member's shard.** Every survivor queued every one of a dead node's shards: any write, get or
second look reaching a survivor for an unheld shard asked for it there. Every member now names the same survivor for
each such shard -- of the live writer members, in name order, the first from the shard's hash with room under its
overcommit ceiling -- and the departure pass splits by it. A doubt about the shard elsewhere is left to that survivor,
and a write for it is forwarded there once, marked to be taken; anything short of an answer from there takes it locally.
Routing only: the head's compare-and-swap still decides. `serverless.takeover.route_to_winner=false` turns it off.

The first version covered writes and doubts. Twice each way (on: `run1791396248723`, `run1791402406554`; off:
`run1791399438196`, `run1791405501132`), all clean:

| | node killed held | deepest survivor queue | kill p50 / p99 | throughput to half / all | writes failing, kill until all back |
| --- | --- | --- | --- | --- | --- |
| routed | 354 | 65-191 | 49 / 72 s | 6.7 / 6.7 shards/s | 31% |
| routed | 366 | 103-142 | 57 / 78 s | 6.3 / 6.8 shards/s | 32% |
| not | 386 | 348-381 | 59 / 90 s | 6.4 / 5.8 shards/s | 35% |
| not | 499 | 33-50 | 75 / 86 s | 5.0 / 7.6 shards/s | 56% |

Queues roughly halved, the p99 and the failing writes better in both pairs. But only 33-37 writes and 2 doubts were
routed: the rest of the queueing came from two paths it did not cover -- the departure pass's second look, ten
seconds on, at which every survivor took whatever was not yet taken, and gets of a shard behind its log, which took the
shard wherever they arrived. Both now leave a shard to the survivor named for it; a last look a lease later takes
whatever is still the dead node's, whoever was named, so a named survivor that cannot is not waited on for ever.

**Hand-off on deviation.** A node above 1.5x the fleet's mean of held shards, and above a quarter of its cap, hands
idle shards off towards 1.25x the mean while a member is below the mean, and steers writes for unheld shards to it --
however far it is from its cap. Near the cap only, load stayed uneven below it.

**Load is uneven.** Even the median node held anywhere from 103 to 448 shards, and the busiest up to 847 against a cap
of 1,000. Hand-off moves shards only above 90% of the cap, so below it nothing evens them out, and whichever node dies
is the one whose share decides the recovery.

### Still open

- **Capacity under this load:** at a cap of 400 the load's working set was close to the fleet's 2,400 slots and
  refusals continued. At 1,200 they stopped.
- **Not measured on S3:** the adaptive activation limit and the fleet size need a run against a store with real
  concurrency, and S3's request cost has not been priced.
- **The first minute after a pause:** every hot shard is re-taken at about 1.4 s each, eight at a time per node.
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
