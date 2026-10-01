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

### Open

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
