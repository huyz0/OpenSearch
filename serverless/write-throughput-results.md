# Write throughput per node: thousands of shards on one writer

How many writes one writer node sustains while it holds 2,000-3,000 shards, and what stops it. The meter is
`WriteThroughputMeterTests`, run by its own task:

```
./gradlew :serverless:testkit:throughputTest -Dtests.tput.step_seconds=60 -Dtests.tput.shards=3000 \
    -Dtests.tput.active=2000 -Dtests.tput.rates=1000,2000,3000 [-Dtests.tput.batch=10] \
    [-Dtests.tput.settings=serverless.write.backpressure.max_in_flight=8192]
```

Every `tests.tput.*` property is in the class javadoc. Three thousand shards need `ulimit -n` well above 20,000
before the Gradle daemon starts (`./gradlew --stop` first); see "File descriptors" below.

## Method

- One ingest node takes every shard (timed), then each shard gets one write so it is in the node log's epoch.
- An **open** load: writes arrive at random times at a fixed rate, whatever is acknowledged, spread uniformly over
  the active shards. Latency is counted from arrival, so waiting for a free client counts. The old soak
  (`NodeLogSoakMeterTests`) is closed-loop with 16 writers, so it measured latency and request cost, never throughput.
- A ladder of rates, 60 s each. A step "keeps up" if it acknowledges at least 95% of what was offered with p99
  under 5 s and nothing refused. The ladder stops at the first step that does not.
- The node runs on its own cadence, as `ServerlessBootstrap` wires it: lazy publish at 1 s on the 250 ms log window,
  edge publishes spaced 5 s, renewals and the backstop on their timers, head and descriptor checks every 5 min.
  Defaults otherwise: node log on, `node_pack` placement, log commits on.
- Store: the filesystem with 20 ms per request and one in fifty at 200 ms (`MeterStores` default).
- Machine: 4 vCPU, 15 GB, test JVM with an 8 GB heap. Clients, arrivals and the node share the 4 cores.
- CPU is reported per doc and by thread group; any write held over a second is sampled for where its thread is.

## Results (2026-11-12)

### Holding shards

| | per open writer |
| --- | --- |
| open (take), 3,000 shards | 90 s in one pass, 30 ms and 9 requests per shard, 0.7 cores |
| file descriptors, idle | 3 (`tlog`, `ckp`, `write.lock`) |
| file descriptors, being written | about 6.7: plus `fdt`, `fdm` and two `tmp` of the segment being built |
| heap, idle | 120-130 KB (3,000 shards: ~400 MB) |
| threads | about 1 per open writer (3,000 shards: ~3,060 threads) |

### Single-document requests

3,000 shards held, writes over 2,000 of them (all 3,000 would need more descriptors than this sandbox allows):

| offered docs/s | acked/s | p50 | p99 | refused | CPU (of 4) | ms CPU per doc |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | 999 | 197 ms | 352 ms | 0 | 1.8 | 1.8 |
| 2,000 | 1,996 | 263 ms | 551 ms | 0.25% | 2.6 | 1.3 |

The 2,000 step's refusals are the node's in-flight cap (`serverless.write.backpressure.max_in_flight`, 1,024): every
write waits out part of a 250 ms window, so 2,000 docs/s keeps ~600 in flight on average and bursts pass 1,024. The
default now scales with the cores (see "What limits a node").

2,000 shards with the cap at 8,192:

| offered docs/s | acked/s | p50 | p99 | refused | CPU (of 4) | ms CPU per doc |
| --- | --- | --- | --- | --- | --- | --- |
| 2,000 | 1,993 | 245 ms | 490 ms | 0 | 2.4 | 1.2 |
| 3,000 | 2,985 | 428 ms | 1,295 ms | 0.4% | 3.0 | 1.0 |

At 3,000 the box is CPU-bound and the oldest write passes the limiter's 1 s target, so it starts refusing.

### Bulk requests of 10 documents (2,000 shards, cap 8,192)

| offered docs/s | acked/s | p50 | p99 | refused | CPU (of 4) | ms CPU per doc |
| --- | --- | --- | --- | --- | --- | --- |
| 5,000 | 4,989 | 187 ms | 339 ms | 0 | 1.9 | 0.38 |
| 10,000 | 10,013 | 283 ms | 636 ms | 0 | 3.0 | 0.31 |
| 20,000 | 14,207 | 1,221 ms | 2,478 ms | 29% | 3.3 | 0.24 |

### Store requests

- Node-log PUTs stay at ~3.9/s whatever the rate: one blob per 250 ms window, 250 to 2,500 docs each.
- All PUTs (log, segment packs, logged commits, manifests): 15-50/s at 1,000-10,000 docs/s.
- GETs: 60-100/s, nearly all register reads (change-stripe confirmations and head checks); no LISTs on the write path.

## What limits a node

1. **CPU: about 1,000 single-document writes or ~3,300 documents in bulks of 10 per core.** Most of it is the write
   path itself (parse, Lucene, log record); the node's background work is a steady ~0.6-0.8 cores here. A 16-core
   node would sustain roughly 12-16K single-document writes/s, more in bulks.
2. **The in-flight cap, for single-document writes.** With a 250 ms window, a node needs `max_in_flight` of about
   0.35 s x its write rate. The default now scales with the cores (`WritePools`: 384 per core, at least 1,024;
   6,144 on 16 cores), about 1,100 single-document writes/s per core, past what a core's CPU takes. Bulks count
   once per request, so they are not affected.
3. **The node-log flusher, for bulk at high rates.** One blob is in flight at a time and the flusher encodes it
   before the PUT. At 20,000 docs/s offered a blob held ~6,000 docs and took 263 ms against a 31 ms PUT, so log PUTs
   fell to 2.4/s and writes queued behind it. On 4 cores CPU binds first; on a bigger node this is the next ceiling
   (roughly 20-25K docs/s), and encoding the next blob while the previous PUT is in flight would lift it.
4. **File descriptors.** 1 per open writer (Lucene's `write.lock`; 3 with `serverless.translog.local`) plus ~3.7 per
   writer being written: 3,000 shards all taking writes need ~14,000. Set `nofile` to 65,536 or more on a node meant to hold thousands of shards. Heap is not the limit: ~400 MB
   for 3,000 idle writers.
5. **Open time.** 30 ms a shard at 20 ms a request: a node taking 3,000 shards takes 1.5 min, and one pass of that
   is longer than a lease TTL, so a pass that opens thousands of shards must not hold back renewals.

## No local translog by default

A writer's disk translog was never replayed: an acknowledged write is durable in the node log, and a shard opens from
its published commit plus that log. It cost every open writer two descriptors (`.tlog`, `.ckp`), a 16 KB write buffer,
and every acknowledged write a local fsync. `serverless.translog.local` (node setting, default `false`) now swaps it for
`DiscardingTranslogManager`, which keeps nothing: locations still grow for `wait_for` refresh listeners, and a sync marks
the operations added since the last one persisted, as the fsync did. A realtime get of a document not yet refreshed
refreshes the shard instead of reading the translog (core's own fallback); a local refresh, not a publish.

Same run both ways, 2,000 shards, flat 20 ms, C2 JIT:

| | disk translog | none (default) |
| --- | --- | --- |
| fds per idle writer | 3.0 | 1.0 |
| heap per idle writer | 129 KB | 114 KB |
| heap used, 1,000 docs/s | 2,343 MB | 1,488 MB |
| heap used, 2,000 docs/s | 3,386 MB | 2,232 MB |
| p50 / p99, 1,000 docs/s | 203 / 372 ms | 187 / 344 ms |
| p50 / p99, 2,000 docs/s | 254 / 566 ms | 229 / 476 ms |
| ms CPU per doc, 1,000 / 2,000 docs/s | 2.0 / 1.27 | 1.6 / 1.10 |
| refused at 2,000 docs/s | 0.5% | 0 |
| store requests | same | same |

Heap used is a sample of the live heap, garbage included, not a post-GC floor. The disk run's refusals were the limiter
shedding when an append passed ~0.9 s. Rerun with assertions off (`-Dtests.asserts=false`, `-da`), as production runs:

| | disk translog | none (default) |
| --- | --- | --- |
| heap used, 1,000 / 2,000 docs/s | 2,020 / 3,110 MB | 1,354 / 2,222 MB |
| p99, 1,000 / 2,000 docs/s | 407 / 487 ms | 336 / 430 ms |
| ms CPU per doc, 1,000 / 2,000 docs/s | 2.0 / 1.29 | 1.7 / 1.11 |

What is retained is small: ~14 KB per writer (the translog writer and reader, two file channels, checkpoint state). The
larger gap under load is most likely garbage between collections: every write copied its operation into the translog's
paged buffer and wrote a checkpoint on sync. That reading is inferred; the meter does not take a post-GC floor.

## Fixed: checking new shards one at a time stalled the whole node

Before a shard's first records go into a log epoch the flusher checks that the shard's head still names this node:
one register read. Those checks ran one after another inside the flusher, before the window's PUT. A window carrying
the first records of thousands of shards (a node's first writes after taking them, or after its epoch changed)
waited one read per shard, with every write on the node behind it. In the first 3,000-shard run the window took
24 s, the write limiter cut its limit to 89 and refused 80% of writes at 500 docs/s.

`NodeLogCommitter` now runs those checks side by side on the generic pool and on the flusher itself, up to
`serverless.wal.node_log.admission_concurrency` at once (default 32; 1 restores the old order). All of them still
finish before the PUT, and a refused shard fails only its own records. The scan of which shards need a check now
takes the committer's lock once per window instead of once per shard.

Same run, 3,000 shards, writes over 2,000, 1,000 docs/s:

| | first write to every shard | refused at 1,000 docs/s | p99 |
| --- | --- | --- | --- |
| one at a time (`admission_concurrency=1`) | 109 s | 9% | 1,747 ms |
| 32 at a time (default) | 7.9 s | 0 | 352 ms |

## 10,000 shards on one writer (2026-11-13)

On the same 4 vCPU box, after #129 (no local translog) and #131 (fresh segment files in memory). `-Dtests.tput.activation`
sets how many shards the take opens at once; pair it with `serverless.activation.concurrency` so the pool matches.

| shards opened at once (4,000 shards) | open time | per shard | cores used |
| --- | --- | --- | --- |
| 8 (default) | 130 s | 32.5 ms | 1.1 |
| 32 | 39 s | 9.8 ms | 2.7 |
| 64 | 26 s | 6.4 ms | 3.1 |

Every shard costs 9 requests to open whatever the concurrency. At 8, the take waits on the store and leaves the CPU idle.

10,000 shards held, opened 32 at a time in 112 s, writes over 3,000 of them:

| | value |
| --- | --- |
| per open writer | 1.0 fd, 112 KB heap, 0.42 threads (node: 10,284 fds, 1.15 GB heap, 4,236 threads) |
| 1,000 docs/s | all acked, p50 226 ms, p99 443 ms, 2.3 cores |
| 2,000 docs/s | 99.2% acked, p50 381 ms, p99 896 ms, 3.3 cores; refusals are the in-flight cap (1,024), as at 3,000 shards |
| log PUTs | 3.9/s at both rates |

Before #131, the same run broke on this sandbox's 20,000-file cap: a shard being written kept about five segment files open,
and writing to 10,000 shards ran out of files. With #131 those files stay in memory, so the node holds one file per shard.
Holding 10,000 writers is not the limit. CPU per document is, as at 3,000. So is open time, which concurrency 8 makes
3 to 5 times longer than it needs to be.

## Not measured here

- All 3,000 shards written at once (the sandbox's descriptor limit is 20,000).
- Takeover of a 3,000-shard node (two nodes in one JVM double the descriptors). The 1,000-shard soak's takeover
  numbers are in `NodeLogSoakMeterTests`' notes: about a minute per 1,000 shards for one successor.
