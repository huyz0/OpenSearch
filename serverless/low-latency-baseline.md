# Initial low-latency baseline

This is an implementation baseline for M0, not qualification of quorum mode.
The current branch still acknowledges writes through its existing S3-mode WAL.
These runs used a filesystem stand-in for object storage, a real forked shell
process, a 512 MiB server heap, and HTTP requests over loopback. They establish
neither real S3 latency nor three-AZ durability/latency.

## Reproduce

Configure the repository's build JDK, then run:

```sh
./gradlew :serverless:testkit:latencyBenchmark \
  -Dtests.latency.rate=20 \
  -Dtests.latency.seconds=5 \
  -Dtests.latency.warmup_seconds=2
```

The dedicated task is excluded from ordinary unit tests and `check`. It creates
unique benchmark indices and emits JSON at
`serverless/testkit/build/benchmarks/latency-baseline.json`. It does not delete
benchmark indices on an external deployment. The default local process and store
are temporary test resources.

`-Dtests.latency.url=https://deployment.example` targets an existing compatible
deployment instead. Do not embed credentials in that URL. This initial harness
does not install authentication credentials or modify an existing index.

Parameters: `rate`, `seconds`, `warmup_seconds`, `workers`, `queue`, `indices`,
`shards`, `source_bytes`, `refresh`, `search_every`, `url`, and `output`, under the
`tests.latency.` prefix. `search_every=0` disables post-write search probes.
At most one million operations may be offered per run. Durations, concurrency,
queue capacity, shard count, and document size also have explicit limits.

The generator offers requests independently of completion, using a bounded worker
queue. Rejected offers remain in the report. ACK latency is measured both from
scheduled arrival and from actual service start, so queue/scheduler delays remain
visible. Search probes begin after successful ACK, query the exact document ID,
and verify the returned source version. Polling and HTTP time contribute to the
reported observation delay; it is not a direct measurement of the instant the
server opened its reader. Search probes share worker capacity with mutations.

Warm-up requests are excluded from latency samples, but before/after node stats
cover warm-up, measurement, and drain. Actual node/object-store counters therefore
must not be divided by only the measurement request count. Nonzero failures and
rejections are reported rather than discarded. Successful test execution means
the harness ran and its accounting balanced, not that a production SLO passed.

## Initial observations

Run on 31 October 2026. Small-document append workload with `refresh=true` and a
search after every measured write. Both configurations include two seconds warm-up.
The short runs validate instrumentation/accounting; repeat with sustained workloads
before drawing capacity conclusions.

| Setting/result | Initial run | Deliberately constrained overload run |
| --- | --- | --- |
| Offered operations/second | 20 | 200 |
| Measurement duration | 5 s | 3 s |
| Indices x shards/index | 1 x 1 | 2 x 2 |
| Worker threads / queued requests | 8 / 64 | 1 / 1 |
| Nominal source size | 1 KiB | 1 KiB |
| Measurement operations offered | 100 | 600 |
| Successful writes | 100 | 193 |
| Generator admission rejections | 0 | 407 |
| Write failures | 0 | 0 |
| Freshness probe failures | 0 | 0 |
| Scheduled-arrival-to-ACK p50 / p99 | 15.82 / 35.66 ms | 30.80 / 84.96 ms |
| Service-start-to-ACK p50 / p99 | 11.58 / 31.46 ms | 8.25 / 32.75 ms |
| Post-ACK search observation p50 / p99 | 5.08 / 22.78 ms | 4.04 / 14.67 ms |

The overload run intentionally restricts the load generator; its rejection count
is not a server admission metric or an estimate of server capacity. Its purpose is
to verify that every offered request remains accounted for when workers cannot
keep up. Artifact paths for this session are `/tmp/low-latency-baseline-fs.json`
and `/tmp/low-latency-baseline-overload.json`; Gradle logs have matching `.log` names.

## Remaining qualification

Repeat with actual S3, sustained offered-load sweeps, more indices/shards, update
and delete contention, expensive queries, cold cache, and failure/recovery. Record
machine/cgroup limits and the generator's CPU utilization. Separate storage latency
from Lucene apply/refresh cost using `operation_timings` and actual metered request
counts. Compare future quorum mode using identical acknowledgement and freshness
conditions. Full qualification follows the matrix in the delivery plan.
