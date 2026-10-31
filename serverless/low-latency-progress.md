# Low-latency implementation progress

Branch: `low-latency-serverless`. The delivery plan is
[low-latency-serverless-plan.md](low-latency-serverless-plan.md).

The current runtime still uses object-store WAL durability. Quorum-mode activation,
replicated SSD logs, committed-only serving engines, streaming search replicas, and
asynchronous S3 archival are not enabled by these foundation commits.

| Milestone | Delivered | Remaining |
| --- | --- | --- |
| M0 | Node timing histograms, WAL group counters, explicit open-loop benchmark, initial real-process filesystem baseline | Additional pipeline stages during extraction; actual S3 and sustained-load qualification |
| M1 | Append and streaming recovery interfaces, optional async dispatch with cancellation/rejection semantics, existing S3 path integration, bounded versioned log/checkpoint identity codecs | Operation-envelope and subscription formats after consensus/engine spike decisions |
| M2 | Consensus dependency/source evaluation started | Real three-replica persistence/election spike, committed-serving engine prototype, architecture decision |
| M3–M9 | Not implemented | Persistent quorum lifecycle, writes, fresh readers, selective replication, archival, lifecycle, migration |
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

These checks validate the listed foundations. They are not evidence of completed
three-AZ durability, millisecond quorum latency, or production readiness.
