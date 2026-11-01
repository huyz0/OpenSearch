# Exploratory cell capacity experiments

These artifacts support the [M3a design decision](../../low-latency-capacity-adr.md).
They do not measure OpenSearch throughput, engine memory, election latency or
multi-AZ durability. Runtime admission/isolation and quorum-mode index writes are
not implemented by these benchmarks.

## Reproduce

From the repository root, using a C++20 compiler and Python 3 standard library:

```sh
g++ -O3 -std=c++20 -Wall -Wextra -Werror \
  serverless/benchmarks/capacity/descriptor_capacity_probe.cpp \
  -o /tmp/descriptor_capacity_probe
/tmp/descriptor_capacity_probe 1000000 100
/tmp/descriptor_capacity_probe 10000000 100
python3 serverless/benchmarks/capacity/cell_capacity_model.py \
  --output /tmp/cell-capacity-model
```

The probe arguments are descriptor count (at most 10M) and active basis points:
10/100/1000 represent 0.1/1/10%. Run each combination of 1M/10M and these three
activity levels without concurrent builds/tests. Its row is an explicitly packed
32-byte primitive with a key, route epoch, checkpoint, mapping reference and flag.
It is not a complete durable descriptor, index mapping or Java object graph.

The probe compares a full eligibility scan, an ordered active-index gather from
the full array, and a separate compact active array. Active positions are evenly
spaced, not a production distribution. Fixtures are immutable. Checksum comparisons
verify all scan variants do equivalent work; the compact copy has no concurrent
update cost. Each timed trial runs twenty passes; three trials follow validation
and warmup. Two million deterministic precomputed uniform lookups measure an
additional local-array primitive. Compiler fences and consumed checksums keep
scan work observable. The JSON retains each trial's total time and per-pass medians.

Descriptor, active-index, compact-copy and lookup-index byte counts are reported
separately. The compact variant needs extra storage and coherent updates in a real
runtime. Allocator/RSS overhead, mappings, engine state, caches, locks, TLS, disk,
network, cold activation and garbage collection are outside this probe.

## Recorded local run

[Raw measurements](results/2026-11-01/descriptor-probe.jsonl) and
[environment](results/2026-11-01/environment.json) are preserved. This run used a
single thread on an AMD EPYC 9V74 host, g++ 14.2.0, Linux 6.18.55, with a four-CPU
cgroup quota and 16-GiB memory limit. No Gradle tests ran concurrently. The numbers
are medians per scan pass, in milliseconds; decimal MB below are array bytes.

| Descriptors | Active | Full array MB | Full scan ms | Active gather ms | Compact active ms |
| --- | --- | --- | --- | --- | --- |
| 1,000,000 | 0.1% | 32 | 0.699945 | 0.001730 | 0.000354 |
| 1,000,000 | 1% | 32 | 0.703779 | 0.038197 | 0.004352 |
| 1,000,000 | 10% | 32 | 0.743797 | 0.304065 | 0.045881 |
| 10,000,000 | 0.1% | 320 | 11.370192 | 0.087832 | 0.004524 |
| 10,000,000 | 1% | 320 | 10.356108 | 0.473993 | 0.045569 |
| 10,000,000 | 10% | 320 | 11.189860 | 14.262899 | 0.828295 |

At 1% activity, gathering active entries reduced work about 18x/22x for 1M/10M
rows in this run. At 10M and 10% activity, gathering was slower than a full scan.
The compact active representation performed better at every tested density. This
supports a bounded, compact resident registry; it does not support a global scan
on each document request or claim a universal sparse-gather speedup. The plan's
previous 43–46x estimates came from another exploratory probe; the committed
source and raw trials above describe this particular implementation and run.

## Resource model

`cell_capacity_model.py` emits [assumptions and complete JSON](results/2026-11-01/model.json),
[resource scenarios](results/2026-11-01/resources.csv),
[failure scenarios](results/2026-11-01/failure_fluid.csv), and
[cost scenarios](results/2026-11-01/costs.csv). Inputs are explicit in `ASSUMPTIONS`;
edit them and rerun for sensitivity rather than treating them as measured defaults.

The 126 resource scenarios cover 1M/10M descriptors at 0.1/1/10% activity,
1/4/16 cells, 16/64/256 groups and 12/24 journal hosts per cell. They include a
separate fixed-resource comparison: 384 journal hosts and 1,024 groups partitioned
into 1/4/16 cells. The ordinary candidate grid varies fleet size and reports its
compute-cost assumption; it is not a fixed-resource performance comparison.

The model assumes active shards are uniformly distributed among groups; workload
skew can exceed its busiest-host estimate. It charges two engines for each owned
resident writer shard, using the
busiest leader host's group count rather than dividing work by every follower
host. It enforces the proposed 256-engine-unit cap and includes assumed
fixed/native/cache/pending memory and per-engine FDs.
The default 32 MiB per engine and 16-GiB host with 75% admission threshold are
assumptions. Actual sharing, mapping expansion, mmap residency, PIT, merge work,
segment counts and plugins require measurement and separate runtime admission.

All 256-group profiles with 12/24 hosts exceed the existing sixteen-division
per-host hard limit. Sixty-four groups on twelve hosts use every normal slot;
repair within the remaining hosts of a failed host's AZ would require more slots
than allowed. Same-AZ host repair and restoring three-AZ diversity while an AZ is
absent are distinct. The model never places three-AZ replicas in only two AZs.

## Failure model

The 96 failure scenarios cover 1/16/64/256 groups on twelve leader-capable hosts,
uniform traffic or a 20%-hot group, host/AZ loss, and assumed 50/200/1000-ms
pauses. A group has three voters in separate modeled AZs. An affected leader is
reassigned to a surviving voter after the assumed pause; choosing the less loaded
surviving candidate is an optimistic model policy, not a prediction of native Raft
elections. Queues retain a fixed half-second of their pre-failure service budget.

`global_equal` reproduces the optimistic historical assumption that fleet service
can be split equally across groups without host limits. `host_bounded` shares each
host's assumed service among the groups it leads; a single group cannot consume
all twelve hosts' CPU. Total service per host is assumed, not measured. The model
uses exact rational, piecewise-constant fluid arrivals/service and checks
`offered = completed + rejected + outstanding`. It records rejection already
present before failure separately and labels its queue samples as segment-boundary
observations, not latency percentiles or exact instantaneous maxima.

For 64 groups, a 200-ms host pause and the 20%-hot stream, `global_equal` rejects
about 834,375 of the five million offered operations; 153,125 are already rejected
before the failure. `host_bounded` rejects about 887,963, with 158,333 before the
failure. The uniform case rejects none in either model. At sixteen uniform groups,
the host-bounded case has about 183,333 outstanding requests at the end despite
zero rejection. Reporting only completed requests would hide that backlog.

Healthy-group service remains available by construction. These assertions do not
prove that today's shared runtime workers/admission queues isolate groups under
stalled locks, native I/O or slow callbacks. M3a/M4a must demonstrate that with
actual fault histories. Nor does a fixed pause parameter measure election time,
prove a synchronous latency target or permit acknowledging minority writes.

## Cost sensitivity

The 27 cost cases vary total groups (64/256/1024), target chunk sizes (4/16/64 MiB)
and maximum flush ages (5/30/120 seconds). They assume one million 1-KiB operations
per second, dirty groups, two PUTs per archive chunk including its catalog update,
and a legacy comparator already batching 64 operations with two PUTs per batch.
These request counts are assumptions, not metered current-shell HTTP requests.
At lower offered rates, only dirty groups should flush; an implementation must
not emit empty archive chunks simply to maintain a timer.

At 1,024 groups and 64-MiB target chunks, the assumed archive PUT cost falls from
$7.37/hour at five seconds to $1.23/hour at thirty and $0.55/hour at 120 seconds.
The assumed legacy comparator costs $562.50/hour. If legacy batches instead hold
256 or 1,024 operations, that comparator falls to $140.63 or $35.16/hour. Do not
claim savings against a fictional one-PUT-per-document baseline.

Two cross-AZ voter copies require about 7,372.8 decimal GB/hour of payload. Two
search copies, with two thirds of deliveries crossing AZs, add about 4,915.2 GB/hour.
At an assumed effective charge of $0.02/transferred-GB including applicable endpoint
charges, that totals $245.76/hour before framing/retries. This is a parameter, not
a current regional price quote. It exceeds the modeled archive PUT cost.

Compute costs are only assumed per-journal-host line items in the resource grid.
The cost table excludes compute, SSD, GET/LIST, storage, compression, segment
publication/copying, queries, retries and protocol overhead. Search capacity must
be sized independently. A total-cost decision requires those measurements and
actual provider/region billing semantics; API amortization alone is insufficient.
