# Fresh segments in memory on writers

A writer's local disk holds nothing durable: an acknowledged write is in the node log, and a published segment is in
the object store before its manifest names it. With the local translog gone (`serverless.translog.local`, off since
#129), what a writer still put on local disk was Lucene's own output: every flushed segment, the stored-fields
temporary files a segment keeps open while it fills, every commit point, and every merge result. Each flushed file
was written to disk only to be read straight back by the publisher uploading it, the reader refreshed onto it, and the
merge that soon folds it away.

`LocalSegmentTier` (one per node) and `TieredSegmentDirectory` (one per shard, under the block-cache directory) hold
those files in memory instead:

- **Flushes, commit points and temporary files start in memory** while the node's budget has room
  (`serverless.local_segments.memory.max_bytes`, default 5% of the heap). They are uploaded, searched, merged and
  pushed to search nodes from there. A file deleted while in memory (a compound file's parts, a merged-away segment, a
  superseded commit point, a temporary file) never touches the disk.
- **A merge writes to disk.** Its output is large, long-lived and written off the write path.
- **Bounded, spilled rather than refused.** A file growing past `serverless.local_segments.memory.max_file_bytes`
  (16 MB) or finding the budget spent moves to disk where it is and carries on. At 90% of the budget the oldest held
  files are written to disk in the background until it is back under 75%. A byte reaches disk at most once, and only
  if it outlived the room memory had for it.
- **A commit point is never moved to disk on its own**, so a local commit can never name a file that went with the
  memory. A crash loses nothing: a shard opens from its published commit after dropping every local commit point, and
  replays the log for the rest. No fsync, as before.
- **`serverless.local_segments.memory.enabled`** (node setting, default `true`). Bytes written to disk are counted by
  source either way (flush, merge, other), in the node stats under `local_segments`.

## Measured

`WriteThroughputMeterTests`, 2,000 shards on one 4-core node, all written, filesystem store with flat 20 ms latency,
C2 JIT, assertions off, 60 s steps, 8 GB heap (budget 409 MB). Same build both ways.

Single-document writes at 2,000/s (`-Dtests.tput.shards=2000 -Dtests.tput.rates=2000,2000,...`):

| | off | on |
| --- | --- | --- |
| local disk written, step 1 / 2 | 410 / 541 B per doc | 0 / 0 |
| of it flush / temporary files, step 2 | 449 / 92 B per doc | 0 / 0 |
| ms CPU per doc, step 1 / 2 | 1.06 / 1.09 | 0.70 / 0.72 |
| p50 / p99, step 1 | 225 / 433 ms | 203 / 376 ms |
| p50 / p99, step 2 | 225 / 424 ms | 211 / 415 ms |
| live heap after GC, step 1 / 2 | 891 / 1,183 MB | 916 / 1,224 MB |
| held in memory, step 1 / 2 | - | 76 / 120 MB |
| store PUT / GET per s, step 2 | 50.4 / 47.3 | 50.1 / 48.4 |
| open files besides `write.lock` | 7,588-7,837 (of them `.fdt`, `.fdm`, `.tmp` 7,300-7,550) | 284 (none) |
| step 3 | process killed: 65,530 memory maps | ran 5 steps |

Bulk requests of 10 at 10,000 docs/s: local disk 163 -> 0 B per doc, CPU 0.242 -> 0.156 ms per doc, p50 309 -> 195 ms,
p99 1,773 -> 1,481 ms. Both runs refused about 1% (the in-flight cap), and the p99 is noisy between runs at this rate.

Over five minutes with the tier on, merges begin in the third minute: they write 32-226 B per doc to disk, by design,
while memory held peaks at 177 MB and falls as merges delete the small segments (no eviction was needed). CPU per doc
rises with them, to 0.83-0.97 ms, and p99 to 449-525 ms. The off run could not be compared that far: in its third
minute the test JVM hit the sandbox's `vm.max_map_count` (65,530), since every small segment file on disk is memory
mapped. Production should run with 262,144 as stock OpenSearch asks, but the tier keeps flushed files out of that count.

What the numbers say:

- **CPU is where it pays**: a third less per document, from not creating, writing, mapping, closing and deleting
  thousands of small files a second. That headroom is write throughput on a CPU-bound node.
- **Latency is unchanged or slightly better.** The write path waits on the log window, not on local disk, and the page
  cache already absorbed the writes (`cancelled_write_bytes` showed most flushed bytes deleted before writeback).
- **Heap**: the budget is a ceiling, not a cost; at 2,000 shards the tier held 76-177 MB, and live heap after GC rose
  25-40 MB over the run without it.
- **Store requests** are the same: what is uploaded and when does not change.

The meter's `process write_bytes` also counts the filesystem object store, which in these runs is the same disk, so
the per-source counters are the measure of local segment writes.

## Not done

- **Dropping published files instead of writing them out.** A held file that has been published could be let go and
  read back through the block cache, rather than written to disk when the budget runs low. It needs the block-cache
  directory to learn files published after it opened; no eviction happened at 2,000 shards in five minutes.
- **Merges in memory** for small merges. Their output is what lives longest, so it is what would fill the budget.
