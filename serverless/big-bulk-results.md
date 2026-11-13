# Big bulk requests

What many `_bulk` requests of about 100 MB each, arriving at once, do to a writer node, and how streaming them keeps
the heap bounded without refusing them. The meter is `BigBulkMeterTests`, run by its own task:

    ./gradlew :serverless:testkit:bigBulkTest -Dtests.asserts=false "-Dtests.jvm.argline=-XX:ReservedCodeCacheSize=240m -da" \
        -Dtests.bigbulk.concurrent=24 -Dtests.bigbulk.rounds=1 -Dtests.bigbulk.settings=serverless.http.streaming=true

## Setup

- One ingest node, 4 vCPU, 4 GB heap, one index of one shard, node log on, filesystem store.
- Each batch is 95 MB of ndjson, about 970 documents of 100 KB. Clients send over HTTP/1.1 from the same JVM, reading
  one shared body as the socket takes it (Java's `BodyPublishers.ofByteArray` copies the whole body up front, 95 MB per
  client, which first hid the node's own heap behind the client's).
- "Live heap" is the heap left after collections at its peak, less the idle node's.

## Why a big batch was expensive

Taken whole, a batch's body was aggregated by the HTTP transport, copied to the heap, each source turned into a Java
string (two bytes a character for typographic text), re-encoded for the engine, and held again in the node-log blob:
four to six times the body on the heap, while indexing pressure charged it once, after parsing. Stock OpenSearch keeps
the body in Netty's pooled direct buffer and slices sources out of it without copying, about once the body, but still
the whole body per request: twenty-four 100 MB requests are 2.4 GB wherever they sit, off heap or on.

## Streaming

`serverless.http.streaming` (off by default) serves HTTP through the Reactor Netty transport, and `_bulk` reads its body
as it arrives:

- **Bounded per connection.** Chunks are read only as the batch asks for them. Once a slice has arrived
  (`serverless.write.bulk.slice_bytes`, 8 MB), its complete operations are parsed and applied while the next slice
  arrives; nothing more is read until the first is done. A connection holds at most two slices, and one while it waits
  its turn. A client sending faster than its batch is applied is slowed by TCP, as an object store slows an upload.
- **Waits rather than refuses.** Each slice is applied holding a share of the node's batch memory
  (`serverless.write.bulk.memory_budget`, 10% of the heap, charged at `memory_factor` 4 times the slice) and one of
  `serverless.write.bulk.max_applying_slices` (the node's cores: applying more at once finishes none sooner and only
  adds heap and log latency), granted first come, first served, holding no thread while waiting. A shard group the object
  store is too slow for applied nothing and is sent again. Only a slice that waited `serverless.write.bulk.max_wait`
  (2 minutes) for either is answered 429.
- **Same response.** The client gets an ordinary bulk response listing every operation; sources are dropped as each
  slice is applied. What differs: a body malformed part way is refused (400) after the slices before it were applied.

It needs one change to the Reactor Netty transport plugin, behind its own setting
(`http.reactor.netty4.streaming.demand_driven`, off by default): stock subscribes to the request body with unbounded
demand and queues every chunk, so a streaming handler had no backpressure, and it commits the response status before the
handler can choose one, so a streamed request could only be answered 200.

Two node-log changes come with it, on by default: `serverless.wal.node_log.split_large_batches` splits one shard's
large group across log windows, so a log blob stays near the 8 MB window cap (a 95 MB batch was one 95 MB blob), and
`serverless.wal.node_log.hedge_by_size` scales the hedge delay with the blob's size, so a big blob is not hedged for
taking longer than a small one. Batches taken whole are charged 4 times their body before parsing
(`serverless.write.bulk.charge_before_parse`, on), so the indexing-pressure limit sees what they will hold.

## Results (measured)

| Run | Accepted | Item errors | Live heap over idle | Median latency | Log blobs |
| --- | --- | --- | --- | --- | --- |
| Today (all switches off), 6 at once × 3 | 12 of 18, 6 × 429 | 0 | +3.96 GB | 74 s | 12, 95 MB each |
| Today, 24 at once | node out of memory | | | | |
| Streaming, 6 at once × 3 | 18 of 18 | 0 | +0.53 GB | 133 s | 345, max 7.9 MB |
| Streaming, 24 at once | 24 of 24 | 0 | +1.0 GB | 154 s | 406, max 7.9 MB |

Today's run indexed 1.1 GB in 231 s and streaming at 6 × 3 1.7 GB in 389 s, both about 4 to 5 MB a second on 4 cores,
where Lucene's analysis is the bottleneck (0.3 to 0.6 core-seconds a MB, see `large-document-results.md`). Streaming does
not make indexing cheaper; it makes every batch get through, each client uploading more slowly, instead of some being
refused and the rest risking the heap. The 24-at-once run was measured last, after a batch waiting its turn stopped
reading ahead: 2.3 GB in 158 s. The same run before that change took 468 s at the same heap; the gap is larger than that
change explains on its own and has not been pinned down, so read the latency columns as one run each, not a benchmark.

## Default

Streaming stays off by default: it changes the HTTP transport for every request, and the REST compatibility suite has
not yet been run on it.
