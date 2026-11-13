# Large documents on the write path

What whole wiki pages and text pulled out of PDF and Word files (tens of KB to several MB of `_source` each) do to the
write path, next to the 1 KB documents every other meter writes. The meter is `LargeDocumentMeterTests`, run by its own
task:

    ./gradlew :serverless:testkit:largeDocTest -Dtests.jvm.argline=-XX:ReservedCodeCacheSize=240m \
        -Dtests.bigdoc.seconds=60 -Dtests.bigdoc.mixes=1k,64k,1m,4m,pages -Dtests.bigdoc.rates=200,40,4,1,80
    ./gradlew :serverless:testkit:largeDocTest -Dtests.jvm.argline=-XX:ReservedCodeCacheSize=240m \
        -Dtests.bigdoc.rest_seconds=30 -Dtests.bigdoc.rest_mixes=1k,1k -Dtests.bigdoc.rest_rates=10,40

Pass `-Dtests.jvm.argline` as shown: the build's default test argline is `-XX:TieredStopAtLevel=1`, which turns the C2
compiler off and roughly doubles Lucene's analysis CPU. Every earlier meter ran that way, so their CPU figures are
pessimistic.

## Setup

- One ingest node, 4 vCPU, 4 GB heap, 20 indices of one shard, node log on with the 250 ms window, refresh 1 s.
- Store: filesystem with 20 ms per request (1 in 50 at 200 ms) plus a transfer time of 80 MB/s per request on PUT and GET
  bodies (`LatencyInjectingBlobStore.setBytesPerSecond`), about what one S3 connection moves. No real S3.
- Open load: random arrivals at a fixed rate. Documents are JSON with a title and a body of prose-like words, with
  typographic quotes and dashes as a document editor writes them (they make a Java string two bytes a character).
- Mixes: `1k`, `64k`, `1m`, `4m`, and `pages`: 85% 4 KB, 12% 60 KB, 2.5% 600 KB, 0.5% 4 MB (about 46 KB a document).
- The direct meter calls the node's write path; the REST meter sends `PUT /{index}/_doc/{id}` over HTTP to a node on the
  filesystem store with no added latency.

Everything below marked **measured** comes from these runs; **estimated** is from reading the code or arithmetic.

## Results (measured)

| mix | docs/s | MB/s | ack p50 / p99 ms | log PUT/s | log blob mean / max | CPU cores | ms CPU per MB | alloc bytes per source byte | heap live after GC, peak |
|---|---|---|---|---|---|---|---|---|---|
| 1k | 200 | 0.2 | 173 / 390 | 3.9 | 81 KB / 0.1 MB | 1.12 | (per doc ~5.6 ms, mostly publishes) | 377 | 58 MB |
| 64k | 40 | 2.5 | 231 / 468 | 3.6 | 745 KB / 1.3 MB | 1.92 | 758 | 40 | 85 MB |
| 1m | 4 | 3.9 | 347-632 / 3,079-3,134 | 2.3 | 1.9 MB / 6.1 MB | 1.1-2.3 | 268-592 | 26-27 | 45-99 MB |
| 4m | 1 | 3.2-4.1 | 496-510 / 2,294-4,701 | 1.2-1.3 | 3.1-3.7 MB / 8.1 MB | 0.45-0.86 | 124-208 | 23-24 | 81-335 MB |
| pages | 80 | 3.7 | 4 KB docs 192 / 452; 4 MB docs 464 / 2,047 | 3.8 | 1.3 MB / 9.0 MB | 1.20 | 322 | 39 | 301 MB |
| pages | 140 | 5.3-5.6 | 4 KB docs 212-216 / 1,981-2,687, 5% refused | 3.8 | 1.6 MB / 7.5 MB | 2.6-2.7 | 461-512 | 33 | 362-464 MB |
| pages | 180 | 5.3 | 4 KB docs 234 / 7,965, 41% refused | 3.1 | | 3.35 | 637 | | 712 MB |

CPU per MB varies a lot run to run on this 4-core sandbox (124-758 ms per MB); 300-600 is the honest range for prose
indexed as `text`. Profiles show it is Lucene: the standard tokenizer, lower-casing, term hashing, and LZ4 for stored
fields. Nothing in the serverless shell shows up.

Through REST (filesystem store, no added latency):

| mix | offered docs/s | acked docs/s | ack p50 / p99 ms |
|---|---|---|---|
| 1k | 10 | 10.6 | 228 / 477 |
| 1k | 20 | 15.6 | 5,059 / 6,383 |
| 1k | 40, 50, 100 | 15.6-15.7 | 6,288-6,321 / 6,360-6,426 |
| pages | 50 | 15.6 | 6,341 / 6,432 |

## Findings

1. **REST single-document writes stop at about 16 a second on a 4-core node, whatever the document size (measured).**
   `DocumentWritePlanner` runs each write on core's `write` pool (4 threads here, processors + 1 at most), and the thread
   is held through the engine and the whole log window. 4 threads / ~0.25 s = 16 writes/s; the rest queue (p50 6 s). The
   forwarded-write pool at the owner is sized the same way, and bulk shard groups also wait a window on `write`, so a
   bulk touching many shards with a few documents each is limited the same way (estimated). The throughput meter called
   the write path directly, so it never saw this. This is the first thing to fix, for small documents as much as large.
2. **CPU is the ceiling for large documents, not the object store (measured).** Indexing prose costs roughly 0.3-0.6
   core-seconds per MB, so a 4-core node takes about 4-5 MB/s of `_source`; at 5.3-5.6 MB/s the pages mix ran at
   2.6-3.4 cores and started refusing. The node log's single connection at 80 MB/s, with 8 MB windows, could carry
   about 60 MB/s (estimated), ten times more. No log split or parallel log PUTs are needed.
3. **Object-store requests do not grow with document size (measured).** Log PUTs stay at one per window (3-4/s on a busy
   node); blobs just get bigger (mean 1.3-3.7 MB, max 8-9 MB: the 8 MB window cap plus the document that crossed it).
   Publishes stay one per shard per refresh. Per document, PUT+LIST falls as documents grow. What grows is bytes: the
   log carries 1.15-1.4x the source (JSON escaping of the source inside the record, plus hedged copies), and segment
   blobs 1.1-1.2x in the first minute, merges included.
4. **A large document slows the small ones that share its window, a little (measured).** In the pages mix at 3.7 MB/s,
   4 KB documents acked at p50 192 / p99 452 ms against 173 / 390 for 1 KB documents alone (+11% / +16%): the window's
   PUT carries the big document (an 8 MB blob takes ~100 ms more at 80 MB/s). A 4 MB document itself acks at p50 ~500
   ms: indexing it takes ~250-650 ms on one core, and that cannot be spread over cores.
5. **Hedged log PUTs fire far more often with big blobs, and mostly lose (measured).** 1-2% of log PUTs are hedged with
   small documents; 6-15% with 1-4 MB documents (won 1-3 of 5-16). The hedge delay is the p95 of recent PUT times, so a
   blob much bigger than its neighbours is hedged although it is only slow because it is big. Each loser is a duplicate
   multi-MB upload. The dollar cost is small (one request each); making the delay size-aware is cheap (estimated).
6. **Heap: 23-40 bytes allocated per source byte; live set 300-460 MB at 4-6 MB/s of the pages mix, 0.7-1.4 GB when
   overloaded (measured, 4 GB heap).** Core's indexing pressure charges a document's UTF-8 bytes once. On the way through
   it is held as the REST body, a Java string (two bytes a character as soon as one typographic quote is in it), UTF-8
   bytes for the engine, Lucene's buffers, the log record until its window lands, and then the window's encode makes
   about four more copies (record JSON, its byte copy, the growing body buffer, the blob, `toByteArray`), so a document
   briefly costs about 6-10x its size (estimated). The tail kept for search nodes (up to 256 MB per node) is not charged
   to any breaker (code).
7. **The write limiter's refusals under CPU overload blame the store (measured).** At pages 140-180/s the store answered
   in 40-50 ms while writes were refused with "the object store is slow to take writes". The limiter counts a write as
   waiting from admission, engine time included. I tried counting only from the log append (a switch): it was worse,
   small-document p99 went from 2.7 to 11.2 s at 140/s and the live heap doubled, because counting engine time is what
   was shedding CPU overload. Dropped. The message should say the node is busy when the store is not slow.
8. **Local translog is not a problem (measured).** On-disk peak 8-17 MB per node at 3-5 MB/s, since each publish trims
   it; the per-write fsync takes 4-10 ms for 1-4 MB documents against 1.4 ms for 1 KB.
9. **In-memory translog gets worse with big documents, as estimated before.** It would move those 8-17 MB onto a heap that
   is already the tightest resource, to save 4-10 ms on acks of 500-1,000 ms. Not worth building.
10. **Readers (estimated, not metered here).** A refresh's segment with large documents is 1.4-3.5 MB (pack mean,
    measured), over `serverless.search.commit_push.inline_bytes` (1 MB), so commit pushes stop carrying the new files and
    each reader fetches them from the store. The tail push sends each window's records to every reader of the shard in one
    message, so a reader gets multi-MB messages; the per-shard keep (16 MB) holds only a few 4 MB documents.
11. **Search-side limit to know about (core behaviour, not measured).** `index.highlight.max_analyzed_offset` defaults to
    1,000,000 characters: highlighting a body longer than that fails unless the field indexes offsets
    (`index_options: offsets`) or the limit is raised.

## Recommendations

- **Write threads (build next).** Run REST and forwarded writes on a pool sized for waiting (many threads), and bound only
  the engine apply by a gate of processors + 1 permits, so CPU use and the number of Lucene indexing threads per shard
  (hence segments and publish PUTs) stay what they are today. Behind a switch, measured with this meter.
- **Max document size.** Only `http.max_content_length` (100 MB) caps it today. A 100 MB document would take tens of
  seconds to index on one core and briefly 0.6-1 GB of heap (estimated). Suggest a cap switch (e.g.
  `serverless.write.max_source_bytes`, off by default) and chunking very long extracted text in the application; a
  300-page PDF is about 0.5-1 MB of text, so 8-16 MB covers practically every real document.
- **Keep the 8 MB log window and one log PUT per window.** The log has ~10x headroom over CPU.
- **Size-aware hedge delay** (small): add the blob's expected transfer time to the p95 before hedging.
- **Limiter message**: say the node is busy, not the store, when appends are fast.
- **Capacity**: plan on ~0.3-0.6 core-seconds per MB of prose `_source`. A tenant at 10K writes/min of the pages mix
  (~7.7 MB/s) needs about 2-5 cores of indexing.
- **Readers**: for big-document indices, raise `serverless.search.commit_push.inline_bytes` (e.g. 8 MB) after a reader
  soak confirms it.
