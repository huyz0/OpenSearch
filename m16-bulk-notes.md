# M16: `_bulk` — the write path priced per request

Before this, one document was one HTTP request and one object-store PUT. The second number was the one
that mattered: `ServerlessCostTests` asserted exactly one write per document, and `WalStore`'s own
documentation said a group commit "belongs here and is not built".

## The number

Measured the same way the per-document baseline was measured — object-store writes, counted before any
publication, on both stores:

| | one at a time | as one batch |
|---|---|---|
| s3 (MinIO) | 50 writes | **1 write** |
| fs | 50 writes | **1 write** |

Across shards and nodes, one append per shard: 60 documents over 3 shards cost 3 writes; 40 documents
across two nodes cost 2.

**No wall-clock comparison is published, deliberately.** The baseline calls the node directly and the
batch goes over HTTP, so timing them against each other measures the presence of a network hop and
reports it as the cost of batching. A first draft of the test logged 23 ms against 280 ms on a filesystem
and made batching look ten times slower. The request count is the number that transfers anyway.

## Where the batching actually is

Three places, and only the first is obvious.

1. **`WalStore.append(long, List)`** — one blob for the batch, newline-delimited JSON.
2. **`ServerlessNode.bulk`** — one append, then each operation applied in order, then *one* `sync()` and
   *one* publish edge for the whole batch rather than one per document.
3. **`ForwardedBulkRequest`** — a batch that crosses the network stays a batch. Forwarding item by item
   would have been simpler and would have restored the per-document cost the moment a shard was not
   local, which is most of the time in a real deployment.

## Why NDJSON, and why it needed no version field

A serialized record cannot contain a raw newline: a document's source is a JSON string, so any newline
inside it is escaped. The delimiter therefore cannot collide with the content. It also means a
**one-record blob is byte-identical to what the pre-batching writer produced**, so a log written by an
older node is read by this one with no version field, no migration and no special case. Asserted directly
on the bytes rather than inferred.

Ordering within a term is now `(ordinal, position within the blob)` rather than `ordinal` alone. That is
still a total order, and it is the part most likely to be broken silently — a container that lost position
would pass every count-based test while turning a write-then-delete into a delete-then-write.

## `create` and `update` are refused, not approximated

`create` means fail-if-exists and `update` means a partial merge. Both need version-conditional writes,
which this system does not have — `WalRecord` records document *state*, not history, and says so. Treating
them as `index` would be a request that is accepted and means something other than what it says, which is
the failure mode D2's 501s exist to prevent. They are refused **per item**, so the rest of the batch lands.

## Canaries

| Planted defect | Caught by |
|---|---|
| Unroll the group commit into one append per document | all three cost tests |
| Replay a batch blob's records in reverse position order | order-survives-replay, and the byte-format test |
| Treat `create`/`update` as `index` | the unsupported-actions test |
| Log only the first record of a batch | durability, and order-survives-replay |
| Pair outcomes with the wrong documents on the wire | the forwarded-batch test — **only after it was fixed** |

## Two tests that proved nothing until they were fixed

Both were mine, and both looked green.

**The forwarded-batch test sent nothing but writes.** Every outcome was therefore identical, so a response
that paired them with the wrong documents was indistinguishable from a correct one — canary E passed
untouched. Fixed by sending two deletes with *different* outcomes (one document that exists, one that
never did) at the same remote shard. Getting there took two more mistakes worth recording: the absent id
was first built by prefixing a written one, which changes its hash and lands it on another shard; and the
remote shard was derived as "the one `f0` missed", which aimed the batch at the local node about half the
time and forwarded nothing at all.

**The byte-format test assumed the container held only its own files.** It failed on roughly one run in
ten with `[…001, …002, extra0]` — the test framework plants foreign files in temp directories at random,
and `extra0` is one. The framework was right and the test was wrong: `WalStore` filters on `\d{20}` and
documents exactly why, so a test asserting otherwise was asserting something production deliberately does
not promise.

## What this does NOT establish

- **Groups are dispatched sequentially.** A batch spanning three remote shards is three sequential round
  trips — the same shape as the search fan-out, and the same thing wrong with it.
- **No `create`, `update`, delete-by-query or version-conditional anything**, per above.
- **The forward timeout is unchanged.** A batch does more work on the far side than a single document, so
  this is where that bound is most likely to be the wrong shape. Recorded rather than tuned, because a
  number picked without a measurement is not better than one already justified.
- **No batch size limit and no back-pressure.** A large enough body is a large enough blob, and nothing
  refuses it.
- **Nothing measured about memory.** The whole batch is held in memory twice — parsed items and the
  serialized blob.
