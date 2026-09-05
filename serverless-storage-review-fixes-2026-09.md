# serverless-storage: the September review, fixed

[`serverless-storage-review-2026-09.md`](serverless-storage-review-2026-09.md) read the plugin against
its RFCs and found five ways to lose or corrupt acknowledged data, a set of defaults that shipped the
stated goals off, and an affordability claim the code no longer supported. This is the other half. The
review is left as written so each fix can be read against its finding.

Six implementation passes ran in parallel over disjoint packages, each with the critical from its own
report first. The plugin's unit suite went from **1,486 tests passing** before to **1,684 passing**
after, with every forbidden-API and logger-usage violation this work introduced removed — what remains
of those is five forbidden-API findings that predate it, in files nothing here touched, which I
confirmed by stashing the change and reproducing them.

## The five data-loss and corruption paths

- **Garbage collection could delete the live commit.** The sweep never read the shard head and took
  "latest" from a blob listing, while the publisher writes the manifest *before* the head swap — so a
  crash in that gap made the real head manifest look superseded. The sweep now reads the head, anchors
  deletability to it, treats manifests newer than the head as unpublished orphans on their own window,
  and fails closed when the head is absent or unreadable. Cross-tick state is persisted per shard,
  which matters more than it did before: the sweep now runs on the writer primary, which relocates on
  exactly the events an in-memory orphan clock would forget.
- **Gated indices had no writer fencing.** Their primary term was pinned at one, which made the
  publisher's refusal branch dead code, and the lease holder's identity was never compared against the
  publishing node. The head now carries a lease term that advances strictly on takeover and never on a
  holder's heartbeat, acquisition refuses a lease another live node holds, and a publish is checked
  against both the node identity and the token it acquired under.
- **Compaction was invisible to a reader, then corrupted it.** Merging into an empty directory made
  every compaction emit a commit whose name *and length* collided with the writer's own, so a reader
  served pre-compaction data while reporting the compacted generation and the materializer's
  equal-length check skipped the fetch. Compaction now merges in place so the merged commit outranks
  its source and cannot recycle segment names; "already present" is decided on the full file reference
  rather than on length; superseded commit pointers are removed so the manifest decides what is open;
  and the reader verifies it opened the commit it was told to.
- **The lazy block cache could serve the wrong bytes.** Keys are content-addressed now — bundle,
  offset, length and checksum — every block is length-validated on read, single-block files are fully
  checksum-verified at no extra request, and closing a shard prunes its blocks.
- **A synchronous write was acknowledged before its log chunk uploaded.** Pending futures are keyed by
  location and registered before the enqueue, so a wait is for the caller's own record rather than
  whichever happened to be latest.

Beside these, every split silently hid nested child documents and the next merge deleted them. Root
documents are now identified the way core's own splitting query does and each root's verdict is
propagated across its block, so nested children survive rather than the feature being refused.

## The defaults that made the goals untrue

Write-ahead-log mirroring now defaults on, paired with flush batching, so the object store really is
the durable home of an acknowledged write without an operator opting in. Enabling it made the log
sweep's disabled default harmful rather than harmless, so that now runs every minute, with the
argument written down: deletability is fixed by published state, never by time, so cadence is a cost
knob and not a safety one. Suspension with mirroring off, and node-capacity evaluation with
scale-to-zero off, are refused at startup rather than silently degrading.

Compaction and garbage collection moved off the reader engine to node-level tasks on the writer
primary. Before this, an index with no search replicas — the common shape — never compacted and never
collected anything.

## Availability

The orchestrated split fenced its source unconditionally and then returned early without enabling
write routing, so the documented default invocation left an index permanently unwritable with deletion
the only escape. Fencing is now conditional and ordered after routing, and an unfence action exists,
because conditional fencing alone would leave already-bricked indices stranded. A cancel action covers
a split whose child is never attempted. A failed engine no longer leaks its admission permit and keeps
polling. The refresh budget measures active cache usage rather than total, which an LRU keeps at
capacity by design — before, a warm node deferred every refresh forever.

## Security

Every encrypted block is now bound by authenticated associated data to its index, shard, container,
blob, block index and length, which also authenticates the previously untagged header; data written
under the old format still reads, with a documented migration and a setting that ends it. Log records
are bound the same way, with a format version and a back-compatible read path. The master key is
decoded without a heap copy and its length is validated at startup instead of failing every write. The
README gained an honest boundaries-and-gaps section: one key per node, registers unauthenticated, the
unwrapped shared log container, and no index-level authorization anywhere.

## The documents

RFC §15 now states the measured core footprint — 63 new production files under `server/`, the
gated-metadata plane, the shard-recovery SPI, `BlobRegister`, the in-place split actions — instead of
the eight seams it claimed, with its two factual errors corrected, the phases that are not done marked
partial, and the risks whose supporting benchmarks were deleted reopened.

## Two things the tests forced us to decide

A test asserted that a retried log write must claim a fresh sequence. That is the behaviour that lets
a write be acknowledged and then excluded from replay, so the test encoded the bug; it was inverted,
with the reasoning written into it, and a separate test now covers the property it was reaching for.
And a clock-skew allowance had been *added* to the retention window rather than capping it, turning a
deliberate one-millisecond window into five minutes — a 300,000-fold amplification that made the
setting meaningless below five minutes. The allowance is now a maximum.

## Deferred, with reasons

Per-block checksums in the bundle format, multipart upload, the change-log listing storm and the
per-shard lease heartbeat are each recorded in the pass reports as design-scale work with a plan
rather than a partial edit. Prune-before-activate remains absent. The directory tier's dead classes
were deleted and what survives says plainly that it is on no path.
