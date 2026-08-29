# The shell on an object store

R11 established what an object store must provide and that MinIO provides it. That is a statement about
the **store**, not about the shell. Every node this project had ever run read and wrote a local
directory, so "the design works on object storage" rested on the two halves never having been put
together — and putting them together immediately found a bug that made it false.

## What was in the way

`ServerlessBootstrap` constructed an `FsBlobStore` inline. There was no seam at all, so the first job was
making one: `ObjectStores`, with `serverless.store.type` of `fs` or `s3`.

**S3 is reached through `S3RepositoryPlugin#getRepositories`, the supported plugin API.** The R11 harness
builds an `S3BlobStore` directly from inside the plugin's own package, because a conformance suite has to
exercise exactly the container production uses. Production code copying that would be depending on
package-private constructors, so it does not. Three things had to be understood to use the public path:

- **The plugin's async machinery is built in `createComponents`**, out of executors it contributes to the
  node's thread pool *before that pool exists*. The shell builds its own pool without asking any plugin,
  so those executors were simply absent — and the symptom was not a missing-executor error but a null
  `AsyncExecutorContainer`, hit on the first delete. The object-store client now gets a pool of its own,
  built from the plugin's own executor builders. A few threads wasted, and object-store IO no longer
  shares a pool with search or indexing.
- **The node has to be built before the store**, because the repository wants a `ClusterService` and the
  node is what owns one. The plane is attached immediately afterwards, so nothing observable happens in
  between.
- **Netty refuses a second attempt to set its global processor count**, and both the node's transport and
  the S3 event loop group try. `opensearch.set.netty.runtime.available.processors=false` is what a real
  node sets too.

## The bug this found

> `AssertionError: No mark support on inputStream breaks the S3 SDK's ability to retry requests`

`SegmentPublisher.IndexInputStream` — the adapter that feeds a Lucene `IndexInput` to `writeBlob` — did
not support mark and reset. The S3 client refuses such a stream, because a request it cannot replay is a
request it cannot retry, so an upload that failed once would fail permanently.

**A filesystem never asks.** Publishing a segment was impossible on S3 and worked perfectly on disk, and
no amount of testing against `FsBlobContainer` would ever have said so. This is the whole argument for
running the shell itself against an object store rather than stopping at R11.

Implemented against the underlying seek rather than by wrapping in a `BufferedInputStream`, whose mark is
bounded by a read-ahead limit: segment files are routinely larger than any limit worth buffering, and a
mark that silently expired part-way through a large upload would turn a retryable failure into a corrupt
one. `readLimit` is ignored deliberately — the source is seekable, so there is no buffer to outgrow.

## MinIO needs a KMS key

MinIO's SSE-S3 is KMS-backed, and the s3 repository defaults `server_side_encryption_type` to `AES256`
with no "off". Without a key MinIO answers **501 "Server side encryption specified but KMS is not
configured"** on every upload. So the container is started with `MINIO_KMS_SECRET_KEY`, which keeps
encryption on — closer to how a real bucket behaves than switching it off would have been.

## Two mistakes in the tests, both instructive

- **Driving a running node from outside its own loop.** The failover test called `publishAll()` by hand
  while the node's scheduler was doing the same thing; they raced and closed the engine underneath the
  next write (`AlreadyClosedException` on refresh). It now waits for the node's own edge-triggered
  publish. Reaching into a running node is a way to test something that cannot happen.
- **Expecting thirteen documents when twelve had been promised.** The `warm` write was refused with a 421
  because no node owned the shard yet, so it was never acknowledged. The assertion is now twelve *and*
  that the refused write has not come back from the dead — durability is a promise about acknowledged
  writes in both directions.

## What this establishes

A node boots against a bucket, creates an index, writes, publishes its commit, holds its lease and
answers searches with nothing on local disk that matters. A successor with its own `path.home`, which
never sees the first node's disk, rebuilds from the bucket alone — a published commit composed with a
write-ahead log, which is the case that actually happens rather than the easy one.

## What this does NOT establish

- **MinIO is not AWS S3.** Conditional-write linearizability under real contention, listing bounds, and
  the behaviour of S3/GCS/R2 remain unproven.
- **The failover test uses a clean stop**, and says so. Crash semantics — `kill -9`, a lapsed lease, a
  zombie resuming — are covered across real processes in `ServerlessContentionTests`, on a filesystem.
  Nothing has yet crashed a node whose data was in a bucket.
- **One node at a time.** The multi-process and fleet tests still run on a local directory; nothing has
  put contention and object storage together.
- **Nothing is measured.** No latency, no throughput, no request counts. A local MinIO over loopback is
  not a network, and the block cache and read amplification numbers in `block-reads-notes.md` were all
  taken against a filesystem.
- **Credentials come from the node keystore** under `s3.client.default.*`, which is the s3 plugin's own
  convention. Nothing here has been run with real IAM, instance roles, or credential rotation.
