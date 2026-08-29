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

## Contention and object storage, together

The last pair of halves that had never met. Ownership, fencing and recovery were tested across processes
on a local filesystem; the object store was tested on its own. **On a filesystem a compare-and-swap is
instant and the window between reading a shard-head and writing to it is microseconds. Over HTTP it is
milliseconds** — and that window is precisely where two nodes can both believe they own a shard. Every
correctness claim in this project had been measured on the version of the world where the gap barely
exists.

Three tests now run real forked JVMs against a real bucket:

| Test | What is new about it |
|---|---|
| Two processes racing over HTTP | The read-then-write of a shard-head is no longer effectively atomic. Deleting "a live lease is not stolen" makes it fail. |
| A killed node's data rebuilt by a node that never saw its disk | On a shared directory a successor reads files its predecessor left behind. Here the dead node's disk is gone and irrelevant — everything recovered came out of object storage. |
| A three-node fleet on one bucket | A fan-out across processes over shards whose segments live in S3, which is the arrangement the design is actually for. |

Four canaries, all caught: a stealable live lease, WAL replay disabled, fan-out restricted to local shards,
and the keystore ignored.

### The gap that made this a feature, not test plumbing

Object-store credentials are `SecureSetting`s, read from a keystore in the config directory. The bootstrap
only ever read system properties — so **a deployed node had no way to be given S3 credentials at all**.
The shell could talk to S3 in a test that handed it `MockSecureSettings`, and nowhere else. It now loads
`opensearch.keystore` the way a real node does:

- an absent keystore is not an error, because a filesystem-backed node needs no secrets and demanding one
  would make the simplest configuration the one that fails;
- a keystore that exists and cannot be read **is** an error, because the alternative is a node that
  silently cannot reach its store and only says so on the first write;
- a password-protected keystore is refused outright rather than half-supported, since this bootstrap has
  nowhere to prompt.

A second, smaller version of the same problem: an s3 store needs `opensearch.path.conf` set, because the
s3 plugin passes it straight to `System.setProperty` and the AWS SDK NPEs on null. A launcher script
normally sets it; without it a node died in `main()` with a bare `NullPointerException` and no clue.
The bootstrap now refuses to start with a message that names the property.

## What this does NOT establish

- **MinIO is not AWS S3.** Conditional-write linearizability under real contention, listing bounds, and
  the behaviour of S3/GCS/R2 remain unproven.
- **No zombie test on a bucket.** `kill -9` and recovery are now covered against S3; a SIGSTOPped node
  resuming after its lease lapsed is still only covered on a filesystem.
- **Nothing is measured.** No latency, no throughput, no request counts. A local MinIO over loopback is
  not a network, and the block cache and read amplification numbers in `block-reads-notes.md` were all
  taken against a filesystem. The cost model that justifies the architecture — 3 operations per tick per
  node — has never been counted against a store that charges per request.
- **The timings say something anyway, and it is not flattering.** The three-node fleet test takes 69
  seconds against a bucket where its filesystem twin takes 6. Nothing here is tuned for an object store's
  latency, and no one has looked at why.
- **Credentials come from the node keystore** under `s3.client.default.*`, which is the s3 plugin's own
  convention. Nothing here has been run with real IAM, instance roles, or credential rotation.
