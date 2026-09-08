# The serverless shell

A node process that serves the OpenSearch API with no cluster manager: index truth,
shard ownership and durability all live in an object store, arbitrated by
compare-and-swap registers. It reuses OpenSearch's data plane -- `IndicesService`,
`IndexShard`, `Engine`, `SearchService` -- and replaces everything above it.

This branch is built directly on `opensearch-project/OpenSearch` `main`.

## What it costs core

Six files, 364 lines, all additive.

| File | Lines | Why |
| --- | --- | --- |
| `common/blobstore/BlobRegister.java` | 42 | the compare-and-swap register |
| `common/blobstore/BlobRegisterCasResult.java` | 53 | its outcome |
| `common/blobstore/BlobContainer.java` | 59 | `readRegister`, `compareAndSwapRegister`, `createRegisterIfAbsent` |
| `common/blobstore/fs/FsBlobContainer.java` | 136 | the filesystem implementation |
| `index/engine/Engine.java` | 20 | `engineRecoveryOperations()` |
| `index/shard/IndexShard.java` | 54 | `recoverAdditionalEngineOperations`, which replays them |

It was 408 until an audit of what core actually needs. `Engine` also carried
`globalCheckpointSupplierForCombinedDeletionPolicy`, a hook nothing overrode and nothing
called -- `InternalEngine` still passes the supplier inline, so it was never even wired to
its own default. `BlobRegister` was a hand-written value class whose `equals` and `hashCode`
were exactly what a record generates. Neither removal changed a call site.

`BlobRegisterCasResult` stays a class deliberately. A record's canonical constructor cannot
be less accessible than the record, so making it one would expose
`new BlobRegisterCasResult(true, generation)` -- the boolean-blind construction its named
`applied` and `conflict` factories exist to prevent -- to save sixteen lines.

The S3, Azure and GCS implementations of the register live in the repository plugins,
outside core.

**Both engine hooks are needed, not just the method.** `ServerlessWriterEngine` overrides
`engineRecoveryOperations` to replay its object-store write-ahead log during recovery, and
`IndexShard` is what actually runs it, through the same `runTranslogRecovery` path local
translog recovery uses. Reverting `IndexShard` alone compiles perfectly and fails 21 tests --
durability, fencing, deletes, sequence numbers -- because nothing replays the log. Easy to
miss twice over: the method hangs off a class core already had, so a survey comparing
imported *classes* does not see it, and the call site is in a third class that names neither
the shell nor the engine.

## One coupling worth naming

`ServerlessNode` constructs `IndicesService` and `SearchService` directly, so it tracks their
constructor signatures. That is not a dependency on anything serverless -- upstream grows those
parameter lists for its own reasons, and the shell passes whatever they currently ask for.
Expect this to be the routine cost of following upstream.

## What is deliberately reused rather than reimplemented

Document routing goes through `OperationRouting.generateShardId`, not a copy of its hash.
Alias bodies are rendered by `AliasMetadata.Builder#toXContent`. Search runs on the reused
`SearchService` and comes back as a real `SearchResponse`; aggregations are reduced by core's
own reducer under core's circuit breakers.

Two places still do not reuse core, each for a reason:

- **The cross-shard hit merge.** `SearchPhaseController.mergeTopDocs` is package-private, and
  the merge here trims to `2 * window` as answers arrive so a wide index costs the coordinator
  two windows of hits rather than one per shard. `TopDocs.merge` needs every shard's results up
  front, which would give that bound up.
- **Snapshot rendering.** `SnapshotInfo`'s constructor taking both a state and start/end times
  is package-private, and the public ones derive state from failures -- so they cannot express
  a capture still running, which this surface reports with real times.

## A lesson that cost a day

`DocumentRouting` first memoised its rendered `IndexMetadata` in a `static` map keyed by index
uuid. Correct, and a leak: one `IndexMetadata` pinned per index for the life of the JVM. In a
test run holding many nodes at once that was enough to turn two suites into GC thrash and an
828-second timeout, which reads exactly like a deadlock and is not one. The memo lives on the
`IndexDescriptor` now -- immutable, replaced whenever the index changes, and collected with it.

If something here looks like a hang, measure before concluding.

## Testing

`:serverless:testkit:test` is 549 tests. `processTest` forks real JVMs, `pluginTest` installs a
real assembled plugin, `tlsTest` does a real handshake. `s3Test` needs a live MinIO or SeaweedFS
endpoint and assume-skips without one.

Two dependency-direction rules are enforced as build tasks rather than conventions:
`server/` may never name `org.opensearch.serverless`, and the auth plugin may reach only for
plugin API.

## Where it is, and what is unfinished

[`STATUS.md`](STATUS.md) is the current statement of what works, what is refused on purpose,
and what is genuinely still open -- eleven items, of which the fencing window below is the only
one that is a correctness question rather than a missing feature.

## The open risk

The whole safety argument rests on the object store's `compareAndSwapRegister` being genuinely
linearizable. There is a linearizability checker, and it passes against MinIO and SeaweedFS.
It has never been pointed at a real cloud provider, and no fake can close that.

What has been done instead is to read the contract. AWS documents the exclusion this design needs --
concurrent conditional writes to one key are resolved to a single winner, the rest refused with `412` --
and has provided strong read-after-write consistency for `GET`, `PUT` and `LIST` in every region since
December 2020. That makes the register a documented primitive rather than an assumed one. It is a floor
under the argument and not a substitute for the checker: a specification says what a provider commits to,
not that the implementation holds under partition, and it speaks for S3 rather than for the S3-compatible
stores `ObjectStores` will equally happily point this shell at.

Reading it was not merely reassuring. The same page documents two further outcomes of those races -- `409`
under concurrency, and `404` when a delete beats an `If-Match` write -- which the S3 register mapped to
exceptions rather than to conflicts. See `STATUS.md`.
