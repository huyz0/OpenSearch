# R11 — the object-store conformance suite

> **Status update: the suite now runs against a real S3 API.** All seven properties pass against MinIO,
> through OpenSearch's own `S3BlobContainer` — the container the shell would use in production, not a
> lookalike written for the test. See "Running it against MinIO" below.
>
> **This is not evidence about AWS S3.** MinIO is an S3-compatible implementation, and the properties
> that matter most — conditional-write linearizability under contention, listing bounds, read-after-write
> — are exactly where a compatible implementation may differ from the real thing. What is established is
> that the assumptions are not filesystem artefacts and that the suite runs end to end over HTTP.

## Running it against MinIO

```
docker run -d --name serverless-minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data

./gradlew :serverless:testkit:s3Test
```

Point it elsewhere with `-Dtests.serverless.s3.endpoint=...`. With nothing listening the tests
assume-skip and say how to start one, so this is wired into `check` without making a container mandatory.

### What it took, since the blocker was recorded wrongly

§12.1 said M12 was "blocked on `S3BlobStore`'s package-private 17-argument constructor rather than on
credentials". That was never really a blocker — a test helper *in the same package* reaches it. Three
things were actually in the way, and none of them was visible from outside:

1. **`S3Service.setDefaultAwsProfilePath` NPEs when `opensearch.path.conf` is unset.** It hands
   `System.getProperty("opensearch.path.conf")` straight to `System.setProperty`, and the AWS SDK v2
   needs it set to keep off the home directory. Supplied by the `s3Test` task rather than by code, so no
   forbidden-API suppression is needed anywhere.
2. **`deleteBlobsIgnoringIfNotExists` looks synchronous and is not.** It delegates to the async delete
   chain, so a store built with a null `S3AsyncService` passes every register test and then fails the
   moment anything is deleted. Deletion is not incidental here — it is how tombstones, garbage collection
   and lease release work.
3. **`serverSideEncryptionType` is compared with `String.equals` on every read and listing.** Passing
   null for "no encryption" is an NPE on the first ranged read. It has to be `""`.

Multipart upload stays off and its transfer manager stays null on purpose: nothing R11 asserts goes near
it, and a test that strayed there gets a NullPointerException rather than a wrong answer.

### Checking the checker, on this transport

`ConformanceSuiteSelfTests` plants defects over `FsBlobContainer`, which shows the assertions are sound.
It does **not** show they still bite once the same calls go over HTTP to a real S3 API, where a violation
could surface as an exception, a retry, or a swallowed 412 instead of a wrong answer. So the same two
defects are planted over the live MinIO container:

| Planted over MinIO | What the suite saw |
|---|---|
| a compare-and-swap that always claims success | **8 winners** out of 8 contenders |
| a ranged read that ignores the range | **4096 bytes returned for a 64-byte request** |

Seven green tests against a live endpoint would mean nothing without this. "Would this run have failed if
MinIO were wrong?" is a question that has to be answered by making it wrong.

- Code: `serverless/testkit/.../BlobContainerConformanceTestCase.java` (abstract),
  `FsBlobContainerConformanceTests` (binding), `ConformanceSuiteSelfTests` (self-check)
- Result: **103 tests, 0 failures**; `check` green on both projects.

## What R11 is, and what changed

Every safety argument in this design rests on behaviour of the object store that **nobody has checked
outside a local filesystem**. Ten milestones have been built and measured against `FsBlobContainer`,
which provides compare-and-swap with real filesystem atomicity and ranged reads with a file channel —
so it satisfies these properties almost by construction, and every green run so far is evidence about
the shell and none about S3.

D5 deferred R11, and four phases have since ended pointing at it. Phase 9 *grew* it (listing bounds) and
the block-range work grew it again (ranged reads). What was missing was not credentials — it was the
tests. Those now exist, and R11's remaining cost is "point it at an endpoint".

## The properties, and what each one holds up

| Property | Depended on by |
|---|---|
| put-if-absent admits exactly one creator | index name uniqueness, with no cluster-manager |
| CAS admits exactly one winner under contention | shard ownership, fencing, every failover |
| a stale generation loses | zombie fencing, ordered index deletes |
| generations strictly increase | the term fed to `updateShardState` |
| read-after-write on a register | a node believing its own successful write |
| **ranged reads return exactly the bytes asked for** | lazy block reads — newest and least tested |
| listings are complete and report accurate lengths | paged enumeration, GC, block-cache file lengths |
| a deleted register reads absent, not stale | index deletion, and the name becoming free again |

Two of these had never been written down as requirements anywhere before, let alone tested: ranged-read
exactness and listing length accuracy. Both are now load-bearing because of work done in the last two
milestones.

## The suite has teeth, and that is checked

A conformance suite is pointed at implementations nobody here controls, so the one thing that must not
be true of it is that it passes regardless. `ConformanceSuiteSelfTests` plants the two failures that
matter and confirms the assertions would come out differently:

| Planted defect | What the suite would see |
|---|---|
| CAS always reports success | 8 winners instead of 1 — two nodes owning the same shard |
| ranged read ignores its range | wrong bytes, and a stream that does not end where it should |

## Running it against a real backend

Subclass and bind a container:

```java
public class MyBackendConformanceTests extends BlobContainerConformanceTestCase {
    @Override protected BlobContainer newContainer() { return /* an empty container */; }
}
```

For S3 specifically, the repository already carries the pieces: `S3HttpHandler`
(`test/fixtures/s3-fixture`) is a pure-Java S3 protocol double that handles `If-Match`/`If-None-Match`,
and `S3ConditionalWriteAgainstRealStoreTests` shows the client wiring for a real endpoint via
`-Dtests.s3.endpoint`. Binding `S3BlobContainer` needs the test to live in
`org.opensearch.repositories.s3`, because `S3BlobStore`'s constructor is package-private and takes
seventeen arguments including async transfer managers — which is why that binding is not written here.

## What this does NOT establish

- **Nothing about S3, GCS or Azure.** The suite exists; it has been run against a filesystem. That is
  the honest state, and the distinction matters more here than anywhere else in the project.
- **No S3 binding is written.** The blocker is `S3BlobStore`'s construction surface, not credentials —
  which is worth knowing, because it means someone with credentials still has an hour of wiring first.
- **Concurrency is tested with 8 threads in one JVM.** Real contention is across processes and machines,
  where a provider's conditional write is doing something genuinely different from a local lock.
- **Nothing about throttling, latency, or partial failure.** The suite asks whether a backend is
  *correct*, not whether it is fast or what it does under pressure — and phase 8's polling measurements
  are the ones that would change shape against a real provider.
- **Listing pagination (`start-after`/`max-keys`) is not covered.** Phase 9 depends on it and
  `FsBlobContainer` has no equivalent primitive, so there is nothing to assert against locally.
