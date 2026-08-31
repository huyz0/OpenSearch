# R11 — the object-store conformance suite

> **Status: the suite runs against two unrelated S3 implementations, and there is now an instrument that
> could answer R11 rather than only properties that could falsify it.** Everything passes against MinIO
> and against SeaweedFS, through OpenSearch's own `S3BlobContainer` — the container the shell would use in
> production, not a lookalike written for the test.
>
> **This is still not evidence about AWS S3, GCS or R2.** Two compatible implementations agreeing is
> evidence that the assumptions are not artefacts of one of them. It is not evidence about a provider
> neither of them is. **R11 remains open, and it is open on credentials alone.**

## The short version

Three things exist now that did not:

1. **A linearizability checker.** The property tests ask questions somebody thought of. This records a
   concurrent workload and asks whether *any* sequential order explains it — which is what
   linearizability means, and is the thing that would actually answer R11 when pointed at an account.
2. **Fault injection.** `MisbehavingBlobContainer` makes a store wrong in one chosen way at a time, so
   the checker can be shown rejecting something and the shell can be run against a store that deviates.
3. **A measurement of the blast radius.** "Everything is downstream of R11" is a sentence. It is now a
   set of tests that say which things, how far, and whether anybody would notice.

## The checker, and why it is not one more property

The seven properties below each look at one scenario: two contenders and one winner, a stale generation
losing. Real, and each is a scenario somebody thought to write.

Linearizability is not a scenario. It is a claim about *every* interleaving: each call appears to take
effect at one instant between when it was made and when it returned, and one sequential order explains
every result. So `LinearizabilityChecker` runs a workload shaped like the real one — each worker holds
the generation it last saw and swaps against it, exactly as a node taking a shard does — records what
every call returned and when, and searches for an order that a correct register would have produced.

That catches deviations nobody wrote a scenario for: a read served by a replica that had not caught up,
a refusal reporting a generation nobody stored, an interleaving that is fine pairwise and impossible
together.

**The specification it checks** is small, and every rule in it is load-bearing:

| Rule | Why the design needs it |
|---|---|
| a read returns exactly the current generation and value | a node believing what it just read |
| a swap against the current generation applies | the owner can renew and publish |
| an applied swap **changes** the generation | a generation that repeats is a fence a zombie walks back through |
| a swap against any other generation is refused | one winner, which is all of failover |
| a refusal reports the generation actually stored | the loser re-reads on that number |

**The checker is canaried twice.** `LinearizabilityCheckerTests` builds a history per deviation by hand,
naming each one. `ConformanceSuiteSelfTests` records histories from a store that really is misbehaving,
and `MinioBlobContainerConformanceTests` does it once more over HTTP — because a green run against a live
endpoint means nothing until somebody has shown it would have gone red.

## Running it against MinIO

```
# The KMS key is not optional: MinIO's SSE-S3 is KMS-backed and the s3 repository defaults
# server_side_encryption_type to AES256, so without one every upload gets a 501.
docker run -d --name serverless-minio -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  -e "MINIO_KMS_SECRET_KEY=serverless-key:$(openssl rand -base64 32)" \
  quay.io/minio/minio server /data

./gradlew :serverless:testkit:s3Test
```

Point it elsewhere with `-Dtests.serverless.s3.endpoint=...`. With nothing listening the tests
assume-skip and say how to start one, so this is wired into `check` without making a container mandatory.

## Running it against a second implementation

One implementation cannot tell "this property holds" from "this implementation happens to do what our
client asks". The conditional-write mapping in particular — `If-Match`, `If-None-Match`, and what the S3
client does with a 412 — is code of ours that a second dialect can find bugs in.

```
# SeaweedFS refuses signed requests unless an identity is configured. Without this, every call fails
# with "Signed request requires setting up SeaweedFS S3 authentication" -- which looks exactly like a
# missing conditional-write primitive if you do not read the cause.
cat > /tmp/seaweed-s3.json <<'JSON'
{"identities":[{"name":"seaweed",
  "credentials":[{"accessKey":"seaweed","secretKey":"seaweedsecret"}],
  "actions":["Admin","Read","Write","List","Tagging"]}]}
JSON

docker run -d --name serverless-seaweed -p 8333:8333 \
  -v /tmp/seaweed-s3.json:/etc/seaweedfs/s3.json \
  chrislusf/seaweedfs:latest server -s3 -s3.port=8333 -s3.config=/etc/seaweedfs/s3.json -dir=/data

./gradlew :serverless:testkit:s3Test
```

All eleven pass, none skipped.

**LocalStack was the obvious second target and is not usable.** Its free S3-only image was discontinued
in March 2026 and the remaining images require a licence, so it is not something a contributor can run.

## What a misbehaving store actually costs

`ServerlessStoreDeviationTests` runs the shell against stores that deviate. The answer is narrower than
"everything", and in one place worse:

| Deviation | What it costs |
|---|---|
| a stale read of a shard-head | **nothing.** Ownership is decided by the swap, not the read; a node reading an out-of-date head swaps against a moved generation and loses, which is correct |
| an ambiguous outcome — the write applied, the answer was lost | **liveness, not safety.** The node does not serve a shard it believes it failed to take; the head names it until the lease lapses |
| **two winners on one generation** | **data.** Both nodes serve and both publish — and the manifest fence refuses only a *strictly newer* term, so it does not catch a second writer at the same one |

The third one is the finding. Before it was measured, the second publish inherited the first's file names
and skipped uploading them, on the grounds that a published name has the same bytes — true under one
writer per term, false here. Where both shards happened to produce the same names, one node's
acknowledged write vanished silently. Where they did not, the surviving manifest named `segments_5`
written by one node beside `_0.cfs` holding the other's bytes: a commit describing an index that never
existed anywhere.

**A manifest now records which node published it, and a publish from a different node at the same term is
refused** with `ForeignWriterException`. The data is still lost — nothing can un-acknowledge a write —
but the node is told, and the message names the object store rather than leaving a hole in an index for
somebody to find later. A manifest with no recorded writer is treated as "cannot tell" rather than as a
mismatch, so an upgrade does not stop every shard from publishing.

That is the sharpest available statement of why R11 matters, and it is a statement about this design
rather than about any provider.

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

- Code: `BlobContainerConformanceTestCase` (abstract), `FsBlobContainerConformanceTests`,
  `MinioBlobContainerConformanceTests`, `SeaweedBlobContainerConformanceTests` (bindings),
  `ConformanceSuiteSelfTests` (self-check), `LinearizabilityChecker` + `RegisterHistory` (the instrument),
  `MisbehavingBlobContainer` + `MisbehavingBlobStore` (fault injection),
  `ServerlessStoreDeviationTests` (blast radius).

## What R11 is, and what changed

Every safety argument in this design rests on behaviour of the object store that **nobody has checked
outside a local filesystem**. Ten milestones have been built and measured against `FsBlobContainer`,
which provides compare-and-swap with real filesystem atomicity and ranged reads with a file channel —
so it satisfies these properties almost by construction, and every green run so far is evidence about
the shell and none about S3.

D5 deferred R11, and four phases have since ended pointing at it. Phase 9 *grew* it (listing bounds) and
the block-range work grew it again (ranged reads). What was missing was the tests; they exist, they run
against two S3 implementations, and there is now a checker that asks the real question rather than a
proxy for it. **What is missing is an account.**

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

## When credentials exist

Everything needed is written. Subclass the S3 target, override the endpoint and keys, and run:

```java
public class AwsS3ConformanceTests extends MinioBlobContainerConformanceTests {
    @Override protected String endpoint()  { return System.getProperty("tests.serverless.aws.endpoint"); }
    @Override protected String accessKey() { return System.getProperty("tests.serverless.aws.key"); }
    @Override protected String secretKey() { return System.getProperty("tests.serverless.aws.secret"); }
}
```

Two things to do beyond running it green:

1. **Raise the concurrency and the round count.** The checker searches short rounds because the search is
   exponential in the worst case; more independent rounds is more chances to catch something, and each
   stays cheap to judge. A violation against a real provider is likely to be rare.
2. **Run it while the provider is unhappy.** Throttling and retry are where an ambiguous outcome is
   produced, and an ambiguous outcome is the one deviation the checker deliberately does not model.

## What this does NOT establish

- **Nothing about AWS S3, GCS, Azure or R2.** Two S3-compatible implementations agree. Neither is a
  provider, and the properties that matter most are exactly where a compatible implementation may differ.
- **Nothing about a store that lies while under load.** Both containers run locally, unthrottled, on one
  machine, and neither has ever had to retry.
- **Concurrency is threads in one JVM.** Real contention is across processes and machines, where a
  provider's conditional write is doing something genuinely different from a local lock.
- **The checker does not model an operation with no result.** A call that threw may or may not have taken
  effect, and admitting that needs a model where the generation after an applied write is unknown rather
  than observed — which weakens every subsequent read into a guess. That case is real, and it is covered
  the other way round: the fault injector produces it and the tests ask what the *shell* does about it.
- **Listing pagination (`start-after`/`max-keys`) is not covered.** Phase 9 depends on it and
  `FsBlobContainer` has no equivalent primitive, so there is nothing to assert against locally.
