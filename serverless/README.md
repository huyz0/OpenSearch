# The serverless shell

A node process that serves the OpenSearch API with no cluster manager: index truth,
shard ownership and durability all live in an object store, arbitrated by
compare-and-swap registers. It reuses OpenSearch's data plane -- `IndicesService`,
`IndexShard`, `Engine`, `SearchService` -- and replaces everything above it.

This branch is built directly on `opensearch-project/OpenSearch` `main`.

## What it costs core

Nine production files in core, with 710 added lines and 20 removed relative to the
branch's upstream base. Cloud repository implementations remain outside core.

| File | Added / removed | Why |
| --- | --- | --- |
| `common/blobstore/BlobRegister.java` | 68 / 0 | the compare-and-swap register and its random initial generation |
| `common/blobstore/BlobRegisterCasResult.java` | 60 / 0 | its conditional-write outcome |
| `common/blobstore/BlobContainer.java` | 166 / 0 | register operations and paged listing defaults |
| `common/blobstore/BlobStore.java` | 10 / 0 | storage-specific key validation before publishing descriptors |
| `common/blobstore/fs/FsBlobContainer.java` | 310 / 19 | atomic filesystem blobs, registers, locking and listings |
| `common/blobstore/fs/FsBlobStore.java` | 22 / 0 | filesystem component-length validation |
| `index/engine/Engine.java` | 40 / 0 | list and streaming snapshot hooks for additional recovery operations |
| `index/shard/IndexShard.java` | 31 / 0 | replaying and closing that snapshot during recovery |
| `cluster/metadata/DataStream.java` | 3 / 1 | preserving explicit backing-index snapshots while checking fresh names at creation |

`BlobRegisterCasResult` stays a class deliberately. A record's canonical constructor cannot
be less accessible than the record, so making it one would expose
`new BlobRegisterCasResult(true, generation)` -- the boolean-blind construction its named
`applied` and `conflict` factories exist to prevent -- to save sixteen lines.

The S3, Azure and GCS implementations of the register and of the paged listings live in the
repository plugins, outside core: `StartAfter` on S3, `startOffset` on GCS, and on Azure -- whose
continuation marker is opaque and cannot be built from a name -- a walk from the prefix that skips up to
the cursor and stops when the page is full.

Filesystem registers publish complete immutable versions with atomic rename and
sync the containing directory before acknowledging a change. Conditional writes
and deletes coordinate through `.opensearch-register-locks#`, a reserved directory
with at most 64 lock files per container. Blob and child listings exclude this
internal metadata. Keep it while the store is in use, and upgrade all writer
processes sharing a filesystem store to the same register protocol. Reads need
no coordination writes, including when reading older registers on a read-only store.
Filesystem put-if-absent writes require hard link support so the final name never
exposes a partly written blob; unsupported filesystems fail the write explicitly.
Index creation checks filesystem component lengths before publishing the descriptor.
The shard directory includes the index name, UUID and shard number, so its combined
length must fit 255 UTF-8 bytes (255 UTF-16 units on Windows). Object stores retain
their own key limits; this check adds no storage requests.

**Both engine hooks are needed, not just the method.** `ServerlessWriterEngine` overrides
`engineRecoverySnapshot` to replay its object-store write-ahead log during recovery, and
`IndexShard` is what actually runs it, through the same `runTranslogRecovery` path local
translog recovery uses. Reverting `IndexShard` alone compiles perfectly and fails 21 tests --
durability, fencing, deletes, sequence numbers -- because nothing replays the log. Easy to
miss twice over: the method hangs off a class core already had, so a survey comparing
imported *classes* does not see it, and the call site is in a third class that names neither
the shell nor the engine.

Recovery streams fenced WAL records into a disposable local spool instead of retaining all
sources and native operations on the heap. Duplicate sequence identities are validated before
replay, with a 16 MiB external-sort buffer when history is out of order; increasing sequences
need no sort. Legacy operations retain their original ordering and are skipped only when a
later sequenced operation supersedes the same document. Record checksums and checksummed
supersession metadata make local corruption fail activation. Temporary files are removed on
success or failure, and the next exclusive engine open reclaims an interrupted spool. This
adds local disk work without another object-store read pass. Existing engines overriding the
list hook remain supported by the default snapshot adapter.

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
and what is genuinely still open. The one open item that was a correctness question rather than a missing
feature -- the window between moving a shard's ownership and sealing its log -- is closed; see
[`rfc-fencing-closure.md`](rfc-fencing-closure.md).

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

## One write log per node

`serverless.wal.node_log.enabled` (on by default; `false` returns to one log per shard) sends every write a node acknowledges to one log of its own,
rather than one per shard: a window of `serverless.wal.node_log.window_millis` (100, 200 or 250; 250 by default)
is one PUT however many shards it writes. Blobs are named `nodelog/<node>/e=<epoch>/<ordinal>`, and the last header before a fence says where every
unpublished record of every shard is, so a successor reads it without a listing.

- **Publishing.** A shard is published once its oldest unpublished write is as old as its index's
  `refresh_interval`, rounded up to the next window, so the shards that come due together go out in one pass. An
  index that sets no interval waits `serverless.wal.node_log.publish_max_age_millis` (a second by default); one that
  sets `-1` is published only by size or on release. A shard is also published once it holds
  `serverless.publish.max_unpublished_operations`, and the first time this node writes it. Searches read published
  commits, so the interval is also how long a write takes to be searchable. Each publish of a busy shard is five or
  more PUTs: at a one-second interval that is several PUTs a second per busy shard, far more than its log, so an
  index that can wait ten or thirty seconds costs much less.
- **Deleting.** A node deletes its own blobs behind the trim point its lease records: the oldest record any of its
  shards has not published. A survivor deletes a dead node's log once every shard it names has published past it.
  Neither lists.
- **Lifecycle rule.** A delete that fails, or a fence above another, leaves a blob nothing names. Give the bucket a
  lifecycle rule that expires objects under the `nodelog/` prefix after some days; seven is a reasonable choice.
  It must exceed the longest any record can stay unpublished, which for a running fleet is the publish age plus a
  takeover. A whole fleet down for longer than the rule loses what its logs held unpublished, so set it beyond any
  outage the deployment means to recover from.

## Scaling search nodes without a cold cache

A search node that joins cold reads every block its first searches touch from the object store. If you set
`serverless.search.warm_on_join=true`, a new node first copies its share of the warm cache from the search nodes
already serving it, and only then takes traffic. If you set `serverless.search.drain.on_shutdown`, which defaults to
the same value, a stopping node hands its shards to their next readers before it goes.

One flag decides whether a node is sent searches. The ingress learns it from the readiness probe, and coordinators
learn it from the search role in the node's lease.

- **Joining.** The node joins with its lease but without the search role, and `GET /_serverless/ready` answers 503.
  It then does four things:
  1. It asks each search peer which shards it will be ranked first for, counting as many readers as a hot shard is
     spread over.
  2. It copies the blocks the peer holds for those shards over the transport, hottest first. The copy is capped at
     `serverless.search.warm_copy.max_bytes_per_second`, 128 MB/s by default.
  3. It opens the shards, and reads anything a peer did not hand over from the object store in ranges of up to
     8 MB.
  4. It turns ready and renews its lease at once.

  A node that is still warming after `serverless.search.warm_on_join.max_wait` (3 minutes by default) turns ready
  anyway, with whatever cache it has.
- **Draining.** Draining starts on `POST /_serverless/drain`, or on shutdown when draining on shutdown is enabled.
  1. Readiness answers 503 and the lease drops the search role.
  2. Each search peer copies, from the draining node, the shards it now ranks first for, and opens them.
  3. The node keeps answering for `serverless.search.drain.settle` (two thirds of the lease TTL by default), while
     coordinators notice. The call then returns.

  All of this is bounded by `serverless.search.drain.max_wait`, 2 minutes by default.
- **Discovery** goes through the leases in the object store, not through Kubernetes. A pod that is not ready is
  still a member, so its peers find it to answer its copy requests. No control plane tells a node anything. Searches
  reach a reader two ways, from the ingress to a coordinating node and from a coordinator to a reader, and the flag
  covers both.

In `WarmJoinMeterTests` every store request takes 20 ms. The first search on a shard (opening it and one query)
took about 178 ms on a node that joined cold, after 11 to 17 block reads per shard. On a node that joined warm it
took about 46 ms with no block reads. Most of the 46 ms is the commit check that every open makes. Warming took
about a second for 4 to 7 shards of a small index. A drain handed over every shard of the draining node in under a
second, and the successors read no blocks from the store.

A search Deployment that scales one pod at a time:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: search
spec:
  replicas: 6
  strategy:
    rollingUpdate:
      maxSurge: 1          # a new pod warms before an old one is stopped
      maxUnavailable: 0
  selector:
    matchLabels: { app: search }
  template:
    metadata:
      labels: { app: search }
    spec:
      terminationGracePeriodSeconds: 300   # drain.max_wait plus settle, with room to spare
      containers:
        - name: node
          image: <your serverless image>
          env:
            # However your image passes -D settings to the JVM.
            - name: OPENSEARCH_JAVA_OPTS
              value: >-
                -Dserverless.roles=search
                -Dserverless.search.warm_on_join=true
          ports:
            - { name: http, containerPort: 9200 }
            - { name: transport, containerPort: 9300 }
          readinessProbe:            # 503 while starting, warming or draining
            httpGet: { path: /_serverless/ready, port: http }
            periodSeconds: 2
            failureThreshold: 1
          livenessProbe:             # a warming node is healthy, not ready
            httpGet: { path: /_serverless/health, port: http }
            periodSeconds: 10
            failureThreshold: 6
          lifecycle:
            preStop:                 # returns once peers have taken the node's shards
              exec:
                command: ["curl", "-sf", "-XPOST", "--max-time", "280", "http://localhost:9200/_serverless/drain"]
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: search
spec:
  maxUnavailable: 1
  selector:
    matchLabels: { app: search }
```

A HorizontalPodAutoscaler can drive `replicas` as usual. A scale-out adds pods that warm before they serve, and a
scale-in drains the pods it removes. The Service in front of the pods needs nothing special, since it routes only to
ready pods. The `preStop` drain and the shutdown drain are the same drain, and whichever runs second does nothing.

## Transport replay capacity

`serverless.transport.replay_cache.capacity` limits the accepted, still-valid nonces
retained per node (default 65,536). A full cache refuses new forwarded requests with
403 until entries expire; it never evicts a valid nonce to admit another request.
Size this for peak incoming forwarded requests per second multiplied by twice the
lease TTL in seconds. The factor of two covers accepted sender clock skew. Higher
capacity uses proportionally more heap. Expiration is ordered by signed timestamp,
so out-of-order delivery does not delay reclaiming expired entries.

Signed transport requests carry a fresh MAC for each hop and omit the reusable
deployment token. Receivers retain token verification for legacy unsigned senders;
MAC-capable senders therefore require MAC-capable receivers.

The MAC also binds the intended receiver's stable and ephemeral process IDs and
the propagated request headers, including caller identity and plugin origin. All members must use the same MAC
algorithm version; deploy this authentication hardening across the fleet together.
Header order does not affect authentication.

Unknown secret generations trigger at most one secret-store probe per node per
second. Requests waiting for a probe receive a retryable 429, so a forged generation
cannot force a store read per frame or turn a brief key-rotation delay into a 403.

PIT lifecycle state uses compare-and-swap registers in `pit-states`, separate
from active discovery markers in `pits`. Existing plain records are adopted on
first access. Upgrade all nodes together: older nodes do not arbitrate release
and keep-alive through these registers. Released IDs retain a compact tombstone
so delayed requests cannot revive them; active listings do not scan this history.
Do not delete these state records while requests from that deployment can still
run. The extra retained object per PIT is a storage cost of this guarantee.

Snapshot names now arbitrate capture completion and deletion in `snapshot-states`.
Each new capture receives a unique discovery marker and a unique copied-data root
under `snapshot-data-v2`; reusing a snapshot name cannot reuse or delete the prior
capture's files. Legacy name-based snapshot records and copies remain readable.
Upgrade all nodes together, since older writers do not consult these state heads.
Deleted names retain compact state tombstones outside active snapshot listings.

When the backend does not pass the conditional-delete probe, index deletion
releases matching shard heads with compare-and-swap instead of deleting head
keys by name. This preserves a concurrently recreated index's writer and keeps
terms increasing. The fallback reads each shard head and swaps each matching
owned head; it retains one small unowned head per previously activated shard
of a historical index name. Reusing the name overwrites those heads. Backends
with verified conditional deletion continue removing unchanged heads outright.

PIT endpoints apply the request admitted by action filters and send HTTP responses
only after the complete filter chain succeeds. PIT listing is deployment-wide,
and capture covers every shard of one concrete index. Node-scoped listing,
routing and preference selectors, and cached or replaced native PIT response objects are
refused with 501 instead of silently returning a different result. Response
buffering uses the HTTP channel's accounted output buffers.

New data-stream backing indices, including those created by rollover, enable core's
`_data_stream_timestamp` mapper. Single and bulk writes must contain exactly one
timestamp value; rejected documents are not added to the WAL. Existing backing
indices keep their stored mappings: roll over a stream to apply this validation
to subsequent writes.

Alias membership changes reserve the alias register before preparing reverse hints,
then publish membership with a second compare-and-swap. Readers see the previous
membership during an update and cannot see an unfinished creation. Newly added
members force a descriptor swap even when the hint is already present, fencing
concurrent cleanup. Failed or expired preparations restore the previous alias (or
remove an unpublished creation); a delayed writer cannot publish afterward.
Recovery uses the node lease TTL and runs when the alias is next accessed.
Mutators back off for up to roughly 250 ms per attempt while another preparation
is active, then report a retryable conflict if it remains busy.

Upgrade all nodes together for this alias protocol. Stable alias records keep
their previous encoding, but older nodes do not understand the temporary
`mutation` field. Each membership change adds one alias register swap; hint
preparation and cleanup read only the affected indices and alias, without a
namespace listing. An abandoned preparation can retain harmless extra reverse
hints until recovery; reverse lookups verify them against published membership.
