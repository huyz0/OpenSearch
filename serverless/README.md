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
real assembled plugin, `tlsTest` does a real handshake. `s3Test` needs a live S3-compatible endpoint
(RustFS, MinIO or SeaweedFS) and assume-skips without one.

### Running against RustFS

RustFS is the S3-compatible store this project tests on; there is no AWS account behind any of it, so
nothing here is evidence about real S3. Start one (single drive, and optionally four drives with erasure
coding):

```
docker run -d --name serverless-rustfs-single -p 9200:9000 \
  -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin rustfs/rustfs:1.0.5 /data
docker run -d --name serverless-rustfs-erasure -p 9300:9000 \
  --tmpfs /data/d1 --tmpfs /data/d2 --tmpfs /data/d3 --tmpfs /data/d4 \
  -e RUSTFS_ACCESS_KEY=rustfsadmin -e RUSTFS_SECRET_KEY=rustfsadmin rustfs/rustfs:1.0.5 '/data/d{1...4}'
```

Point the S3 tests at it. Every `s3Test` class reads the endpoint and keys from these properties
(the keys default to MinIO's `minioadmin`):

```
S3="-Dtests.serverless.s3.endpoint=http://127.0.0.1:9200 -Dtests.serverless.s3.access_key=rustfsadmin \
    -Dtests.serverless.s3.secret_key=rustfsadmin -Dtests.serverless.s3.conditional_delete=true"
./gradlew :serverless:testkit:s3Test $S3 -Dtests.serverless.rustfs.endpoint=http://127.0.0.1:9200 \
    -Dtests.serverless.rustfs_erasure.endpoint=http://127.0.0.1:9300
./gradlew :serverless:testkit:processTest --tests '*ServerlessBucketContentionTests' $S3
```

That runs the register conformance suite (create-if-absent, compare-and-swap, conditional delete,
linearizability), log fencing, the cost and delete-cost suites and the object-store end-to-end tests.

The meters and request-count tests below run on the in-process filesystem store by default.
`-Dtests.serverless.meter.store=s3` puts them on the endpoint above instead (a fresh bucket each),
and `-Dtests.serverless.meter.latency` sets the delay added while they measure: `flat` (default:
20 ms per request, one in fifty at 200 ms), `s3` (GET/HEAD 20-30 ms, PUT 40-60, conditional PUT
50-70, DELETE 25-40, LIST 40-70, one request in fifty four times slower, one in a thousand 0.5-1 s,
from a seeded random) or `none`. On RustFS, `none` leaves RustFS's own latency.

| Test | Switch | What it measures |
|---|---|---|
| `NodeLogSoakMeterTests` | `-Dtests.soak.seconds=150 -Dtests.soak.shards=1000 -Dtests.soak.modes=pack+log,blob` | write ack p50/p99, PUT+LIST per publish, takeover of every shard, cold open and first search, pack collection |
| `WriteThroughputMeterTests` (`throughputTest` task) | `-Dtests.tput.step_seconds=60 -Dtests.tput.shards=3000 -Dtests.tput.rates=1000,2000,3000` | writes/s one node sustains across thousands of shards, p50/p99 from arrival, CPU by thread, fds/heap/open time per writer; results in [`write-throughput-results.md`](write-throughput-results.md) |
| `RefreshVisibilityMeterTests` | `-Dtests.meter.refresh=true` | `refresh=true` ack time, misses on search nodes, first open |
| `FreshnessMeterTests` | `-Dtests.meter.freshness=all` | write-to-searchable delay by path |
| `BucketCommitsTests`, `BucketCheckpointTests` | none | the bucket manifest swap: one conditional write per checkpoint, one lost swap per race |
| `NodePackCollectorTests` | none | pack deletes, and that the collector never lists packs or shards |

```
./gradlew :serverless:testkit:s3Test --tests '*NodeLogSoakMeterTests' $S3 -Dtests.serverless.meter.store=s3 \
    -Dtests.serverless.meter.latency=none -Dtests.soak.seconds=150 -Dtests.soak.shards=1000 -Dtests.soak.modes=pack+log
```

A thousand shards need `ulimit -n 20000` or more before the Gradle daemon starts (`./gradlew --stop` first); raise it
to the hard limit, 1,048,576 where the machine allows (see [Open files per node](#open-files-per-node)).
Buckets the meters create are not removed; delete the container to reclaim them.

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
  commits, so the interval is roughly how long a write takes to be searchable, plus how long a search node takes to
  notice the new commit (see below). Each publish of a busy shard is five or
  more PUTs: at a one-second interval that is several PUTs a second per busy shard, far more than its log, so an
  index that can wait ten or thirty seconds costs much less.
- **Acknowledgement.** A write is acknowledged once the blob of its window lands. Against a store at 20 ms a request
  (1 in 50 at 200 ms) and writes arriving at random, about three quarters of a write's wait is its window: p50 177 ms at
  100 writes/s, of which 131 ms waiting for the window to close, 25 ms the PUT, 17 ms the incarnation read and ~3 ms
  the rest (`WriteAckMeterTests`, `-Dtests.ack.seconds=40`). Writers that write again the moment they are answered, as
  the soak's do, all arrive just after a window lands and wait out a whole one, which is why the soak reports ~300 ms.
  - `serverless.wal.node_log.hedge_after_millis` (60 by default; 0 turns it off) sends a window's PUT again when it
    has not answered in that time, or in the 95th percentile of the last 128 PUTs when that is longer, and lands on
    whichever answers first. A PUT that finds its slot taken reads it back to tell its own other PUT from a fence. It
    costs about one PUT in a hundred on the meter, and at most one in twenty, and cut p99 from 386 to 350 ms there.
  - `serverless.write.confirm_incarnation_by_stripe` (on by default) takes the incarnation read off the ack path.
    Every descriptor update bumps one of 16 change-stripe registers (`index-changes/stripe-NN`) after it applies,
    and every delete bumps one before and after; a create bumps none. A writer that has read an index's descriptor
    confirms it afterwards by the index's stripe, which names every change since; each stripe being written is read
    in the background about twice a fence window, so a write normally reads nothing. A stripe naming the index, an
    unfinished delete, or a read older than what the stripe keeps sends the write back to the descriptor read. On
    the meter: the 17 ms read went to 0.1 ms, p50 173 -> 158 ms, GETs per write 0.75 -> 0.27 (at most about 32 stripe
    GETs a second per writing node, against one descriptor GET per index written per second). Cost: one PUT per
    mapping or settings update and two per delete.
  - `serverless.wal.node_log.idle_flush` (off by default) paces windows by the last blob instead of by their first
    record: a write that finds the log quiet for a whole window goes at once. On a quiet node it roughly halves the
    ack (p50 295 -> 141 ms at 4 writes/s), and a busy one is paced as before, but in between it costs log PUTs: +32%
    at 4 writes/s, +44% at 10, +3% at 100. A 100 ms window instead gives p50 97 ms at 100 writes/s for 2.3 times the
    log PUTs. Leave it off on nodes that take writes all day, where it gains little. Turn it on for low-traffic nodes
    (a few writes a second or fewer) whose clients need a p50 near 150 ms rather than near 250-300 ms, and that can
    pay roughly a third more log PUTs for it.
- **Deleting.** A node deletes its own blobs behind the trim point its lease records: the oldest record any of its
  shards has not published. A survivor deletes a dead node's log once every shard it names has published past it.
  Neither lists.
- **Lifecycle rule.** A delete that fails, or a fence above another, leaves a blob nothing names. Give the bucket a
  lifecycle rule that expires objects under the `nodelog/` prefix after some days; seven is a reasonable choice.
  It must exceed the longest any record can stay unpublished, which for a running fleet is the publish age plus a
  takeover. A whole fleet down for longer than the rule loses what its logs held unpublished, so set it beyond any
  outage the deployment means to recover from.
- **Node packs and log commits (both on by default).** A publish's segment blobs of up to
  `serverless.segments.node_pack.max_blob_bytes` go into the node's next pack, one PUT shared by every shard of any
  index that publishes within `serverless.segments.node_pack.linger_millis`. A publish's commit is staged on the writer
  and logged in its write log, riding the next log blob with no PUT of its own; the shard's manifest is written only
  every `serverless.wal.node_log.commits.checkpoint_millis` (60 s by default), and a successor opens from the newest
  logged commit. Switch packs per index with `index.serverless.segment_placement` (`segment_blob` or `node_pack`), for
  the cluster with `serverless.segment_placement` in the cluster settings, or for a node with
  `serverless.segments.node_pack.enabled`; the index wins over the cluster, the cluster over the node, and a switch
  moves nothing already written. Turn log commits off with `serverless.wal.node_log.commits=false`. Measured locally at
  1,000 shards on one node (20 ms per store request, 1 in 50 at 200 ms), against one blob per segment and a manifest per
  publish: 0.68 PUT+LIST per shard publish instead of 2.09, 4.7 GETs instead of 5.7, write p99 823 ms instead of about
  885, a cold open 21.8 GETs instead of 25.3 and its first search about 268 ms instead of 295. The price is takeover:
  after a crash a successor took 60 to 61 s to open all 1,000 shards instead of 48 to 50 (about 18 s instead of 15 at
  300 shards), because more shards hold unpublished records when packs linger, and each must publish its replay before
  it serves. A deleted index's bytes inside packs come back when the node's pack collector next finds them unnamed,
  which runs every `serverless.segments.node_pack.collect_interval_millis` (a minute by default) and waits out a grace
  before it deletes, so not at the delete.

- **Telling search nodes about a new commit.** A search node that has a shard open checks the shard's register
  for a newer commit every other lease renewal, which is 20 seconds at the default TTL. Until it checks, its searches
  miss what was published since. By default (`serverless.search.commit_push.enabled`, set it to `false` to stop), each publish
  is also sent over transport to the search nodes that placement ranks first for the shard. A node with the shard
  open on an older commit lets that reader go and reopens it on the pushed commit in the background. The push
  carries the commit, so it adds no PUT, LIST or GET (the reopen reads only the new segment's blocks, which the next
  search would have read anyway). It is sent after the publish, so it adds nothing to a write's acknowledgement.
  Locally, with 20 ms per store request, a one-second refresh and the 30-second TTL, an open reader saw a write
  1.5 s (p50) and 1.7 s (p99) after its acknowledgement with pushes, against 9.9 s and 20 s without. The register
  check stays, as the backstop for a lost push. On by default: it adds no store request and nothing to a write's ack.
- **Searching writes before they are published (tail overlay).** On by default; turn it off per index with
  `index.serverless.tail_overlay: false`, for the cluster with `serverless.tail_overlay` in the cluster settings, or
  for a node with `serverless.search.tail_overlay.enabled=false`. The index setting wins over the cluster's, and the
  cluster's over the node's, and both are dynamic. A search node that opens a shard asks the shard's writer, over
  transport, for what it has acknowledged past the opened commit, and that subscribes it: the writer then pushes each
  write it acknowledges to the shard's subscribed search nodes, a couple of milliseconds after the acknowledgement,
  batched by node. The search node indexes them in memory and searches them together with its commit; a document
  the overlay replaces or deletes is hidden from the commit. So a write is searchable about a network hop after its
  acknowledgement, with no PUT, LIST or GET. Pushes go only to the search nodes serving the shard now, normally one or
  two, so the number of search nodes does not change the cost. A node that has let the shard go answers the next push
  "not wanted", and the writer stops. Roughly, a writer sends write bytes/s times its subscribed readers; links to
  another zone are compressed. A lost push, a document that needs a field the search node's mapping lacks, or an overlay
  past 50,000 documents leaves the search node serving its commit for those writes, never an older version than the
  commit's. Because searches no longer wait for publishes, a shard with overlays on whose index sets no
  `refresh_interval` is published after `serverless.search.tail_overlay.publish_max_age_millis` (10 seconds by default)
  rather than the node's one second, which cuts publish PUTs. A shard with overlays off keeps the node's age.
- **`refresh=true` and `refresh=wait_for` on search nodes.** Both spellings mean the same here: the writer refreshes
  its own copy, then waits until every search node serving the shard has taken the write into its overlay, at most
  500 ms, before it answers. A search node that opens the shard waits for the writer's answer to its fetch before it
  searches, so a node that joins or opens the shard after the write finds it too. `forced_refresh` is `true` only when
  that held: a node the push did not reach, a wait that ran out, or an index with the overlay off (whose search nodes see
  only commits) answers without it. Set `serverless.refresh_visibility` in the cluster settings, or
  `serverless.search.refresh_visibility` on a node, to `serving` for this, or to `owner` to refresh the writer's copy
  only, as before; the default is `everywhere`, below. The wait adds no store request and only the push a write would have had anyway, sent at once
  rather than after its 2 ms linger.

  `everywhere`, the default, keeps the stock promise on every node: a search that starts after a write was answered with
  `forced_refresh: true` finds the write, or reports that shard in `_shards.failures`; it never answers without it.
  A search node answers for a shard only while it holds a grant from the writer, good for 500 ms after the node last
  asked; a node without one asks first (one round trip), and one that still cannot be sure (a lost push, a gap, a
  document its mapping cannot take yet, the overlay off) answers only if its commit holds every write the writer has
  answered as refreshed, otherwise it refuses and the search asks the next copy, down to the writer. The writer, before it
  answers a refreshing write, waits out the grant of any node it stopped pushing to, and of any node an earlier writer
  pushed to. `true` and `wait_for` behave the same. One exception: a search node that cannot reach the writer at all
  treats it as dead and serves its commit, as it does for every acknowledged write until the shard is taken over, rather
  than fail every search of the shard for the rest of the writer's lease. Measured, it adds no store request and no write latency, and costs
  a search almost nothing on a shard searched continuously (about one round trip per 70 searches) but one round trip
  on a shard searched less often than every 500 ms (p50 about 3 ms more). Set `serverless.refresh_visibility: serving`
  to skip that round trip and promise only the search nodes serving the shard when the write is answered.

## Node roles and request routing

Every node runs the same binary. `serverless.roles` says what it holds:

| `serverless.roles` | Holds | Use |
|---|---|---|
| unset, or `ingest,search` | writers and readers | one small deployment, tests |
| `ingest` | writers: it takes a shard's head, indexes, appends to its node's write log, publishes | the ingest pool |
| `search` | readers: read-only copies opened from published commits | the search pool |
| empty (`-Dserverless.roles=`) | nothing | a coordinating tier, if you want one |

The roles are enforced, not only routed around. A node without `ingest` never takes a shard's head and only opens
shards with the read-only engine, so it cannot write. Only `ingest` nodes are picked to take over a dead writer's
shards. A node advertises its roles in its lease in the object store, which is how its peers find out, not through
Kubernetes. Any other value is refused at startup, so a misspelled role cannot start a node that serves nothing.

**Every node coordinates.** Whatever node a request reaches answers it:

- A **write** (`_doc`, `_create`, `_update`, `_bulk`, `_delete_by_query`) is grouped by shard on the node that
  received it. Each group goes over the transport to the shard's writer, named by the shard's head. A group the node
  writes itself costs no hop. A shard nobody holds is taken by the node that received the write if it takes writers;
  a search node or a coordinating node sends the group to the writer member it ranks first for that shard, and that
  member takes it. Every node ranks members the same way, so two nodes do not race to take one cold shard.
- A **search** goes to search nodes: to a reader already open on the receiving node, if it is a ready search node,
  otherwise to readers picked by rendezvous hashing over the leases, spread over more readers for a hot shard and
  kept in the receiving node's zone where it can be. The writer answers only when no search node can.
- A **get** is answered by the shard's writer, so it sees every acknowledged write.
- `refresh=true` and `refresh=wait_for` are settled between the writer and the search nodes serving the shard, so
  they hold whichever node the write and the later search reached.

So no proxy has to know where a shard is. What a front door can still do is keep write work off the search pool.
Writes sent to a search node are parsed there, pressure-accounted there and forwarded, so they take CPU and heap
from searches and add a hop for every write. A rule on the request path is enough to avoid that, with no knowledge
of shards: a write that lands on the wrong ingest node is forwarded once, and a request the rule misses still works.

### Two pools behind one ingress

Run the pools as two Deployments that differ in `serverless.roles`, each with its own Service. Send write paths to
the ingest Service and everything else to the search Service. The search Deployment is the one in
[Scaling search nodes without a cold cache](#scaling-search-nodes-without-a-cold-cache); the ingest Deployment is the
same without the warm-on-join and drain settings, with `-Dserverless.roles=ingest`.

```yaml
apiVersion: v1
kind: Service
metadata: { name: ingest }
spec:
  selector: { app: ingest }
  ports: [ { name: http, port: 9200, targetPort: http } ]
---
apiVersion: v1
kind: Service
metadata: { name: search }
spec:
  selector: { app: search }
  ports: [ { name: http, port: 9200, targetPort: http } ]
---
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: opensearch
  annotations:
    # ingress-nginx: regular-expression paths. Other controllers have their own way to match a path by pattern.
    nginx.ingress.kubernetes.io/use-regex: "true"
spec:
  ingressClassName: nginx
  rules:
    - http:
        paths:
          # Writes. A pattern for each write API; index names cannot contain '/', so [^/]+ is one index or a list.
          - path: /(_bulk|[^/]+/_bulk)$
            pathType: ImplementationSpecific
            backend: { service: { name: ingest, port: { name: http } } }
          - path: /[^/]+/(_doc|_create|_update)(/.*)?$
            pathType: ImplementationSpecific
            backend: { service: { name: ingest, port: { name: http } } }
          - path: /[^/]+/_delete_by_query$
            pathType: ImplementationSpecific
            backend: { service: { name: ingest, port: { name: http } } }
          # Everything else: searches, gets, index and cluster APIs.
          - path: /
            pathType: Prefix
            backend: { service: { name: search, port: { name: http } } }
```

Some points about the split:

- A path rule cannot tell `GET /idx/_doc/1`, a get, from `PUT /idx/_doc/1`, a write, and does not need to: a get
  sent to the ingest pool is forwarded to the shard's writer like any other. If your proxy can match the method too
  (an ALB listener rule, an Envoy route, an HAProxy ACL), send only `PUT`, `POST` and `DELETE` on those paths to
  ingest.
- `POST /_search` and the other read APIs use `POST` too, so never route on the method alone.
- Index creation, mappings and settings are written to the object store by whichever node receives them, so they
  can go to either pool.
- A coordinating tier (`serverless.roles` empty) is only worth running when the parsing and merging that coordinating
  costs should scale apart from both pools, for example many small searches over many shards each. It holds no
  shard, so it needs no cache and no drain. Put it behind the default route and keep the write rules pointing at
  ingest.
- On a node with both roles, a search can be answered by the writer's own copy when the receiving node holds it.
  That is fine for a small deployment and one more reason to split the pools when search load matters.
- Writes cross zones whenever the writer is in another zone than the client, because each shard has one writer. Only
  the compression of [Keeping traffic inside a zone](#keeping-traffic-inside-a-zone) helps there.

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

## Keeping traffic inside a zone

Traffic between availability zones is billed by the byte (on AWS, $0.01/GB in each direction; on GCP, about $0.01/GB),
and traffic inside a zone is not. Traffic to the object store in the same region is free from every zone. So the shell
keeps peer traffic inside a zone wherever there is a choice, and never makes it a rule: a peer in another zone is
still a peer.

**Each node declares its zone with the stock awareness attribute,** `node.attr.zone`. If your deployment names the
attribute differently, list it first in `cluster.routing.allocation.awareness.attributes`. The zone travels in the
node's lease. A node without one is in no zone, and is treated as neither near nor far.

With zones set:

- A search on a hot shard (at least `serverless.search.zone_local.min_per_second`, 1 by default, from one
  coordinator) goes first to a reader in the coordinator's zone. Each zone keeps its own warm reader for such a
  shard. A cold shard keeps its single reader wherever placement puts it. A zone's reader with
  `serverless.search.zone_local.spill_in_flight` (8) more searches in flight than placement's own choice gives way
  to it. Turn it off with `serverless.search.zone_local.enabled=false`.
- A joining node copies warm blocks only from peers in its zone, and reads the rest from the object store. A copy
  from another zone costs about 400 times the store reads it replaces. A drain hands over the same way.
  `serverless.search.warm_copy.cross_zone=true` restores copying from any zone.
- A dead writer's shards go to a survivor in its zone when one has room.
- Forwards to a peer in another zone are compressed, and so are their answers
  (`serverless.network.compress_cross_zone`, on by default). Forwards inside a zone are not compressed.
- `GET /_serverless/stats` reports `zone_traffic`: bytes sent to and read back from peers in the same zone, in
  another zone, or where a zone is unknown, with the cross-zone bytes broken down by action. Payload bytes are counted
  before compression; `transport_tx_bytes` is what went on the wire. Any `unknown_zone_bytes` means some node has no
  zone set.

### Setting the zone on Kubernetes

The zone is the node's `topology.kubernetes.io/zone` label on both EKS and GKE. A pod cannot read a node label
directly, so pass it in one of these ways:

- **One Deployment per zone** (works on any cluster). Pin each Deployment with
  `nodeSelector: { topology.kubernetes.io/zone: us-east-1a }` and set `-Dnode.attr.zone=us-east-1a` to match. Each
  zone then scales on its own HPA.
- **Topology labels copied onto pods.** Where the cluster copies node topology labels onto pods (the
  `PodTopologyLabelsAdmission` feature in newer Kubernetes releases), one Deployment can read the label through the
  downward API:

  ```yaml
  env:
    - name: NODE_ZONE
      valueFrom:
        fieldRef: { fieldPath: "metadata.labels['topology.kubernetes.io/zone']" }
    - name: OPENSEARCH_JAVA_OPTS
      value: "-Dserverless.roles=search -Dnode.attr.zone=$(NODE_ZONE)"
  ```

Spread pods evenly over zones either way, so each zone has readers and writers:

```yaml
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: topology.kubernetes.io/zone
    whenUnsatisfiable: ScheduleAnyway
    labelSelector:
      matchLabels: { app: search }
```

### Keeping client traffic in the client's zone

A client in one zone that reaches a pod in another pays the same rate as a peer does. For clients that reach the pods
through a Kubernetes Service (in-cluster clients, or an ingress controller in front of the Service), set
`trafficDistribution` on the Service. This works with kube-proxy on both EKS and GKE:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: search
spec:
  selector: { app: search }
  ports: [{ name: http, port: 9200, targetPort: http }]
  trafficDistribution: PreferSameZone   # PreferClose before Kubernetes 1.34
```

The Service still falls back to other zones when a zone has no ready pod.

### AWS (EKS)

- **S3 gateway endpoint (prerequisite).** Add a gateway VPC endpoint for S3 to the route tables of the node subnets.
  It is free. Without it, S3 traffic goes through a NAT gateway, which charges $0.045/GB processed, and at scale that
  costs more than every other line here together. Check with
  `aws ec2 describe-vpc-endpoints --filters Name=service-name,Values=com.amazonaws.<region>.s3`.
- **ALB.** Traffic from an ALB to targets in the same VPC is not billed across zones, but traffic from a client to an
  ALB node in another zone is. In-region clients reach every zone's ALB nodes, so about two thirds of that hop still
  crosses. With the AWS Load Balancer Controller, use IP targets and turn cross-zone off on the target group
  (`alb.ingress.kubernetes.io/target-group-attributes: load_balancing.cross_zone.enabled=false`), so each ALB node
  sends only to pods in its own zone.
- **NLB.** Cross-zone load balancing is off by default; keep it off. Point each client at the NLB's zonal DNS name
  for its own zone (`<zone>.<nlb-dns-name>`), so neither hop crosses.
- **Direct to the Service.** Use `trafficDistribution` as shown above.

### GCP (GKE)

- **GCS without Cloud NAT (prerequisite).** Turn on Private Google Access for the node subnet, so GCS traffic does not
  go through Cloud NAT, which charges per GB processed. Reads from a GCS bucket in the same region as the nodes are not
  billed for egress. Keep the bucket in the cluster's region, not multi-region.
- **Load balancers.** A GKE internal passthrough Network Load Balancer, or container-native load balancing through
  NEGs, sends to pods in any zone. For in-region clients, prefer in-cluster access through the Service with
  `trafficDistribution`, or check your load balancer's zonal affinity option.
- **Direct to the Service.** Use `trafficDistribution` as shown above.

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

## Write threads

A write is acknowledged once its node-log window lands, so its thread mostly waits. On core's `write` pool (one thread per
core, plus one at most) that waiting capped REST single-document writes at about 16 a second on a 4-core node, whatever
the document size, with the rest queued for seconds. `serverless.write.wait_pool.enabled` (on by default) runs REST
writes, bulk shard groups and updates on `serverless_write`, and forwarded writes on `serverless_forwarded_write`, both
with `serverless.write.wait_pool.threads_per_core` threads per core (64 by default). The engine apply itself still goes
through a gate of cores + 1 permits, as many as the old pool had threads, so CPU use and the number of Lucene indexing
threads per shard (hence segments and publish PUTs) stay as they were. With the switch on, the write limiter's default
`serverless.write.backpressure.max_in_flight` grows with the cores: 384 per core, never below 1,024.

Measured through REST on 4 cores (`LargeDocumentMeterTests`, `-Dtests.bigdoc.rest_seconds=30`, filesystem store), off
then on: 1 KB documents at 40/s offered, 15.6/s acked at p50 24 s, then 39.6/s at p50 159 / p99 265 ms; at 600/s,
15.9/s with most refused or failed, then 600/s at p50 144 / p99 276 ms; the page mix at 100/s, 14.8/s at p50 60 s,
then 99.4/s with 4 KB pages at p50 156 / p99 304 ms. See `large-document-results.md`.

## Open files per node

Each open writer shard holds one file descriptor, Lucene's `write.lock`: a writer keeps no local translog, since an
acknowledged write is durable in the node log (`serverless.translog.local: true` brings core's disk translog back, and
two more descriptors per writer). Segment files hold none, since reads go through the block cache. A hibernated or released writer
holds none. Sockets, the node's own files and the JVM come on top, a few hundred in all.

Deploy with the open-file limit at 1,048,576 (`nofile`, soft and hard). It is the most Linux allows a process by
default (`fs.nr_open`), each descriptor costs the kernel about a kilobyte, and even at three per writer it fits about
330,000 open writers, far past where heap, open time and takeover time stop a node first.

- **Kubernetes** has no pod-level setting: the container runtime sets the limit. With containerd, set
  `LimitNOFILE=1048576` in its systemd unit (EKS and GKE node images usually already do), or the runtime's default
  `rlimits` in a custom base spec. With Docker, `--ulimit nofile=1048576:1048576` or `default-ulimits` in
  `daemon.json`. Check from inside the pod with `ulimit -n` and `ulimit -Hn`.
- **The node warns at startup** when the limit is below three per writer it may hold
  (`serverless.activation.max_shards`) plus 10,000 for everything else.
- **Tests and soaks** run with the limit raised as far as the machine's hard limit allows before the Gradle daemon
  starts (`ulimit -n`, then `./gradlew --stop`). Lucene's test filesystem caps a test JVM at 2,048 open handles
  regardless (`HandleLimitFS`); a test that opens more than about 600 writers suppresses it, as
  `WriterHibernationMeterTests` does.

## Hibernating idle writers

`serverless.activation.hibernate_after_millis` (off by default, `0`) puts a writer to sleep once it has gone unused
that long: it publishes what it holds, puts the commit's small files into the block cache from its own copies, closes
its engine and keeps its head. The next write, search or forwarded write for it reads the head once and, if it still
names this process at the same term, opens it at that term without a takeover. A head that moved while the writer
slept is taken the ordinary way. A sleeping writer's head is given back after `serverless.activation.idle_after_millis`,
as an open one's is. A clean shutdown gives the heads back.

Measured by `WriterHibernationMeterTests` (single-shard indices with a 24-field mapping, a 30 s backstop, the
bootstrapped head check of an hour, 20 ms store latency):

| | open | asleep | released |
|---|---|---|---|
| heap per writer (1,000 writers) | 144 KB | 25 KB | 21 KB |
| file descriptors per writer | 3 | 0 | 0 |
| idle head reads per writer-hour | ~1 | ~0 | ~0 |
| going idle: store requests | | 5 (sleep) | ~4-5 (give back) |
| first write: open time, store requests | | 230 ms, 8 | 253 ms, 12 |

An open idle writer's head is read once an hour by the periodic head check, which was every five minutes: a taker
revokes the owner's lease before taking a shard, and the owner's next renewal fails on it and re-reads every head at
once, so the scan is only a backstop. Resyncs carry open writers over without reading their heads or descriptors again,
when that check last read every head without a failure within two of its intervals and saw the head naming this node
at the writer's term. The descriptor refresh of open indices (`serverless.descriptors.refresh_interval_millis`, five
minutes) still reads each open index's descriptor; the meter does not count it, since it runs on the monotonic clock,
so add about 12 reads per open index-hour.

Sleeping and waking (5 + 8) costs less than giving the head back and taking it back (about 17), but only for an idle
gap that ends before the head is given back at `idle_after_millis`; beyond that it costs the give-back on top. With the
5 minute idle release on by default, a shorter `hibernate_after_millis` only covers gaps under 5 minutes, where it
adds requests; so hibernation stays off by default. Setting `hibernate_after_millis` to 5 minutes and
`idle_after_millis` to, say, 30 minutes trades about 4 requests and 20-80 ms saved on each 5-30 minute gap for about 4
requests more on each longer one, at released heap and descriptors. The cache warm before closing fills only free
cache room, so it never evicts a block in use and never adds heap.
