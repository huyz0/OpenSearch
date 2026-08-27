# Phase 3 — the metadata plane

- Code: `serverless/shell/src/main/java/org/opensearch/serverless/metadata/`
- Tests: `MetadataPlaneTests` (11), `ServerlessEndToEndTests` (3)
- Run: `./gradlew :serverless:testkit:test -Dbuild.docker=false`
- Result: **40 tests, 0 failures** across the whole serverless tree; `check` green on both projects.

## What works

An index is created in the object store and a node serves it, with nothing telling the node what to do:

```java
plane.createIndex(new IndexDescriptor("alpha", uuid, 1, mapping, null));   // one put-if-absent
plane.activate("alpha", 0, nodeId, ephemeralId);                          // one compare-and-swap
node.syncFrom(plane);                                                     // node reads and works it out
```

That last line is what replaces cluster-state publication.

## The register map (§9.3)

| Register | Container | Writers | Write rate |
|---|---|---|---|
| `cluster/config` | — | operators | human-scale |
| `cluster/members/lease-<nodeId>` | `cluster/members` | that node only | one per TTL |
| `indices/<name>` | `indices` | index lifecycle | rare, per index |
| `shards/<index>#<id>` | `shards` | activation, failover | rare, per shard |

**No register for the routing table or the membership list.** Both are derived — membership by listing
live leases, routing by reading shard-heads. Nothing agrees on them and nothing needs to.

One implementation note: prefixes are `BlobPath` segments, not slashes inside blob names.
`FsBlobContainer` resolves names flatly and globs within a single directory, so a name containing a
slash would neither write nor list. The logical layout is unchanged; only who holds the separator moved.

## Semantics that are tested, not assumed

- **Creation is put-if-absent**, so name uniqueness is arbitrated by the object store rather than by a
  cluster-manager. A second create of the same name throws.
- **A live lease is never stolen.** A contender is refused *and told who won*, so it routes to the
  winner rather than retrying elsewhere — the ping-pong `rfc-serverless-metadata-plane.md` §6 warns
  about. `Acquisition` returns the winner's head precisely so the correct behaviour is easier to write
  than the wrong one.
- **An expired lease is acquirable and bumps the term.** Failover needs no coordinator.
- **Renewal does not bump the term.** A term bump means ownership changed; anything downstream treating
  it as a fence would be wrong to see one where nothing moved.
- **Truth carries the head's term**, never a per-node counter — the hazard S1 reproduced.
- **`truthFor(node)` returns only what that node hosts.** A node owning nothing learns nothing, which is
  the residency property the whole design exists for.
- **Delete removes heads before the descriptor.** The other order leaves heads no reader can enumerate,
  since the shard count lives in the descriptor.

## The two-input rule, made explicit in the API

Phase 2 established that absence from a projected *view* is never a removal signal. Phase 3 adds a
method that does close shards, so the distinction is now carried by the argument types:

| Method | Input | May close? |
|---|---|---|
| `applyTruth(descriptors, assignments)` | a projected view | **never** |
| `syncFrom(plane)` | shard-heads | yes |

Closing in `syncFrom` is not a violation: if truth no longer assigns a shard here, another node has
already won it by compare-and-swap, and continuing to hold it is the actual hazard. The comment at the
call site says so, because the two methods otherwise look interchangeable.

## Canaries — both safety claims verified by making them fail

Eleven tests passing first run is when to trust them least, so:

| Canary planted | Result |
|---|---|
| Ignore the create CAS result | `exactly one winner expected:<1> but was:<7>` |
| Remove the live-lease guard | `a live lease must not be stolen` |

Both removed afterwards, suite re-verified green. The concurrency test genuinely detects a broken CAS
rather than passing because contention never happened.

## What phase 3 does NOT establish

- **Anything about S3, GCS or Azure.** D5 defers R11, and everything here ran on `FsBlobContainer`,
  whose CAS is real local-filesystem atomicity. That is not evidence about a provider's conditional
  write under contention, and the entire safety argument rests on it. **No durability claim outside Fs.**
- **Data failover.** The failover test moves shard *ownership*, not shard *data*: each node has its own
  `path.home` and recovers from its own empty store, so the winner serves an empty shard. Moving bytes
  needs the object-store engine — phases 4 and 5. The test asserts term, ownership and that the loser
  lets go, and deliberately does not assert a document count across the handover.
- **Fencing (R9).** `ShardHead` carries a term, but nothing yet uses it to make a zombie writer's bytes
  inert. Term-scoped key paths (§9.6) are phase 6. The class documentation says so rather than letting
  the presence of a term imply a fence.
- **`cluster/config`.** Named in the register map, not yet implemented; nothing needs it before phase 7.
- **Scale.** Two nodes, two indices, a handful of shards.

## Next

Phase 4 — the write path: `ingest` role, bulk indexing through the reused `TransportShardBulkAction`,
writer engine, and WAL and segment publication to the object store. That is where R9 becomes reachable
and where the failover test above can finally assert a document count.
