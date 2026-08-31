# M32 — What deleting an index actually deleted

Deleting an index removed its descriptor and its shard-heads, which is enough for it to stop being
routable. Three questions that answer does not cover turned out to have two bad answers.

## A recreated index served the deleted one's documents

An index's storage is keyed by its **name**. So creating an index called `reused` after deleting an index
called `reused` opened the old commit and returned the old documents:

```
a freshly created index must be empty, whatever its name was used for before:
{"hits":{"total":{"value":1},"hits":[{"_index":"reused","_id":"1","_source":{"msg":"the first index"}}]}}
```

That is not a leak. It is a caller creating an empty index, searching it, and being shown data.

**Fixed in the path.** A shard's storage is now keyed by the index's **uuid** as well as its name —
`segments/{name}#{uuid}#{shard}` — and a uuid is minted per create. Two indices that share a name cannot
share a path, however the earlier one ended, so a new index is empty because there is nowhere for it to
inherit from rather than because a cleanup finished.

The name stays in front of the uuid because an operator looking at a bucket should be able to tell what
they are looking at.

### It was first fixed at creation, and that was worse

The first attempt cleared the storage path when an index was created. It worked, and it cost a second
object-store operation on the cheapest operation in the system — which `testCreationCostIsFlatInThePopulation`
reported immediately, as it is there to do. Keying by uuid gives the same guarantee for nothing, and the
cost assertion is back to one write.

That the wrong version shipped for an hour is the argument for having a cost test at all: it is not a
performance regression suite, it is a design review that runs.

### The first test could not tell the difference

`testARecreatedIndexIsEmpty` passed with the create-time clear removed, because the delete had already
purged the data and creation had nothing left to clear. The canary said so.

`testAnIndexCreatedOverTheRemainsOfAnInterruptedDeleteIsEmpty` copies the shard's directory aside, deletes
the index, copies it back, and creates the index again — reconstructing the interrupted delete exactly. It
fails if storage is keyed by name alone, which is the canary that keeps the uuid in the path.

## A deleted index went on costing storage

44,025 bytes before the delete, 43,877 after. Nothing removed an index's segments, manifests or log —
`WalStore.deleteAll()` had been written for this and was called by nobody.

Deletion now purges the shard data after removing the heads. Heads first, so a writer cannot renew and the
node holding it closes the shard on its next tick; data after.

## The residual, and where it now lands

Publishing is fenced by the manifest register's term rather than by the head, so a writer that has lost its
head can still finish a publish it had already begun and leave blobs behind the delete's sweep. Under
name-keyed storage those blobs were a correctness problem, waiting for somebody to reuse the name. Under
uuid-keyed storage they are unreferenced bytes under a dead uuid: a leak, and only a leak.

A sweep that enumerates live indices and deletes shard containers whose uuid is not among them would
reclaim them. Enumeration is allowed offline and refused on the request path (§6.3), so that belongs to the
garbage collector.

## What was already right

A node holding a writer for a deleted index closes the shard on its next tick, because the index is no
longer in the truth it reconciles against. That test passed on the first run and is kept, because "already
correct" is only knowable once something asserts it.

## Canaries

| Defect | Caught by |
| --- | --- |
| Storage is keyed by name again, not by uuid | `testAnIndexCreatedOverTheRemainsOfAnInterruptedDeleteIsEmpty` |
| Deleting an index leaves its bytes behind | `testADeletedIndexStopsCostingStorage` |

## What is still missing

- **No sweep for orphans.** Bytes left by the residual publish race are never reclaimed now, because
  nothing will ever reuse that uuid. The garbage collector is where that belongs.
- **Deletion is not fenced against a live writer**, only raced with. Making a publish check the head would
  close it and would put an extra register read on the publish path.
- **No migration.** This changes where shards are stored, so an existing deployment's data is not found by
  a node running this build. Pre-release, and stated rather than discovered.

263 tests green across `test` (228), `pluginTest` (2), `processTest` (13) and `s3Test` (20), none skipped,
MinIO live. `server/` untouched.
