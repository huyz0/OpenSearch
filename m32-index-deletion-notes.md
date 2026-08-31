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

**Fixed at creation, not at deletion**, and the choice is the whole point. Deletion now purges the data too,
but a delete can be interrupted — a process killed between removing the descriptor and removing the bytes
leaves exactly the state that produced the output above. Clearing the path when an index is *created* makes
the emptiness of a new index a property of creation, which either happened or did not.

It is safe against a concurrent writer by construction: writing needs a shard-head, taking a head needs the
descriptor, and the clear runs after the descriptor's put-if-absent — so any writer that could reach that
path is a writer for *this* index.

### The first test could not tell the difference

`testARecreatedIndexIsEmpty` passed with the create-time clear removed, because the delete had already
purged the data and creation had nothing left to clear. The canary said so.

`testAnIndexCreatedOverTheRemainsOfAnInterruptedDeleteIsEmpty` copies the shard's directory aside, deletes
the index, copies it back, and then creates the index again — reconstructing the interrupted delete exactly.
That one fails without the clear, which is what makes the clear justified rather than decorative.

## A deleted index went on costing storage

44,025 bytes before the delete, 43,877 after. Nothing removed an index's segments, manifests or log —
`WalStore.deleteAll()` had been written for this and was called by nobody.

Deletion now purges the shard data after removing the heads. Heads first, so a writer cannot renew and the
node holding it closes the shard on its next tick; data after.

**The residual race is real and bounded.** Publishing is fenced by the manifest register's term rather than
by the head, so a writer that has lost its head can still finish a publish it had already begun and leave
blobs behind the sweep. They are unreachable — no descriptor, no head — and the next index of that name
clears them on creation. Which is, again, why the correctness argument lives at creation.

## Creating an index now costs two operations, not one

`testCreationCostIsFlatInThePopulation` caught this immediately, which is what it is for. A create is the
descriptor's put-if-absent plus one container clear per shard.

The architectural claim it guards is unchanged: **creation cost does not depend on how many indices already
exist.** What changed is the constant, and it doubled on the cheapest operation in the system. That is worth
stating rather than absorbing quietly.

The alternative that keeps a create at one operation is **keying storage by index uuid rather than by
name** — then a recreated index has a different path and needs no clearing, and the remains of an
interrupted delete are unreferenced garbage under a dead uuid rather than a correctness problem. That is
the better design and it is a change to the on-disk layout: every path helper, every caller, and the
garbage collector's notion of what an orphan is. It belongs to its own milestone, and this one records why
it is wanted rather than half-doing it.

## What was already right

A node holding a writer for a deleted index closes the shard on its next tick, because the index is no
longer in the truth it reconciles against. That test passed on the first run and is kept, because "already
correct" is only knowable once something asserts it.

## Canaries

| Defect | Caught by |
| --- | --- |
| Creating an index does not clear its path | `testAnIndexCreatedOverTheRemainsOfAnInterruptedDeleteIsEmpty` |
| Deleting an index leaves its bytes behind | `testADeletedIndexStopsCostingStorage` |

## What is still missing

- **Storage keyed by uuid**, as above — the change that would make this structural rather than swept.
- **No sweep for orphans**, so bytes left by the residual publish race are reclaimed only if the name is
  reused. A garbage collector that could enumerate live indices and delete unreferenced shard containers
  would close it; enumeration is allowed offline and forbidden on the request path (§6.3).
- **Deletion is not fenced against a live writer**, only raced with. Making a publish check the head would
  close that and would put an extra register read on the publish path.

263 tests green across `test` (228), `pluginTest` (2), `processTest` (13) and `s3Test` (20), none skipped,
MinIO live. `server/` untouched.
