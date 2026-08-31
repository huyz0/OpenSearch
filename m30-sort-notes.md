# M30 — Sort, which was one comparator away

`sort` has been answering 501 since M19, with the reason recorded in the code: *"hits from several shards
are merged by score, and a custom sort would need its own merge"*. That was accurate and it made the gap
sound bigger than it was.

**Every shard could already sort itself.** A shard's query goes through the same `SearchService` a classic
node uses, so a sorted request has always come back as Lucene's `TopFieldDocs`, with each hit's sort keys on
its `FieldDoc`. The missing half was on the coordinating node: it dropped those keys and merged by score.

Two changes, and neither is large:

**The shard attaches its sort keys to its hits.** `SearchHit` carries `SearchSortValues` and serialises them
itself, so once attached they travel to the coordinator whether the shard was local or on another node.
Without this the coordinator had nothing to merge on but score, which for a sorted query is the wrong key
entirely.

**The coordinator merges on them when the request asked for a sort.** Which way round each key runs comes
from the request rather than the hits — a hit carries its sort values and no idea whether smaller means
earlier.

## Deciding what "missing" means

Two orderings had to be chosen rather than inherited:

- **A hit with no sort values sorts last**, exactly as an unscored hit already did. A shard that answered
  without them — an older build, a field absent from one shard's mapping — must not be able to take the top
  of the page from one that answered properly. The failure that prevents is a page which silently begins in
  the wrong place.
- **A `null` value within the keys sorts last**, which is Lucene's own convention.

And one that should never happen: if two shards return different *types* for the same key, they disagree
about the field. Ordering them by their rendered form is arbitrary but stable, which beats an exception
raised in the middle of a merge over a page a caller is waiting for.

## The fixture is the argument

Three shards, and ten documents whose ranks are deliberately unrelated to their ids so that the routing
which scatters them cannot accidentally agree with the sort order. The expected sequence interleaves
shards, so a merge that concatenated them, or ranked them by score, or dropped the keys coming back from a
remote shard, produces a visibly different answer.

A single-shard fixture would have passed with the entire merge deleted. So would a fixture whose documents
happened to land in sorted order.

**Paging is asserted separately** — `from: 4, size: 3` must be the fifth, sixth and seventh documents
*overall*. That is the assertion that catches a merge which sorted correctly and then cut the page before
combining, which is the same bug M19 found in the unsorted path and worth not repeating.

**And one test puts the shards on two nodes**, one holding a shard and its peer holding the other two, so
most of the answer arrives over the transport. Sort keys crossing the wire inside a serialised `SearchHit`
is the half a single-node test cannot reach, and if they did not survive it, two thirds of the hits would
sort last and the order would be wrong.

## Canaries

| Defect | Caught by |
| --- | --- |
| Sorted searches are still merged by score | 3 tests |
| Sort values are never attached to hits | 3 tests |

## What this does not do

- **`search_after` is still refused**, and is now asserted to be — the same test that used to assert sort
  was refused now asserts sort works and `search_after` does not. Deep paging needs the sort keys of the
  last hit to become a per-shard cursor, which is a different mechanism from merging a window.
- **Aggregations are still refused**, and remain the largest gap in the search surface. They need a reduce
  phase, not a comparator.
- **`collapse` and `suggest` are still refused.**
- **Sorting by a field one shard does not have** works in the sense that its hits sort last, which is a
  defensible answer and not the one a classic node gives.

250 tests green across `test` (219), `pluginTest` (2), `processTest` (13) and `s3Test` (20), none skipped,
MinIO live. `server/` untouched.
