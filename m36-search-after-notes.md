# M36 — `search_after`, and a branch that could not fail

Deep paging is how anybody exports a result set, and `from`/`size` is the wrong tool for it: page one
thousand of a three-shard search makes every shard produce and discard everything before the window. A
cursor makes page one thousand cost what page one costs.

`search_after` was refused with a one-line reason and no explanation. It works now, and it needed almost no
code — a shard applies the cursor itself, because its half of the search runs through the same
`SearchService` a classic node uses, and M30 had already taught the coordinator to merge on sort keys.

## What is refused, and why each refusal is a real one

- **Without a sort.** There is no order for "after" to be a position in.
- **Beside `from`.** The cursor *is* the offset; a request carrying both means two different things at
  once, and OpenSearch's own answer is the same refusal.
- **With the wrong number of values.** A cursor has one value per sort key. Fewer or more is a caller who
  changed the sort and not the cursor, and the useful answer is to say so rather than to page from
  somewhere arbitrary.

## The test walks the whole set

Twelve documents over three shards, in pages of three, following the cursor — and the concatenation must be
the same sequence a single large page gives, with nothing repeated and nothing skipped. Boundaries are
where a cursor goes wrong, and with three shards every page boundary falls between documents that came from
different shards.

## A branch that could not fail

The first version also changed how much each shard is asked for: with a cursor, every hit a shard returns
is already past it, so a shard need only offer its own next page rather than `from + size`.

**The canary for it passed.** Reverting the branch broke nothing — because `search_after` and `from` are
refused together, so `from` is always zero when a cursor is present, and `from + size` already *is* `size`.
The branch was dead code with a comment claiming it did something.

It is deleted, and the reason it was unnecessary is now a comment where the branch was. This is the
discipline working in the direction it is usually not needed for: a canary that cannot fail normally means
the test is weak, and this time it meant the code was.

## Canaries

| Defect | Caught by |
| --- | --- |
| `search_after` is accepted without a sort | `testSearchAfterIsRefusedWhereItCannotMeanAnything` |
| A cursored search still asks each shard for `from + size` | **nothing — the branch was dead and was deleted** |

## What is still missing

- **No point-in-time.** A cursor over a shifting index can miss or repeat documents that were written
  between pages, exactly as it can on a classic node without a PIT. There is nothing here to pin a reader
  across requests.
- **`collapse`, `suggest` and `profile`** remain refused, and are now the whole of what the search surface
  declines.
- **Aliases and `ignore_unavailable`** remain absent.

278 tests green across `test` (243), `pluginTest` (2) and `processTest` (13), none skipped.
