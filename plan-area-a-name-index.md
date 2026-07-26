# Area A task plan: name index tier

Parent: `plan-100m-index-implementation.md` Part 5, Area A. Evidence: S13 in
`benchmarks/SCALABLE_METADATA_SPIKE_RESULTS.md`.

Package: `org.opensearch.serverless.storage.nameindex` in the serverless-storage plugin. It lives in the
plugin rather than core because it is a serverless-only concern and core has no notion of a partitioned
name space.

## Why this area is first

It retires the highest-risk unknown in the architecture. Everything else scales by hashing on the index,
but wildcard and alias resolution is global by construction: answering `logs-*` requires knowing every
name. The fallbacks were scattering every wildcard to every partition, or restricting the query language.
S13 showed a compact structure holds 100M names in 4.2 GiB with prefix cost proportional to matches
rather than to index count, so neither fallback is needed. Nothing is built.

It also has no dependencies on any other area and does not depend on the fork decision.

## Design shape

Three layers, each independently testable:

1. **`CompactNameIndex`** is immutable, sorted, and holds the bulk. Names concatenated into one
   `byte[]` addressed by an offset array; UUIDs as raw 16-byte values rather than 36-character strings;
   one status byte per entry. No per-entry object, which is where the 145.5 B to 45.0 B saving comes
   from.
2. **`NameIndexOverlay`** is small and mutable, holding creates and deletes since the last rebuild.
   Deletes are tombstones, because the base is immutable and cannot have entries removed.
3. **`NameIndex`** composes the two and is the only public entry point. Reads merge base and overlay;
   writes go to the overlay; a rebuild folds the overlay into a new base and swaps atomically.

Aliases are deferred to a second pass (A9 onward) so the index-name path can be finished and tested
first.

## Tasks

Each task is done when it compiles, has tests, the tests have been shown to fail without the production
change, and the full affected suite passes.

### Phase 1: the immutable base

**A1. `IndexNameEntry`.** Value type returned by lookups: name, UUID as a 16-byte array, status byte.
Small and immutable. Needed so callers are not handed raw ordinals into a structure that may be swapped
underneath them.

**A2. `CompactNameIndex`.** The immutable sorted structure.
- Fields: `byte[] nameBlob`, `int[] offsets` (length size+1), `byte[] uuids` (16 per entry), `byte[]
  statuses`.
- `int size()`, `int ordinalOf(String name)` returning a negative insertion point when absent (the
  `Arrays.binarySearch` convention, so callers can use it for range scans without a second search).
- `String nameAt(int)`, `byte[] uuidAt(int)`, `byte statusAt(int)`, `IndexNameEntry entryAt(int)`.
- `int lowerBound(byte[] prefix)`: first ordinal at or after the prefix. Separate from `ordinalOf`
  because prefix scans need it and exact lookup does not.
- UTF-8 byte comparison throughout, unsigned. Java `String.compareTo` is UTF-16 code-unit order, which
  differs from UTF-8 byte order above the BMP, and mixing the two silently breaks binary search on
  non-ASCII names.
- `long ramBytesUsed()` so the sizing claim can be asserted rather than assumed.

**A3. `CompactNameIndexBuilder`.** Accumulates entries and packs them.
- `add(String name, byte[] uuid, byte status)`, `build()`.
- Sorts by UTF-8 bytes at build time. Rejects duplicates, since two entries with the same name make
  `ordinalOf` ambiguous and the caller almost certainly has a bug.
- Sizes the blob in one pass before allocating, so a 100M build does not repeatedly grow an array.

**A4. Prefix and pattern iteration on the base.**
- `forEachWithPrefix(String prefix, IntConsumer)`: `lowerBound` then scan forward while the prefix
  matches.
- Streaming rather than returning a collection. A wildcard matching 10M indices must not materialize
  10M results, which is the difference between this being usable at 100M and not.

**A5. Glob matching.** Real patterns are not bare prefixes: `logs-*-2024` and `*` both occur.
- `literalPrefixOf(String pattern)`: the leading run before the first `*` or `?`.
- `forEachMatching(String pattern, IntConsumer)`: seek by literal prefix, then apply full glob matching
  to candidates. A pattern with no leading literal degenerates to a full scan, which A11 addresses.
- Match `*` and `?` only. OpenSearch index patterns do not use regex, and supporting more would be a
  liability.

### Phase 2: mutability

**A6. `NameIndexOverlay`.** Creates and deletes since the last rebuild.
- `put(name, uuid, status)`, `delete(name)`, `size()`, `isEmpty()`.
- Deletes are tombstones. The base is immutable so an entry cannot be removed from it; the overlay must
  be able to say "this name is gone" about a name it does not otherwise contain.
- A create following a delete of the same name must resolve to the create, and the reverse must resolve
  to the delete. Ordering within the overlay is the thing most likely to be got wrong.

**A7. `NameIndex`.** The composite and the public API.
- `IndexNameEntry lookup(String name)`: overlay first (including tombstones), then base.
- `forEachMatching(String pattern, Consumer<IndexNameEntry>)`: merged iteration over base and overlay,
  skipping base entries the overlay has tombstoned, and including overlay creates that match. Results in
  sorted order, since callers resolving wildcards expect determinism.
- `create(...)`, `delete(...)` writing to the overlay.
- Concurrency: readers must not block on a rebuild. Hold base and overlay in a single immutable holder
  swapped by one reference assignment, so a reader sees a consistent pair or the previous consistent
  pair, never a mix.

**A8. Rebuild and swap.**
- `boolean shouldRebuild()`: overlay size against base size, plus an absolute floor so a small index
  does not rebuild constantly.
- `rebuild()`: merge base and overlay into a new base, swap, clear the overlay.
- Peak memory during rebuild holds two bases at once. Measure it and state it, because at 4.2 GiB the
  transient is 8.4 GiB and that is a capacity planning input, not a detail.

### Phase 3: aliases

**A9. Alias representation.** An alias is a name pointing at a set of indices, so it shares the name
space with indices and needs a type distinction plus a target list.
- Extend the status byte to carry a type bit, or add a parallel type array. Decide based on measured
  cost.
- Targets as an `int[]` of base ordinals with a per-alias offset array, mirroring the name blob layout.

**A10. Alias resolution and fan-out measurement.** S13 explicitly did not measure this. A
tenant-per-index deployment may have aliases over very large index sets, and resolution cost is unknown.
Measure before designing around it.

### Phase 4: the open question

**A11. Leading wildcards.** `*-logs` has no literal prefix and degenerates to a full scan of 100M names.
Three options, and the choice needs a measurement rather than an opinion:
- (a) a second structure over reversed names, roughly doubling memory to 8.4 GiB;
- (b) reject leading wildcards with a clear error;
- (c) accept the scan, bounded by a timeout.
Build (a) as a measurement first, then decide and record why.

### Phase 5: making it a service

Deferred until phases 1 to 4 are done and reviewed, because their outcome changes the shape:

**A12. Persistence and bootstrap.** Rebuild from the manifest on start, or checkpoint to object storage.
Measure manifest rebuild time at scale before choosing.

**A13. Transport action and service wrapper.** Full-copy replication, single writer applying updates.

**A14. Coordinator integration.** Route wildcard and alias resolution here instead of to
`Metadata.indicesLookup`.

## Acceptance criteria for the area

- 100M names within 5 GiB, asserted through `ramBytesUsed()` and verified by a scaled test.
- Exact lookup under 10 us.
- Prefix and glob resolution cost proportional to match count, not to index count.
- Creates and deletes visible to resolution immediately.
- Sorted, deterministic result order.
- Every claim above has a test that fails when the corresponding mechanism is disabled.

## Known risks

- **The update path is the hard part, not the read path.** S13 proved the read structure. Insertion into
  a sorted blob is not proven, and the overlay-plus-rebuild pattern has a memory spike at swap.
- **UTF-8 against UTF-16 ordering.** Mixing `String.compareTo` with byte comparison breaks binary search
  on non-ASCII names, silently and only for some inputs.
- **Alias fan-out is unmeasured.**
- **Consistency model must be stated explicitly**, not discovered. If a create is not instantly visible
  to a concurrent wildcard, say so.
