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

## A11 decided: build the reversed-name index

Measured by `LeadingWildcardSpikeTests` over 200,000 names, comparing a full scan with glob matching
against a seek into a second structure holding reversed names:

| query | full scan | reversed seek | speedup |
|---|---|---|---|
| `*-logs`, 10,000 matches | 36,400 us | 1,142 us | 31.9x |
| `*-nothing-matches-this`, 0 matches | 34,952 us | 301 us | 116x |

Memory for both structures together is 2.00x one of them, as expected.

**Decision: option (a), the reversed index.** Option (c), accepting the scan with a timeout, does not
survive extrapolation. 100M names is 500 times this population and the scan is linear in it, so a single
leading wildcard costs about **18 seconds**. That is not a slow query, it is an outage for whoever runs
it, and a timeout converts it into a feature that never works rather than one that works slowly.

Option (b), rejecting leading wildcards, is defensible but unnecessary once the cost is known: 4.2 GiB
becomes 8.4 GiB, which is still one node.

The miss case is the clearest argument. A leading wildcard matching nothing costs a full pass on the
forward structure and 301 us on the reversed one, because the reversed seek's cost tracks matches while
the scan's tracks population. That gap widens with every index added.

**Follow-on tasks this creates:**

- **A11a.** Second `CompactNameIndex` over reversed names, built alongside the forward one.
- **A11b.** Route a pattern by shape: literal prefix to the forward structure, literal suffix to the
  reversed one, and both wildcarded (`*-mid-*`) to whichever side has a longer literal run.
- **A11c.** Keep the two in step. The overlay and rebuild currently know about one structure; both have
  to be updated together or a create is visible to prefix queries and invisible to suffix ones.
- **A11d.** Decide what a pattern with no literal run at all (`*`, `?x?`) does. It has no seekable
  anchor in either direction and is a genuine full scan; `*` alone is common enough to special-case as
  "everything" rather than as a pattern.

## Review pass after phases 1 to 4

A deliberate re-read of the committed, green code found one blocking defect and one performance defect
that the tests could not have caught, because both only appear at populations no unit test builds.

### A15: the name blob could not hold 100M names (done)

The blob was a single `byte[]` addressed by `int` offsets. A Java array cannot exceed
`Integer.MAX_VALUE` elements, about 2.15 GB, and 100M names at a realistic 30 to 40 bytes each is 3 to
4 GB of text. **The structure could not hold the population it was designed for.**

What makes it worth recording rather than quietly fixing: S13's synthetic names were 21 bytes, so 100M
of them total 2.10 GB and land 2% under the limit. The benchmark that justified the entire design would
have passed at full scale while any real deployment failed. The builder's guard threw rather than
corrupting, so it was a hard failure rather than silent truncation, but the target was unreachable.

Fixed by chunking the blob, with names never split across a boundary. `CHUNK_SIZE` is deliberately not
final so a test can shrink it and cross a boundary; building a real 1 GB chunk in a unit test is not
viable, and chunking that is never exercised is chunking that does not work. Reverting the chunk-end
calculation fails seven of the new tests and none of the 58 that existed before, which is the measure of
how blind the original suite was to this.

### A16: the overlay was scanned linearly on every pattern query (done)

`NameIndex.forEachMatching` walks every overlay entry to find matches. That is fine while the overlay is
small, and the rebuild policy is what keeps it small, but the policy allows it to reach
`DEFAULT_REBUILD_RATIO` of the base before folding. At 100M that is **5M entries scanned per wildcard
query**, on the order of hundreds of milliseconds, for a structure whose entire purpose is that cost
tracks matches rather than population.

The fix is to hold the overlay in a sorted structure, `ConcurrentSkipListMap` keyed by UTF-8 byte order,
so a prefix query becomes a range scan. That also removes the sort previously done per query on the
matched subset. Done: the overlay is now a `ConcurrentSkipListMap` in UTF-8 byte order, and
`forEachMatching` walks `putsFrom(prefix)` and stops at the first name that leaves the prefix.
Asserted structurally rather than by timing, since a bounded range view is the property a hash map
cannot have at any speed.

## Remaining work in this area

| task | state |
|---|---|
| A1 to A8, base, overlay, merge, rebuild | done |
| A9, A10, aliases and fan-out | done |
| A11, leading-wildcard decision | decided: build the reversed index |
| A15, chunked name blob | done |
| A11a to A11d, reversed index implementation | done |
| A16, sorted overlay | done |
| A12, persistence and bootstrap | open |
| A13, transport action and service wrapper | open |
| A14, coordinator integration | open |
| Double-buffered rebuild so writers do not block | open |

## Second review pass, after the reversed index landed

### A17: the suffix path returned aliases with no targets (done)

The same failure class as the rebuild bug in phase 3, in a second place. `forEachMatchingBySuffix`
built its result entries from the reversed twin, which holds no alias targets. An alias resolved by a
suffix query therefore came back pointing at nothing, and only ever on that path, so a prefix query for
the same alias was correct.

The twin deliberately does not carry targets: target ordinals refer to positions in the forward
structure and mean nothing in a differently-sorted one, and resolving them would cost memory for data
no caller reads. The fix is for the suffix path to read the entry out of the forward base once it has
the name, at the cost of one extra binary search per match.

Worth noting the pattern: three separate places have now needed "read the whole entry rather than
reconstruct it from a projection", and two of them shipped wrong first. Any future structure derived
from the base should be assumed lossy until proven otherwise.

### A18: rebuild measured, and the answer was not the one reasoned (done)

The reasoned figure was 16.8 GiB, four structures alive at once. Measurement at 300,000 names found
something different and more useful.

**The buffered rebuild allocated 13.8x the steady-state size**, which extrapolates to about 108 GiB at
100M. The cause was not double-buffering the bases. It was `CompactNameIndexBuilder`'s own intermediate
list: one `Entry` object per name, each with its own arrays, which is precisely the per-entry overhead
the packed representation exists to avoid. The structure was careful and the thing that built it was not.

Rebuild never needed that buffer. Its two inputs, the base and the overlay, are both already in UTF-8
byte order, so their merge is sorted by construction. `buildFromSorted` now takes two passes over a lazy
merge, one to count and one to write, and allocates only the arrays it produces. That took allocation
from 13.8x to 7.2x.

**A first pass at the measurement was itself wrong, and the correction is the more important finding.**
Heap read immediately after a rebuild counts garbage the collector has not run on yet, so it measures
allocation churn, not what the rebuild is holding. The figure that decides whether a rebuild can run out
of memory is the retained delta after a collection, and that is **near zero**: the new structures
replace the old ones. Capacity was never the problem.

What remains real is GC pressure and wall time. 983 ms for 300,000 names extrapolates to minutes at
100M, which is a strong argument for A19's double-buffering (so writers do not block for that long) and
against any design that rebuilds often.

**Consequence for A12:** checkpointing the packed arrays to object storage is viable after all, since
the rebuild does not need headroom proportional to the index. The persistence choice can be made on
start-up time rather than on memory.

### Remaining work

| task | state |
|---|---|
| A1 to A11d, A15 to A17: the data structure | done |
| A18, measure rebuild cost, and stream the rebuild | done |
| A12, persistence and bootstrap | open |
| A13, transport action and service wrapper | open |
| A14, coordinator integration | open |
| Double-buffered rebuild so writers do not block | open |

## Phase 5 detailed plan

The four open items are one piece of work: turning a data structure into a replicated service. They are
specified together because their decisions interact.

**A18. Measure rebuild cost and peak memory.** Do this first, because it may change A12's design. Build
a 5M-name index, rebuild, and record wall time and peak heap for forward-only and forward-plus-reversed.
If the transient is genuinely 4x the steady state, the service needs either a rebuild that streams to
disk or a scheme that rebuilds one direction at a time.

**A19. Double-buffered rebuild.** Today `rebuild()` holds the write lock throughout, so writers block
for its duration, which at 100M is seconds. Swap the write target to a fresh overlay before starting,
build from the frozen pair, then swap in the new base plus the accumulated overlay. Reads already never
block; this makes writes not block either. Needs a test that writes during a rebuild are neither lost
nor duplicated.

**A12. Persistence and bootstrap.** Two options, and A18 decides between them:
   (a) rebuild from the manifest on start, adding no durable state but paying a start-up cost
       proportional to the index count;
   (b) checkpoint the packed arrays to object storage, making start fast at the cost of a second thing
       to keep consistent with the manifest.
   Measure manifest rebuild time at 5M and extrapolate before choosing. If (b), the checkpoint needs a
   version stamp so a node cannot load a checkpoint older than its manifest without noticing.

**A13. Transport action and service wrapper.** A read-only replicated service: one writer applies
creates and deletes, replicas hold full copies. Needs a wire format for `IndexNameEntry` including
alias targets (`Writeable`, following `UploadedManifestShard`'s shape), an action for lookup and
resolve, and a decision on how replicas learn of changes: tail the manifest, or receive a change
stream from the writer. Tailing the manifest is fewer moving parts and reuses machinery that already
exists.

**A14. Coordinator integration.** Route wildcard and alias resolution to this tier instead of
`Metadata.indicesLookup`. This is where the surface area is largest and where the existing behaviour has
to be matched exactly, including the options `IndicesOptions` carries: `expandWildcardsOpen`,
`expandWildcardsClosed`, `allowNoIndices`, `ignoreUnavailable`. The status byte already distinguishes
open from closed, which is why it is a status rather than an absence, but nothing maps those flags yet.

**Acceptance for phase 5.** A cluster resolves wildcards and aliases through the name index tier with
no reference to `Metadata.indicesLookup`, survives a restart of that tier, and matches core's existing
`IndicesOptions` semantics under test.
