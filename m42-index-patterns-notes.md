# M42 — `logs-*`, and a refusal that had outlived its reason

Index patterns were the largest hole in the REST surface. `logs-*` answered 501 with a clear explanation:
resolving it means enumerating the deployment's indices, which §6.3 does not offer on a request path.

That was right, and it stopped being right, and the difference is one API.

## The rule was never "listings are forbidden"

§6.3's rule is that a request must not cost the size of the deployment, and must not answer from a subset
while looking complete. Reading every index name to match a wildcard fails both. But
`listBlobsByPrefixInSortedOrder(prefix, limit, LEXICOGRAPHIC)` — which `S3BlobContainer` implements as a
single `ListObjectsV2` with `max-keys` — fails neither: one round trip, cost set by the limit rather than
by the population.

The core-side `rfc-100m-index-architecture.md` reached the same conclusion from the other direction. It
measured, built and then **deleted** a global name index, because "a wildcard turned out to be answerable
from one bounded `ListObjectsV2` against the descriptor prefix with a `maxKeys` cap. That is a single
request whose cost is set by the cap rather than by the population, which is what the tier existed to
provide." The shell's descriptors are one blob per name in one container, so the same answer was already
available here.

## The cost, measured, because that is the whole argument

| | requests |
|---|---|
| resolving `logs-*` against 3 indices | **1** |
| resolving `logs-*` against 63 indices | **1** |

Identical on `FsBlobContainer` and on S3. A search still reads one descriptor per *matching* name, because
it needs each one's shard count — so the cost follows what the pattern matched, never what the deployment
contains. That is the shape that would have been O(population) before.

## Three refusals kept, and each for its own reason

- **More than the cap is refused, not truncated.** A prefix listing is only bounded because it stops at a
  maximum, and an answer that stopped at a maximum and said nothing about it would be a partial result
  wearing the shape of a complete one — worse than the refusal it replaced, because at least that was
  honest. The listing asks for the cap *plus one*, which is what makes "there are more than this" a fact
  rather than a guess: a listing returning exactly the cap is indistinguishable from one cut off there.
- **A pattern matching nothing is an error.** A search covering no indices and reporting itself complete
  is the confident empty answer this surface exists to avoid. `ignore_unavailable` is how a caller says
  they meant it, the same way they do for a named index that may not exist yet.
- **A pattern that is not a prefix stays refused.** `*-2026` cannot be answered by a listing at all, only
  by reading every name and matching each. Supporting a wildcard syntax whose cost depends on where the
  caller put the star would be worse than the split, because nobody could predict which of their queries
  was the expensive one.

`/_serverless/indices` also stays refused, and the distinction is worth stating: an inventory endpoint is
asked to return *everything*, where a pattern is capped and refuses when the cap is exceeded.

## A pattern is a filter, a name is an assertion

The two have to mean different things when something is missing, and this is where the code earns its
complexity. A named index that is not there is a mistake — a typo is far more likely than an absence
somebody planned for. A name the *listing* returned and the register does not describe is a tombstone from
a deletion, or an index deleted between the listing and the read: not a match, rather than a missing index.

Reporting it as `skipped` would be wrong for the same reason it must not be an error — skipped means "you
asked for this and it is missing from the answer", and nobody asked for it.

## The cap is a setting, and that is not incidental

A default of 500 is unreachable in a test, which would have left the refusal path unexercised — the most
important path in the feature, and the one that would have been verified by reading it. It is
`serverless.search.pattern.max_indices`, and the tests set it to 3 so both sides of the boundary are
actually exercised: three matches with a cap of three is allowed, four is refused.

500 itself is chosen so a pattern stays one request: S3 returns at most a thousand keys per `ListObjectsV2`,
this asks for the cap plus one, and a search fanning out over five hundred indices is not a search anybody
debugs.

## Found on the way

`DescriptorStore.listPage`'s javadoc claimed its listing was "bounded natively by S3's `ListObjectsV2`
`start-after` plus `max-keys`". It never was: the code calls `listBlobs()`, which paginates the whole
container on every backend. The claim described what the API would need rather than what it does, and
there is no way to do better through `BlobContainer` — its listing carries a prefix and a limit but no
`start-after`, which is exactly what resuming a cursor needs. The comment now says so, and points at
`namesWithPrefix` as the shape that works: a prefix and a maximum, with no resumption to carry.

## Canaries

- **96 — the listing asks for the cap rather than the cap plus one.** Caught: the boundary between
  "exactly the cap" and "more than the cap" disappears.
- **97 — too many matches truncated rather than refused.** Caught.
- **98 — a non-prefix pattern quietly accepted.** Caught.
- **99 — a pattern matching nothing answers emptily.** Caught.
- **100 — a tombstone matched by a pattern reported as a missing index.** Passed at first, because nothing
  covered a pattern meeting a deleted index. It does now, and the canary is caught.
