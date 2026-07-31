# Wildcards and listing over a gated population

Three spikes measured what a wildcard does to an index that has no cluster state entry. The short version
is that wildcards do not see gated indices at all, listing sees the first page and reports it as the whole
population, and the obvious repair is affordable only with a setting the descriptor index does not have.

| | spike | measured |
|---|---|---|
| resolution | S40 (T25) | `tenant-*` over five gated tenants resolves 0 names, no error |
| listing | S42 (T27) | a walk over twelve gated indices returns 4 and stops |
| cost | S41 (T26) | a capped prefix query is O(page) only when index-sorted with hit tracking off |
| write cost | S41b | that index sort costs the write path nothing (0.94x) |

## What is actually broken

**Resolution is silent.** `WildcardExpressionResolver.matches` reads `metadata.getIndicesLookup()` in all
three of its branches, and a gated index is absent from that map by construction. Under the options nearly
every client uses, `allowNoIndices=true`, a pattern matching nothing returns nothing rather than raising. So
a tenant searching `tenant-*` is told there are no matching indices. Only `allowNoIndices=false` produces an
`IndexNotFoundException`, and almost nothing sets it.

This is the failure this area keeps producing: not an error, an answer that is wrong and looks fine.

**Listing stops after one page**, for three independent reasons, any one of which is enough:

1. `getResponseToken` is computed from the cluster state page alone. With every index gated that count is
   zero, and zero is not greater than the page size, so the token is null. A null token means "that was
   everything".
2. `mergeGatedIndices` orders by `(creationDate, name)` and documents that both sides arrive in page order.
   `DescriptorGate.pagerFor` orders by `name` and discards the `afterCreationDate` it is handed. Pages are
   selected by name and then ordered by date.
3. Descending is served by fetching the ascending page and reversing it, which reverses the first page
   rather than producing the last one.

Defect 1 hides the other two, because a walk that never reaches page two cannot be wrong about page two.

## What is answerable, and what is not

The descriptor index is a sorted structure keyed by name. That decides the shape of what can be offered,
and it is the same conclusion S13 reached about the in-memory name index:

- **A prefix pattern** (`tenant-42-*`) is a range scan. Answerable, and S41 shows it can be made O(page).
- **A leading or embedded pattern** (`*-logs`, `a*b`) has no range to scan. It degenerates to reading every
  name in the population. Not answerable, at any population size where this work matters.
- **An unbounded match set** (`tenant-*` across a hundred million tenants) is answerable but not useful.
  Resolving the names is the cheap part: a request that resolves a hundred thousand names then fans out to
  a hundred thousand indices has a much larger problem than its resolution latency.

That third point is the one that decides the contract. The cap exists to protect the cluster from the
fan-out, not to protect the resolver from the scan.

## The contract

> Over gated indices, a wildcard must be a **prefix** pattern, and must match **no more than `N`** gated
> indices. Anything else is refused with an error naming the reason. Expansion may be up to one refresh
> interval stale.

Four consequences, stated rather than discovered later:

- `tenant-42-*` works. This is the shape a tenant with time-sliced or otherwise grouped indices actually
  writes, and it is bounded by that tenant's own index count.
- `*-logs` and `a*b` are refused. No storage arrangement in this design answers them. Supporting them means
  a second global structure keyed on reversed names, which doubles the write and adds a consistency
  question between the two, for a pattern that is rare.
- `*` and `_all` are not special-cased. They are a prefix pattern with an empty prefix, so they work on a
  cluster with few gated indices and are refused on one with a hundred million. That is the honest answer,
  and it falls out of the rule rather than being bolted on. Listing a large population is what the
  paginated API is for.
- Staleness is API-visible, at the `index.refresh_interval` H18 pinned at one second. A prefix expansion is
  a search, and searches are refresh-bound. An index created moments ago may not appear in a wildcard yet.
  It does appear immediately by exact name, which is a realtime GET.

`N` should default to 1,000. S24 measured about 33 ms per thousand names returned, so a thousand is a cost
a request can carry, and a request touching a thousand indices is already large.

### Why not simply refuse all wildcards

That was offered as an acceptable compromise and it is close to the right answer, but it gives up the one
wildcard shape that tenants genuinely need while costing about the same to build. Refusing everything and
allowing bounded prefixes differ by a range query and a counter. The narrower rule also has a worse failure
mode in practice: a tenant whose indices are `tenant-42-2026-01` through `tenant-42-2026-12` has to
enumerate twelve names by hand on every request, and will get it wrong at month boundaries.

Refusing all wildcards remains the correct fallback if the prefix seam turns out to cost more than this
estimates. It is `N = 0` in the rule above, not a different design.

## Implementation

**1. The descriptor index gains an index sort.** `index.sort.field: name`, `index.sort.order: asc` in
`descriptorIndexRequest`, beside the merge policy and for the same kind of reason: it is a contract, not
tuning. S41 measured that without it a capped prefix query costs about six seconds at a hundred million
instead of staying at the transport floor, and S41b measured that it costs the write path nothing.

The prefix searches must also set `track_total_hits: false`. Both are required and neither is sufficient:
index sorting alone still visits every match, because an exact hit total cannot be produced otherwise.

Index sorting can only be set at index creation. An existing descriptor index cannot acquire it, so this is
a recreate rather than an upgrade. Acceptable now because the feature has no production population, and it
stops being acceptable the moment one exists, which is a reason to do it now rather than later.

**2. A prefix seam beside the existing exact-name one.** `AbsentIndexDescriptorSuppliers` grows an
expander alongside `supply` and `page`. It asks for `N + 1` names and reports the overflow rather than
truncating, because a truncated expansion is a silently wrong answer of exactly the kind this whole area is
about. The seam returns the reason for a refusal rather than a boolean, following `DescriptorRepresentable`,
so the error can say which rule the pattern broke.

**3. The resolver consults it.** `WildcardExpressionResolver.matches` already branches three ways, and the
`suffixWildcard` branch is exactly the answerable one. That is not a coincidence: both this and the
descriptor index are sorted-prefix structures. The other two branches refuse when the seam is registered,
instead of quietly returning the ordinary indices only. `resolveEmptyOrTrivialWildcard` is a fourth path and
needs the same treatment.

Wildcard resolution then issues a search where it previously read a map. Point lookups already issue a
realtime GET on this path, so a blocking remote read during resolution is not new, but a search is slower
than a GET and the resolution path should be checked for callers that cannot block. The one constraint
already known is W4's: nothing on the cluster state thread may block on a descriptor read.

**4. The three listing defects.** The pager sorts by `(creationDate, name)` and resumes from both, matching
the strategy rather than approximating it; descending issues a descending search rather than reversing an
ascending page; and the next-page token accounts for the gated side instead of being derived from a cluster
state page that is empty.

These are independent of the contract above and are unambiguous bugs, so they do not need the contract
settled first.

## What this does not solve

Counting. `_cat/indices` without pagination, `_cluster/health` over `*`, and anything else that wants "how
many indices are there" still has no bounded answer, and the cap makes that explicit rather than solving it.
H20's mapping stats aggregate is the pattern that works: ask a question whose cost is set by something other
than the number of indices. Nothing here does that for counting.

Aliases are also unmodelled. An alias is a name pointing at a set of indices, and the descriptor carries an
alias list, but no spike has measured what resolving an alias over a gated population costs or whether the
same prefix rule can be stated for it.
