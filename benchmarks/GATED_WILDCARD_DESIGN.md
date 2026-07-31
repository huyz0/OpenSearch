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
- `*` and `_all` keep their existing meaning, which is everything in cluster state, and do not expand over
  gated indices at all. **This is the one place the contract gives something up rather than bounding it,
  and it was not the original plan.** See below.
- Staleness is API-visible, at the `index.refresh_interval` H18 pinned at one second. A prefix expansion is
  a search, and searches are refresh-bound. An index created moments ago may not appear in a wildcard yet.
  It does appear immediately by exact name, which is a realtime GET.

### The cap

**`N` defaults to 100, and is a cluster setting rather than a constant.**

Refusal beats truncation and that is the part worth being firm about. Answering with the first `N` names
would be the same class of defect as the four already found here: a plausible result that is silently
incomplete, where a tenant searching `tenant-*` gets a thousand of their five thousand indices and has no
way to tell.

Two measurements bound the value, and only one of them is about resolution:

- S24 measured about 33 ms per thousand names returned, so a hundred is about 3 ms. Negligible.
- T20 and T21 measured 118 KB and 3.06 file descriptors per awake shard, and 41.5 ms to wake one. With
  index per tenant most tenant indices are asleep, so **an expansion decides how many sleeping shards one
  request wakes**. A hundred is roughly 12 MB and 300 file descriptors. A thousand is 118 MB and 3,000. A
  hundred thousand is a node.

That second point is the reason the cap exists. Resolving names is the cheap part of a wildcard against a
scale-to-zero fleet, and the number that matters is how much of the fleet a single request pulls into
memory.

100 rather than 1,000 because the failure is asymmetric. A cap set too low refuses a request that would have
worked, which the caller sees immediately and an operator can raise in one setting change. A cap set too
high wakes a large part of the fleet on one request, which shows up as memory and file descriptor pressure
on whichever node coordinated it, and does not point back at the wildcard that caused it. Start where the
blast radius is small.

Neither number is a measured optimum, and the fan-out figure is the weaker of the two: 118 KB is per shard
at rest, and a hundred simultaneous wakes is not a hundred times one wake. Worth re-measuring against a real
wake storm before the default is defended rather than merely chosen.

`N = 0` is the "no wildcards at all" position, reachable by configuration rather than by a different design.

### Why match-all is exempt, which cost two attempts to learn

The plan treated `*` as a prefix pattern with an empty prefix, so it would work on a small cluster and be
refused on a large one, and called that the honest answer falling out of the rule. Built and measured, it
was not honest, it was an outage.

**First attempt: cap `_all` like any other prefix.** With the population over the cap, cluster health
failed, and then the test cluster hung until the suite timed out at twenty minutes, because the test
framework's own consistency checks ask the same question. Health, node stats and a good deal of internal
housekeeping resolve match-all, so capping it means the cluster stops being able to describe itself the
moment it grows past the limit. A query limit had become an availability limit.

**Second attempt: expand only patterns the caller actually wrote**, on the theory that an empty index list
is an API default meaning "this cluster" while an explicit `*` is a user enumerating. Measured, health and
index stats arrive with an explicit `_all` anyway. The resolver sees an expression, not a caller, and
threading that distinction through every transport action buys a semantic nobody asked for.

**So match-all is exempt.** A gated index is reached by prefix or by name. This is consistent rather than
population-dependent, and it cannot fail.

The cost is real and worth stating plainly: on a cluster small enough that `*` would have worked, `*`
silently omits gated indices. That is the same shape as the defect T25 measured, narrowed to one pattern and
made deliberate. It is pinned by a test that asserts the omission rather than left to be rediscovered, and
it is the honest price of not letting a wildcard cap decide whether cluster health answers.

It also lands in the same place H19 and H20 reached independently: cluster-wide questions over a population
this size are served by aggregates, not by enumerating indices.

### Why not simply refuse all wildcards

That was offered as an acceptable compromise and it is close to the right answer, but it gives up the one
wildcard shape that tenants genuinely need while costing about the same to build. Refusing everything and
allowing bounded prefixes differ by a range query and a counter. The narrower rule also has a worse failure
mode in practice: a tenant whose indices are `tenant-42-2026-01` through `tenant-42-2026-12` has to
enumerate twelve names by hand on every request, and will get it wrong at month boundaries.

Refusing all wildcards remains the correct fallback if the prefix seam turns out to cost more than this
estimates. It is `N = 0` in the rule above, not a different design.

## Implementation

Built, in T28. What follows is what went in rather than what was planned, and the two differ in one place,
noted under step 3.

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

**3. The resolver consults it.** Two call sites rather than the four the plan expected, and the difference
is worth recording because the plan was reasoning about the wrong layer.

`matches()` branches three ways over cluster state, and the plan proposed teaching each branch about gated
indices separately. That was unnecessary. The gated side has no `IndexAbstraction` to produce and no
aliases or data streams to reconcile, so it does not belong inside `matches()` at all: it is a second source
of names folded in beside `expand()`. One call in `innerResolve` covers every pattern, and the
prefix-or-refuse decision lives in one method instead of being spread across three branches that would each
have to agree.

The second call site is `resolveEmptyOrTrivialWildcard`, which `_all` and a bare `*` reach without passing
through `innerResolve` at all. That one is genuinely separate, and missing it would have left match-all
silently blind to gated indices while every other pattern worked, which is the kind of gap that shows up
only under the one API nobody tested.

State and hidden filtering happens at the resolver rather than in the store, because `IndicesOptions`
decides both per request while the expansion is shared. Closed and hidden gated indices are fetched and then
discarded, which costs two doc values inside a page that is already capped.

Wiring this up also exposed a wasted read that had been there since H8a. `innerResolve` asks
`aliasOrIndexExists` about every expression before deciding whether it is a pattern, and that seam consults
the descriptor store on a miss. An index name cannot contain a star, so for a wildcard that read can only
miss: every wildcard request spent one remote GET asking whether an index is literally named `tenant-*`.
Harmless while no wildcard reached the seam, and a per-request cost the moment wildcards became a normal
path. Guarded, and counted in a test rather than assumed, because a cost that only appears under the feature
that made it reachable is exactly the kind that goes unnoticed.

Wildcard resolution then issues a search where it previously read a map. Point lookups already issue a
realtime GET on this path, so a blocking remote read during resolution is not new, but a search is slower
than a GET and the resolution path should be checked for callers that cannot block. The one constraint
already known is W4's: nothing on the cluster state thread may block on a descriptor read.

**4. The listing defects. Done.** The pager sorts by `(creationDate, name)` and resumes from both, matching
the strategy rather than approximating it; descending issues a descending search rather than reversing an
ascending page; the next-page token accounts for the gated side instead of being derived from a cluster
state page that is empty; and tombstones are excluded, since H4 records a deletion as a document and nothing
was filtering it out. The same walk now returns 12 of 12 over 3 pages in both directions.

These were independent of the contract above and were unambiguous bugs, so they did not need the contract
settled first.

`findByPrefix`, the full-descriptor sibling of the pager's read, still has no state filter and so would list
tombstones too. It has no production caller today, which is the only reason it was left alone: changing an
unused method's behaviour without a test that fails first is how this area accumulated six mechanisms that
were correct and unreachable. The prefix seam in step 2 will read through something of that shape and must
carry the filter.

## What this does not solve

Counting. `_cat/indices` without pagination, `_cluster/health` over `*`, and anything else that wants "how
many indices are there" still has no bounded answer, and the cap makes that explicit rather than solving it.
H20's mapping stats aggregate is the pattern that works: ask a question whose cost is set by something other
than the number of indices. Nothing here does that for counting.

Aliases, which T29 then measured and settled. An alias on a gated index resolved to nothing, silently under
the options most clients use, and the repair is not to resolve it: an alias cannot be added, removed or
repointed on a gated index at all, because every alias operation is a cluster state update over metadata the
index does not have. An index declaring any alias therefore keeps its cluster state entry. That costs the
alias-carrying part of the population its gating, and makes the constraint visible at creation rather than
as an alias that answers nothing. Details in S44.
