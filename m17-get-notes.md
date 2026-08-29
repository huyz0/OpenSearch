# M17: `GET /{index}/_doc/{id}` — reading back what was written

A document could be written, deleted, bulk-loaded and searched for, but not read by id. The smallest gap
in the surface and the most conspicuous one to anyone trying the system.

## The design question is which copy answers

A get is routed to the shard's **owner** — the opposite of how a search is routed — and that is not an
inconsistency between the two paths, it is the consequence of when a write becomes visible.

| | in the log | in the engine | searchable | in the object store |
|---|---|---|---|---|
| write acknowledged | ✅ | ✅ | after a refresh | after a publish |

A search fans out to readers because it answers from published commits and placement is a hint. A get
served that way would answer "not found" for a document the caller had just been told was written, which
from outside is indistinguishable from data loss. So the owner answers, from the live version map.

Three cases, and the third is the one the design is actually about:

1. **An owner exists** → it answers, realtime.
2. **Nobody owns the shard** → the published commit is served. With no writer there are no unpublished
   writes, so the commit *is* the current state — which is what makes a get work against an index that
   has scaled to zero, the same property the search path already has.
3. **An owner exists and cannot be reached** → **refuse**. The commit is sitting right there and would
   answer instantly, and would be wrong: a live writer holds writes the commit does not, so the answer
   would be a stale document — or a 404 for a document that exists — reported as success. 503 says "ask
   again", which is true.

The response says which copy answered (`"realtime": true|false`) rather than leaving it to be inferred.

## A bug this found in code it depends on

`ShardRouter.peer()`'s documentation has always said it returns empty for a node with "no live lease". It
never checked. `membership().read()` returns the lease blob regardless of expiry, and the blob outlives
its holder until the sweep collects it — so a forward to a node that died an hour ago was
indistinguishable from one to a node that died a moment ago: both burned the full connect timeout, and
both reported a failed forward rather than the truth.

Found because canary 2 below could not be caught: the test could only ever reach the connect-failure
branch, so the "no live lease" branch was free to serve stale data unnoticed. `peer()` now consults the
clock, which is what it always claimed to do.

## Canaries

| Planted defect | Caught by |
|---|---|
| Make the get non-realtime | realtime, deleted-document, and forwarded-get |
| Serve the published commit when the owner is unreachable | the stale-data test — **only after it was split** |
| Route a get like a search (always serve the commit) | three tests |
| Refuse when nobody owns the shard, rather than serving the commit | the scale-to-zero test |

## The test that proved nothing until it was split

`testAGetRefusesRatherThanServeStaleDataWhenTheOwnerIsUnreachable` passed with the fallback planted. There
are **two** ways an owner is out of reach and they take different branches: a lease still inside its TTL,
where the owner looks alive and the connection fails; and a lease that has expired, where there is no
address worth trying. Killing a node produces the first. The second was never exercised, so the code that
handled it was never tested — and that is the branch the planted fallback lived in.

Split into both, with the error type asserted in each (`forward_failed` and `owner_unreachable`), the
canary fails exactly as it should: it returns `"found":false` for a document the owner is holding.

## What this does NOT establish

- **No `_source` filtering**, no `stored_fields`, no `_source_includes`/`_source_excludes`.
- **No `GET /{index}/_source/{id}`**, no `_mget`, no `?routing=`.
- **No version or sequence number in the response.** `WalRecord` records document state, not history, so
  there is nothing meaningful to report and nothing to compare against.
- **`HEAD` is an existence check only** — it shares every rule above and returns no body.
- **Not measured.** The cost tests count writes and idle ticks; nothing counts what a get costs, on either
  store. A get against an unowned shard opens a reader, which is a cold read of a manifest and some
  blocks, and that number is unknown.
