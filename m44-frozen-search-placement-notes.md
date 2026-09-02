# M44 (finished) — a frozen search fans out by placement too

M37 shipped point-in-time views and left two items on its own "what is not here" list: a view is
node-local once opened, and two nodes serving the same view open it twice. Read together with M11's own
reader placement, the gap was bigger than that sentence let on: a frozen search never used placement at
all. This closes it, without adding anything the design didn't already have a shape for.

## What was actually true before this

`SearchFanout.runFrozen` opened every shard of a view **unconditionally on the coordinating node** —
whichever node happened to answer the HTTP request. Not "opened once and cached" in the bad sense; M37's
`openFrozenReader` is idempotent per node, so a single node re-asked for the same view doesn't redo the
work. The real cost was structural: a live search already fans shard reads out across the fleet, by
`ReaderPlacement`'s rendezvous hash with a forward-and-fallback (`SearchFanout.askOneShard`); a frozen
search had no equivalent at all. A wide view's entire cost — heap, file descriptors, and a slot against
`maxShardsHeld` for every shard — landed on one node, and a caller whose paged requests hit a different
coordinator each time (any load balancer with no session affinity) paid that cost again from nothing on
each one.

## The fix is the existing pattern, not a new one

`runFrozen`'s per-shard work now goes through `askOneFrozenShard`, which is `askOneShard`'s shape applied
to a view instead of a live shard: serve it here if already open, otherwise ask `ReaderPlacement` for the
preferred node and forward, otherwise open it here as the guaranteed-to-succeed fallback. Keyed the same
way live placement is — by the view's underlying index name and shard number, not by the view's own id —
so a view's placement tends to agree with its live shard's, where a node is more likely to already have
some of the same immutable segment files cached from ordinary reads.

The one real difference from the live path: a live shard falls back to its **owner** (the writer) when no
reader answers. A view has no owner — nothing writes to it — so its fallback is the coordinator opening it
itself, which is exactly what every call did unconditionally before this existed. Placement failing now
costs what it always cost on the live path: a cold open, never a wrong or missing answer.

## What crosses the wire

`ForwardedFrozenSearchRequest` carries the **whole `PointInTime`**, not a reduced form with just the shard
being asked for. `ServerlessNode#openFrozenView` needs every shard's term to build the view's
`IndexMetadata` even when opening a single shard of it (an existing M37 constraint, not new), so sending
the whole view lets the receiving node call the exact same method the coordinator would have called
locally — one code path for "opened here" and "opened because it was forwarded here," rather than a second
implementation that could drift from the first. The response reuses `ForwardedSearchResponse` unchanged:
a shard's total, hits and aggregations owe nothing to whether the commit behind them is live or frozen.

`PointInTime` and `CommitManifest` gained transport (`StreamInput`/`StreamOutput`) serialization alongside
their existing `toBytes()`/`fromStream()` register-blob (JSON) serialization. The two exist for different
reasons and shouldn't be collapsed: JSON is what the register format has always been, chosen so a stored
record can be read by hand and survives a version change better than a stream format would; the binary
form exists only so this transport request can carry a manifest at all.

## Does this contradict the serverless principle?

No, and the reasoning is worth writing down because it's the same reasoning `ReaderPlacement`'s own javadoc
already makes for the live path, extended rather than reinvented:

- **The routing decision is a hint, never a requirement.** Every node computes the same rendezvous hash
  independently from the same membership snapshot (`metadata.membership().current()`) — no vote, no
  agreement protocol, no durable "this view lives on node X" record. A stale or unlucky pick costs a
  network hop or a cold open and never a wrong answer.
- **No new coordination primitive.** `ForwardedFrozenSearchRequest`/`forwardFrozenSearch` are new *code*,
  not a new *mechanism* — they're `ForwardedSearchRequest`/`forwardSearch` with a `PointInTime` in place of
  an index name, registered the same way, timed out by the same `searchForwardTimeout()`.
- **The line this stays on the right side of**, quoting `ReaderPlacement` directly: "The moment placement
  becomes required rather than preferred, this design has reinvented the stateful cluster it exists to
  escape: agreement on placement, drain-before-move, handoff." Nothing here requires agreement. A durable
  view→node assignment record, or a central directory of who currently holds which view's shards, would
  cross that line; this does neither.

## What this does NOT establish

- **Duplication is reduced, not eliminated.** Keying placement the same way live search does means paged
  requests for one view *tend* to converge on the same preferred node across different coordinators, as a
  side effect of reusing the existing hash — not because anything new tracks or guarantees it. Under
  membership churn, or simply an unlucky first pick, two nodes can still each open the same view. That is
  the accepted cost of a hint-based design, identical to what live search already accepts, not something
  this closes.
- **No slicing.** Parallel consumption of one view across workers — `slice.id`/`slice.max` — is unrelated
  to this and unimplemented; `slice` is not referenced anywhere in `serverless/shell` as of this milestone.
  It would need no new coordination to add (each worker's request is independent, and the doc-id-modulo
  partitioning happens inside a shard's own query execution) but has never been exercised or tested here.
- **Nothing pre-warms a placement.** A view's preferred node is only asked for it once a search actually
  arrives; nothing opens it proactively, matching M11's own standing limitation for live shards.

## Canaries

- **130 — placement and forwarding are skipped; every frozen search opens locally regardless.**
  `askOneFrozenShard` short-circuited to always take the "already local" branch. Caught by
  `testAFrozenSearchIsForwardedToThePlacementPreferredNode`: the placement-preferred node's
  `frozenShards()` stayed empty and the coordinator's held the view instead.
- **131 — the local-open fallback is removed when every placement candidate fails.** The final fallback
  in `askOneFrozenShard` replaced with a thrown exception. Caught far more broadly than intended: **five**
  pre-existing single-node PIT tests failed alongside the new fallback test, because a plain
  `serverless.roles=ingest` node never advertises `search` and so `ReaderPlacement.candidatesFor` always
  returns no candidates for it — meaning this fallback is not a rare unreachable-peer corner case, it is
  the *entire* code path a single-node or ingest-only deployment's frozen search takes. Removing it broke
  point-in-time search outright for the common case, not an edge one.
- **132 — the forwarded handler opens the live commit instead of the frozen one.**
  `ShardRouter.handleFrozenSearch` changed to call `serveAsReader` (live) instead of `openFrozenView`
  (frozen). Caught two ways at once: `testAFrozenSearchIsForwardedToThePlacementPreferredNode` failed
  because the shard landed in `openShards()`/reader accounting rather than `frozenShards()`, and
  `testAForwardedFrozenSearchStillSeesTheFrozenCommitNotLiveWrites` failed on the number that actually
  matters — the forwarded search reported `"value":2`, the write made *after* the freeze, instead of the
  `"value":1` the view actually froze.

Three planted, three caught, one (131) far more broadly than aimed at — which is itself evidence for how
load-bearing the fallback path already was before this milestone touched it.

M44 is done.
