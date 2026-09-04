# M63 (finished) — every finding of the September review, fixed

[`serverless-index-review-2026-09.md`](serverless-index-review-2026-09.md) read the implementation rather than
the notes and found thirty-three things: three critical, twelve high, eighteen medium. This milestone takes
all of them. The review is left as written so each fix can be read against its finding; this note is the
other half of that pairing, grouped the way the fixes were made rather than the way the findings were
numbered.

## Identity: name, uuid, generation, instance

- **Names are validated before they are paths (C1).** `Names` refuses what core refuses — `..`, separators,
  leading `_`/`-`/`+`, `.`/`..`, control characters, over-long — and every store that turns a name into a
  blob path validates first: descriptors, aliases, templates, scripts, prefix listings. The probe that
  wrote outside the base path answers 400.
- **The system-index guard checks the written target, not the URL (C2).** A write through an alias, a
  data stream, a template-derived name or a rollover is checked against the index it lands on; every
  handler that touches a system index by any spelling refuses it.
- **Generations never restart (H1).** A deleted name keeps a tombstone that carries the last generation,
  and a re-creation compares-and-swaps over it, so a generation read before a delete-and-recreate cannot
  win a swap after it. The tombstone survives `deleteIfUnchanged`.
- **Shards are identified by uuid, not name (H2, H8).** Shard heads carry the index uuid; a head from an
  earlier incarnation is dead to `acquire`. Every local shard lookup — placement, the router's
  `localShard`, the fan-out's `localShard` — matches uuid, and a forwarded search carries the uuid so a
  recreated index cannot answer a search that began before it. The block cache is keyed by storage uuid,
  shard and term.
- **Node identity is the ephemeral id (H9).** A renewal with a live lease under another ephemeral id is
  refused, so two processes with one node id cannot both hold it; a restart with the same id hands over
  its shards on close and acquires at a fresh term rather than inheriting.

## Durability

- **The WAL ordinal is seeded from the container and appends are put-if-absent (H3).** A same-term reopen
  cannot overwrite record 1.
- **A write is acknowledged only if the lease was valid on both sides of the PUT (M1).** The lease is
  checked before the append, as before, and now after it: a successor seals only after this node's lease
  expired, so a record landed while the lease was valid throughout is ahead of any seal. A writer that
  paused across the expiry releases the shard and fails the write rather than acknowledging a record the
  successor will never replay.
- **A forwarded create is a create (H4).** `requireAbsent` travels and is honoured on the far side.
- **`_update` writes back conditionally (H5).** The read's sequence number is the write's condition, so
  `retry_on_conflict` can trigger and does.
- **Two publishes of one shard are serialised (H6)** under a per-shard lock, on both the backstop and the
  edge-triggered path, and a lost swap by the same term and writer returns the current manifest rather
  than fencing the owner out of its own shard.
- **Readers reopen the newest commit (H7)** and drop the local commits they are no longer serving.
- **Tombstones and heads are not left for a crash to make permanent (M12).** A delete tombstones the head
  rather than deleting it, so an in-flight acquire cannot recreate one the next same-name index inherits;
  the uuid on the head makes a stale one harmless even if it lands.

## Authorization, threads, error bodies

- **Transport is authenticated (C3).** Every forwarded request carries a deployment secret from a register
  in the cluster configuration, generated once and read by every member; a handler refuses a request
  without it, in constant time. A caller that has not read the object store is not a member, whatever
  address it came from. The caller's own headers still travel, so a plugin's principal is not lost.
- **The action gate covers the whole surface (H11).** Multi-search, rank-eval, rollover, data streams,
  points in time, mapping and settings updates, templates, pipelines, scripts, search pipelines, aliases,
  field caps, analyze, list and resolve, nodes and health each run under the action name core would use,
  with the request an action filter expects. The gate hands the filtered request to the work, so a filter
  that rewrites one is honoured.
- **No thread pool waits on itself (H10).** Fan-out tasks run on a pool of their own (`serverless_fanout`),
  and the router's search handlers run there too; nothing on GENERIC or SEARCH blocks on a task that needs
  GENERIC or SEARCH to start. Search planning — name resolution, membership, the point-in-time read — runs
  off the HTTP thread (M16).
- **Error bodies name the blob, not the path (M18).** One `failure` method renders every handler's
  fallback; an object-store failure reports the blob name and the kind of failure, never the path, node
  id or address.
- **Authentication is throttled and bounded (M13).** A per-principal and per-address throttle with a
  configurable lockout, a bounded checker queue that answers 503 when full, a decoy derivation for an
  unknown user so a miss costs the same as a hit, and a credential marker read at most once a second.

## Propagation and the control loop

- **Mapping and settings changes reach every node (M4).** A heartbeat reads one descriptor per open index
  and applies a moved mapping or settings version to the open shard; `growMapping` refreshes when another
  node already added the field. A version already held is not re-applied, which core asserts on.
- **`_bulk` grows a dynamic mapping (M3)** the way a single write does, once per batch.
- **A reader can become the writer (M5).** Activation releases a reader before acquiring the writer.
- **Edge-triggered publishes are swept (M8).** A publish outside a pass notes the shard for the next
  sweep; a pass with nothing published and nothing on watch costs no listing.
- **Alias updates read value and generation once, and never demote a data stream (H12).**
- **The alias hint cannot under-approximate silently (M10).** An update re-notes the hint for every index
  the alias names after the swap; a creation that loses its race takes its hint back; a hint that could
  not be written after retries is logged as the wrong answer it would be.
- **A rollover that loses its swap undoes the index it created (M11)**, so the next rollover computes the
  same name and finds it free.
- **The reaper re-reads before it deletes (M9)**, so an extension that succeeded between a reaper's read
  and its delete keeps the view.
- **Templates, component templates, pipelines, search pipelines and scripts are cached per node behind
  one marker.** Reading every template on every index creation costs one register read while nothing
  has changed; the stores share the marker the scripts already had.

## The write and read paths

- **Indexing pressure sees every write (M6, M2).** A forwarded write is accounted on the coordinating node
  for as long as the forward is in flight and on the owner as a primary operation; a delete-by-query page
  is accounted as the batch of writes it is. A forward that times out says so — the write may have
  landed — rather than "stale routing, retry", so a client does not retry an auto-id write blind.
- **`_delete_by_query` is conditional (M2).** Each delete carries the sequence number the frozen view saw;
  a document rewritten since is a version conflict, counted in `version_conflicts`, and `conflicts=abort`
  (core's default) stops at the first one with core's `failures` entry and a 409, `conflicts=proceed`
  goes on.
- **A get answers from the owner (M7).** The router refuses a forwarded get or explain on a shard it
  holds only as a reader; a local writer answers only while the head still names this node, otherwise the
  node the head names is asked.
- **The peer lookup is the membership snapshot (cost)**, refreshed every pass; the store is read only for
  a node the snapshot has not seen.

## Search

- **The coordinator's working set is bounded and inside the breakers (M15).** Hits are charged to core's
  request breaker as they arrive and released when the response is built; the merge keeps at most two
  windows of hits at any moment, sorting and cutting as it goes; the reduce runs under core's bucket
  consumer with `search.max_buckets`. Batches — `_msearch`, `_msearch/template`, `_rank_eval` — carry at
  most a hundred searches.
- **The sort merge is core's (M14).** A missing value goes where the sort's own `missing` says, not where
  the direction says; a long from one index and a double from another compare as numbers.
- **The owner's head is read only when placement could not answer (cost).** A head read per shard per
  search was the largest single cost of a hot search.
- **A stored script that does not exist costs one register read (M17, cost).** A miss checks the marker
  rather than re-listing the store, and an unsupported language is refused at `PUT` instead of stored.

## Listings, after the review: a read where a listing was

A second pass over every `listBlobs`, `listBlobsByPrefix` and `children()` call in the shell, traced to
its caller and cadence, found the idle pass clean and four hot paths listing for nothing. Each is now a
read, or nothing, and the rule for every one of them was the same: an index is worth keeping only where
the write that maintains it already exists.

- **Membership is a register, not a listing.** A `members` register beside the leases names every node.
  A node adds itself once, on its first renewal, and removes itself on a clean release; a refresh that
  finds a listed node with no lease prunes it. No per-pass write touches it, so the steady state gains
  no writes -- one compare-and-swap per join and per clean leave. A refresh is now one register read
  and one read per member; a deployment from before the index is listed once and the index written from
  that listing. Requests refresh only when the snapshot is older than half a lease, off the HTTP thread,
  and one read of the index's generation between times is what makes a join visible to the very next
  request rather than after the snapshot ages out. A search used to list on every request.
- **The WAL is truncated from memory.** Within a term there is one writer, and it seeded its ordinal from
  the container, so it knows which records exist without listing them; older terms are emptied once per
  term per instance rather than on every publish. Two to three listings per dirty shard per pass are
  gone.
- **A sweep runs when it can find something.** A file becomes unreferenced only when a manifest stops
  naming it, so a shard is swept when its newest manifest lost a file other than its `segments_N`, when
  it has candidates on watch, when a reaped view may have freed something, or when this node has never
  swept it since opening it. The views and snapshots every sweep must respect are read only when some
  shard qualifies. A publish that only added segments, which is most of them, lists nothing.
- **The manifest records every file's length.** The publisher knows each length as it uploads, so
  recording them costs nothing beyond the register write the manifest already is; a reader lists a term
  container only for a file whose length no manifest recorded, which is a manifest from before this. A
  frozen view carries the lengths too. Readers reopen on every superseded commit, so this was one
  listing per term container per publish cycle per shard served.

`ServerlessReconcileTests.testABusyPassDoesNotListEither` pins the result: five passes that each write
and publish cost no listing, every published file's length is in the manifest, and a reader opened on
it lists nothing.

## What was verified

Every test class the fixes touch was run as it was changed; the whole `:serverless:testkit:test` task was
run twice at the end and `:serverless:shell:precommit` passes. Three regressions the suite caught on the
way are worth recording, because each was a fix that was right and a detail that was not: the parameter
check ran before the point-in-time plan, so a body `pit` was "unrecognized"; the mapping refresh re-applied
a version the node already held, which core asserts against; and the hoisted views-and-snapshots read ran
on idle passes, which the listing-count test noticed. The review's per-pass cost table stands, with the
idle pass now costing what it says.
