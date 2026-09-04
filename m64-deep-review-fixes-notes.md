# M64 (finished) — the deep review, fixed

[`serverless-deep-review-2026-09.md`](serverless-deep-review-2026-09.md) read the shell and the auth plugin
against their RFCs and found the acknowledgement fence missing, a long tail of correctness defects, a cost
model that had drifted at fleet scale, and a set of written decisions the code no longer honoured. This
milestone takes all of it. The review is left as written; this note is the other half.

Six implementation passes ran in parallel over disjoint files, each with the review's own fix and a test
that failed before the change: 549 tests, 58 of them new in twelve classes, and `precommit` on the shell
and the plugin, all green at the end. Three things the suite caught on the way are worth recording: a
lapsed-lease rule that released shards nobody had stolen (now suspend → renew → verify → lift); the
per-request MAC digesting a wire form whose map order was salted, so a forwarded frozen search read as
forged one time in eight (the wire form is sorted now); and the removed translog syncs pinning a
sixteen-kilobyte page on the request breaker per open writer (the sync is back, for memory rather than
durability).

## The acknowledgement fence

The rule the design implied and the code did not enforce is explicit now: **a node whose own lease
lapsed has lost every shard it held.**

- **Suspend, renew, verify, lift.** A lapse marks ownership unverified before the renewal; the write path
  and the forwarded-write check refuse while it is set; the renewal then re-reads every head and lifts the
  suspension only if the verification began after the lapse and read every head. Shards nobody stole are
  kept, which is what the idle-release tests pin, and the window in which a resumed zombie acknowledged
  writes behind a successor's seal is gone.
- **A per-shard fence.** The write path holds it across apply → append → acknowledge; an idle release,
  an eviction, a shutdown handover and a fenced publish hold it across publish → release head → close.
  A write cannot be acknowledged after the head it was written under is given away.
- **Two clocks, one margin.** The holder treats its lease as lapsed a margin early (TTL/32 by default,
  configurable), so the taker's clock and the holder's clock have to disagree by more than the margin
  before an acknowledgement and a takeover can overlap.
- **Forwarded writes check the head.** The owner accepts a forwarded write only for a shard it holds as a
  writer, whose uuid matches, and whose head read within one renewal names it.
- **The lease renews on its own timer**, before and independently of the head reads, which now run in
  eight parallel lanes with single-flight. A healthy node no longer lapses at a thousand shards.
- **A failed log append fences rather than closes.** The shard stays readable, refuses writes, is
  skipped by publish, and is reopened from the log by the next heartbeat — the brownout behaviour the
  RFC asks for.
- **`syncFrom` never closes on listing evidence** and skips readers; activation opens only the shard it
  acquired; a full resync runs every twentieth backstop. A head won but never opened is given back on
  either failure half. The seal records only terms strictly below the sealer's.

`ServerlessFenceTests` pins the zombie-renew window, write-during-handover with a gated store, the missing
claim, the two-clock case, the seal, the forwarded-write refusal and the brownout.
`ServerlessHeartbeatCostTests` pins that a renewal reads no heads and a pass reads each head once.

## Routing, forwarding, readers

- **Hints are forgotten on every forward failure**, bounded to one lease in age, and a stale hint costs
  one register read and one more forward. A refusal is a typed exception with a wire-stable marker, not a
  substring. Forwarded writes carry the index uuid; the owner refuses a mismatch; bulk groups filter by
  uuid, so a recreated name cannot take writes into its predecessor's log.
- **Forwarded answers are complete.** A forwarded get carries `_seq_no`, `_primary_term` and `_version`;
  a forwarded bulk conflict is a 409; a plugin's 403 and an owner's 429 keep their status through the
  hop; `_index` is the backing index on both paths; bulk deadlines scale with batch size and a deadline is
  reported as "may have been applied". Mapping growth in a batch is coalesced to one swap.
- **A reader never serves a fenced writer's bytes.** Every manifest-named local file and every local
  commit is deleted before a reader bootstraps; a writer reopen drops stale local commits; writers open
  on the lazy block-cache directory, so a cold start no longer downloads the shard. Any failure after
  `createShard` removes the shard and the retry works. Readers refresh every second renewal and a reader
  under a running query is not released.

## Stores, sweeps, records

- **Descriptor tombstones are time-stamped, quarantined for six hours and swept**, and a prefix pattern
  counts only live names against its cap.
- **The template, component-template, pipeline, search-pipeline and script stores** are content-addressed
  blobs behind one compare-and-swapped index, with migration from the old layout: a crash between write
  and marker cannot leave every node's cache stale.
- **The orphan sweep respects live snapshots, in-progress captures and views**; an unreadable view record
  pins conservatively; a view carries its index uuid and is refused after a delete-and-recreate of the
  name; extension is conditional on the expiry it read.
- **The block cache** is a striped LRU with single-flight fetches and exposed counters.

## Aliases, streams, templates, snapshots

- An alias record and its generation come from **one register read**; every alias, stream and rollover
  mutation swaps against that token. Alias mutations refuse a data-stream name in core's words and keep
  the stream's fields; backing indices inherit the stream's template; a stream delete is a swap; a lost
  create cleans up its backing index; `.ds-` names are reserved; rollover bumps past taken names and its
  409 says what became of the index it made; the write index cannot be deleted.
- Template settings in `index.*`, nested and bare spellings are normalised as core does and
  `number_of_shards` is honoured; `PUT _settings` parses before it swaps; the three pipeline settings are
  refused rather than silently accepted; template mappings layer the way core's do.
- Snapshots write a provisional record **before** reading manifests, capture settings, restore them,
  honour the manifest swap on restore (409 with rollback), refuse system indices on capture and as
  targets, and use core's exception types.

## Search and admin

- The coordinator's shard tasks run on the generic pool with caller-runs, so two saturated search nodes
  cannot deadlock; a shard timeout produces `timed_out: true`; every shard evaluates `now` at the
  coordinator's instant; the plugin client's search covers every index and alias it names; the breaker is
  charged per shard as answers arrive and released on every path; `track_total_hits` is a ceiling; a
  failed local copy falls through to a peer only when the copy is unusable.
- Health answers 408 on a timeout, parses core's `wait_for_nodes` grammar, reports a shard owned while
  its owner's lease is live rather than dormant after one TTL, and refuses an unscoped `wait_for_status`.
  Catalog and cluster-settings reads run off the transport thread and through the gate. `/_serverless/stats`
  reports object-store request counters, the block cache, the scheduler, lease health and per-shard
  ownership from a production counting store.
- Pattern expansion skips a system index instead of refusing the search; `_mget` refuses one per item.

## The plugin and the transport

- **The throttle** keys refusals on the address-and-account pair, only slows an account, never locks the
  recovery account, has a per-address budget only a cross-name flood can reach, trusts `X-Forwarded-For`
  from configured proxies, and is bounded in memory. Verify refuses a planted huge-iteration record;
  the cache fingerprint is keyed; passwords are `char[]`; the plugin marks its own client calls beside
  the caller's principal rather than replacing it.
- **Forwarded requests carry a per-request MAC** over the action, the sender, a timestamp, a nonce and a
  digest of the request, under a secret with a generation and a rotation window; the static token is
  accepted only in the MAC's absence, for the transition. A forwarded write to a system index needs the
  plugin-origin marker. A plaintext transport is warned about at start and refused when
  `serverless.transport.require_secure` is set. A plugin's transport action is built from its own
  components only.
- [`serverless-security-posture.md`](serverless-security-posture.md) states what §12 requires and what
  exists.

## The documents

`rfc-serverless-shell.md` records that §6.3's reuse did not happen and what to adopt next, that the
members index is a cache of the listing and never the truth, how D4 and D5 are actually operated, and
re-rates R3 and R6. `serverless-status.md` no longer contradicts itself. The control-plane absence scan
covers §6.2's whole list, and `MetaStateService` is a no-op. Node roles come from the lease; peers carry
theirs. S3 boots with a warning naming R11 and the providers actually tested.

## Not done, and why

- Membership at fleet scale is still one lease read per member per refresh; the index removed the
  listing, not the reads. A sharded members index or gossip is the next step, and neither is small.
- The publish debounce stays at 500 ms; raising it is a cost knob the operator now has, not a fix.
- Reader refresh is still by reopen, bounded to every second renewal; in-place refresh needs an engine
  swap under the engine lock.
- The block cache is sized in blocks, not against the request breaker: accounting it there would trip
  the breaker tests that run with a one-kilobyte limit. Size it from heap in the bootstrap.
- Every node still reaps expired views; a reaper election is a register nobody has designed.
- §9.5's epoch piggyback on forwarded responses is not built; freshness is the members-index generation
  probe, at most once a second per handler, plus the routing corrections above.
- §6.3's adoption of `TransportShardBulkAction.performOnPrimary` is recorded as the next step, not done:
  it replaces the shell's own write semantics one path at a time and deserves its own milestone.
- `NodeConnectionsService` is still a stub; the router opens connections as it needs them and closes none.
- The block cache is bounded by a fraction of the heap when unconfigured, not accounted to a breaker.
