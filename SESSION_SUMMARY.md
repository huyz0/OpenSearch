# Session summary — resume from here

Working directory for this work is **this repo** (`/home/tuong/work/OpenSearch`), branch
**`feature/serverless`** (earlier versions of this file named `feature/pluggable-engine-per-shard-role`,
which this work has since moved off). Not the parent `/home/tuong/work` directory — that's an
unrelated empty git init, only relevant if a shell's cwd silently resets there mid-session (a known
environment quirk hit repeatedly earlier; always `cd` back into this repo before running anything).

This file is a pointer/index, not the log itself — the detailed, per-decision record already lives in
this repo:

- **`dynamic-partitioning-progress.md`** (repo root, ~2200 lines) — the authoritative, continuously
  updated log for the in-place shard split/merge feature and the correctness-review pass that
  followed it. Read this first for anything related to split/merge, WAL mirroring/batching, or the
  scale-to-zero fixes. Section headers (`grep '^## ' dynamic-partitioning-progress.md`) are a full
  table of contents.
- **`dynamic-partitioning-plan.md`** — the original design doc the above progress log tracks against.
- **`rfc-serverless-opensearch.md`** — the whole plugin's RFC; `TransportOrchestrateShardSplitAction`'s
  class javadoc and this doc's own cutover section now document the resharding-by-copy source-fencing
  limitation found this session (commit `ae0ee3d25d4`).
- **`.claude/plans/wobbly-sprouting-dove.md`** — no longer relevant here (that path is under
  `/home/tuong/.claude/`, not this repo); the WAL batching design it captured is now fully summarized
  in `dynamic-partitioning-progress.md`'s last section instead.
- `git log --oneline` — every change this session made is its own real commit with a descriptive
  message; nothing was squashed. Skimming `git log --oneline -60` gives the full sequence.

## What's done (high-level arc)

1. **In-place shard split** — full feature: metadata model (`SplitShardsMetadata`), routing/recovery
   wiring, engine-level materialization, operator REST action, automatic size/write-rate-driven
   trigger. Built in stages across Phase 0/1.
2. **In-place shard merge** (the inverse) — same staged build-out: metadata primitive → routing/
   recovery wiring → engine hook (real Lucene `addIndexes` fold over both children's filtered
   readers) → operator REST action → automatic trigger. Ends as a genuine two-phase operation with
   real automatic rollback if a revived parent fails to recover (children stay live until commit).
3. **A full-project correctness review pass** (four parallel deep-dive reviews: the split/merge fixes
   themselves under adversarial re-review, compaction rebase, the older resharding-by-copy mechanism,
   and scale-to-zero suspend/reactivate) — found and fixed:
   - **HIGH**: GC could delete a bundle a concurrent commit had just published (no safety window
     between two snapshots taken at different instants) — `ddb681dc2d8`.
   - **MEDIUM**: a search arriving during reader-shard cold-start could skip the reactivation wait
     entirely and fail client-visibly — `c59a7934c8d`.
   - **MEDIUM**: nothing blocked splitting a shard that was a live child of an in-progress merge,
     which could wedge the merge permanently — `ac3a74340dc`.
   - Two latent security bypasses: `writeBlob*WithMetadata` fell through both the encryption and the
     permission-scoping `BlobContainer` wrappers untouched (zero current callers, but a real landmine)
     — `07f9c2df07d`.
   - Smaller hardening: `applyCancel` re-validation on both split/merge commit services, real
     `clusterChanged` listener-dispatch test coverage (previously only exercised via direct method
     calls, never the actual registered listener) — `c4aea050155`, `1bb2faee730`.
   - Investigated and **confirmed safe** (not a bug, documented why): suspend eviction proceeding even
     when the final best-effort flush fails — `5edcc433c32`.
   - **Documented, not code-fixed** (a real, deliberate scope boundary, not an oversight): the older
     resharding-by-copy mechanism never fences the source index's writes during orchestration — an
     operator precondition now spelled out in the RFC and the transport action's own javadoc —
     `ae0ee3d25d4`.
   - Compaction rebase and the resharding-by-copy routing-hash design were both reviewed and found
     genuinely correct — no fix needed, findings not separately committed (see the conversation
     transcript if the reasoning is needed again; the short version: compaction's CAS-retry loop
     re-derives the merge fresh every attempt so no write is ever silently dropped, and
     resharding-by-copy's partition hash never needs to match core's `OperationRouting` since its
     targets are independent indices, unlike in-place split's child shards).
4. **WAL mirroring group-commit batching** — the most recent work. WAL mirroring
   (`serverless_storage.wal_mirroring.enabled`) previously cost ~1 object-store PUT per indexed
   document (synchronous per-op flush). Replaced with real batching by reusing core OpenSearch's own
   proven machinery instead of hand-rolling it: `BufferedAsyncIOProcessor` (the same group-commit
   primitive `RemoteFsTranslog` already uses) plus the existing `index.translog.durability`
   (REQUEST/ASYNC) setting — reused as-is to control whether a write's ack waits for the WAL upload,
   with zero new setting needed for that question. Landed in three phases, `05ba5b8ca2f` →
   `19edc4ee48c` → `c84505d06ef`. Proven with a real cost-accounting test: 201 ops across 4 shards
   cost 4 PUTs, not ~402. **Off by default** — `serverless_storage.wal_flush.batching.enabled`
   defaults false; flipping that default is explicitly deferred (documented as "Phase 3," not done)
   until real load testing confirms it.

## What's explicitly NOT done / open

A second full-project sweep (this session, after the list below was first written) re-checked every
item still open at that point, plus ran a fresh whole-plugin gap scan. Two of that scan's two largest
flagged items — WAL append-time fencing, and client write-routing cutover after split — turned out to
already be fully implemented; the scan was reading stale doc comments, not actual code state. Both
have since been corrected (`c3e6b34d898`, `f60da2b320d`). What's left, confirmed still genuinely open:

- **WAL batching Phase 3** — flipping `wal_flush.batching.enabled`'s default to `true`. Deliberately
  deferred: no real cloud object store or load generator exists in this environment to satisfy the
  RFC's own gating condition (`7d013dd7ceb`).
- **Resharding-by-copy source write-fencing** — documented as an operator precondition, not built.
  Building real fencing (or a dual-write bridge) would be a genuinely new mechanism, out of scope for
  a documentation pass. Re-confirmed still accurate (`dbde25b96be`).
- **Auto-triggering resharding-by-copy off a write-rate/size threshold** — `ShardSplitCandidatesAction`
  surfaces the signal; no scheduler acts on it. Distinct from the *in-place-split* mechanism's own
  auto-trigger, which is real and already wired (`f60da2b320d`).
- **Node-wide/per-shard-budget ordering for the 429 backpressure**, and **feeding the WAL backlog
  signal into autoscaling** (rfc-serverless-opensearch.md §10) — the shared `WalBatchingProcessor` is
  already node-wide by design, so there's no separate per-shard budget to order against on the
  batching path today.
- **Nested split-of-a-split merge-back** — deliberately guarded, not built (throws a clear error
  rather than silently misbehaving); confirmed the guard is now actually tested (`27fe367cf72`).
- Chaos/fuzz test coverage against real cloud-backend failure modes still limited to mock infra;
  event-driven (non-polling) commit notification remains a documented, non-blocking refinement whose
  milestone target is already met by the current 5s-poll + push-notification combination.

Resolved since the list above was first written: **byte-threshold early-flush trigger** for WAL
batching (`17bb9ce3dee`), **request-level 429 backpressure** for WAL upload backlog (`52324677588`),
**`plugins/serverless-storage/README.md`** (`c2c1625dadf`), and **unit test coverage for every
Rest*Action class** (`99a9266d04c`) are all done. Settings registered-vs-read, scale-to-zero/scale-up
lifecycle wiring, and commit-notification polling were all re-checked and confirmed to already be
correct/wired — no code change needed for those.
- The plugin has a real, substantial engineering base with correctness-reviewed core mechanisms, but
  has never been run against a real cloud object store at production scale, load-tested, or reviewed
  by anyone outside this session — see the "are we complete?" discussion in this session's transcript
  for the full honest gap list (durability tradeoffs with WAL mirroring off, timeout risk under
  degraded object-store latency, etc.) if that context is needed again.

## How to resume

1. `cd /home/tuong/work/OpenSearch && git status` — confirm clean tree, confirm branch.
2. Read `dynamic-partitioning-progress.md`'s section headers to find the relevant prior context.
3. Check `git log --oneline -20` for the most recent concrete state.
4. Everything in this session followed the same discipline: implement → write a real test → verify
   the test is meaningful (break the fix, confirm the test fails, restore) → run the relevant
   regression sweep → commit specific files (never `git add -A`). Keep following it.
