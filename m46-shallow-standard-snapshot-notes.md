# M46 (finished) — shallow and standard snapshot, API-compatible with real OpenSearch

M45 shipped one snapshot mode: shallow, referencing a shard's own live blobs, costing no data movement to
take. This adds the other real mode — standard, copying a shard's blobs into the repository's own storage
at capture time — as a repository setting exactly as real OpenSearch has it, and reshapes the whole REST
surface's request and response bodies to match real OpenSearch's `_snapshot` API rather than the ad hoc
shapes M45 invented. What real OpenSearch's `_snapshot` API actually does was verified against its source
(`server/src/main/java/org/opensearch/action/admin/cluster/{repositories,snapshots}/**`,
`RestoreSnapshotRequest.java`, `SnapshotInfo.java`), not assumed from memory.

## The scope decision restated, now that both modes exist

M45 already decided a repository is a namespace within this deployment's own object store, not a distinct
storage backend (D3: one object store). That does not change here. What changes is that "shallow vs.
standard" turns out to be **orthogonal** to "which backend" — real OpenSearch's own `remote_store_index_
shallow_copy` toggles between referencing a remote store's blobs and copying them into repository storage,
and both happen on the *same* repository, the same backend, every time. So delivering "both shallow and
standard, API-compatible" did not require solving cross-backend repositories at all — it required adding
the toggle real OpenSearch already has, and it slots into the same single-object-store design M45 already
built. Cross-backend repositories (a repo genuinely on S3 while the deployment runs on GCS) remain
unbuilt and out of scope — see "What this does NOT establish."

## `remote_store_index_shallow_copy` is a repository setting, not a per-snapshot choice

`PUT /_snapshot/{repo}` accepts `{"type": "fs", "settings": {"remote_store_index_shallow_copy": true}}` —
the real request shape (`PutRepositoryRequest.java`). `type` is accepted for compatibility; only
`RepositoryDescriptor.TYPE_NATIVE` ("fs") is functional, and any other value is refused with 501 rather
than silently treated as a synonym — a caller naming S3 or GCS explicitly is asking for something this
does not do. The default, matching real OpenSearch's own default for this setting, is **false — standard**.
Every snapshot taken against a repository inherits its mode at capture time and keeps it for its own
lifetime, even if the repository's setting is later changed (`SnapshotRecord` records `shallow` on itself,
the same way a `CommitManifest` records its own `writer` rather than trusting a caller to still agree
later).

## Standard capture: real cost, real independence

A standard snapshot copies each shard's blobs into `RegisterMap#snapshotShardData` — a path keyed by the
repository and snapshot's own names, not any live index's — at capture time, using the same generic
`copyShardBlobs` routine restore already needed (read-then-write through the `BlobContainer` interface,
scoped exactly to the bytes a shard's manifest names). The payoff: a standard snapshot's data is entirely
independent of the index it was taken from. Deleting that index doesn't touch it. The garbage collector
sweeping that index's shard doesn't touch it, because neither one ever lists `snapshot-data/`.
`testAStandardSnapshotSurvivesIndexDeletionWithNoSpecialProtectionNeeded` proves this by running the exact
scenario the shallow-mode protection (below) exists for, and finding nothing to protect against.

**Deleting a standard snapshot has to reclaim its own storage.** Unlike a shallow snapshot, where deleting
the record is the whole of it, a standard snapshot's bytes belong to nothing else — `MetadataPlane
#deleteSnapshot` now purges `snapshotShardData` for every captured shard when the snapshot itself is
non-shallow, or that storage would leak forever, unreachable and unrecoverable, the moment the record
naming it is gone.

## A real bug found while building this: index deletion could destroy a shallow snapshot's data

Scoping standard mode meant asking, precisely, what shallow mode depends on that standard mode doesn't —
and the answer exposed a gap in M45 itself: `MetadataPlane#deleteIndex`'s `purgeShardData` deleted a
shard's storage unconditionally. A shallow snapshot's entire argument for costing nothing at capture time
is that it references the *live* shard's own blobs. Taking a snapshot and then deleting the index it was
taken from — the ordinary shape a backup is used for — silently destroyed the backup along with the index.
The garbage collector already knew to check `SnapshotRecord#referencedBlobs` before sweeping; index
deletion never asked the same question. Fixed by extending the identical uuid-keyed check into
`purgeShardData`: a shard a live snapshot still pins is skipped rather than purged, left as ordinary
orphaned storage the moment its last pinning snapshot is deleted — reclaimable the same way any other
orphan already is. `testDeletingAnIndexDoesNotDestroyWhatALiveShallowSnapshotIsHolding` is the regression
test; canary 136 (the check disabled) is caught, loudly — the follow-on restore fails with "blob no longer
exists" rather than silently returning corrupt data, the same shape canary 133 caught for the collector's
own sweep.

## The REST surface now matches real OpenSearch's shapes, verified against source

- **Repository PUT is an upsert**, not create-once: real OpenSearch's own repository PUT updates settings
  on a repeat call rather than refusing it, and `MetadataPlane#putRepository` now matches (a plain
  overwrite, `failIfAlreadyExists=false` — no CAS needed, since there is no concurrent-writer race this
  needs to arbitrate the way index creation does).
- **`GET /_snapshot`** (no name), **`GET /_snapshot/_all`**, and a comma-separated repository list are all
  supported — real OpenSearch's own bare `GET _snapshot` is exactly this listing, unconditionally, and a
  repository population is operator-scale (a handful, hand-registered) the same way `/_cluster/settings`
  is allowed while `/_serverless/indices` is refused for a genuinely unbounded one.
- **`indices` accepts a comma string or a JSON array**, matching `CreateSnapshotRequest`/
  `RestoreSnapshotRequest`'s own parsing.
- **`wait_for_completion` defaults to `false`**, matching real OpenSearch, but every operation here is
  synchronous regardless — there is no task registry to run one against in the background. The parameter
  only changes which response shape is sent (`{"accepted": true}` vs. the full `SnapshotInfo`/`RestoreInfo`
  shape); the work is always fully done before either response is sent.
  `testWaitForCompletionFalseStillCompletesSynchronously` asserts exactly this: a caller that doesn't wait
  still finds the snapshot fully captured the moment it asks.
- **`rename_pattern`/`rename_replacement` is a Java regex applied via `String#replaceAll`**, capture
  groups included (`$1`), not the literal name-to-name map M45 shipped —
  `RestoreService.java:1370-1383`'s exact mechanism. `testRenamePatternAppliesARegexNotALiteralMap` proves
  a capture group substitutes correctly; canary 140 (rename ignored) is caught by five tests at once.
- **A restore's target-name collisions refuse the whole restore**, not one item of a per-item outcome —
  matching real OpenSearch, which validates every target name before restoring any of them. M45's
  per-item-outcome design is gone; canary 139 (the up-front check removed) shows why it still matters even
  though `createIndex`'s own uniqueness check is a backstop: without the up-front validation the same
  collision still gets refused, just as an uncaught 500 instead of a clean 400 — the up-front check exists
  for the error's *shape*, not for basic safety that was already there.
- **Response envelopes match real OpenSearch's field names**: `GET` always returns `{"snapshots": [...]}`,
  even for one name; a repository's `GET` is keyed by name (`{"backups": {"type": ..., "settings": {...}}}`
  ), not a flat object; a completed snapshot's response nests under `"snapshot"` with `uuid`, `version`/
  `version_id` (`Version.CURRENT`, which is genuinely accurate here — this shell *is* built on a real
  OpenSearch core version), `state`, `start_time`/`end_time` (both the ISO-8601 string and the
  `_in_millis` form), `duration_in_millis`, `shards.{total,failed,successful}`.

## Gating: the whole operation, not just the final write — a second real bug found

Extending `ServerlessAuthorizationTests#testEveryDataEndpointReachesAFilter` to cover the seven new action
names surfaced that `handleCreate` and `handleRestore` were **not** actually following this surface's own
established rule. Both did their read work — resolving a repository, gathering manifests, resolving and
validating restore targets — *before* calling `gated(...)`, with only the final write wrapped. A caller
refused before reaching that final step (missing repository, unpublished shard, a snapshot that doesn't
exist) never reached a filter at all, silently exempt from any privilege evaluator watching this surface —
exactly the shape `update` and `delete-by-query` were built to avoid from the start. Both handlers are
restructured so the entire operation — every read, every validation, the write — runs inside one
`gated(...)` call, matching `UpdateHandler`'s own shape: business-logic failures are now thrown as
exceptions from inside the gate and caught once, after it returns, rather than answered by an early
`return` before the gate is ever reached. Canary 142 (capture moved back outside the gate) is caught by
the authorization sweep, not by any snapshot-specific test — which is the point of that sweep existing at
all.

## Canaries

- **136** (M45, revisited) — the delete-index snapshot-pin check, still correct, still caught the same way.
- **137 — standard-mode copy at capture time disabled**, capturing shallow regardless of the repository's
  setting. Caught by four tests, each failing loudly on restore ("blob no longer exists") rather than
  quietly serving stale or missing data.
- **138 — standard-snapshot storage cleanup on delete disabled.** Caught by
  `testDeletingAStandardSnapshotReclaimsItsOwnStorage`.
- **139 — restore's up-front target-collision/already-exists validation removed.** Caught: the same
  collision is still refused, but as an uncaught 500 instead of the intended 400 — proving the check's
  value is in the error's shape, not in safety `createIndex`'s own uniqueness guarantee didn't already have.
- **140 — `rename_pattern`/`rename_replacement` ignored**, restoring under the original name regardless.
  Caught by five tests across create, sweep-protection and rename-specific coverage.
- **141 — repository PUT reverted to refuse re-registration** instead of upserting. Caught by
  `testARepositoryIsIdempotentlyRegisteredAndDescribed`.
- **142 — a composite operation's read/validation work moved back outside its gate**, reproducing the
  exact defect this milestone found and fixed. Caught by `ServerlessAuthorizationTests`, not by any
  snapshot test — the sweep test's entire reason for existing.

Seven planted, seven caught (six new to M46, plus M45's canary 136 reconfirmed), two of them (133-shaped
and 139) demonstrating that a check already backstopped by something else downstream still has independent
value — for a cleaner failure mode in one case, for a completely different failure class in the other.

## What this does NOT establish

- **No cross-backend repositories.** A repository is still this deployment's own object store, always —
  `type` values other than `"fs"` are refused with 501, not silently accepted as a synonym. This is the
  scope decision restated above, not a gap discovered late: shallow-vs-standard turned out orthogonal to
  it, and closing it is a materially larger, separate piece of work (wiring a second `BlobStore` per
  repository, most likely reusing `S3RepositoryPlugin`/a GCS equivalent the same way M14 reused
  `S3RepositoryPlugin` for this deployment's own store) that nothing here attempts.
- **No incremental/deduplicated snapshots**, standard or shallow. Every capture is a full, independent read
  of each named index's current commits; two snapshots of an unchanged index reference or copy the same
  blobs by coincidence of Lucene's own immutability, not because anything here tracks it.
- **No `include_aliases`, `index_settings`, `ignore_index_settings`, `storage_type`,
  `source_remote_store_repository`/`source_remote_translog_repository`, or `alias_write_index_policy`
  behaviour.** All are accepted on a restore request (an unrecognized key is still refused, matching real
  OpenSearch's own strictness) but are no-ops — this shell has no aliases captured by a snapshot yet, no
  per-restore settings overrides, and no second storage tier to choose between.
- **No `_status` progress endpoint**, because nothing here runs long enough in the background to need one
  — every operation, standard-mode blob copies included, completes inline before its response is sent.
- **An unreadable snapshot record still does not pin its blobs**, the gap M45 already disclosed rather
  than fixed. Unchanged here.

M46 is done.
