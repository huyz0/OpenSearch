/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.rest;

import org.opensearch.Version;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.ConditionalBlobContainer;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.IndexAlreadyExistsException;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.metadata.RepositoryDescriptor;
import org.opensearch.serverless.metadata.RepositoryMissingException;
import org.opensearch.serverless.metadata.SnapshotAlreadyExistsException;
import org.opensearch.serverless.metadata.SnapshotRecord;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Runs the snapshot requests {@link SnapshotHandler} parsed: creating a snapshot by capturing each index's published
 * commits, describing, listing and deleting snapshots, and restoring one into new indices.
 */
final class SnapshotOperations {

    private final SnapshotHandler handler;

    SnapshotOperations(SnapshotHandler handler) {
        this.handler = handler;
    }

    /**
     * Takes a snapshot: index resolution, manifest gathering, a standard capture's blob copy and the
     * final record write all run inside one gate, under one action name — not just the write at the end.
     * A privilege evaluator that only saw the write would never see a caller who was refused before
     * reaching it, and every composite operation on this surface (update, delete-by-query, restore) makes
     * the same choice for the same reason.
     */
    void handleCreate(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        String repo,
        String snapshot,
        List<String> indexNames,
        boolean ignoreUnavailable,
        boolean partial,
        boolean includeGlobalState,
        boolean waitForCompletion
    ) throws Exception {
        final SnapshotRecord record;
        try {
            record = handler.gated(
                org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotAction.NAME,
                new org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest(repo, snapshot),
                // capture() writes the record itself: the provisional one before a manifest is read, and the
                // finished one over it. A second createSnapshot here found its own record and refused.
                () -> capture(metadata, repo, snapshot, indexNames, ignoreUnavailable, partial)
            );
        } catch (RepositoryMissingException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "repository_missing_exception", e.getMessage()));
            return;
        } catch (SnapshotHandler.IndexNotFoundForOperationException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", e.getMessage()));
            return;
        } catch (SnapshotHandler.SystemIndexException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.FORBIDDEN, "system_index", e.getMessage()));
            return;
        } catch (SnapshotHandler.NothingPublishedException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.CONFLICT, "nothing_published", e.getMessage()));
            return;
        } catch (SnapshotAlreadyExistsException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "snapshot_already_exists", e.getMessage()));
            return;
        }

        if (waitForCompletion == false) {
            handler.respondAccepted(channel);
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startObject("snapshot");
            writeSnapshotInfo(builder, record, includeGlobalState);
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Resolves every named index's current commit and, for a standard repository, copies it into the
     * repository's own storage. Runs entirely inside {@link #handleCreate}'s gate; everything it throws is
     * caught there, once, after the gate returns.
     *
     * <p><b>The record is written before a manifest is read.</b> First every index is described, then a
     * provisional record naming each one -- uuid, shard count, shape, and no commits -- is written under
     * put-if-absent, and only then are the manifests read and, for a standard repository, the blobs
     * copied. The provisional record is what {@code MetadataPlane#purgeShardData} and the collector consult,
     * so an index deleted during the capture keeps its shard data until the capture has finished with it.
     * Without it a delete landing after the manifests were read and before the record existed purged
     * exactly the blobs a {@code SUCCESS} snapshot then named, and the first restore was the first anyone
     * heard of it. Writing the record first also settles the name before any blob is copied: a duplicate
     * name is refused before it pays for the copy, and never writes into the live snapshot's storage.
     *
     * <p>A capture that fails after the provisional record deletes it, and with it whatever a standard
     * repository had copied so far, so a failed attempt leaves no storage that nothing names.
     */
    SnapshotRecord capture(
        MetadataPlane metadata,
        String repo,
        String snapshot,
        List<String> indexNames,
        boolean ignoreUnavailable,
        boolean partial
    ) throws Exception {
        final Optional<RepositoryDescriptor> repoDescriptor = metadata.describeRepository(repo);
        if (repoDescriptor.isEmpty()) {
            throw new RepositoryMissingException(repo);
        }
        final boolean shallow = repoDescriptor.get().shallowByDefault();
        final long startTime = metadata.clock().getAsLong();
        final BlobStore blobStore = metadata.blobStore();
        final ServerlessNode serving = handler.node.get();

        // Every index described first: what the provisional record needs, and where a name is refused
        // before anything has been written.
        final Map<String, IndexDescriptor> described = new LinkedHashMap<>();
        for (String raw : indexNames) {
            final String trimmed = raw.trim();
            if (serving != null && serving.isSystemIndex(trimmed)) {
                // A plugin's own index. Captured and restored under another name, its records -- a
                // credential store's password hashes -- would be readable through the ordinary API, which is
                // exactly what declaring it a system index exists to prevent.
                throw new SnapshotHandler.SystemIndexException(trimmed);
            }
            final Optional<IndexDescriptor> descriptor = metadata.describe(trimmed);
            if (descriptor.isEmpty()) {
                if (ignoreUnavailable) {
                    continue;
                }
                throw new SnapshotHandler.IndexNotFoundForOperationException(trimmed);
            }
            described.put(trimmed, descriptor.get());
        }

        final String snapshotUuid = UUIDs.randomBase64UUID();
        final Map<String, SnapshotRecord.SnapshottedIndex> provisional = new LinkedHashMap<>();
        for (Map.Entry<String, IndexDescriptor> index : described.entrySet()) {
            provisional.put(
                index.getKey(),
                new SnapshotRecord.SnapshottedIndex(
                    index.getValue().uuid(),
                    index.getValue().numberOfShards(),
                    index.getValue().mapping(),
                    SnapshotBlobs.settingsOf(index.getValue()),
                    Map.of()
                )
            );
        }
        final SnapshotRecord reservation = metadata.createSnapshot(
            new SnapshotRecord(repo, snapshot, snapshotUuid, shallow, startTime, startTime, provisional)
        );

        try {
            // The provisional pin is visible before verifying the incarnation. An orphan sweep that
            // already observed the index's deletion can therefore neither miss a valid new capture nor
            // delete data a capture started from a stale descriptor is about to read.
            for (Map.Entry<String, IndexDescriptor> index : described.entrySet()) {
                metadata.verifyCaptureIncarnation(index.getKey(), index.getValue().uuid());
            }
            final Map<String, SnapshotRecord.SnapshottedIndex> captured = new LinkedHashMap<>();
            for (Map.Entry<String, IndexDescriptor> index : described.entrySet()) {
                final String trimmed = index.getKey();
                final IndexDescriptor descriptor = index.getValue();
                final Map<Integer, CommitManifest> shards = new LinkedHashMap<>();
                boolean complete = true;
                for (int shard = 0; shard < descriptor.numberOfShards(); shard++) {
                    final Optional<CommitManifest> manifest = metadata.segmentPublisher(trimmed, descriptor.uuid(), shard).readManifest();
                    if (manifest.isEmpty()) {
                        if (partial) {
                            complete = false;
                            break;
                        }
                        throw new SnapshotHandler.NothingPublishedException(trimmed, shard);
                    }
                    shards.put(shard, manifest.get());
                }
                if (complete == false) {
                    // partial=true and this index could not be captured in full -- skipped rather than
                    // failing the whole snapshot, the one meaning "partial" has here: less than requested,
                    // never a lie about what was captured.
                    continue;
                }

                final Map<Integer, CommitManifest> stored;
                if (shallow) {
                    stored = shards;
                } else {
                    stored = new LinkedHashMap<>();
                    for (Map.Entry<Integer, CommitManifest> shard : shards.entrySet()) {
                        final BlobPath sourceBase = RegisterMap.shardData(metadata.basePath(), trimmed, descriptor.uuid(), shard.getKey());
                        final BlobPath destBase = RegisterMap.snapshotShardData(metadata.basePath(), reservation, trimmed, shard.getKey());
                        stored.put(shard.getKey(), SnapshotBlobs.copyShardBlobs(blobStore, sourceBase, destBase, shard.getValue()));
                    }
                }
                captured.put(
                    trimmed,
                    new SnapshotRecord.SnapshottedIndex(
                        descriptor.uuid(),
                        descriptor.numberOfShards(),
                        descriptor.mapping(),
                        SnapshotBlobs.settingsOf(descriptor),
                        stored
                    )
                );
            }

            final long endTime = metadata.clock().getAsLong();
            final SnapshotRecord finished = new SnapshotRecord(repo, snapshot, snapshotUuid, shallow, startTime, endTime, captured)
                .withStorageGeneration(reservation.storageGeneration());
            metadata.finishSnapshot(finished);
            return finished;
        } catch (Exception e) {
            // The provisional record, and whatever a standard repository had copied so far. The record
            // names no shards yet, so deleting it alone would leave the copies unnamed and unreachable.
            try {
                metadata.deleteSnapshot(reservation);
            } catch (Exception cleanup) {
                e.addSuppressed(cleanup);
            }
            if (shallow == false) {
                try {
                    blobStore.blobContainer(RegisterMap.snapshotData(metadata.basePath(), reservation.storageGeneration())).delete();
                } catch (Exception cleanup) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        }
    }

    void handleDescribe(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String repo, String snapshot) throws Exception {
        final Optional<SnapshotRecord> record = handler.gated(
            org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsAction.NAME,
            new org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest(repo, new String[] { snapshot }),
            () -> metadata.snapshot(repo, snapshot)
        );
        if (record.isEmpty()) {
            channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.NOT_FOUND,
                    "snapshot_missing_exception",
                    "no such snapshot: " + repo + "/" + snapshot
                )
            );
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray("snapshots");
            builder.startObject();
            writeSnapshotInfo(builder, record.get(), true);
            builder.endObject();
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * @param only the snapshot names to include, or null for every snapshot in the repository
     */
    void handleList(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String repo, List<String> only) throws Exception {
        final List<SnapshotRecord> records = handler.gated(
            org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsAction.NAME,
            new org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest(repo),
            () -> metadata.describeRepository(repo).isEmpty() ? null : metadata.listSnapshots(repo)
        );
        if (records == null) {
            // Core's answer for a repository that is not registered, rather than an empty list that reads
            // as "registered and holding nothing".
            channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "repository_missing_exception", "[" + repo + "] missing")
            );
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray("snapshots");
            for (SnapshotRecord record : records) {
                if (only != null && only.contains(record.name()) == false) {
                    continue;
                }
                builder.startObject();
                writeSnapshotInfo(builder, record, true);
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    void handleDelete(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String repo, List<String> names) throws Exception {
        for (String name : names) {
            final boolean existed = handler.gated(
                org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction.NAME,
                new org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest(repo, name),
                () -> metadata.deleteSnapshot(repo, name)
            );
            if (existed == false) {
                channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.NOT_FOUND,
                        "snapshot_missing_exception",
                        "no such snapshot: " + repo + "/" + name
                    )
                );
                return;
            }
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    @SuppressWarnings("unchecked")
    /**
     * Restores a snapshot: reading the record, resolving and validating every target name, and the whole
     * copy all run inside one gate, under {@code RestoreSnapshotAction}'s name — including the snapshot
     * lookup itself, so a restore attempt against a snapshot that turns out not to exist is still an
     * attempt a privilege evaluator sees, the same choice {@link #handleCreate} makes for the same reason.
     */
    void handleRestore(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        String repo,
        String snapshot,
        Map<String, Object> body,
        boolean waitForCompletion
    ) throws Exception {
        final RestoreOutcome outcome;
        try {
            outcome = handler.gated(
                org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction.NAME,
                new org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest(repo, snapshot),
                () -> restore(metadata, repo, snapshot, body)
            );
        } catch (SnapshotHandler.SnapshotMissingException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "snapshot_missing_exception", e.getMessage()));
            return;
        } catch (SnapshotHandler.IndexNotCapturedException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", e.getMessage()));
            return;
        } catch (SnapshotHandler.IndexNameCollisionException | IndexAlreadyExistsException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "index_already_exists", e.getMessage()));
            return;
        } catch (SnapshotHandler.SystemIndexException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.FORBIDDEN, "system_index", e.getMessage()));
            return;
        } catch (SnapshotHandler.SnapshotInProgressException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.CONFLICT, "snapshot_in_progress", e.getMessage()));
            return;
        } catch (SnapshotHandler.RestoreConflictException e) {
            // Reported, not counted as a success. The manifest compare-and-swap result used to be
            // discarded, so a shard that a writer had already published on the fresh index stayed empty
            // behind {"failed": 0}, with the copied blobs unreferenced in the live term.
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.CONFLICT, "restore_conflict", e.getMessage()));
            return;
        }

        if (waitForCompletion == false) {
            handler.respondAccepted(channel);
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startObject("snapshot");
            builder.field("snapshot", snapshot);
            builder.field("indices", outcome.targets().values());
            builder.startObject("shards");
            builder.field("total", outcome.totalShards());
            builder.field("failed", 0);
            builder.field("successful", outcome.totalShards());
            builder.endObject();
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /** What one restore did: every source index's target name, and how many shards it copied. */
    record RestoreOutcome(Map<String, String> targets, int totalShards) {
    }

    /**
     * Resolves the snapshot, validates every target name (no rename collision, no existing index in the
     * way), then creates and populates each restored index. Runs entirely inside {@link #handleRestore}'s
     * gate; everything it throws is caught there, once, after the gate returns.
     */
    RestoreOutcome restore(MetadataPlane metadata, String repo, String snapshot, Map<String, Object> body) throws Exception {
        final Optional<SnapshotRecord> found = metadata.snapshot(repo, snapshot);
        if (found.isEmpty()) {
            throw new SnapshotHandler.SnapshotMissingException(repo, snapshot);
        }
        final SnapshotRecord record = found.get();
        if (MetadataPlane.inProgress(record)) {
            throw new SnapshotHandler.SnapshotInProgressException(repo, snapshot);
        }
        final ServerlessNode serving = handler.node.get();

        final List<String> requested = body.containsKey("indices")
            ? SnapshotRequestBodies.readIndices(body.get("indices"))
            : List.copyOf(record.indices().keySet());
        for (String name : requested) {
            if (record.indices().containsKey(name) == false) {
                throw new SnapshotHandler.IndexNotCapturedException(name);
            }
        }

        final Object patternField = body.get("rename_pattern");
        final Object replacementField = body.get("rename_replacement");
        final boolean rename = patternField != null && replacementField != null;
        final Pattern pattern = rename ? Pattern.compile(String.valueOf(patternField)) : null;
        final String replacement = rename ? String.valueOf(replacementField) : null;

        // Every target name resolved and validated before anything is restored -- matching real
        // OpenSearch, which refuses the whole restore rather than half of it if a target collides.
        final Map<String, String> targets = new LinkedHashMap<>();
        final Map<String, String> byTarget = new LinkedHashMap<>();
        for (String original : requested) {
            final String target = rename ? pattern.matcher(original).replaceAll(replacement) : original;
            final String collidingSource = byTarget.put(target, original);
            if (collidingSource != null) {
                throw new SnapshotHandler.IndexNameCollisionException(collidingSource, original, target);
            }
            if (serving != null && (serving.isSystemIndex(original) || serving.isSystemIndex(target))) {
                // Neither a captured system index nor a target under a system-index pattern: the first is
                // a plugin's records restored somewhere readable, the second a name minted under a pattern
                // the plugin owns, on a path PUT /{index}'s guard never sees.
                throw new SnapshotHandler.SystemIndexException(serving.isSystemIndex(original) ? original : target);
            }
            if (metadata.describe(target).isPresent()) {
                throw new IndexAlreadyExistsException(target);
            }
            targets.put(original, target);
        }

        final BlobStore blobStore = metadata.blobStore();
        // Every target created so far, so a restore that fails part-way removes what it made rather than
        // leaving earlier targets restored and the current one with nothing published behind its name.
        final Map<String, String> created = new LinkedHashMap<>();
        int shardsRestored = 0;
        try {
            for (Map.Entry<String, String> restoring : targets.entrySet()) {
                final SnapshotRecord.SnapshottedIndex capturedIndex = record.indices().get(restoring.getKey());
                final String newUuid = UUIDs.randomBase64UUID();
                // The settings the index had when it was captured, restored with it: the analyzers a mapping
                // names live there, and a restored index without them could neither index nor search.
                final org.opensearch.common.settings.Settings settings = SnapshotBlobs.restorableSettings(capturedIndex.settings());
                metadata.createIndex(
                    new IndexDescriptor(restoring.getValue(), newUuid, capturedIndex.numberOfShards(), capturedIndex.mapping(), settings)
                        .createdAt(metadata.clock().getAsLong(), org.opensearch.Version.CURRENT.id)
                );
                created.put(restoring.getValue(), newUuid);
                for (Map.Entry<Integer, CommitManifest> shard : capturedIndex.shards().entrySet()) {
                    final BlobPath sourceBase = record.shallow()
                        ? RegisterMap.shardData(metadata.basePath(), restoring.getKey(), capturedIndex.uuid(), shard.getKey())
                        : RegisterMap.snapshotShardData(metadata.basePath(), record, restoring.getKey(), shard.getKey());
                    final BlobPath destBase = RegisterMap.shardData(metadata.basePath(), restoring.getValue(), newUuid, shard.getKey());
                    final CommitManifest restored = SnapshotBlobs.copyShardBlobs(blobStore, sourceBase, destBase, shard.getValue());
                    // Entered before the manifest lands, with a digest that rules nothing out: a restored index holds
                    // documents no writer marked, and an entry a deleted index left under the name would otherwise
                    // let a search rule the restored documents out -- or leave the index off a pattern's name list.
                    metadata.rollups()
                        .widen(
                            restoring.getValue(),
                            newUuid,
                            capturedIndex.numberOfShards(),
                            shard.getKey(),
                            org.opensearch.serverless.store.PruningDigest.EMPTY
                        );
                    // The manifest lands only if nothing has published on the new index yet. A writer that
                    // activated the shard between the descriptor's creation and this swap has already
                    // published a commit of its own, and the restored commit must not be reported as in
                    // place when it is not.
                    final var landed = ConditionalBlobContainer.of(blobStore.blobContainer(destBase))
                        .compareAndSwapRegister(SegmentPublisher.MANIFEST, BlobRegister.ABSENT_GENERATION, restored.toBytes());
                    if (landed.applied() == false) {
                        throw new SnapshotHandler.RestoreConflictException(restoring.getValue(), shard.getKey());
                    }
                    shardsRestored++;
                }
            }
        } catch (Exception e) {
            for (Map.Entry<String, String> target : created.entrySet()) {
                try {
                    metadata.deleteIndex(target.getKey(), target.getValue());
                } catch (Exception cleanup) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        }
        return new RestoreOutcome(targets, shardsRestored);
    }

    void writeSnapshotInfo(XContentBuilder builder, SnapshotRecord record, boolean includeGlobalState) throws IOException {
        builder.field("snapshot", record.name());
        builder.field("uuid", record.uuid());
        builder.field("version_id", Version.CURRENT.id);
        builder.field("version", Version.CURRENT.toString());
        builder.field("remote_store_index_shallow_copy", record.shallow());
        builder.field("indices", record.indices().keySet());
        builder.startArray("data_streams").endArray();
        builder.field("include_global_state", includeGlobalState);
        // A record whose indices have no commits yet is a capture still running -- see
        // MetadataPlane.capturing -- and says so rather than reporting a success with no shards.
        builder.field("state", MetadataPlane.inProgress(record) ? "IN_PROGRESS" : "SUCCESS");
        builder.field("start_time", Instant.ofEpochMilli(record.startTimeMillis()).toString());
        builder.field("start_time_in_millis", record.startTimeMillis());
        builder.field("end_time", Instant.ofEpochMilli(record.endTimeMillis()).toString());
        builder.field("end_time_in_millis", record.endTimeMillis());
        builder.field("duration_in_millis", record.endTimeMillis() - record.startTimeMillis());
        builder.startArray("failures").endArray();
        final int shards = record.indices().values().stream().mapToInt(i -> i.shards().size()).sum();
        builder.startObject("shards");
        builder.field("total", shards);
        builder.field("failed", 0);
        builder.field("successful", shards);
        builder.endObject();
    }
}
