/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.IndexAlreadyExistsException;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.metadata.RepositoryMissingException;
import org.opensearch.serverless.metadata.SnapshotAlreadyExistsException;
import org.opensearch.serverless.metadata.SnapshotRecord;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code /_snapshot/{repo}/{snapshot}} — take, describe, list, delete, and {@code /_restore}.
 *
 * <p><b>Taking a snapshot costs no data movement.</b> Every file a captured shard's commit names is
 * already durable in the object store; this reads each named index's current published manifest and
 * writes one record referencing them, the same shallow shape a {@link org.opensearch.serverless.metadata
 * .PointInTime} already has. What it does not do is enumerate: {@code indices} must be named explicitly,
 * the same rule {@code _search} across several indices already follows, and for the same reason
 * {@code /_serverless/indices} is refused — this surface does not offer a listing over the deployment's
 * population.
 *
 * <p><b>Restoring is not free, and the reason is a real invariant, not an oversight.</b> This shell's
 * shard storage is keyed by an index's own uuid ({@code RegisterMap#shardData}), and a manifest's blob
 * paths are relative to that uuid's own container. A restore creates a genuinely new index — a fresh uuid,
 * by construction, because reusing one is the exact bug M32 closed for index deletion — so the blobs a
 * snapshot named have to be copied into the new index's own storage. The snapshot itself stays shallow;
 * restoring is the one operation that pays for it.
 */
public final class SnapshotHandler extends BaseRestHandler {

    private final Supplier<MetadataPlane> plane;
    private final Supplier<ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the node, whose action gate the plugins' filters live behind
     */
    public SnapshotHandler(Supplier<MetadataPlane> plane, Supplier<ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_snapshot_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.PUT, "/_snapshot/{repo}/{snapshot}"),
            new Route(RestRequest.Method.GET, "/_snapshot/{repo}/{snapshot}"),
            new Route(RestRequest.Method.DELETE, "/_snapshot/{repo}/{snapshot}"),
            new Route(RestRequest.Method.POST, "/_snapshot/{repo}/{snapshot}/_restore")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final MetadataPlane metadata = plane.get();
        final String repo = request.param("repo");
        final String snapshot = request.param("snapshot");
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final boolean restore = request.path().endsWith("/_restore");

        if (restore) {
            final Map<String, Object> body = request.hasContent() ? parseBody(request) : Map.of();
            return channel -> dispatch(channel, () -> handleRestore(channel, metadata, repo, snapshot, body));
        }
        if ("_all".equals(snapshot) && request.method() == RestRequest.Method.GET) {
            return channel -> dispatch(channel, () -> handleList(channel, metadata, repo));
        }

        switch (request.method()) {
            case PUT: {
                final Map<String, Object> body = request.hasContent() ? parseBody(request) : Map.of();
                final Object indicesField = body.get("indices");
                if ((indicesField instanceof String) == false || ((String) indicesField).isBlank()) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.BAD_REQUEST,
                            "missing_indices",
                            "a snapshot needs an 'indices' field naming what to capture, comma separated; "
                                + "this surface does not enumerate what exists"
                        )
                    );
                }
                final List<String> indices = List.of(((String) indicesField).split(","));
                return channel -> dispatch(channel, () -> handleCreate(channel, metadata, repo, snapshot, indices));
            }
            case GET:
                return channel -> dispatch(channel, () -> handleDescribe(channel, metadata, repo, snapshot));
            case DELETE:
                return channel -> dispatch(channel, () -> handleDelete(channel, metadata, repo, snapshot));
            default:
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.METHOD_NOT_ALLOWED,
                        "method_not_allowed",
                        request.method() + " is not supported here"
                    )
                );
        }
    }

    private void handleCreate(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        String repo,
        String snapshot,
        List<String> indexNames
    ) throws Exception {
        final Map<String, SnapshotRecord.SnapshottedIndex> captured = new LinkedHashMap<>();
        for (String indexName : indexNames) {
            final String trimmed = indexName.trim();
            final Optional<IndexDescriptor> descriptor = metadata.describe(trimmed);
            if (descriptor.isEmpty()) {
                channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + trimmed)
                );
                return;
            }
            final Map<Integer, CommitManifest> shards = new LinkedHashMap<>();
            for (int shard = 0; shard < descriptor.get().numberOfShards(); shard++) {
                final Optional<CommitManifest> manifest = metadata.segmentPublisher(trimmed, descriptor.get().uuid(), shard).readManifest();
                if (manifest.isEmpty()) {
                    // The same refusal a point in time gives for the same reason: a partial capture would
                    // silently cover only part of the index, which is exactly what this surface exists to
                    // make impossible rather than to explain after the fact.
                    channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.CONFLICT,
                            "nothing_published",
                            "shard " + shard + " of " + trimmed + " has published nothing yet, so there is no commit to capture"
                        )
                    );
                    return;
                }
                shards.put(shard, manifest.get());
            }
            captured.put(
                trimmed,
                new SnapshotRecord.SnapshottedIndex(
                    descriptor.get().uuid(),
                    descriptor.get().numberOfShards(),
                    descriptor.get().mapping(),
                    shards
                )
            );
        }

        try {
            gated(
                org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotAction.NAME,
                new org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest(repo, snapshot),
                () -> {
                    metadata.createSnapshot(new SnapshotRecord(repo, snapshot, metadata.clock().getAsLong(), captured));
                    return null;
                }
            );
        } catch (RepositoryMissingException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "repository_missing", e.getMessage()));
            return;
        } catch (SnapshotAlreadyExistsException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "snapshot_already_exists", e.getMessage()));
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("accepted", true);
            builder.field("repository", repo);
            builder.field("snapshot", snapshot);
            builder.field("indices", captured.keySet());
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void handleDescribe(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String repo, String snapshot)
        throws Exception {
        final Optional<SnapshotRecord> record = gated(
            org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsAction.NAME,
            new org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest(repo, new String[] { snapshot }),
            () -> metadata.snapshot(repo, snapshot)
        );
        if (record.isEmpty()) {
            channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "snapshot_missing", "no such snapshot: " + repo + "/" + snapshot)
            );
            return;
        }
        writeSnapshot(channel, record.get());
    }

    private void handleList(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String repo) throws Exception {
        final List<SnapshotRecord> records = gated(
            org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsAction.NAME,
            new org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest(repo),
            () -> metadata.listSnapshots(repo)
        );
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startArray("snapshots");
            for (SnapshotRecord record : records) {
                builder.startObject();
                builder.field("snapshot", record.name());
                builder.field("created_at", record.createdAtMillis());
                builder.field("indices", record.indices().keySet());
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private void handleDelete(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String repo, String snapshot)
        throws Exception {
        final boolean existed = gated(
            org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotAction.NAME,
            new org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest(repo, snapshot),
            () -> metadata.deleteSnapshot(repo, snapshot)
        );
        if (existed == false) {
            channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "snapshot_missing", "no such snapshot: " + repo + "/" + snapshot)
            );
            return;
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.field("snapshot", snapshot);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    @SuppressWarnings("unchecked")
    private void handleRestore(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        String repo,
        String snapshot,
        Map<String, Object> body
    ) throws Exception {
        final Optional<SnapshotRecord> record = metadata.snapshot(repo, snapshot);
        if (record.isEmpty()) {
            channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "snapshot_missing", "no such snapshot: " + repo + "/" + snapshot)
            );
            return;
        }
        final Map<String, Object> rename = body.get("rename") instanceof Map ? (Map<String, Object>) body.get("rename") : Map.of();

        gated(
            org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction.NAME,
            new org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest(repo, snapshot),
            () -> {
                doRestore(channel, metadata, record.get(), rename);
                return null;
            }
        );
    }

    /**
     * Restores every captured index independently, per-item outcome rather than all-or-nothing — the same
     * shape {@code _bulk} and {@code _mget} already have: one index that cannot be restored (its target
     * name is taken) must not cost the ones that can be.
     */
    private void doRestore(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        SnapshotRecord record,
        Map<String, Object> rename
    ) throws IOException {
        final BlobStore blobStore = metadata.blobStore();
        final BlobPath base = metadata.basePath();
        final Map<String, Object> outcomes = new LinkedHashMap<>();

        for (Map.Entry<String, SnapshotRecord.SnapshottedIndex> entry : record.indices().entrySet()) {
            final String originalName = entry.getKey();
            final SnapshotRecord.SnapshottedIndex captured = entry.getValue();
            final Object renamed = rename.get(originalName);
            final String targetName = renamed instanceof String && ((String) renamed).isBlank() == false ? (String) renamed : originalName;

            if (metadata.describe(targetName).isPresent()) {
                outcomes.put(originalName, Map.of("error", "an index named " + targetName + " already exists"));
                continue;
            }

            final String newUuid = UUIDs.randomBase64UUID();
            try {
                metadata.createIndex(new IndexDescriptor(targetName, newUuid, captured.numberOfShards(), captured.mapping(), null));
            } catch (IndexAlreadyExistsException e) {
                // Lost a race with a concurrent create between the check above and this. As real a
                // conflict as the name being taken already, reported the same way.
                outcomes.put(originalName, Map.of("error", "an index named " + targetName + " already exists"));
                continue;
            }

            for (Map.Entry<Integer, CommitManifest> shard : captured.shards().entrySet()) {
                final BlobPath sourceBase = RegisterMap.shardData(base, originalName, captured.uuid(), shard.getKey());
                final BlobPath destBase = RegisterMap.shardData(base, targetName, newUuid, shard.getKey());
                final CommitManifest restored = copyShardBlobs(blobStore, sourceBase, destBase, shard.getValue());
                blobStore.blobContainer(destBase)
                    .compareAndSwapRegister(SegmentPublisher.MANIFEST, BlobRegister.ABSENT_GENERATION, restored.toBytes());
            }
            outcomes.put(originalName, Map.of("restored_as", targetName, "shards", captured.numberOfShards()));
        }

        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("snapshot", record.name());
            builder.startObject("indices");
            for (Map.Entry<String, Object> outcome : outcomes.entrySet()) {
                builder.field(outcome.getKey(), outcome.getValue());
            }
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Copies every blob a captured shard's manifest names into a new shard's own storage, under one fresh
     * term container — a restored shard starts its own history rather than inheriting the captured one's
     * term numbering, which means nothing here.
     *
     * @param blobStore the backing store
     * @param sourceBase the captured index's own shard base path
     * @param destBase the new index's shard base path
     * @param source the captured manifest
     * @return the manifest to publish at the destination
     * @throws IOException if a referenced blob is missing, or copying fails
     */
    private CommitManifest copyShardBlobs(BlobStore blobStore, BlobPath sourceBase, BlobPath destBase, CommitManifest source)
        throws IOException {
        // Term 1, not 0: a shard-head's term must be positive (s0-findings.md F4 -- "primary term must be
        // positive" is load-bearing in the data plane, not an optimisation, and a reader opens through the
        // same machinery a writer does). A restored shard's history starts here regardless of what term
        // the snapshot captured; nothing downstream reads meaning into this specific number.
        final String destTermDir = SegmentPublisher.termSegment(1L);
        final BlobContainer destContainer = blobStore.blobContainer(destBase.add(destTermDir));
        final Map<String, String> destFiles = new LinkedHashMap<>();
        final Map<String, Map<String, BlobMetadata>> sourceListings = new LinkedHashMap<>();
        for (Map.Entry<String, String> file : source.files().entrySet()) {
            final String fileName = file.getKey();
            final String termDir = file.getValue();
            final BlobContainer sourceContainer = blobStore.blobContainer(sourceBase.add(termDir));
            Map<String, BlobMetadata> listing = sourceListings.get(termDir);
            if (listing == null) {
                listing = sourceContainer.listBlobs();
                sourceListings.put(termDir, listing);
            }
            final BlobMetadata sourceMeta = listing.get(fileName);
            if (sourceMeta == null) {
                throw new IOException("snapshot references a blob that no longer exists: " + termDir + "/" + fileName);
            }
            try (InputStream in = sourceContainer.readBlob(fileName)) {
                destContainer.writeBlob(fileName, in, sourceMeta.length(), false);
            }
            destFiles.put(fileName, destTermDir);
        }
        return new CommitManifest(1L, destFiles, null);
    }

    private Map<String, Object> parseBody(RestRequest request) throws IOException {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            return parser.map();
        }
    }

    /**
     * Runs work off the transport thread, or inline when there is no node to borrow a pool from.
     *
     * <p>Every operation here touches the object store — a snapshot's manifest reads, a restore's blob
     * copies — and the transport thread is not the thread to wait on remote IO from.
     */
    private void dispatch(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        final var serving = node.get();
        if (serving == null) {
            runQuietly(channel, work);
            return;
        }
        serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> runQuietly(channel, work));
    }

    private void runQuietly(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        try {
            work.run();
        } catch (Exception e) {
            try {
                channel.sendResponse(new BytesRestResponse(channel, e));
            } catch (IOException nested) {
                logger.error("failed to report a snapshot operation failure", nested);
            }
        }
    }

    private <T> T gated(
        String action,
        org.opensearch.action.ActionRequest request,
        org.opensearch.common.CheckedSupplier<T, Exception> work
    ) throws Exception {
        final var serving = node.get();
        return serving == null ? work.get() : serving.actionGate().run(action, request, work);
    }

    private void writeSnapshot(org.opensearch.rest.RestChannel channel, SnapshotRecord record) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("repository", record.repo());
            builder.field("snapshot", record.name());
            builder.field("created_at", record.createdAtMillis());
            builder.startObject("indices");
            for (Map.Entry<String, SnapshotRecord.SnapshottedIndex> index : record.indices().entrySet()) {
                builder.startObject(index.getKey());
                builder.field("uuid", index.getValue().uuid());
                builder.field("shards", index.getValue().numberOfShards());
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }
}
