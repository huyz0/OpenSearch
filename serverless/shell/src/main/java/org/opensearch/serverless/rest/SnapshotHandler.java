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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.core.common.bytes.BytesReference;
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
import org.opensearch.serverless.metadata.RepositoryDescriptor;
import org.opensearch.serverless.metadata.RepositoryMissingException;
import org.opensearch.serverless.metadata.SnapshotAlreadyExistsException;
import org.opensearch.serverless.metadata.SnapshotRecord;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * {@code /_snapshot/{repo}/{snapshot}} — take, describe, list, delete, and {@code /_restore}, with the
 * same request and response shapes real OpenSearch's snapshot API uses.
 *
 * <p><b>Shallow or standard is a repository setting, not a per-request choice</b> — see
 * {@link RepositoryDescriptor#SHALLOW_SETTING} — so every snapshot taken against one repository behaves
 * the same way, matching real OpenSearch's own {@code remote_store_index_shallow_copy}.
 *
 * <p><b>Shallow capture costs no data movement.</b> It reads each named index's current published
 * manifest and writes one record referencing blobs that already exist, the same shallow shape a
 * {@link org.opensearch.serverless.metadata.PointInTime} already has.
 *
 * <p><b>Standard capture copies each shard's blobs into the repository's own storage at capture time</b>
 * ({@code RegisterMap#snapshotShardData}) — real cost, in exchange for a snapshot whose lifecycle owes
 * nothing to the index it was taken from: deleting that index, or the garbage collector sweeping its
 * shard, never touches this storage, because neither one lists it.
 *
 * <p><b>Restoring always creates a new index.</b> This shell's shard storage is keyed by an index's own
 * uuid, so a restore mints a fresh one — reusing one is the exact bug M32 closed — and copies whatever the
 * snapshot's mode requires: a shallow snapshot's source is the original index's own storage, a standard
 * snapshot's source is its own repository-scoped copy.
 *
 * <p><b>Every operation here is synchronous</b>, {@code wait_for_completion} included: this shell has no
 * background task registry to run one asynchronously against, so the work is always done in full before a
 * response is sent, and {@code wait_for_completion=false} only changes which response shape is sent — the
 * caller sees the same outcome either way, sooner than an async caller polling a real cluster would.
 */
public final class SnapshotHandler extends BaseRestHandler {

    /** What a restore here reads. Everything else core's restore accepts is refused with its reason, below. */
    private static final Set<String> RESTORE_KNOWN_KEYS = Set.of("indices", "rename_pattern", "rename_replacement");

    /**
     * Core's other restore fields, each with why it is refused rather than accepted and ignored. An
     * allowlist used to admit all of these and act on three, which told a caller -- by answering 400 for
     * an unknown key -- that a known key was honoured.
     */
    private static final java.util.Map<String, String> RESTORE_REFUSED_KEYS = java.util.Map.ofEntries(
        java.util.Map.entry("partial", "a restore here creates every shard of an index or refuses; there is no partial index to make"),
        java.util.Map.entry("include_global_state", "there is no global state in a snapshot here to restore"),
        java.util.Map.entry("include_aliases", "aliases are not captured in a snapshot here"),
        java.util.Map.entry("rename_alias_pattern", "aliases are not captured in a snapshot here"),
        java.util.Map.entry("rename_alias_replacement", "aliases are not captured in a snapshot here"),
        java.util.Map.entry(
            "index_settings",
            "the settings an index had when it was captured are restored with it, unchanged; change them on the "
                + "restored index with PUT /{index}/_settings"
        ),
        java.util.Map.entry(
            "ignore_index_settings",
            "the settings an index had when it was captured are restored with it, unchanged; change them on the "
                + "restored index with PUT /{index}/_settings"
        ),
        java.util.Map.entry("settings", "repository settings are fixed when the repository is registered"),
        java.util.Map.entry(
            "storage_type",
            "a restored index is served from this deployment's own object store; there is no other storage type"
        ),
        java.util.Map.entry("source_remote_store_repository", "there is one object store here, and it is the repository"),
        java.util.Map.entry("source_remote_translog_repository", "there is one object store here, and it is the repository"),
        java.util.Map.entry("alias_write_index_policy", "aliases are not captured in a snapshot here"),
        java.util.Map.entry("ignore_unavailable", "a restore names the indices it wants; one the snapshot did not capture is a 404"),
        java.util.Map.entry("allow_no_indices", "a restore names the indices it wants; patterns are not matched against the snapshot"),
        java.util.Map.entry("expand_wildcards", "a restore names the indices it wants; patterns are not matched against the snapshot")
    );

    /** What a create here reads. */
    private static final Set<String> CREATE_KNOWN_KEYS = Set.of("indices", "ignore_unavailable", "partial", "include_global_state");

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
            new Route(RestRequest.Method.POST, "/_snapshot/{repo}/{snapshot}"),
            new Route(RestRequest.Method.GET, "/_snapshot/{repo}/{snapshot}"),
            new Route(RestRequest.Method.DELETE, "/_snapshot/{repo}/{snapshot}"),
            new Route(RestRequest.Method.POST, "/_snapshot/{repo}/{snapshot}/_restore")
        );
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final MetadataPlane metadata = plane.get();
        final String repo = request.param("repo");
        final String snapshotParam = request.param("snapshot");
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", false);

        // Hints: there is no cluster manager to time out against.
        request.param("master_timeout");
        request.param("cluster_manager_timeout");
        // Both names are halves of the register key a snapshot is stored under (repo#name) and of the
        // prefix a repository's listing uses, so a '#' or a ',' in either made one repository's listing
        // include another's snapshots, and a name like "x,y" was created and then never found by the GET
        // and DELETE that split on the comma. _all and * are the two spellings that are not names.
        final boolean listing = request.method() == RestRequest.Method.GET && ("_all".equals(snapshotParam) || "*".equals(snapshotParam));
        final String badName = invalidName(repo, "repository", false);
        final String badSnapshot = listing
            ? null
            : invalidName(
                snapshotParam,
                "snapshot",
                request.method() != RestRequest.Method.PUT && request.method() != RestRequest.Method.POST
            );
        if (badName != null || badSnapshot != null) {
            final String reason = badName != null ? badName : badSnapshot;
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "invalid_snapshot_name_exception", reason)
            );
        }
        if (request.path().endsWith("/_restore")) {
            final Map<String, Object> body = request.hasContent() ? parseBody(request) : Map.of();
            for (Map.Entry<String, String> refused : RESTORE_REFUSED_KEYS.entrySet()) {
                if (body.containsKey(refused.getKey())) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.NOT_IMPLEMENTED,
                            "unsupported_restore",
                            refused.getKey() + " is not supported: " + refused.getValue()
                        )
                    );
                }
            }
            final String unknown = firstUnknownKey(body, RESTORE_KNOWN_KEYS);
            if (unknown != null) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "unknown_parameter", "Unknown parameter " + unknown)
                );
            }
            return channel -> dispatch(channel, () -> handleRestore(channel, metadata, repo, snapshotParam, body, waitForCompletion));
        }
        if (request.method() == RestRequest.Method.GET && ("_all".equals(snapshotParam) || "*".equals(snapshotParam))) {
            return channel -> dispatch(channel, () -> handleList(channel, metadata, repo, null));
        }

        switch (request.method()) {
            case PUT:
            case POST: {
                final Map<String, Object> body = request.hasContent() ? parseBody(request) : Map.of();
                if (body.containsKey("metadata")) {
                    // Refused rather than admitted and dropped: the record has no field for it, and a
                    // caller who sent it and then read the snapshot back found nothing, with no reason why.
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.NOT_IMPLEMENTED,
                            "unsupported_snapshot_option",
                            "metadata is not supported: a snapshot record here carries no user metadata, so it would be accepted "
                                + "and never returned"
                        )
                    );
                }
                final String unknownCreateKey = firstUnknownKey(body, CREATE_KNOWN_KEYS);
                if (unknownCreateKey != null) {
                    // The same check the restore path has always made, which the create path did not: an
                    // unknown key was accepted and dropped.
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.BAD_REQUEST,
                            "unknown_parameter",
                            "Unknown parameter " + unknownCreateKey
                        )
                    );
                }
                final List<String> indices = readIndices(body.get("indices"));
                if (indices.isEmpty()) {
                    return channel -> channel.sendResponse(
                        IndexAdminHandler.error(
                            channel,
                            RestStatus.BAD_REQUEST,
                            "missing_indices",
                            "a snapshot needs an 'indices' field naming what to capture; this surface does not enumerate what exists"
                        )
                    );
                }
                final boolean ignoreUnavailable = readBoolean(body.get("ignore_unavailable"), false);
                final boolean partial = readBoolean(body.get("partial"), false);
                // Read, and reported as what happened rather than as what was asked: there is no global
                // state here to capture, so a snapshot never includes it. Core's default is true, and
                // refusing that default would refuse every client that sends it; echoing true would report
                // a capture that did not happen.
                readBoolean(body.get("include_global_state"), true);
                final boolean includeGlobalState = false;
                return channel -> dispatch(
                    channel,
                    () -> handleCreate(
                        channel,
                        metadata,
                        repo,
                        snapshotParam,
                        indices,
                        ignoreUnavailable,
                        partial,
                        includeGlobalState,
                        waitForCompletion
                    )
                );
            }
            case GET: {
                final List<String> names = List.of(snapshotParam.split(","));
                if (names.size() == 1) {
                    return channel -> dispatch(channel, () -> handleDescribe(channel, metadata, repo, names.get(0)));
                }
                return channel -> dispatch(channel, () -> handleList(channel, metadata, repo, names));
            }
            case DELETE: {
                final List<String> names = List.of(snapshotParam.split(","));
                return channel -> dispatch(channel, () -> handleDelete(channel, metadata, repo, names));
            }
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

    /**
     * Takes a snapshot: index resolution, manifest gathering, a standard capture's blob copy and the
     * final record write all run inside one gate, under one action name — not just the write at the end.
     * A privilege evaluator that only saw the write would never see a caller who was refused before
     * reaching it, and every composite operation on this surface (update, delete-by-query, restore) makes
     * the same choice for the same reason.
     */
    private void handleCreate(
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
            record = gated(
                org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotAction.NAME,
                new org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest(repo, snapshot),
                // capture() writes the record itself: the provisional one before a manifest is read, and the
                // finished one over it. A second createSnapshot here found its own record and refused.
                () -> capture(metadata, repo, snapshot, indexNames, ignoreUnavailable, partial)
            );
        } catch (RepositoryMissingException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "repository_missing_exception", e.getMessage()));
            return;
        } catch (IndexNotFoundForOperationException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", e.getMessage()));
            return;
        } catch (SystemIndexException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.FORBIDDEN, "system_index", e.getMessage()));
            return;
        } catch (NothingPublishedException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.CONFLICT, "nothing_published", e.getMessage()));
            return;
        } catch (SnapshotAlreadyExistsException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "snapshot_already_exists", e.getMessage()));
            return;
        }

        if (waitForCompletion == false) {
            respondAccepted(channel);
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
    private SnapshotRecord capture(
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
        final ServerlessNode serving = node.get();

        // Every index described first: what the provisional record needs, and where a name is refused
        // before anything has been written.
        final Map<String, IndexDescriptor> described = new LinkedHashMap<>();
        for (String raw : indexNames) {
            final String trimmed = raw.trim();
            if (serving != null && serving.isSystemIndex(trimmed)) {
                // A plugin's own index. Captured and restored under another name, its records -- a
                // credential store's password hashes -- would be readable through the ordinary API, which is
                // exactly what declaring it a system index exists to prevent.
                throw new SystemIndexException(trimmed);
            }
            final Optional<IndexDescriptor> descriptor = metadata.describe(trimmed);
            if (descriptor.isEmpty()) {
                if (ignoreUnavailable) {
                    continue;
                }
                throw new IndexNotFoundForOperationException(trimmed);
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
                    settingsOf(index.getValue()),
                    Map.of()
                )
            );
        }
        metadata.createSnapshot(new SnapshotRecord(repo, snapshot, snapshotUuid, shallow, startTime, startTime, provisional));

        try {
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
                        throw new NothingPublishedException(trimmed, shard);
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
                        final BlobPath destBase = RegisterMap.snapshotShardData(
                            metadata.basePath(),
                            repo,
                            snapshot,
                            trimmed,
                            shard.getKey()
                        );
                        stored.put(shard.getKey(), copyShardBlobs(blobStore, sourceBase, destBase, shard.getValue()));
                    }
                }
                captured.put(
                    trimmed,
                    new SnapshotRecord.SnapshottedIndex(
                        descriptor.uuid(),
                        descriptor.numberOfShards(),
                        descriptor.mapping(),
                        settingsOf(descriptor),
                        stored
                    )
                );
            }

            final long endTime = metadata.clock().getAsLong();
            final SnapshotRecord finished = new SnapshotRecord(repo, snapshot, snapshotUuid, shallow, startTime, endTime, captured);
            metadata.finishSnapshot(finished);
            return finished;
        } catch (Exception e) {
            // The provisional record, and whatever a standard repository had copied so far. The record
            // names no shards yet, so deleting it alone would leave the copies unnamed and unreachable.
            try {
                metadata.deleteSnapshot(repo, snapshot);
            } catch (Exception cleanup) {
                e.addSuppressed(cleanup);
            }
            if (shallow == false) {
                for (Map.Entry<String, IndexDescriptor> index : described.entrySet()) {
                    for (int shard = 0; shard < index.getValue().numberOfShards(); shard++) {
                        try {
                            blobStore.blobContainer(
                                RegisterMap.snapshotShardData(metadata.basePath(), repo, snapshot, index.getKey(), shard)
                            ).delete();
                        } catch (Exception cleanup) {
                            e.addSuppressed(cleanup);
                        }
                    }
                }
            }
            throw e;
        }
    }

    /**
     * An index's settings as a snapshot records them: the structured form the descriptor itself writes,
     * so a list-valued setting (an analyzer's filters) comes back as a list rather than as the bracketed
     * string {@code Settings#get} would render it.
     */
    private static Map<String, Object> settingsOf(IndexDescriptor descriptor) throws IOException {
        try (XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
            builder.startObject();
            descriptor.extraSettings().toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return org.opensearch.common.xcontent.XContentHelper.convertToMap(
                BytesReference.bytes(builder),
                false,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
        }
    }

    /** Thrown from inside {@link #handleCreate}'s gate when a named index does not exist. */
    private static final class IndexNotFoundForOperationException extends Exception {
        IndexNotFoundForOperationException(String indexName) {
            super("no such index: " + indexName);
        }
    }

    /** Thrown from inside a gate when a capture or a restore names a plugin's own index. */
    private static final class SystemIndexException extends Exception {
        SystemIndexException(String indexName) {
            super("[" + indexName + "] belongs to a plugin and cannot be captured in, or restored from, a snapshot");
        }
    }

    /**
     * Thrown from inside {@link #handleRestore}'s gate when a restored shard's manifest was written by
     * something else first -- a writer that activated on the freshly created index and published.
     */
    private static final class RestoreConflictException extends Exception {
        RestoreConflictException(String indexName, int shard) {
            super(
                "shard "
                    + shard
                    + " of ["
                    + indexName
                    + "] was written by something else while it was being restored; the restore was rolled back and "
                    + "nothing of it remains -- retry once nothing is writing to that name"
            );
        }
    }

    /** Thrown from inside {@link #handleCreate}'s gate when a shard has nothing published to capture. */
    private static final class NothingPublishedException extends Exception {
        NothingPublishedException(String indexName, int shard) {
            super("shard " + shard + " of " + indexName + " has published nothing yet, so there is no commit to capture");
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
    private void handleList(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String repo, List<String> only)
        throws Exception {
        final List<SnapshotRecord> records = gated(
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

    private void handleDelete(org.opensearch.rest.RestChannel channel, MetadataPlane metadata, String repo, List<String> names)
        throws Exception {
        for (String name : names) {
            final boolean existed = gated(
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
    private void handleRestore(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        String repo,
        String snapshot,
        Map<String, Object> body,
        boolean waitForCompletion
    ) throws Exception {
        final RestoreOutcome outcome;
        try {
            outcome = gated(
                org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotAction.NAME,
                new org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest(repo, snapshot),
                () -> restore(metadata, repo, snapshot, body)
            );
        } catch (SnapshotMissingException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "snapshot_missing_exception", e.getMessage()));
            return;
        } catch (IndexNotCapturedException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", e.getMessage()));
            return;
        } catch (IndexNameCollisionException | IndexAlreadyExistsException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "index_already_exists", e.getMessage()));
            return;
        } catch (SystemIndexException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.FORBIDDEN, "system_index", e.getMessage()));
            return;
        } catch (SnapshotInProgressException e) {
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.CONFLICT, "snapshot_in_progress", e.getMessage()));
            return;
        } catch (RestoreConflictException e) {
            // Reported, not counted as a success. The manifest compare-and-swap result used to be
            // discarded, so a shard that a writer had already published on the fresh index stayed empty
            // behind {"failed": 0}, with the copied blobs unreferenced in the live term.
            channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.CONFLICT, "restore_conflict", e.getMessage()));
            return;
        }

        if (waitForCompletion == false) {
            respondAccepted(channel);
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
    private record RestoreOutcome(Map<String, String> targets, int totalShards) {
    }

    /**
     * Resolves the snapshot, validates every target name (no rename collision, no existing index in the
     * way), then creates and populates each restored index. Runs entirely inside {@link #handleRestore}'s
     * gate; everything it throws is caught there, once, after the gate returns.
     */
    private RestoreOutcome restore(MetadataPlane metadata, String repo, String snapshot, Map<String, Object> body) throws Exception {
        final Optional<SnapshotRecord> found = metadata.snapshot(repo, snapshot);
        if (found.isEmpty()) {
            throw new SnapshotMissingException(repo, snapshot);
        }
        final SnapshotRecord record = found.get();
        if (MetadataPlane.inProgress(record)) {
            throw new SnapshotInProgressException(repo, snapshot);
        }
        final ServerlessNode serving = node.get();

        final List<String> requested = body.containsKey("indices")
            ? readIndices(body.get("indices"))
            : List.copyOf(record.indices().keySet());
        for (String name : requested) {
            if (record.indices().containsKey(name) == false) {
                throw new IndexNotCapturedException(name);
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
                throw new IndexNameCollisionException(collidingSource, original, target);
            }
            if (serving != null && (serving.isSystemIndex(original) || serving.isSystemIndex(target))) {
                // Neither a captured system index nor a target under a system-index pattern: the first is
                // a plugin's records restored somewhere readable, the second a name minted under a pattern
                // the plugin owns, on a path PUT /{index}'s guard never sees.
                throw new SystemIndexException(serving.isSystemIndex(original) ? original : target);
            }
            if (metadata.describe(target).isPresent()) {
                throw new IndexAlreadyExistsException(target);
            }
            targets.put(original, target);
        }

        final BlobStore blobStore = metadata.blobStore();
        // Every target created so far, so a restore that fails part-way removes what it made rather than
        // leaving earlier targets restored and the current one with nothing published behind its name.
        final List<String> created = new ArrayList<>();
        int shardsRestored = 0;
        try {
            for (Map.Entry<String, String> restoring : targets.entrySet()) {
                final SnapshotRecord.SnapshottedIndex capturedIndex = record.indices().get(restoring.getKey());
                final String newUuid = UUIDs.randomBase64UUID();
                // The settings the index had when it was captured, restored with it: the analyzers a mapping
                // names live there, and a restored index without them could neither index nor search.
                final org.opensearch.common.settings.Settings settings = restorableSettings(capturedIndex.settings());
                metadata.createIndex(
                    new IndexDescriptor(restoring.getValue(), newUuid, capturedIndex.numberOfShards(), capturedIndex.mapping(), settings)
                        .createdAt(metadata.clock().getAsLong(), org.opensearch.Version.CURRENT.id)
                );
                created.add(restoring.getValue());
                for (Map.Entry<Integer, CommitManifest> shard : capturedIndex.shards().entrySet()) {
                    final BlobPath sourceBase = record.shallow()
                        ? RegisterMap.shardData(metadata.basePath(), restoring.getKey(), capturedIndex.uuid(), shard.getKey())
                        : RegisterMap.snapshotShardData(metadata.basePath(), repo, snapshot, restoring.getKey(), shard.getKey());
                    final BlobPath destBase = RegisterMap.shardData(metadata.basePath(), restoring.getValue(), newUuid, shard.getKey());
                    final CommitManifest restored = copyShardBlobs(blobStore, sourceBase, destBase, shard.getValue());
                    // The manifest lands only if nothing has published on the new index yet. A writer that
                    // activated the shard between the descriptor's creation and this swap has already
                    // published a commit of its own, and the restored commit must not be reported as in
                    // place when it is not.
                    final var landed = blobStore.blobContainer(destBase)
                        .compareAndSwapRegister(SegmentPublisher.MANIFEST, BlobRegister.ABSENT_GENERATION, restored.toBytes());
                    if (landed.applied() == false) {
                        throw new RestoreConflictException(restoring.getValue(), shard.getKey());
                    }
                    shardsRestored++;
                }
            }
        } catch (Exception e) {
            for (String target : created) {
                try {
                    metadata.deleteIndex(target);
                } catch (Exception cleanup) {
                    e.addSuppressed(cleanup);
                }
            }
            throw e;
        }
        return new RestoreOutcome(targets, shardsRestored);
    }

    /**
     * The captured settings a restored index is created with: everything core's restore carries over,
     * and none of what it does not. The uuid, creation date, version and provided name are the new
     * index's own; the shard count is structural and travels as {@code numberOfShards}; replicas are
     * refused here. A descriptor never stores these in its extra settings, but a snapshot record is data
     * this handler did not necessarily write, and a restored descriptor carrying another index's uuid is
     * a shard that opens under the wrong storage path.
     */
    private static org.opensearch.common.settings.Settings restorableSettings(Map<String, Object> captured) {
        if (captured.isEmpty()) {
            return org.opensearch.common.settings.Settings.EMPTY;
        }
        final org.opensearch.common.settings.Settings.Builder settings = org.opensearch.common.settings.Settings.builder()
            .loadFromMap(captured)
            .normalizePrefix(org.opensearch.cluster.metadata.IndexMetadata.INDEX_SETTING_PREFIX);
        for (String key : new String[] {
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_HISTORY_UUID,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_UPGRADED,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_PROVIDED_NAME,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS,
            org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS }) {
            settings.remove(key);
        }
        return settings.build();
    }

    /** Thrown from inside {@link #handleRestore}'s gate when the snapshot is still being taken. */
    private static final class SnapshotInProgressException extends Exception {
        SnapshotInProgressException(String repo, String snapshot) {
            super("snapshot " + repo + "/" + snapshot + " is still being taken; there is nothing to restore yet");
        }
    }

    /** Thrown from inside {@link #handleRestore}'s gate when a requested index was not in the snapshot. */
    private static final class IndexNotCapturedException extends Exception {
        IndexNotCapturedException(String indexName) {
            super("the snapshot did not capture an index named " + indexName);
        }
    }

    /** Thrown from inside {@link #handleRestore}'s gate when the named snapshot does not exist. */
    private static final class SnapshotMissingException extends Exception {
        SnapshotMissingException(String repo, String snapshot) {
            super("no such snapshot: " + repo + "/" + snapshot);
        }
    }

    /** Thrown from inside {@link #handleRestore}'s gate when two indices rename to the same target. */
    private static final class IndexNameCollisionException extends Exception {
        IndexNameCollisionException(String first, String second, String target) {
            super("indices [" + first + "] and [" + second + "] are renamed into the same index [" + target + "]");
        }
    }

    private void writeSnapshotInfo(XContentBuilder builder, SnapshotRecord record, boolean includeGlobalState) throws IOException {
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

    private void respondAccepted(org.opensearch.rest.RestChannel channel) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("accepted", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /**
     * Copies every blob a captured shard's manifest names into a new location, under one fresh term
     * container. Used both by a standard snapshot's own capture (source: the live index; destination: the
     * repository's own storage) and by every restore (source: either of those, by
     * {@link SnapshotRecord#shallow()}; destination: the new index being created) — one copy routine, not
     * two, because both are the same operation with different endpoints.
     *
     * @param blobStore the backing store
     * @param sourceBase where the manifest's blob paths are relative to
     * @param destBase where to copy them to
     * @param source the manifest to copy
     * @return the manifest to record or publish at the destination
     * @throws IOException if a referenced blob is missing, or copying fails
     */
    private CommitManifest copyShardBlobs(BlobStore blobStore, BlobPath sourceBase, BlobPath destBase, CommitManifest source)
        throws IOException {
        // Term 1, not 0: a shard-head's term must be positive (s0-findings.md F4), and this manifest may
        // end up opened as a reader if it is ever restored -- true even when this call is only copying
        // into repository storage rather than a live shard, since that same manifest is what a later
        // restore publishes verbatim at its own destination.
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

    private static List<String> readIndices(Object field) {
        if (field instanceof String string) {
            final List<String> split = new ArrayList<>();
            for (String piece : string.split(",")) {
                if (piece.isBlank() == false) {
                    split.add(piece.trim());
                }
            }
            return split;
        }
        if (field instanceof List<?> list) {
            final List<String> result = new ArrayList<>(list.size());
            for (Object item : list) {
                result.add(String.valueOf(item));
            }
            return result;
        }
        return List.of();
    }

    private static boolean readBoolean(Object field, boolean defaultValue) {
        if (field == null) {
            return defaultValue;
        }
        if (field instanceof Boolean bool) {
            return bool;
        }
        return Boolean.parseBoolean(String.valueOf(field));
    }

    /**
     * Why a repository or snapshot name is not one, or null when it is.
     *
     * @param name the name, or a comma-separated list of them when {@code list} is true
     * @param what "repository" or "snapshot", for the message
     * @param list whether several names may be given
     */
    private static String invalidName(String name, String what, boolean list) {
        if (name == null) {
            return null;
        }
        for (String each : list ? name.split(",") : new String[] { name }) {
            try {
                org.opensearch.serverless.metadata.Names.validateId(each, what);
            } catch (IllegalArgumentException e) {
                return e.getMessage();
            }
        }
        return null;
    }

    private static String firstUnknownKey(Map<String, Object> body, Set<String> known) {
        for (String key : body.keySet()) {
            if (known.contains(key) == false) {
                return key;
            }
        }
        return null;
    }

    private Map<String, Object> parseBody(RestRequest request) throws IOException {
        try (XContentParser parser = request.contentOrSourceParamParser()) {
            return parser.map();
        }
    }

    /**
     * Runs work off the transport thread, or inline when there is no node to borrow a pool from.
     *
     * <p>Every operation here touches the object store -- a snapshot's manifest reads, a standard
     * snapshot's or a restore's blob copies -- and the transport thread is not the thread to wait on
     * remote IO from.
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
                channel.sendResponse(IndexAdminHandler.failure(channel, e));
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
}
