/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RepositoryDescriptor;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

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
 * uuid, so a restore mints a fresh one — reusing one would let two indices share storage — and copies whatever the
 * snapshot's mode requires: a shallow snapshot's source is the original index's own storage, a standard
 * snapshot's source is its own repository-scoped copy.
 *
 * <p><b>Every operation here is synchronous</b>, {@code wait_for_completion} included: this shell has no
 * background task registry to run one asynchronously against, so the work is always done in full before a
 * response is sent, and {@code wait_for_completion=false} only changes which response shape is sent — the
 * caller sees the same outcome either way, sooner than an async caller polling a real cluster would.
 */
public final class SnapshotHandler extends BaseRestHandler {

    final Supplier<MetadataPlane> plane;
    final Supplier<ServerlessNode> node;
    /** Runs the snapshot requests this handler parsed. */
    final SnapshotOperations operations = new SnapshotOperations(this);

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
        final String badName = SnapshotRequestBodies.invalidName(repo, "repository", false);
        final String badSnapshot = listing
            ? null
            : SnapshotRequestBodies.invalidName(
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
            final Map<String, Object> body = request.hasContent() ? SnapshotRequestBodies.parseBody(request) : Map.of();
            for (Map.Entry<String, String> refused : SnapshotRequestBodies.RESTORE_REFUSED_KEYS.entrySet()) {
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
            final String unknown = SnapshotRequestBodies.firstUnknownKey(body, SnapshotRequestBodies.RESTORE_KNOWN_KEYS);
            if (unknown != null) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "unknown_parameter", "Unknown parameter " + unknown)
                );
            }
            return channel -> dispatch(
                channel,
                () -> operations.handleRestore(channel, metadata, repo, snapshotParam, body, waitForCompletion)
            );
        }
        if (request.method() == RestRequest.Method.GET && ("_all".equals(snapshotParam) || "*".equals(snapshotParam))) {
            return channel -> dispatch(channel, () -> operations.handleList(channel, metadata, repo, null));
        }

        switch (request.method()) {
            case PUT:
            case POST: {
                final Map<String, Object> body = request.hasContent() ? SnapshotRequestBodies.parseBody(request) : Map.of();
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
                final String unknownCreateKey = SnapshotRequestBodies.firstUnknownKey(body, SnapshotRequestBodies.CREATE_KNOWN_KEYS);
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
                final List<String> indices = SnapshotRequestBodies.readIndices(body.get("indices"));
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
                final boolean ignoreUnavailable = SnapshotRequestBodies.readBoolean(body.get("ignore_unavailable"), false);
                final boolean partial = SnapshotRequestBodies.readBoolean(body.get("partial"), false);
                // Read, and reported as what happened rather than as what was asked: there is no global
                // state here to capture, so a snapshot never includes it. Core's default is true, and
                // refusing that default would refuse every client that sends it; echoing true would report
                // a capture that did not happen.
                SnapshotRequestBodies.readBoolean(body.get("include_global_state"), true);
                final boolean includeGlobalState = false;
                return channel -> dispatch(
                    channel,
                    () -> operations.handleCreate(
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
                    return channel -> dispatch(channel, () -> operations.handleDescribe(channel, metadata, repo, names.get(0)));
                }
                return channel -> dispatch(channel, () -> operations.handleList(channel, metadata, repo, names));
            }
            case DELETE: {
                final List<String> names = List.of(snapshotParam.split(","));
                return channel -> dispatch(channel, () -> operations.handleDelete(channel, metadata, repo, names));
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

    /** Thrown from inside {@link SnapshotOperations#handleCreate}'s gate when a named index does not exist. */
    static final class IndexNotFoundForOperationException extends Exception {
        IndexNotFoundForOperationException(String indexName) {
            super("no such index: " + indexName);
        }
    }

    /** Thrown from inside a gate when a capture or a restore names a plugin's own index. */
    static final class SystemIndexException extends Exception {
        SystemIndexException(String indexName) {
            super("[" + indexName + "] belongs to a plugin and cannot be captured in, or restored from, a snapshot");
        }
    }

    /**
     * Thrown from inside {@link SnapshotOperations#handleRestore}'s gate when a restored shard's manifest was written by
     * something else first -- a writer that activated on the freshly created index and published.
     */
    static final class RestoreConflictException extends Exception {
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

    /** Thrown from inside {@link SnapshotOperations#handleCreate}'s gate when a shard has nothing published to capture. */
    static final class NothingPublishedException extends Exception {
        NothingPublishedException(String indexName, int shard) {
            super("shard " + shard + " of " + indexName + " has published nothing yet, so there is no commit to capture");
        }
    }

    /** Thrown from inside {@link SnapshotOperations#handleRestore}'s gate when the snapshot is still being taken. */
    static final class SnapshotInProgressException extends Exception {
        SnapshotInProgressException(String repo, String snapshot) {
            super("snapshot " + repo + "/" + snapshot + " is still being taken; there is nothing to restore yet");
        }
    }

    /** Thrown from inside {@link SnapshotOperations#handleRestore}'s gate when a requested index was not in the snapshot. */
    static final class IndexNotCapturedException extends Exception {
        IndexNotCapturedException(String indexName) {
            super("the snapshot did not capture an index named " + indexName);
        }
    }

    /** Thrown from inside {@link SnapshotOperations#handleRestore}'s gate when the named snapshot does not exist. */
    static final class SnapshotMissingException extends Exception {
        SnapshotMissingException(String repo, String snapshot) {
            super("no such snapshot: " + repo + "/" + snapshot);
        }
    }

    /** Thrown from inside {@link SnapshotOperations#handleRestore}'s gate when two indices rename to the same target. */
    static final class IndexNameCollisionException extends Exception {
        IndexNameCollisionException(String first, String second, String target) {
            super("indices [" + first + "] and [" + second + "] are renamed into the same index [" + target + "]");
        }
    }

    void respondAccepted(org.opensearch.rest.RestChannel channel) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("accepted", true);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
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

    <T> T gated(String action, org.opensearch.action.ActionRequest request, org.opensearch.common.CheckedSupplier<T, Exception> work)
        throws Exception {
        final var serving = node.get();
        return serving == null ? work.get() : serving.services().actionGate().run(action, request, work);
    }
}
