/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.delete.DeleteAction;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetAction;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexAction;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardOperations;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.support.AbstractClient;

import java.util.function.Supplier;

/**
 * The {@link org.opensearch.transport.client.Client} a plugin gets, over the shell's own operations.
 *
 * <p><b>Why this is one class rather than an action layer.</b> Plugins do not call {@code TransportAction}
 * directly; they call {@code Client}. And {@code AbstractClient} funnels its entire surface — dozens of
 * methods, every builder — through a single abstract {@code doExecute}. So a working client for the shell
 * is a dispatch table, not the {@code action/} package §6.3 decided not to build. That decision is what
 * keeps this project small, and this class is what makes it survivable: plugins get the interface they
 * expect without the machinery behind it coming back.
 *
 * <p><b>An allowlist, and it refuses out loud.</b> Supported actions are the ones a plugin needs to keep
 * state in an index: index, get, delete and create-index. Everything else throws, naming the action. That
 * is D2 applied one layer down — an unimplemented endpoint returns 501 with a reason rather than an empty
 * success, and an unimplemented action does the same rather than quietly returning a default-constructed
 * response, which for a search would be "no results" and for a delete would be "done".
 *
 * <p><b>Synchronous underneath.</b> The shell's operations block; {@code Client} is callback-shaped. The
 * work is handed to the generic pool and the listener is called from there, so a plugin calling this from
 * a transport thread does not wait on object-store IO on it.
 */
public final class ServerlessClient extends AbstractClient {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the client.
     *
     * @param settings the node settings
     * @param threadPool the node thread pool
     * @param node supplies the node
     * @param plane supplies the metadata plane
     */
    public ServerlessClient(Settings settings, ThreadPool threadPool, Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        super(settings, threadPool);
        this.node = node;
        this.plane = plane;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        final ServerlessNode serving = node.get();
        final MetadataPlane metadata = plane.get();
        if (serving == null || metadata == null) {
            listener.onFailure(new IllegalStateException("this node has no metadata plane configured"));
            return;
        }
        threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                listener.onResponse((Response) run(action, request, serving, metadata));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        });
    }

    /**
     * Runs one action, and translates the shell's failures into the ones a plugin already catches.
     *
     * <p><b>Why the translation exists.</b> {@link ShardOperations} throws its own types, which is right
     * for the shell: they carry the owner and whether a retry is worth it, and the REST layer renders
     * them into a 421 or a 503. A plugin cannot catch them — the shell is deliberately not on its
     * classpath — so across this boundary they would arrive as an anonymous {@code IOException} whose
     * meaning is only in its message. Writing the shell's own authentication as a plugin is what made
     * that concrete: it had to decide "index missing" from "index unreadable" by matching prose.
     *
     * <p>Core's exceptions are the shared vocabulary both sides do have.
     * {@link org.opensearch.index.IndexNotFoundException} means the index is not there and asking again
     * will not help; {@link org.opensearch.action.NoShardAvailableActionException} means no copy can
     * answer right now, which is exactly what "not here" means to somebody who cannot do anything about
     * ownership.
     */
    private ActionResponse run(ActionType<?> action, ActionRequest request, ServerlessNode serving, MetadataPlane metadata)
        throws Exception {
        try {
            return dispatch(action, request, serving, metadata);
        } catch (ShardOperations.NoSuchIndexException e) {
            throw new org.opensearch.index.IndexNotFoundException(e.index());
        } catch (ShardOperations.NotHereException e) {
            throw new org.opensearch.action.NoShardAvailableActionException(null, e.getMessage(), e);
        }
    }

    private ActionResponse dispatch(ActionType<?> action, ActionRequest request, ServerlessNode serving, MetadataPlane metadata)
        throws Exception {
        final ShardOperations operations = new ShardOperations(serving, metadata);
        if (IndexAction.INSTANCE.name().equals(action.name())) {
            return index((IndexRequest) request, operations, metadata);
        }
        if (GetAction.INSTANCE.name().equals(action.name())) {
            return get((GetRequest) request, operations, metadata);
        }
        if (DeleteAction.INSTANCE.name().equals(action.name())) {
            return delete((DeleteRequest) request, operations, metadata);
        }
        if (CreateIndexAction.INSTANCE.name().equals(action.name())) {
            return createIndex((CreateIndexRequest) request, metadata);
        }
        if (org.opensearch.action.search.SearchAction.INSTANCE.name().equals(action.name())) {
            return search((org.opensearch.action.search.SearchRequest) request, operations);
        }
        throw new UnsupportedOperationException(
            "the serverless shell does not implement action ["
                + action.name()
                + "]; supported actions are "
                + IndexAction.INSTANCE.name()
                + ", "
                + GetAction.INSTANCE.name()
                + ", "
                + DeleteAction.INSTANCE.name()
                + ", "
                + CreateIndexAction.INSTANCE.name()
                + " and "
                + org.opensearch.action.search.SearchAction.INSTANCE.name()
        );
    }

    private IndexResponse index(IndexRequest request, ShardOperations operations, MetadataPlane metadata) throws Exception {
        final String id = request.id() == null ? org.opensearch.common.UUIDs.base64UUID() : request.id();
        operations.index(
            request.index(),
            id,
            request.source().utf8ToString(),
            request.getRefreshPolicy() != IndexRequest.RefreshPolicy.NONE
        );
        // Sequence numbers and versions are not modelled: WalRecord records document state, not history,
        // and inventing numbers here would let a plugin build optimistic concurrency on a guarantee this
        // system does not make. They are reported as unassigned, which is what they are.
        return new IndexResponse(shardIdOf(request.index(), id, metadata), id, SequenceNumbers.UNASSIGNED_SEQ_NO, 0L, 1L, true);
    }

    private GetResponse get(GetRequest request, ShardOperations operations, MetadataPlane metadata) throws Exception {
        final var read = operations.get(request.index(), request.id());
        final var document = read.document();
        return new GetResponse(
            new GetResult(
                request.index(),
                request.id(),
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0L,
                document.found() ? 1L : -1L,
                document.found(),
                document.found() ? new org.opensearch.core.common.bytes.BytesArray(document.source()) : null,
                java.util.Map.of(),
                java.util.Map.of()
            )
        );
    }

    private DeleteResponse delete(DeleteRequest request, ShardOperations operations, MetadataPlane metadata) throws Exception {
        final boolean found = operations.delete(
            request.index(),
            request.id(),
            request.getRefreshPolicy() != IndexRequest.RefreshPolicy.NONE
        );
        return new DeleteResponse(
            shardIdOf(request.index(), request.id(), metadata),
            request.id(),
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            0L,
            1L,
            found
        );
    }

    /**
     * Runs a search through the same fan-out a user's search uses.
     *
     * <p>This is how a plugin loads all of its state — Security reads its whole config that way — so it
     * matters that it is the same code path and not a second one. What cannot be merged across shards is
     * refused by {@code SearchHandler} for REST callers and by the fan-out for everyone; a plugin asking
     * for an aggregation gets the same answer a user does.
     */
    private org.opensearch.action.search.SearchResponse search(
        org.opensearch.action.search.SearchRequest request,
        ShardOperations operations
    ) throws Exception {
        final var source = request.source() == null
            ? new org.opensearch.search.builder.SearchSourceBuilder().query(org.opensearch.index.query.QueryBuilders.matchAllQuery())
            : request.source();
        if (source.size() < 0) {
            source.size(10);
        }
        if (source.from() < 0) {
            source.from(0);
        }
        final var outcome = operations.search(request.indices()[0], source);
        final org.opensearch.search.SearchHits hits = new org.opensearch.search.SearchHits(
            outcome.hits().toArray(new org.opensearch.search.SearchHit[0]),
            new org.apache.lucene.search.TotalHits(outcome.total(), org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
            outcome.hits().isEmpty() ? Float.NaN : outcome.hits().get(0).getScore()
        );
        return new org.opensearch.action.search.SearchResponse(
            new org.opensearch.action.search.SearchResponseSections(hits, null, null, false, false, null, 1),
            null,
            outcome.shards(),
            outcome.answered(),
            0,
            0L,
            org.opensearch.action.search.ShardSearchFailure.EMPTY_ARRAY,
            org.opensearch.action.search.SearchResponse.Clusters.EMPTY
        );
    }

    private CreateIndexResponse createIndex(CreateIndexRequest request, MetadataPlane metadata) throws Exception {
        if (metadata.describe(request.index()).isPresent()) {
            throw new org.opensearch.ResourceAlreadyExistsException(request.index());
        }
        final int shards = request.settings().getAsInt("index.number_of_shards", 1);
        final String mapping = request.mappings() == null || request.mappings().isEmpty() ? "{\"properties\":{}}" : request.mappings();
        metadata.createIndex(new IndexDescriptor(request.index(), org.opensearch.common.UUIDs.randomBase64UUID(), shards, mapping, null));
        return new CreateIndexResponse(true, true, request.index());
    }

    /** The shard a document routes to, for the response's benefit only. */
    private ShardId shardIdOf(String index, String id, MetadataPlane metadata) throws java.io.IOException {
        final var descriptor = metadata.describe(index).orElseThrow(() -> new ShardOperations.NoSuchIndexException(index));
        return new ShardId(new Index(index, descriptor.uuid()), org.opensearch.serverless.rest.DocumentRouting.shardFor(descriptor, id));
    }

    @Override
    public void close() {
        // The node owns the thread pool and everything else here; there is nothing of this client's own
        // to release.
    }
}
