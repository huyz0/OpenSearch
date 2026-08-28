/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.Version;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shard.ShardQuery;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Sends work to the node that owns a shard, and answers work sent here.
 *
 * <p>The only node-to-node traffic in this design. Metadata never crosses a node boundary — every node
 * reads the object store for itself — so this carries documents and query results and nothing else.
 *
 * <p><b>Peers are found through their leases.</b> A node's lease already records the transport address
 * it bound, so membership doubles as the address book: there is no separate discovery, no seed hosts,
 * and a node that stops renewing simply stops being addressable, which is the same event that makes it
 * stop owning shards.
 */
public final class ShardRouter {

    private final ServerlessNode node;
    private final TransportService transportService;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates a router for one node.
     *
     * @param node the local node
     * @param transportService its transport service
     * @param plane supplies the metadata plane
     */
    public ShardRouter(ServerlessNode node, TransportService transportService, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.transportService = transportService;
        this.plane = plane;
    }

    /** Registers the handlers that answer forwarded work. Called once, at startup. */
    public void registerHandlers() {
        transportService.registerRequestHandler(
            ForwardedIndexRequest.ACTION,
            ThreadPool.Names.WRITE,
            ForwardedIndexRequest::new,
            this::handleIndex
        );
        transportService.registerRequestHandler(
            ForwardedSearchRequest.ACTION,
            ThreadPool.Names.SEARCH,
            ForwardedSearchRequest::new,
            this::handleSearch
        );
    }

    private void handleIndex(ForwardedIndexRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        final ShardId shardId = localShard(request.index(), request.shard());
        if (shardId == null) {
            // Ownership moved between the sender reading the head and this arriving. Failing is right:
            // the sender re-reads and retries, and accepting a write for a shard we do not own is the
            // one outcome that would be unsafe.
            throw new IllegalStateException("this node does not own " + request.index() + "[" + request.shard() + "]");
        }
        node.index(shardId, request.id(), request.source());
        if (request.refresh()) {
            node.reconciler().shard(shardId).refresh("serverless-forwarded-refresh");
        }
        channel.sendResponse(new ForwardedIndexResponse(node.localNode().getId()));
    }

    private void handleSearch(ForwardedSearchRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        ShardId shardId = localShard(request.index(), request.shard());
        if (shardId == null) {
            // Open it. This is the property that makes placement a hint rather than a requirement: a
            // node asked for a shard it does not have fetches the manifest and serves, so a stale or
            // unlucky routing decision costs a cold read and never a wrong answer.
            shardId = node.serveAsReader(plane.get(), request.index(), request.shard());
        }
        final ShardQuery.Result result = ShardQuery.execute(
            node.searchService(),
            shardId,
            request.field(),
            request.value(),
            request.size()
        );
        final List<String> ids = new ArrayList<>();
        final List<String> sources = new ArrayList<>();
        for (var hit : result.hits()) {
            ids.add(hit.getId());
            sources.add(hit.getSourceAsString() == null ? "" : hit.getSourceAsString());
        }
        channel.sendResponse(new ForwardedSearchResponse(result.total(), ids, sources));
    }

    private ShardId localShard(String index, int shard) {
        return node.reconciler()
            .openShards()
            .stream()
            .filter(s -> s.getIndexName().equals(index) && s.id() == shard)
            .findFirst()
            .orElse(null);
    }

    /**
     * Resolves a peer from its lease, connecting if necessary.
     *
     * @param nodeId the node to reach
     * @return the peer, or empty if it has no live lease or an unusable address
     * @throws IOException if the lease cannot be read
     */
    public Optional<DiscoveryNode> peer(String nodeId) throws IOException {
        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return Optional.empty();
        }
        final var lease = metadata.membership().read(nodeId);
        if (lease.isEmpty()) {
            return Optional.empty();
        }
        final TransportAddress address;
        try {
            address = parse(lease.get().address());
        } catch (Exception e) {
            // An unparseable address is a node we cannot reach, not a crash. The caller reports that it
            // could not route, which is true and actionable.
            return Optional.empty();
        }
        // The ephemeral id must come from the lease, not be invented. The transport handshake validates
        // the identity of whoever answers, and a synthesized ephemeral id fails it with
        // "unexpected remote node" -- which reads like the wrong node answered rather than like the
        // caller described it wrongly.
        final DiscoveryNode peer = new DiscoveryNode(
            nodeId,
            nodeId,
            lease.get().ephemeralId(),
            address.address().getHostString(),
            address.getAddress(),
            address,
            java.util.Map.of(),
            org.opensearch.cluster.node.DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        if (transportService.nodeConnected(peer) == false) {
            transportService.connectToNode(peer);
        }
        return Optional.of(peer);
    }

    private static TransportAddress parse(String address) throws Exception {
        final int colon = address.lastIndexOf(':');
        final String host = address.substring(0, colon).replace("[", "").replace("]", "");
        final int port = Integer.parseInt(address.substring(colon + 1));
        return new TransportAddress(InetAddress.getByName(host), port);
    }

    /**
     * Sends a write to the node that owns the shard and waits for its acknowledgement.
     *
     * @param peer the owning node
     * @param request the write
     * @return the owner's acknowledgement
     */
    public ForwardedIndexResponse forwardIndex(DiscoveryNode peer, ForwardedIndexRequest request) {
        final PlainActionFuture<ForwardedIndexResponse> future = PlainActionFuture.newFuture();
        transportService.sendRequest(
            peer,
            ForwardedIndexRequest.ACTION,
            request,
            TransportRequestOptions.EMPTY,
            new Handler<>(future, ForwardedIndexResponse::new)
        );
        return future.actionGet();
    }

    /**
     * Sends a shard query to the node holding it and waits for the answer.
     *
     * @param peer the holding node
     * @param request the query
     * @return that shard's answer
     */
    public ForwardedSearchResponse forwardSearch(DiscoveryNode peer, ForwardedSearchRequest request) {
        final PlainActionFuture<ForwardedSearchResponse> future = PlainActionFuture.newFuture();
        transportService.sendRequest(
            peer,
            ForwardedSearchRequest.ACTION,
            request,
            TransportRequestOptions.EMPTY,
            new Handler<>(future, ForwardedSearchResponse::new)
        );
        return future.actionGet();
    }

    /** Bridges a transport response into a future the calling thread can wait on. */
    private static final class Handler<T extends org.opensearch.core.transport.TransportResponse> implements TransportResponseHandler<T> {

        private final PlainActionFuture<T> future;
        private final org.opensearch.core.common.io.stream.Writeable.Reader<T> reader;

        Handler(PlainActionFuture<T> future, org.opensearch.core.common.io.stream.Writeable.Reader<T> reader) {
            this.future = future;
            this.reader = reader;
        }

        @Override
        public T read(org.opensearch.core.common.io.stream.StreamInput in) throws IOException {
            return reader.read(in);
        }

        @Override
        public void handleResponse(T response) {
            future.onResponse(response);
        }

        @Override
        public void handleException(org.opensearch.transport.TransportException exp) {
            future.onFailure(exp);
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }
    }
}
