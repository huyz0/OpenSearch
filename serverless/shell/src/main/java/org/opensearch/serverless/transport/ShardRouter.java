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
import org.opensearch.common.unit.TimeValue;
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

    /** Used only when no metadata plane is attached and the lease TTL is therefore unknown. */
    private static final long DEFAULT_FORWARD_TIMEOUT_MILLIS = 30_000L;

    /**
     * How long a search may wait on a peer.
     *
     * <p>Deliberately not the lease TTL, which bounds a <em>write</em> forward. The two have different
     * stakes. Waiting past the lease to write is pointless and unsafe: ownership may already have moved,
     * and the right answer is to re-read the head. A search has no such problem — a slow peer is slow,
     * not wrong — so giving up at the TTL converts a complete answer into an incomplete one for no
     * correctness benefit.
     *
     * <p>Found by a four-node herd test failing under load with {@code searched:0, unreachable:1}: a busy
     * peer missed a three-second window and the coverage report said so honestly. Bounding both kinds of
     * forward by the same number was one decision that should have been two.
     */
    private static final TimeValue SEARCH_FORWARD_TIMEOUT = TimeValue.timeValueSeconds(30);

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
            ForwardedBulkRequest.ACTION,
            ThreadPool.Names.WRITE,
            ForwardedBulkRequest::new,
            this::handleBulk
        );
        transportService.registerRequestHandler(
            ForwardedGetRequest.ACTION,
            ThreadPool.Names.GET,
            ForwardedGetRequest::new,
            this::handleGet
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
        if (request.deletion()) {
            node.delete(shardId, request.id());
        } else {
            node.index(shardId, request.id(), request.source());
        }
        if (request.refresh()) {
            node.reconciler().shard(shardId).refresh("serverless-forwarded-refresh");
        }
        channel.sendResponse(new ForwardedIndexResponse(node.localNode().getId()));
    }

    private void handleBulk(ForwardedBulkRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        final ShardId shardId = localShard(request.index(), request.shard());
        if (shardId == null) {
            // Same refusal as a forwarded single write, and for the same reason: ownership moved between
            // the sender reading the head and this arriving. Refusing the whole batch is right -- every
            // item in it routes to this one shard, so there is no partial answer to give.
            throw new IllegalStateException("this node does not own " + request.index() + "[" + request.shard() + "]");
        }
        final var outcomes = node.bulk(shardId, request.operations());
        if (request.refresh()) {
            node.reconciler().shard(shardId).refresh("serverless-forwarded-refresh");
        }
        channel.sendResponse(new ForwardedBulkResponse(node.localNode().getId(), outcomes));
    }

    private void handleGet(ForwardedGetRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        final ShardId shardId = localShard(request.index(), request.shard());
        if (shardId == null) {
            // Refuse rather than open it as a reader, which is what handleSearch does here and is right
            // there. A get was forwarded to this node precisely because the shard-head named it the
            // owner; if the shard is not open as a writer, that head is stale, and answering from a
            // published commit would return a stale document to a caller that asked for a fresh one.
            // Failing sends the caller back to re-read the head, which is the only thing that fixes it.
            throw new IllegalStateException("this node does not own " + request.index() + "[" + request.shard() + "]");
        }
        channel.sendResponse(new ForwardedGetResponse(node.localNode().getId(), node.get(shardId, request.id())));
    }

    private void handleSearch(ForwardedSearchRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        ShardId shardId = localShard(request.index(), request.shard());
        if (shardId == null) {
            // Open it. This is the property that makes placement a hint rather than a requirement: a
            // node asked for a shard it does not have fetches the manifest and serves, so a stale or
            // unlucky routing decision costs a cold read and never a wrong answer.
            shardId = node.serveAsReader(plane.get(), request.index(), request.shard());
        }
        node.markUsed(shardId);
        final ShardQuery.Result result = ShardQuery.execute(node.searchService(), shardId, request.source());
        channel.sendResponse(new ForwardedSearchResponse(result.total(), result.hits(), result.aggregations()));
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
        return peer(nodeId, forwardTimeout());
    }

    /**
     * Resolves a peer, bounding connection establishment by an explicit timeout.
     *
     * @param nodeId the node to reach
     * @param timeout how long to allow for connecting and handshaking
     * @return the peer, or empty if it has no live lease or an unusable address
     * @throws IOException if the lease cannot be read
     */
    public Optional<DiscoveryNode> peer(String nodeId, TimeValue timeout) throws IOException {
        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return Optional.empty();
        }
        final var lease = metadata.membership().read(nodeId);
        if (lease.isEmpty()) {
            return Optional.empty();
        }
        if (lease.get().isExpiredAt(metadata.clock().getAsLong())) {
            // Expired, which this did not check until a get exposed it -- the method's own documentation
            // has always said "no live lease", and it was reading the blob and never the clock. A lease
            // outlives its holder until the sweep collects it, so without this a forward to a node that
            // died an hour ago is indistinguishable from one to a node that died a moment ago: both burn
            // the full connect timeout before failing, and both report a failed forward rather than the
            // truth, which is that nothing has been alive there for an hour.
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
            // Bounded, and the connect is where this actually bites. A frozen process still has an open
            // listening socket, so TCP succeeds and the transport handshake is what hangs -- for the
            // default 30 seconds, before a single byte of the write has been sent and before any
            // per-request timeout can apply. The request timeout below was not enough on its own; this
            // was found by measuring how long a write to a SIGSTOPped owner took to come back.
            transportService.connectToNode(
                peer,
                new org.opensearch.transport.ConnectionProfile.Builder(
                    org.opensearch.transport.ConnectionProfile.buildDefaultConnectionProfile(org.opensearch.common.settings.Settings.EMPTY)
                ).setConnectTimeout(timeout).setHandshakeTimeout(timeout).build()
            );
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
        final org.opensearch.common.unit.TimeValue timeout = forwardTimeout();
        transportService.sendRequest(
            peer,
            ForwardedIndexRequest.ACTION,
            request,
            TransportRequestOptions.builder().withTimeout(timeout).build(),
            new Handler<>(future, ForwardedIndexResponse::new)
        );
        return future.actionGet(timeout);
    }

    /**
     * Sends a whole batch to the node that owns the shard and waits for its per-item answer.
     *
     * <p>Bounded by the same deadline a single forwarded write uses. A batch does more work on the far
     * side than one document does, so this is the place where that bound is most likely to be the wrong
     * shape -- recorded rather than tuned, because a number picked without a measurement is not better
     * than the one already justified.
     *
     * @param peer the owning node
     * @param request the batch
     * @return the owner's per-item outcomes
     */
    public ForwardedBulkResponse forwardBulk(DiscoveryNode peer, ForwardedBulkRequest request) {
        final PlainActionFuture<ForwardedBulkResponse> future = PlainActionFuture.newFuture();
        final org.opensearch.common.unit.TimeValue timeout = forwardTimeout();
        transportService.sendRequest(
            peer,
            ForwardedBulkRequest.ACTION,
            request,
            TransportRequestOptions.builder().withTimeout(timeout).build(),
            new Handler<>(future, ForwardedBulkResponse::new)
        );
        return future.actionGet(timeout);
    }

    /**
     * Sends a read by id to the node that owns the shard and waits for the answer.
     *
     * @param peer the owning node
     * @param request the read
     * @return what the owner found
     */
    public ForwardedGetResponse forwardGet(DiscoveryNode peer, ForwardedGetRequest request) {
        final PlainActionFuture<ForwardedGetResponse> future = PlainActionFuture.newFuture();
        // The write bound, not the search one. A get is a point lookup on the owner rather than a fan-out,
        // so it is the cheaper of the two and has no reason to wait as long as a query.
        final org.opensearch.common.unit.TimeValue timeout = forwardTimeout();
        transportService.sendRequest(
            peer,
            ForwardedGetRequest.ACTION,
            request,
            TransportRequestOptions.builder().withTimeout(timeout).build(),
            new Handler<>(future, ForwardedGetResponse::new)
        );
        return future.actionGet(timeout);
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
        final TimeValue timeout = searchForwardTimeout();
        transportService.sendRequest(
            peer,
            ForwardedSearchRequest.ACTION,
            request,
            TransportRequestOptions.builder().withTimeout(timeout).build(),
            new Handler<>(future, ForwardedSearchResponse::new)
        );
        return future.actionGet(timeout);
    }

    /**
     * How long to wait for the node the shard-head named.
     *
     * <p>Bounded by the lease TTL, and that is the principled number rather than a round one: there is no
     * value in waiting longer than the lease, because a peer that has not answered within it is a peer
     * whose lease is lapsing, and the correct move then is to re-read the head rather than keep holding a
     * thread open.
     *
     * <p>Found by freezing a node with SIGSTOP. Unbounded, a paused owner — a stop-the-world GC, a stuck
     * disk, a suspended container — wedges every writer that routes to it, forever, and takes the whole
     * write thread pool with it. The node doing the waiting is healthy; it just never stops waiting.
     *
     * @return the timeout
     */
    private TimeValue forwardTimeout() {
        final MetadataPlane metadata = plane.get();
        return TimeValue.timeValueMillis(metadata == null ? DEFAULT_FORWARD_TIMEOUT_MILLIS : Math.max(1_000L, metadata.leaseTtlMillis()));
    }

    /**
     * How long a search forward may wait: the longer of the write bound and {@link #SEARCH_FORWARD_TIMEOUT}.
     *
     * @return the timeout
     */
    public TimeValue searchForwardTimeout() {
        final TimeValue write = forwardTimeout();
        return write.millis() > SEARCH_FORWARD_TIMEOUT.millis() ? write : SEARCH_FORWARD_TIMEOUT;
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
