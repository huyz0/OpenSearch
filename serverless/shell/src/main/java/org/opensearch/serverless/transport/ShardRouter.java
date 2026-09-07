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
import org.opensearch.serverless.shard.ShardExplain;
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
    private final TransportAuthenticator authenticator;

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
        // Suppliers throughout: the router is built before the node's plane is attached and before its
        // local node exists, and the window is the lease TTL, which is the plane's to say.
        this.authenticator = new TransportAuthenticator(
            () -> node.transportSecrets(false),
            () -> node.transportSecrets(true),
            () -> node.localNode().getId(),
            () -> plane.get() == null ? System.currentTimeMillis() : plane.get().clock().getAsLong(),
            () -> forwardTimeout().millis()
        );
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
            org.opensearch.serverless.shell.ServerlessNode.FANOUT_POOL,
            ForwardedSearchRequest::new,
            this::handleSearch
        );
        transportService.registerRequestHandler(
            ForwardedExplainRequest.ACTION,
            org.opensearch.serverless.shell.ServerlessNode.FANOUT_POOL,
            ForwardedExplainRequest::new,
            this::handleExplain
        );
        transportService.registerRequestHandler(
            ForwardedFrozenSearchRequest.ACTION,
            org.opensearch.serverless.shell.ServerlessNode.FANOUT_POOL,
            ForwardedFrozenSearchRequest::new,
            this::handleFrozenSearch
        );
        // On MANAGEMENT rather than on the fan-out pool: this is an operator asking a question, and it must
        // not compete for the threads that answer searches. A node under enough load to be worth asking
        // about is exactly the node whose fan-out pool is full.
        transportService.registerRequestHandler(
            ForwardedStatsRequest.ACTION,
            ThreadPool.Names.MANAGEMENT,
            ForwardedStatsRequest::new,
            this::handleStats
        );
    }

    private void handleStats(ForwardedStatsRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        requireMac(ForwardedStatsRequest.ACTION, request);
        // Rendered by the same code that answers this node's own /_serverless/stats, so a node described
        // through a peer and a node described directly cannot disagree.
        final org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder();
        builder.startObject();
        org.opensearch.serverless.rest.StatsHandler.describe(builder, node, plane.get());
        builder.endObject();
        channel.sendResponse(
            new ForwardedStatsResponse(
                node.localNode().getId(),
                org.opensearch.core.common.bytes.BytesReference.bytes(builder).utf8ToString()
            )
        );
    }

    /**
     * Asks one node for its own statistics.
     *
     * <p>Bounded by the single-write deadline. A node that cannot describe itself within one lease is a
     * node the answer should report as unreachable rather than one the whole fan-out should wait for: the
     * point of the accounting is to say which nodes did not answer, and that is only useful if it arrives.
     *
     * @param peer the node to ask
     * @return that node's statistics
     * @throws IOException if the request cannot be sent
     */
    public ForwardedStatsResponse forwardStats(DiscoveryNode peer) throws IOException {
        final ForwardedStatsRequest request = new ForwardedStatsRequest();
        final PlainActionFuture<ForwardedStatsResponse> future = PlainActionFuture.newFuture();
        final org.opensearch.common.unit.TimeValue timeout = forwardTimeout();
        try (var ignored = withMac(ForwardedStatsRequest.ACTION, request)) {
            transportService.sendRequest(
                peer,
                ForwardedStatsRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedStatsResponse::new)
            );
        }
        return await(future, timeout, peer, ForwardedStatsRequest.ACTION);
    }

    private void handleIndex(ForwardedIndexRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        requireMac(ForwardedIndexRequest.ACTION, request);
        requirePluginOriginForSystemIndex(request.index());
        final ShardId shardId = writerShard(request.index(), request.shard(), request.indexUuid());
        // Accounted on the owner as a primary operation, the way core accounts a write that arrived from
        // a coordinating node: a flood of forwarded writes used to be invisible to this node's pressure.
        try (
            org.opensearch.common.lease.Releasable primary = node.indexingPressure()
                .markPrimaryOperationStarted(request.source() == null ? 0L : request.source().length(), false)
        ) {
            handleIndexAccounted(request, channel, shardId);
        }
    }

    private void handleIndexAccounted(ForwardedIndexRequest request, TransportChannel channel, ShardId shardId) throws Exception {
        if (shardId == null) {
            // Ownership moved between the sender reading the head and this arriving. Failing is right:
            // the sender re-reads and retries, and accepting a write for a shard we do not own is the
            // one outcome that would be unsafe. Typed, so the sender can tell this refusal from any other
            // failure without reading prose. Matched as a writer and by uuid: this used to match by name
            // alone, so a shard held here only as a search reader took the write and refused it with a
            // message the sender did not recognise, and a shard of a deleted index of the same name took
            // the write and acknowledged it.
            throw new NotShardOwnerException(
                request.index(),
                request.shard(),
                whyNotHeld(request.index(), request.shard(), request.indexUuid())
            );
        }
        requireLiveOwnership(shardId);
        final org.opensearch.serverless.shell.ServerlessNode.WriteOutcome outcome;
        if (request.deletion()) {
            outcome = node.delete(shardId, request.id(), request.ifSeqNo(), request.ifPrimaryTerm());
        } else {
            // requireAbsent travels on the wire and used to be dropped here, so a forwarded _create was an
            // overwrite -- and a data-stream write, which is create-only, was one too.
            outcome = node.index(
                shardId,
                request.id(),
                request.source(),
                request.ifSeqNo(),
                request.ifPrimaryTerm(),
                request.requireAbsent()
            );
        }
        if (request.refresh()) {
            node.reconciler().shard(shardId).refresh("serverless-forwarded-refresh");
        }
        channel.sendResponse(
            new ForwardedIndexResponse(
                node.localNode().getId(),
                outcome.seqNo(),
                outcome.primaryTerm(),
                outcome.version(),
                outcome.created(),
                outcome.found()
            )
        );
    }

    private void handleBulk(ForwardedBulkRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        requireMac(ForwardedBulkRequest.ACTION, request);
        requirePluginOriginForSystemIndex(request.index());
        final ShardId shardId = writerShard(request.index(), request.shard(), request.indexUuid());
        // Counted by the request as it was built, not by re-encoding every record here to measure it.
        try (org.opensearch.common.lease.Releasable primary = node.indexingPressure().markPrimaryOperationStarted(request.bytes(), false)) {
            handleBulkAccounted(request, channel, shardId);
        }
    }

    private void handleBulkAccounted(ForwardedBulkRequest request, TransportChannel channel, ShardId shardId) throws Exception {
        if (shardId == null) {
            // Same refusal as a forwarded single write, and for the same reason: ownership moved between
            // the sender reading the head and this arriving. Refusing the whole batch is right -- every
            // item in it routes to this one shard, so there is no partial answer to give.
            throw new NotShardOwnerException(
                request.index(),
                request.shard(),
                whyNotHeld(request.index(), request.shard(), request.indexUuid())
            );
        }
        requireLiveOwnership(shardId);
        final var outcomes = node.bulkOperations(shardId, request.batch());
        if (request.refresh()) {
            node.reconciler().shard(shardId).refresh("serverless-forwarded-refresh");
        }
        channel.sendResponse(new ForwardedBulkResponse(node.localNode().getId(), outcomes));
    }

    private void handleGet(ForwardedGetRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        requireMac(ForwardedGetRequest.ACTION, request);
        final ShardId shardId = writerShard(request.index(), request.shard());
        if (shardId == null) {
            // Refuse rather than open it as a reader, which is what handleSearch does here and is right
            // there. A get was forwarded to this node precisely because the shard-head named it the
            // owner; if the shard is not open as a writer, that head is stale, and answering from a
            // published commit would return a stale document to a caller that asked for a fresh one.
            // Failing sends the caller back to re-read the head, which is the only thing that fixes it.
            throw new NotShardOwnerException(request.index(), request.shard(), whyNotHeld(request.index(), request.shard(), null));
        }
        channel.sendResponse(new ForwardedGetResponse(node.localNode().getId(), node.get(shardId, request.id())));
    }

    private void handleExplain(ForwardedExplainRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        requireMac(ForwardedExplainRequest.ACTION, request);
        final ShardId shardId = writerShard(request.index(), request.shard());
        if (shardId == null) {
            // Refused rather than opened as a reader, following handleGet and not handleSearch. An explain
            // was forwarded here because the shard-head named this node the owner; if the shard is not open
            // as a writer that head is stale, and explaining from a published commit would score against
            // segment statistics that are missing every unpublished write. The number would look right.
            throw new NotShardOwnerException(request.index(), request.shard(), whyNotHeld(request.index(), request.shard(), null));
        }
        final ShardExplain.Outcome outcome = ShardExplain.execute(node.searchService(), shardId, request.id(), request.query());
        channel.sendResponse(new ForwardedExplainResponse(node.localNode().getId(), outcome.exists(), outcome.explanation()));
    }

    private void handleSearch(ForwardedSearchRequest request, TransportChannel channel, org.opensearch.tasks.Task task) throws Exception {
        requireMac(ForwardedSearchRequest.ACTION, request);
        ShardId shardId = localShard(request.index(), request.shard(), request.indexUuid());
        if (shardId == null) {
            // Open it. This is the property that makes placement a hint rather than a requirement: a
            // node asked for a shard it does not have fetches the manifest and serves, so a stale or
            // unlucky routing decision costs a cold read and never a wrong answer.
            shardId = node.serveAsReader(plane.get(), request.index(), request.shard());
            if (request.indexUuid() != null && request.indexUuid().equals(shardId.getIndex().getUUID()) == false) {
                throw new IllegalStateException("index [" + request.index() + "] was recreated while this search was in flight");
            }
        }
        node.markUsed(shardId);
        // The coordinator's instant for "now", so this shard scores against the same clock as every other
        // shard of the same search. Entered and exited around the query so the reconciler cannot release
        // the reader from under a running query.
        node.reconciler().enter(shardId);
        final ShardQuery.Result result;
        try {
            result = ShardQuery.execute(node.searchService(), shardId, request.source(), request.nowInMillis());
        } finally {
            node.reconciler().exit(shardId);
        }
        channel.sendResponse(new ForwardedSearchResponse(result));
    }

    private void handleFrozenSearch(ForwardedFrozenSearchRequest request, TransportChannel channel, org.opensearch.tasks.Task task)
        throws Exception {
        requireMac(ForwardedFrozenSearchRequest.ACTION, request);
        // No "not held" refusal here, unlike handleIndex/handleGet: a view has no owner to be stale about,
        // and openFrozenView is idempotent -- opening it here for the first time is exactly what placement
        // being a hint means. plane.get() rather than the request: the view travelled the wire, but which
        // object store to open its files from is this node's own configuration, never the sender's.
        final ShardId shardId = node.openFrozenView(plane.get(), request.pit(), request.shard());
        node.reconciler().enter(shardId);
        final ShardQuery.Result result;
        try {
            result = ShardQuery.execute(node.searchService(), shardId, request.source(), request.nowInMillis());
        } finally {
            node.reconciler().exit(shardId);
        }
        channel.sendResponse(new ForwardedSearchResponse(result));
    }

    /** The header a forwarded request carries its deployment secret in, during the move to the MAC. */
    static final String TOKEN_HEADER = "x-serverless-transport-token";

    /**
     * The header that says the forwarded request originated inside a plugin, which is the only caller that
     * may write a system index. Only as trustworthy as the authentication it rides with, which is the point:
     * a member that is also a plugin.
     */
    static final String PLUGIN_ORIGIN_HEADER = "x-serverless-plugin-origin";

    /**
     * The thread-context transient the auth plugin sets while a plugin acts as itself. Must equal
     * {@code ServerlessAuthPlugin.PLUGIN_SUBJECT}; the two are pinned together by a test rather than by a
     * dependency, because the shell must not depend on the plugin.
     */
    public static final String PLUGIN_SUBJECT_TRANSIENT = "serverless_plugin_subject";

    /**
     * Signs the outgoing request into the caller's context, restored when the try closes.
     *
     * <p>Added to the caller's context rather than to a fresh one: the caller's own headers -- the
     * authenticated principal a plugin set -- must travel with the forwarded request too. Captured before
     * anything goes on, so closing the try takes it off again; this used to return a no-op and the secret
     * lingered on the REST request's context, riding every later outgoing call from it.
     *
     * <p>During the move from the static token to the per-request MAC both are presented: a receiver that
     * knows the MAC prefers it, one that does not still finds the token.
     *
     * <p>Package-private rather than private because {@link PluginHopAuthentication} signs a plugin's own
     * transport request with it. One implementation, so a plugin's hop and the shell's own are authenticated
     * the same way and cannot drift apart.
     */
    org.opensearch.common.util.concurrent.ThreadContext.StoredContext withMac(
        String action,
        org.opensearch.transport.TransportRequest request
    ) throws IOException {
        final org.opensearch.common.util.concurrent.ThreadContext context = transportService.getThreadPool().getThreadContext();
        final org.opensearch.common.util.concurrent.ThreadContext.StoredContext restore = context.newStoredContext(false);
        final String mac = authenticator.sign(action, request);
        if (mac != null && context.getHeader(TransportAuthenticator.MAC_HEADER) == null) {
            context.putHeader(TransportAuthenticator.MAC_HEADER, mac);
        }
        final String token = node.transportToken();
        if (token != null && context.getHeader(TOKEN_HEADER) == null) {
            context.putHeader(TOKEN_HEADER, token);
        }
        if (context.getTransient(PLUGIN_SUBJECT_TRANSIENT) != null && context.getHeader(PLUGIN_ORIGIN_HEADER) == null) {
            context.putHeader(PLUGIN_ORIGIN_HEADER, "true");
        }
        return restore;
    }

    /**
     * Refuses a forwarded request that does not authenticate.
     *
     * <p>Every handler calls this first. The transport port used to serve anyone who reached it; a caller
     * that has not read the object store is not a member, whatever address it came from. The MAC is
     * preferred and checked whenever presented; the static token is accepted only in its absence, and only
     * until every sender signs.
     *
     * <p>Package-private for {@link PluginHopAuthentication}, which applies it to a plugin's action arriving
     * over the transport. Before that, a plugin's {@code HandledTransportAction} registered its own handler
     * and nothing checked who called it -- so anything that could reach the port could invoke a plugin's
     * action, the auth plugin's own included.
     */
    void requireMac(String action, org.opensearch.transport.TransportRequest request) throws IOException {
        final String mac = transportService.getThreadPool().getThreadContext().getHeader(TransportAuthenticator.MAC_HEADER);
        if (mac != null) {
            authenticator.verify(action, request, mac);
            return;
        }
        requireToken();
    }

    /**
     * Refuses a forwarded write to a system index unless it originated inside a plugin.
     *
     * <p>Defence in depth: the coordinator already refuses REST callers by name, but a forwarded write
     * arrives here with no caller at all, and a member that has been compromised -- or a plugin on another
     * node that was handed a {@code Client} -- should not be able to write another plugin's records by
     * routing them through the transport.
     */
    private void requirePluginOriginForSystemIndex(String index) {
        if (node.isSystemIndex(index)
            && "true".equals(transportService.getThreadPool().getThreadContext().getHeader(PLUGIN_ORIGIN_HEADER)) == false) {
            throw new org.opensearch.OpenSearchSecurityException(
                "forwarded write to system index [" + index + "] without a plugin origin",
                org.opensearch.core.rest.RestStatus.FORBIDDEN
            );
        }
    }

    /** The static-token check, kept for senders that do not yet sign. */
    private void requireToken() {
        final String expected = node.transportToken();
        final String presented = transportService.getThreadPool().getThreadContext().getHeader(TOKEN_HEADER);
        if (expected == null
            || presented == null
            || java.security.MessageDigest.isEqual(
                expected.getBytes(java.nio.charset.StandardCharsets.UTF_8),
                presented.getBytes(java.nio.charset.StandardCharsets.UTF_8)
            ) == false) {
            throw new org.opensearch.OpenSearchSecurityException(
                "forwarded request without a valid transport token",
                org.opensearch.core.rest.RestStatus.FORBIDDEN
            );
        }
    }

    private ShardId localShard(String index, int shard, String indexUuid) {
        final ShardId found = localShard(index, shard);
        return found == null || indexUuid == null || indexUuid.equals(found.getIndex().getUUID()) ? found : null;
    }

    /**
     * The shard as this node holds it for writing, for the incarnation of the index the sender named.
     *
     * @param index the index name
     * @param shard the shard number
     * @param indexUuid the uuid the sender resolved, or null to match by name alone
     * @return the writer shard, or null when this node holds no such writer
     */
    private ShardId writerShard(String index, int shard, String indexUuid) {
        final ShardId found = localShard(index, shard, indexUuid);
        return found == null || node.reconciler().readerShards().contains(found) ? null : found;
    }

    /**
     * Refuses a forwarded write for a shard this node holds but no longer owns.
     *
     * <p>Holding a shard open is not the same as owning it. A writer that lapsed past its lease and has
     * not yet run the heartbeat that would notice still has the shard open; a write applied then goes into
     * a log the successor sealed, acknowledged and never replayed. The node's own check asks the shard-head
     * it read most recently, and a head older than a lease is no answer at all.
     */
    private void requireLiveOwnership(ShardId shardId) {
        if (node.holdsAsWriter(shardId, headAgeBoundMillis()) == false) {
            throw new NotShardOwnerException(
                shardId.getIndexName(),
                shardId.id(),
                "the shard is open here but no recent shard-head names this node as its writer"
            );
        }
    }

    /** How old a shard-head may be and still count as evidence of ownership: one lease. */
    private long headAgeBoundMillis() {
        final MetadataPlane metadata = plane.get();
        return metadata == null ? DEFAULT_FORWARD_TIMEOUT_MILLIS : Math.max(1_000L, metadata.leaseTtlMillis());
    }

    /** Says what this node holds instead of the writer a forward asked for, for the refusal's message. */
    private String whyNotHeld(String index, int shard, String indexUuid) {
        final ShardId open = localShard(index, shard);
        if (open == null) {
            return "the shard is not open here";
        }
        if (indexUuid != null && indexUuid.equals(open.getIndex().getUUID()) == false) {
            return "the index was recreated, and the shard open here belongs to the previous incarnation";
        }
        if (node.reconciler().readerShards().contains(open)) {
            return "it is open here only as a search reader";
        }
        return "the shard-head no longer names this node";
    }

    /**
     * The shard as this node holds it for writing, or null if it holds it only as a search reader.
     *
     * <p>A forwarded get or explain was sent here because the head named this node the owner. A reader
     * opened for a search is a published commit, not the owner's live shard, and answering from it
     * stamped {@code realtime: true} on a document that was missing every unpublished write.
     */
    private ShardId writerShard(String index, int shard) {
        final ShardId found = localShard(index, shard);
        return found == null || node.reconciler().readerShards().contains(found) ? null : found;
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
        // The membership snapshot first: it is refreshed every pass and costs nothing to consult, where
        // a lease read per forwarded write was one of the larger costs of a hot write path. The store is
        // read only for a node the snapshot has not seen yet.
        Optional<org.opensearch.serverless.membership.NodeLease> lease = metadata.membership()
            .current()
            .stream()
            .filter(l -> nodeId.equals(l.nodeId()))
            .findFirst();
        if (lease.isEmpty() || lease.get().isExpiredAt(metadata.clock().getAsLong())) {
            // Not in the snapshot, or in it with an expiry that has passed: the snapshot may simply be
            // older than the renewal, so the register is what answers, not the copy.
            lease = metadata.membership().read(nodeId);
        }
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
            // The lease's roles, as the node itself advertises them, rather than every built-in role: a
            // peer that never accepts writer activation should not be described as ingest.
            ServerlessNode.discoveryAttributesFor(lease.get().roles()),
            ServerlessNode.discoveryRolesFor(lease.get().roles()),
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
    public ForwardedIndexResponse forwardIndex(DiscoveryNode peer, ForwardedIndexRequest request) throws IOException {
        final PlainActionFuture<ForwardedIndexResponse> future = PlainActionFuture.newFuture();
        final org.opensearch.common.unit.TimeValue timeout = forwardTimeout();
        try (var ignored = withMac(ForwardedIndexRequest.ACTION, request)) {
            transportService.sendRequest(
                peer,
                ForwardedIndexRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedIndexResponse::new)
            );
        }
        return await(future, timeout, peer, ForwardedIndexRequest.ACTION);
    }

    /**
     * Sends a whole batch to the node that owns the shard and waits for its per-item answer.
     *
     * <p>Bounded by {@link #bulkForwardTimeout}, which scales the single-write deadline with the size of
     * the batch. It used to be the single-write deadline regardless: a five-thousand-document batch that
     * grew the mapping on a slow object store timed out on the sender, every item answered 503, the client
     * retried, and every auto-id document landed twice.
     *
     * @param peer the owning node
     * @param request the batch
     * @return the owner's per-item outcomes
     */
    public ForwardedBulkResponse forwardBulk(DiscoveryNode peer, ForwardedBulkRequest request) throws IOException {
        final PlainActionFuture<ForwardedBulkResponse> future = PlainActionFuture.newFuture();
        final org.opensearch.common.unit.TimeValue timeout = bulkForwardTimeout(request);
        try (var ignored = withMac(ForwardedBulkRequest.ACTION, request)) {
            transportService.sendRequest(
                peer,
                ForwardedBulkRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedBulkResponse::new)
            );
        }
        return await(future, timeout, peer, ForwardedBulkRequest.ACTION);
    }

    /** Operations per lease of deadline: a batch this size may wait one more lease than a single write. */
    private static final int BULK_OPERATIONS_PER_LEASE = 1_000;

    /** Bytes per lease of deadline, on the same footing. */
    private static final long BULK_BYTES_PER_LEASE = 8L << 20;

    /** The most leases a batch may wait, so a huge batch is still bounded by something a caller can predict. */
    private static final long BULK_MAX_LEASES = 5L;

    /**
     * How long a forwarded batch may wait: one lease, plus one per {@value #BULK_OPERATIONS_PER_LEASE}
     * operations or 8 MiB of source, whichever is more, capped at {@value #BULK_MAX_LEASES} leases.
     *
     * <p>The single-write bound is principled -- past the lease the owner is lapsing and the head should
     * be re-read -- but a batch that legitimately takes longer than a lease to apply is not a lapsing
     * owner, and giving up on it converts a slow success into a duplicated retry.
     *
     * @param request the batch
     * @return the timeout
     */
    TimeValue bulkForwardTimeout(ForwardedBulkRequest request) {
        final TimeValue base = forwardTimeout();
        final long byOperations = 1L + (request.batch().size() - 1L) / BULK_OPERATIONS_PER_LEASE;
        final long byBytes = 1L + Math.max(0L, request.bytes() - 1L) / BULK_BYTES_PER_LEASE;
        final long leases = Math.min(BULK_MAX_LEASES, Math.max(byOperations, byBytes));
        return TimeValue.timeValueMillis(base.millis() * leases);
    }

    /**
     * Waits on a forward, reporting the future's own deadline in the transport's terms.
     *
     * <p>{@code actionGet(timeout)} throws {@code OpenSearchTimeoutException} when the wait itself
     * expires, while the transport throws {@code ReceiveTimeoutTransportException} when its request
     * timeout does. They are the same event -- the owner did not answer in time, and may have applied the
     * request -- but only the second was classified as such by the callers, so the first was worded as
     * "stale routing, retry", which is the one message that must not be attached to a write that may
     * already have landed.
     */
    private static <T> T await(PlainActionFuture<T> future, TimeValue timeout, DiscoveryNode peer, String action) {
        try {
            return future.actionGet(timeout);
        } catch (org.opensearch.OpenSearchTimeoutException expired) {
            final org.opensearch.transport.ReceiveTimeoutTransportException timedOut =
                new org.opensearch.transport.ReceiveTimeoutTransportException(peer, action, "no answer within [" + timeout + "]");
            timedOut.addSuppressed(expired);
            throw timedOut;
        }
    }

    /**
     * Sends a read by id to the node that owns the shard and waits for the answer.
     *
     * @param peer the owning node
     * @param request the read
     * @return what the owner found
     */
    public ForwardedGetResponse forwardGet(DiscoveryNode peer, ForwardedGetRequest request) throws IOException {
        final PlainActionFuture<ForwardedGetResponse> future = PlainActionFuture.newFuture();
        // The write bound, not the search one. A get is a point lookup on the owner rather than a fan-out,
        // so it is the cheaper of the two and has no reason to wait as long as a query.
        final org.opensearch.common.unit.TimeValue timeout = forwardTimeout();
        try (var ignored = withMac(ForwardedGetRequest.ACTION, request)) {
            transportService.sendRequest(
                peer,
                ForwardedGetRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedGetResponse::new)
            );
        }
        return await(future, timeout, peer, ForwardedGetRequest.ACTION);
    }

    /**
     * Sends an explain to the node owning the document's shard and waits for the answer.
     *
     * @param peer the owning node
     * @param request the explain
     * @return what that node found
     */
    public ForwardedExplainResponse forwardExplain(DiscoveryNode peer, ForwardedExplainRequest request) throws IOException {
        final PlainActionFuture<ForwardedExplainResponse> future = PlainActionFuture.newFuture();
        // The get bound rather than the search one, for the reason a get uses it: this is a point lookup on
        // one shard with no fan-out to wait behind.
        final org.opensearch.common.unit.TimeValue timeout = forwardTimeout();
        try (var ignored = withMac(ForwardedExplainRequest.ACTION, request)) {
            transportService.sendRequest(
                peer,
                ForwardedExplainRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedExplainResponse::new)
            );
        }
        return await(future, timeout, peer, ForwardedExplainRequest.ACTION);
    }

    /**
     * Sends a shard query to the node holding it and waits for the answer.
     *
     * @param peer the holding node
     * @param request the query
     * @return that shard's answer
     */
    public ForwardedSearchResponse forwardSearch(DiscoveryNode peer, ForwardedSearchRequest request) throws IOException {
        final PlainActionFuture<ForwardedSearchResponse> future = PlainActionFuture.newFuture();
        final TimeValue timeout = searchForwardTimeout();
        try (var ignored = withMac(ForwardedSearchRequest.ACTION, request)) {
            transportService.sendRequest(
                peer,
                ForwardedSearchRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedSearchResponse::new)
            );
        }
        return future.actionGet(timeout);
    }

    /**
     * Sends a shard query against a frozen view to the node placement prefers and waits for the answer.
     *
     * <p>The search bound, same as {@link #forwardSearch} and for the same reason: a peer that is merely
     * busy opening a wide view should cost latency, not coverage.
     *
     * @param peer the preferred node
     * @param request the query
     * @return that shard's answer
     */
    public ForwardedSearchResponse forwardFrozenSearch(DiscoveryNode peer, ForwardedFrozenSearchRequest request) throws IOException {
        final PlainActionFuture<ForwardedSearchResponse> future = PlainActionFuture.newFuture();
        final TimeValue timeout = searchForwardTimeout();
        try (var ignored = withMac(ForwardedFrozenSearchRequest.ACTION, request)) {
            transportService.sendRequest(
                peer,
                ForwardedFrozenSearchRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedSearchResponse::new)
            );
        }
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
