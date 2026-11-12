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
import org.opensearch.serverless.shell.NodeComponents;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.threadpool.ThreadPool;
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

    final ServerlessNode node;
    final TransportService transportService;
    final Supplier<MetadataPlane> plane;
    final TransportAuthenticator authenticator;
    /** Answers the work other nodes forward here. */
    final ForwardedWorkHandlers handlers = new ForwardedWorkHandlers(this);
    /**
     * Whether forwards to a peer in another zone are compressed, true by default. Forwarded documents and search hits
     * are mostly JSON source, which compresses several times over, and traffic between zones is billed by the byte.
     */
    public static final String COMPRESS_CROSS_ZONE = "serverless.network.compress_cross_zone";

    /** Bytes exchanged with peers, by whether they stayed in this node's zone. */
    final ZoneTraffic zoneTraffic = new ZoneTraffic();
    private final CommitPush commitPush;
    private final boolean meterZones;
    private final boolean compressCrossZone;
    private final String localZone;

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
            () -> node.secrets().transportSecrets(false),
            () -> node.secrets().transportSecrets(true),
            () -> TransportAuthenticator.identity(node.localNode()),
            () -> plane.get() == null ? System.currentTimeMillis() : plane.get().clock().getAsLong(),
            () -> forwardTimeout().millis(),
            node.settings().getAsInt("serverless.transport.replay_cache.capacity", 65_536)
        );
        this.meterZones = node.settings().getAsBoolean(ZoneTraffic.ENABLED, true);
        this.compressCrossZone = node.settings().getAsBoolean(COMPRESS_CROSS_ZONE, true);
        this.localZone = org.opensearch.serverless.cluster.Zones.localZone(node.settings());
        this.commitPush = new CommitPush(this);
    }

    /**
     * Returns what pushes this node's published commits to their search nodes, and takes the pushes of others.
     *
     * @return the commit push
     */
    public CommitPush commitPush() {
        return commitPush;
    }

    /**
     * Returns this node's zone, from its awareness attribute; see {@link org.opensearch.serverless.cluster.Zones}.
     *
     * @return the zone, or null when the node declares none
     */
    public String localZone() {
        return localZone;
    }

    /**
     * Returns the bytes this node has exchanged with peers, by zone path.
     *
     * @return the meter
     */
    public ZoneTraffic zoneTraffic() {
        return zoneTraffic;
    }

    /**
     * Returns the bytes this node's transport has put on the wire, compressed where compression applies: what the
     * payload counts of {@link #zoneTraffic()} become once sent.
     *
     * @return bytes sent since start
     */
    public long transportBytesSent() {
        return transportService.stats().getTxSize().getBytes();
    }

    /** Where an exchange with a peer goes, from the zone the peer's lease advertises. */
    ZoneTraffic.Path pathTo(DiscoveryNode peer) {
        final MetadataPlane metadata = plane.get();
        if (localZone == null || metadata == null || peer == null) {
            return ZoneTraffic.Path.UNKNOWN;
        }
        return ZoneTraffic.pathBetween(
            localZone,
            org.opensearch.serverless.cluster.Zones.zoneOf(metadata.membership().current(), peer.getId())
        );
    }

    private void meterRequest(String action, org.opensearch.transport.TransportRequest request, DiscoveryNode recipient) {
        if (meterZones == false) {
            return;
        }
        try {
            zoneTraffic.record(pathTo(recipient), action, ZoneTraffic.sizeOf(request));
        } catch (IOException | RuntimeException e) {
            // A meter that cannot count is a meter that skips one request, never a request that fails.
        }
    }

    /** Registers the handlers that answer forwarded work. Called once, at startup. */
    public void registerHandlers() {
        transportService.registerRequestHandler(
            ForwardedIndexRequest.ACTION,
            // Not WRITE: see FORWARDED_WRITE_POOL for the deadlock that was.
            org.opensearch.serverless.shell.NodeThreadPools.FORWARDED_WRITE_POOL,
            ForwardedIndexRequest::new,
            handlers::handleIndex
        );
        transportService.registerRequestHandler(
            ForwardedBulkRequest.ACTION,
            org.opensearch.serverless.shell.NodeThreadPools.FORWARDED_WRITE_POOL,
            ForwardedBulkRequest::new,
            handlers::handleBulk
        );
        transportService.registerRequestHandler(
            ForwardedGetRequest.ACTION,
            org.opensearch.serverless.shell.NodeThreadPools.FORWARDED_READ_POOL,
            ForwardedGetRequest::new,
            handlers::handleGet
        );
        transportService.registerRequestHandler(
            ForwardedSearchRequest.ACTION,
            org.opensearch.serverless.shell.NodeThreadPools.FANOUT_POOL,
            ForwardedSearchRequest::new,
            handlers::handleSearch
        );
        transportService.registerRequestHandler(
            ForwardedExplainRequest.ACTION,
            org.opensearch.serverless.shell.NodeThreadPools.FANOUT_POOL,
            ForwardedExplainRequest::new,
            handlers::handleExplain
        );
        transportService.registerRequestHandler(
            ForwardedFrozenSearchRequest.ACTION,
            org.opensearch.serverless.shell.NodeThreadPools.FANOUT_POOL,
            ForwardedFrozenSearchRequest::new,
            handlers::handleFrozenSearch
        );
        // On MANAGEMENT rather than on the fan-out pool: this is an operator asking a question, and it must
        // not compete for the threads that answer searches. A node under enough load to be worth asking
        // about is exactly the node whose fan-out pool is full.
        transportService.registerRequestHandler(
            ForwardedStatsRequest.ACTION,
            ThreadPool.Names.MANAGEMENT,
            ForwardedStatsRequest::new,
            handlers::handleStats
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
        try (var ignored = withMac(ForwardedStatsRequest.ACTION, request, peer)) {
            transportService.sendRequest(
                peer,
                ForwardedStatsRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedStatsResponse::new, this, peer, ForwardedStatsRequest.ACTION)
            );
        }
        return await(future, timeout, peer, ForwardedStatsRequest.ACTION);
    }

    /**
     * Sends a signed request to a peer and waits for its answer, bounded by the single-write deadline.
     *
     * @param peer the node to ask
     * @param action the action
     * @param request the request
     * @param reader reads the answer
     * @param <T> the answer's type
     * @return the answer
     * @throws IOException if the request cannot be signed or sent
     */
    <T extends org.opensearch.core.transport.TransportResponse> T send(
        DiscoveryNode peer,
        String action,
        org.opensearch.transport.TransportRequest request,
        org.opensearch.core.common.io.stream.Writeable.Reader<T> reader
    ) throws IOException {
        return send(peer, action, request, reader, forwardTimeout());
    }

    /** {@link #send} with an explicit deadline, for an answer that takes longer than a write to come. */
    <T extends org.opensearch.core.transport.TransportResponse> T send(
        DiscoveryNode peer,
        String action,
        org.opensearch.transport.TransportRequest request,
        org.opensearch.core.common.io.stream.Writeable.Reader<T> reader,
        TimeValue timeout
    ) throws IOException {
        final PlainActionFuture<T> future = PlainActionFuture.newFuture();
        try (var ignored = withMac(action, request, peer)) {
            transportService.sendRequest(
                peer,
                action,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, reader, this, peer, action)
            );
        }
        return await(future, timeout, peer, action);
    }

    /** How long a steered write waits for this node to take its shard. */
    static final long STEERED_ACTIVATION_WAIT_MILLIS = 30_000L;

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
     * <p>Each hop signs afresh. A signed frame carries no reusable deployment token; legacy token
     * authentication remains available only when this node cannot yet sign.
     *
     * <p>Package-private rather than private because {@link PluginHopAuthentication} signs a plugin's own
     * transport request with it. One implementation, so a plugin's hop and the shell's own are authenticated
     * the same way and cannot drift apart.
     */
    org.opensearch.common.util.concurrent.ThreadContext.StoredContext withMac(
        String action,
        org.opensearch.transport.TransportRequest request,
        DiscoveryNode recipient
    ) throws IOException {
        final org.opensearch.common.util.concurrent.ThreadContext context = transportService.getThreadPool().getThreadContext();
        final java.util.Map<String, String> headers = new java.util.HashMap<>(context.getHeaders());
        headers.remove(TransportAuthenticator.MAC_HEADER);
        headers.remove(TOKEN_HEADER);
        if (context.getTransient(PLUGIN_SUBJECT_TRANSIENT) != null) {
            headers.put(PLUGIN_ORIGIN_HEADER, "true");
        }
        final var restore = context.stashContext();
        try {
            // stashContext preserves task headers and registered transient propagators. Copy the caller's
            // remaining headers, but never reuse another hop's authentication for this request.
            for (var entry : headers.entrySet()) {
                if (context.getHeader(entry.getKey()) == null) {
                    context.putHeader(entry.getKey(), entry.getValue());
                }
            }
            final String mac;
            try (var out = new org.opensearch.common.io.stream.BytesStreamOutput()) {
                context.captureAsWriteable().writeTo(out);
                try (var in = out.bytes().streamInput()) {
                    mac = authenticator.sign(
                        action,
                        request,
                        TransportAuthenticator.identity(recipient),
                        org.opensearch.common.util.concurrent.ThreadContext.readHeadersFromStream(in).v1()
                    );
                }
            }
            meterRequest(action, request, recipient);
            if (mac != null) {
                context.putHeader(TransportAuthenticator.MAC_HEADER, mac);
            } else {
                final String token = node.secrets().transportToken();
                if (token != null) {
                    context.putHeader(TOKEN_HEADER, token);
                }
            }
            return restore;
        } catch (IOException | RuntimeException e) {
            restore.close();
            throw e;
        }
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
            authenticator.verify(action, request, mac, transportService.getThreadPool().getThreadContext().getRequestHeadersOnly());
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
    void requirePluginOriginForSystemIndex(String index) {
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
        final String expected = node.secrets().transportToken();
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

    ShardId localShard(String index, int shard, String indexUuid) {
        if (indexUuid == null) {
            return localShard(index, shard); // compatibility for legacy senders without an incarnation
        }
        final var held = node.reconciler().servingShard(new ShardId(index, indexUuid, shard));
        return held == null ? null : held.shardId();
    }

    /**
     * The shard as this node holds it for writing, for the incarnation of the index the sender named.
     *
     * @param index the index name
     * @param shard the shard number
     * @param indexUuid the uuid the sender resolved, or null to match by name alone
     * @return the writer shard, or null when this node holds no such writer
     */
    ShardId writerShard(String index, int shard, String indexUuid) {
        final ShardId found = localShard(index, shard, indexUuid);
        return found == null || node.reconciler().isReader(found) ? null : found;
    }

    /**
     * Refuses a forwarded write for a shard this node holds but no longer owns.
     *
     * <p>Holding a shard open is not the same as owning it. A writer that lapsed past its lease and has
     * not yet run the heartbeat that would notice still has the shard open; a write applied then goes into
     * a log the successor sealed, acknowledged and never replayed. The node's own check asks the shard-head
     * it read most recently, and a head older than a lease is no answer at all.
     */
    void requireLiveOwnership(ShardId shardId) {
        if (node.fences().holdsAsWriter(shardId, headAgeBoundMillis()) == false) {
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

    /**
     * Says what this node holds instead of the writer a forward asked for, for the refusal's message.
     *
     * <p>And doubts its own ownership when it holds no writer at all. The forward came because a head named this
     * node; if it still does, nothing else will ever open the shard -- peers forward here while this node's lease
     * is live -- so the doubt's activation re-takes it and replays its log. If the head names someone else, the
     * activation finds that out and does nothing.
     */
    String whyNotHeld(String index, int shard, String indexUuid) {
        final ShardId open = localShard(index, shard);
        if (open == null || node.reconciler().isReader(open)) {
            node.signals().ownershipDoubted(index, shard);
        }
        if (open == null) {
            return "the shard is not open here";
        }
        if (indexUuid != null && indexUuid.equals(open.getIndex().getUUID()) == false) {
            return "the index was recreated, and the shard open here belongs to the previous incarnation";
        }
        if (node.reconciler().isReader(open)) {
            return "it is open here only as a search reader";
        }
        return "the shard-head no longer names this node";
    }

    ShardId localShard(String index, int shard) {
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
            NodeComponents.discoveryAttributesFor(lease.get().roles()),
            NodeComponents.discoveryRolesFor(lease.get().roles()),
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
                ).setConnectTimeout(timeout)
                    .setHandshakeTimeout(timeout)
                    // Compressed only to a peer in another zone, where every byte is billed; the answer comes back
                    // compressed too, because a node answers compressed what was asked compressed. Inside a zone the
                    // bytes are free and the CPU is not.
                    .setCompressionEnabled(compressCrossZone && pathTo(peer) == ZoneTraffic.Path.CROSS_ZONE)
                    .build()
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
        try (var ignored = withMac(ForwardedIndexRequest.ACTION, request, peer)) {
            transportService.sendRequest(
                peer,
                ForwardedIndexRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedIndexResponse::new, this, peer, ForwardedIndexRequest.ACTION)
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
        try (var ignored = withMac(ForwardedBulkRequest.ACTION, request, peer)) {
            transportService.sendRequest(
                peer,
                ForwardedBulkRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedBulkResponse::new, this, peer, ForwardedBulkRequest.ACTION)
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
        try (var ignored = withMac(ForwardedGetRequest.ACTION, request, peer)) {
            transportService.sendRequest(
                peer,
                ForwardedGetRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedGetResponse::new, this, peer, ForwardedGetRequest.ACTION)
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
        try (var ignored = withMac(ForwardedExplainRequest.ACTION, request, peer)) {
            transportService.sendRequest(
                peer,
                ForwardedExplainRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedExplainResponse::new, this, peer, ForwardedExplainRequest.ACTION)
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
        try (var ignored = withMac(ForwardedSearchRequest.ACTION, request, peer)) {
            transportService.sendRequest(
                peer,
                ForwardedSearchRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedSearchResponse::new, this, peer, ForwardedSearchRequest.ACTION)
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
        try (var ignored = withMac(ForwardedFrozenSearchRequest.ACTION, request, peer)) {
            transportService.sendRequest(
                peer,
                ForwardedFrozenSearchRequest.ACTION,
                request,
                TransportRequestOptions.builder().withTimeout(timeout).build(),
                new Handler<>(future, ForwardedSearchResponse::new, this, peer, ForwardedFrozenSearchRequest.ACTION)
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
        private final ShardRouter router;
        private final DiscoveryNode peer;
        private final String action;

        Handler(
            PlainActionFuture<T> future,
            org.opensearch.core.common.io.stream.Writeable.Reader<T> reader,
            ShardRouter router,
            DiscoveryNode peer,
            String action
        ) {
            this.future = future;
            this.reader = reader;
            this.router = router;
            this.peer = peer;
            this.action = action;
        }

        @Override
        public T read(org.opensearch.core.common.io.stream.StreamInput in) throws IOException {
            if (router.meterZones == false) {
                return reader.read(in);
            }
            // The answer arrives whole in memory, so what is left to read before and after is its size.
            final int before = in.available();
            final T answer = reader.read(in);
            try {
                router.zoneTraffic.record(router.pathTo(peer), action, before - in.available());
            } catch (IOException | RuntimeException e) {
                // Counting must never cost the answer.
            }
            return answer;
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
