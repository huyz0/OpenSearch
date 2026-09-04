/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.ObjectStores;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * {@code GET /_serverless/stats} — what this node is holding, and how close it is to its limits.
 *
 * <p><b>Why not {@code /_nodes/stats}.</b> That endpoint answers for a cluster: it reports every node, and
 * a node here cannot speak for any other without the cluster-wide state this design does not have. It stays
 * refused with the rest of {@code /_cluster/*} for exactly that reason. This answers only for the node it
 * was sent to, which is the honest scope, and lives under {@code /_serverless/} with the rest of what is
 * ours rather than borrowing a name whose meaning is different.
 *
 * <p><b>What it is for.</b> Real circuit breakers and a bound on in-flight writes both turn a node that
 * would have died into a node that refuses a request. That is the right
 * trade and it leaves an operator with a 429 and no way to see the trend that produced it — whether the
 * node has been near its limit for an hour or was hit by one enormous request. These are the numbers behind
 * those refusals.
 *
 * <p><b>And what it costs.</b> The object store is priced per request, and the RFC treats request counts
 * as service levels from day one; until now the only counter lived in the test kit, so a deployment could
 * not see its own bill forming. The store's counters, the block cache's, the scheduler's and this node's
 * own lease are reported here from memory: nothing below reads the object store to describe it.
 *
 * <p><b>Counters, not a diagnosis.</b> Nothing here is derived, thresholded or averaged. A number that has
 * been through a formula is a number whose formula becomes the thing you have to understand before you can
 * trust it, and every one of these has a meaning an operator already knows. The one exception is
 * {@code implied_s3_requests}, which is labelled as the estimate it is.
 */
public final class StatsHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler without a metadata plane; the store, scheduler and lease sections are then absent.
     *
     * @param node supplies the node being asked about
     */
    public StatsHandler(Supplier<ServerlessNode> node) {
        this(node, () -> null);
    }

    /**
     * Creates the handler.
     *
     * @param node supplies the node being asked about
     * @param plane supplies the metadata plane, for the object store's counters and this node's lease
     */
    public StatsHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_stats_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_serverless/stats"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final ServerlessNode serving = node.get();
        final MetadataPlane metadata = plane.get();
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                builder.field("node", serving.localNode().getId());
                builder.field("name", serving.localNode().getName());

                // What it is serving, split the way the design splits it: a writer holds a shard-head and
                // can accept writes, a reader holds a commit and cannot. Reporting one number for both
                // would hide the distinction the whole architecture turns on.
                final Set<ShardId> open = serving.reconciler().openShards();
                final Set<ShardId> frozen = serving.reconciler().frozenShards();
                // A frozen view is a reader the reconciler keeps in its reader set and leaves out of its
                // open set, so "open minus readers" went to zero with one writer and one view open, and
                // negative with two views and none. Counted directly instead: a writer is an open shard
                // that is not a reader, and a reader is a reader that is not a view.
                final Set<ShardId> readers = new java.util.HashSet<>(serving.reconciler().readerShards());
                readers.removeAll(frozen);
                long writers = 0;
                for (ShardId shard : open) {
                    if (readers.contains(shard) == false) {
                        writers++;
                    }
                }
                builder.startObject("shards");
                builder.field("open", open.size());
                builder.field("readers", readers.size());
                builder.field("writers", writers);
                // Counted apart from both: a frozen view is a reader that no policy will take away, so
                // folding it into the reader count would make a node look like it had readers it could
                // shed.
                builder.field("frozen_views", frozen.size());
                builder.endObject();

                builder.startArray("roles");
                for (String role : serving.roles()) {
                    builder.value(role);
                }
                builder.endArray();

                // The breakers, as core reports them. "limit" and "estimated" are the two numbers a
                // refusal is decided from, so they are the two an operator needs to see it coming.
                builder.startObject("breakers");
                // From the service's own stats rather than by asking for breakers by name: the parent is
                // not a child in the registry, so a hand-written list of names silently omitted the one
                // that actually trips first. This is the same source _nodes/stats reads.
                for (var breaker : serving.circuitBreakerService().stats().getAllStats()) {
                    builder.startObject(breaker.getName());
                    builder.field("limit_bytes", breaker.getLimit());
                    builder.field("estimated_bytes", breaker.getEstimated());
                    builder.field("tripped", breaker.getTrippedCount());
                    builder.endObject();
                }
                builder.endObject();

                // In-flight writes, which the breakers do not cover: these are the bytes of the writes
                // themselves rather than what computing an answer allocated.
                final var pressure = serving.indexingPressure().stats();
                builder.startObject("indexing_pressure");
                builder.field("current_bytes", pressure.getCurrentCombinedCoordinatingAndPrimaryBytes());
                builder.field("total_bytes", pressure.getTotalCombinedCoordinatingAndPrimaryBytes());
                builder.field("rejections", pressure.getCoordinatingRejections());
                builder.endObject();

                objectStore(builder, metadata);
                blockCache(builder, serving);
                scheduler(builder, serving);
                lease(builder, serving, metadata);
                heldShards(builder, serving, readers, frozen);

                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }

    /** The object store's request counters since this node started, when the store is the metered one. */
    private static void objectStore(XContentBuilder builder, MetadataPlane metadata) throws IOException {
        builder.startObject("object_store");
        final ObjectStores.Metered metered = metadata != null && metadata.blobStore() instanceof ObjectStores.Metered counted
            ? counted
            : null;
        if (metered == null) {
            // Said rather than reported as zeros: a zero would be a claim that no request was made.
            builder.field("metered", false);
            builder.endObject();
            return;
        }
        builder.field("metered", true);
        builder.field("register_reads", metered.registerReads());
        builder.field("register_cas", metered.registerWrites());
        builder.field("blob_reads", metered.blobReads());
        builder.field("blob_writes", metered.blobWrites());
        builder.field("listings", metered.listings());
        builder.field("deletes", metered.deletes());
        builder.field("bytes_read", metered.bytesRead());
        builder.field("bytes_written", metered.bytesWritten());
        builder.field("errors", metered.errors());
        // An estimate, labelled as one: a compare-and-swap is one call here and two requests on S3.
        builder.field("implied_s3_requests", metered.impliedS3Requests());
        builder.endObject();
    }

    /** The block cache's hit rate and what it has actually pulled from the store. */
    private static void blockCache(XContentBuilder builder, ServerlessNode serving) throws IOException {
        final var cache = serving.blockCache();
        builder.startObject("block_cache");
        builder.field("hits", cache.hits());
        builder.field("misses", cache.misses());
        builder.field("bytes_fetched", cache.bytesFetched());
        builder.endObject();
    }

    /** What the three drivers have done, when a scheduler is driving this node. */
    private static void scheduler(XContentBuilder builder, ServerlessNode serving) throws IOException {
        if (serving.signals() instanceof org.opensearch.serverless.reconcile.ReconcileScheduler scheduler) {
            final var counts = scheduler.counts();
            builder.startObject("scheduler");
            builder.field("renewals", counts.renewals());
            builder.field("publishes", counts.publishes());
            builder.field("activations", counts.activations());
            builder.field("backstops", counts.backstops());
            builder.endObject();
        }
    }

    /**
     * This node's own lease, from the membership snapshot it already holds.
     *
     * <p>The number an operator needs before shards vanish: a lease that is about to lapse under a healthy
     * node is a renewal that is taking too long, and it is invisible until the peers start stealing.
     */
    private static void lease(XContentBuilder builder, ServerlessNode serving, MetadataPlane metadata) throws IOException {
        if (metadata == null) {
            return;
        }
        final long now = metadata.clock().getAsLong();
        builder.startObject("lease");
        builder.field("valid_now", metadata.membership().selfLeaseValidAt(now));
        final String self = serving.localNode().getId();
        for (var lease : metadata.membership().current()) {
            if (self.equals(lease.nodeId())) {
                builder.field("expires_at_millis", lease.expiresAtMillis());
                builder.field("expires_in_millis", lease.expiresAtMillis() - now);
            }
        }
        builder.field("ttl_millis", metadata.leaseTtlMillis());
        builder.field("known_nodes", metadata.membership().current().size());
        builder.endObject();
    }

    /**
     * Every shard this node holds, one line each: which, as what, at what term, how far it has written,
     * and when it was last asked for anything. The per-node list the fleet-wide view deliberately does
     * not offer, and it is cheap because it is read from memory.
     */
    private static void heldShards(XContentBuilder builder, ServerlessNode serving, Set<ShardId> readers, Set<ShardId> frozen)
        throws IOException {
        final List<ShardId> held = new ArrayList<>(serving.reconciler().heldShards());
        held.sort(Comparator.comparing(ShardId::getIndexName).thenComparing(s -> s.getIndex().getUUID()).thenComparingInt(ShardId::id));
        builder.startArray("held_shards");
        for (ShardId shard : held) {
            builder.startObject();
            builder.field("index", shard.getIndexName());
            builder.field("uuid", shard.getIndex().getUUID());
            builder.field("shard", shard.id());
            builder.field("kind", frozen.contains(shard) ? "frozen_view" : readers.contains(shard) ? "reader" : "writer");
            final var opened = serving.reconciler().shard(shard);
            if (opened != null) {
                try {
                    builder.field("term", opened.getOperationPrimaryTerm());
                    builder.field("max_seq_no", opened.seqNoStats().getMaxSeqNo());
                } catch (Exception e) {
                    // Closing under us. The line still names the shard; the numbers are simply absent.
                }
            }
            final var lastUsed = serving.reconciler().lastUsed(shard);
            if (lastUsed.isPresent()) {
                builder.field("last_used_millis", lastUsed.getAsLong());
            }
            builder.endObject();
        }
        builder.endArray();
    }
}
