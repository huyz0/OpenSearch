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
 * <p><b>Why not {@code /_nodes/stats}.</b> Not because a node cannot reach another — {@code ?nodes=_all}
 * here does exactly that, over the same authenticated transport every other hop uses. Because that endpoint
 * has a <em>schema</em>: indices, os, jvm, fs, transport, thread_pool, none of which this shell produces.
 * Serving this document under that name would hand a client parsing core's schema something that looks
 * right and answers nothing, which is worse than a refusal that points at the endpoint that does answer. So
 * this lives under {@code /_serverless/} with the rest of what is ours, and reports what is actually known.
 *
 * <p><b>Answering for the fleet.</b> With no {@code nodes} parameter this describes the node it was sent to.
 * With one — {@code _all}, or a comma-separated list of ids or names — it asks each live member and returns
 * that member's own document under its id, unmodified. A node that did not answer is <em>named</em>, with a
 * reason, and counted: the members worth asking about are the ones most likely to time out, so a fan-out
 * that quietly returned what it could reach would let an operator read "everything is fine" off a report
 * that is missing the node that is not.
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
        final String nodes = request.param("nodes");
        if (nodes != null) {
            // Off the calling thread. The fan-out blocks on peers, and Fanout runs the caller's share on
            // whichever thread called it, so doing this inline would block an HTTP worker for as long as
            // the slowest node takes to answer.
            return channel -> serving.threadPool()
                .executor(org.opensearch.threadpool.ThreadPool.Names.MANAGEMENT)
                .execute(() -> {
                    try {
                        fleet(channel, serving, metadata, nodes);
                    } catch (Exception e) {
                        try {
                            channel.sendResponse(
                                IndexAdminHandler.error(channel, RestStatus.INTERNAL_SERVER_ERROR, "stats_failed", describeFailure(e))
                            );
                        } catch (Exception ignored) {
                            // The channel is gone; there is nowhere left to report this.
                        }
                    }
                });
        }
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                builder.startObject();
                describe(builder, serving, metadata);
                builder.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }

    /**
     * Answers for more than one node: {@code ?nodes=_all}, or a comma-separated list of ids or names.
     *
     * <p><b>Why this is here and not at {@code /_nodes/stats}.</b> That endpoint has a schema — indices,
     * os, jvm, fs, transport, thread_pool — and this shell does not produce those numbers. Serving this
     * body under that name would hand a client that parses core's schema a document that looks right and
     * answers nothing, which is worse than the refusal. So {@code /_nodes/stats} stays refused, and what
     * was actually missing — a way to ask one node about the fleet — lives in this shell's own namespace
     * next to the single-node answer whose shape it repeats.
     *
     * <p><b>The accounting is the point.</b> A node that did not answer is named in {@code failures} with
     * the reason, and counted in {@code _nodes.failed}. A fan-out that silently returned the nodes it
     * could reach would let an operator read "everything is fine" off a report that is missing the node
     * that is not fine — and the nodes worth asking about are exactly the ones most likely to time out.
     *
     * <p>Asked concurrently, and bounded per node by the forward deadline, for the same reason: the whole
     * answer must not be held up by the slowest member, or it arrives after it was useful.
     */
    private void fleet(org.opensearch.rest.RestChannel channel, ServerlessNode serving, MetadataPlane metadata, String nodes)
        throws IOException {
        if (metadata == null) {
            channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
            return;
        }
        final java.util.Set<String> wanted = new java.util.LinkedHashSet<>(List.of(nodes.split(",")));
        final boolean all = wanted.contains("_all");

        metadata.membership().refreshIfOlderThan(Math.max(1_000L, metadata.leaseTtlMillis() / 2));
        final long now = metadata.clock().getAsLong();
        final List<org.opensearch.serverless.membership.NodeLease> selected = new ArrayList<>();
        for (org.opensearch.serverless.membership.NodeLease lease : metadata.membership().current()) {
            if (lease.isExpiredAt(now)) {
                continue;
            }
            if (all || wanted.contains(lease.nodeId()) || wanted.contains(lease.name())) {
                selected.add(lease);
            }
        }
        selected.sort(Comparator.comparing(org.opensearch.serverless.membership.NodeLease::nodeId));

        // A name that matched nothing is said, rather than quietly producing a shorter list. An operator
        // who mistyped a node name and got an answer about the others would read it as the fleet.
        if (all == false) {
            final java.util.Set<String> matched = new java.util.HashSet<>();
            for (var lease : selected) {
                matched.add(lease.nodeId());
                matched.add(lease.name());
            }
            final java.util.List<String> unknown = wanted.stream().filter(one -> matched.contains(one) == false).sorted().toList();
            if (unknown.isEmpty() == false) {
                channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        RestStatus.NOT_FOUND,
                        "node_not_found",
                        "no live node matches " + unknown + "; GET /_nodes lists the fleet"
                    )
                );
                return;
            }
        }

        final String localId = serving.localNode().getId();
        final List<java.util.concurrent.Callable<Answered>> asks = new ArrayList<>(selected.size());
        for (var lease : selected) {
            asks.add(() -> ask(serving, metadata, lease, localId));
        }
        final List<Answered> answers;
        try {
            answers = Fanout.run(
                serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.MANAGEMENT),
                Fanout.DEFAULT_CONCURRENCY,
                asks
            );
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "interrupted", "the fan-out was interrupted")
            );
            return;
        }

        // Fanout leaves a null where a task threw. Nothing above should throw -- ask() catches -- but a
        // null here would become a NullPointerException in the middle of rendering, which is the one
        // outcome an accounting endpoint must not have. Filled in as the failure it is.
        final List<Answered> reported = new ArrayList<>(answers.size());
        for (int i = 0; i < answers.size(); i++) {
            final Answered answered = answers.get(i);
            reported.add(
                answered != null
                    ? answered
                    : new Answered(selected.get(i).nodeId(), selected.get(i).name(), null, "this node could not ask it")
            );
        }
        int successful = 0;
        for (Answered answered : reported) {
            if (answered.statistics != null) {
                successful++;
            }
        }
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.startObject("_nodes");
            builder.field("total", reported.size());
            builder.field("successful", successful);
            builder.field("failed", reported.size() - successful);
            builder.endObject();
            builder.startObject("nodes");
            for (Answered answered : reported) {
                if (answered.statistics == null) {
                    continue;
                }
                builder.field(answered.nodeId);
                // Copied through rather than re-rendered: the node that owns the numbers decided their
                // shape, and this is the only place that would have to change if it ever stopped agreeing.
                builder.rawValue(
                    new java.io.ByteArrayInputStream(answered.statistics.getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                    org.opensearch.common.xcontent.XContentType.JSON
                );
            }
            builder.endObject();
            builder.startArray("failures");
            for (Answered answered : reported) {
                if (answered.statistics != null) {
                    continue;
                }
                builder.startObject();
                builder.field("node", answered.nodeId);
                builder.field("name", answered.name);
                builder.field("reason", answered.reason);
                builder.endObject();
            }
            builder.endArray();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    /** One node's answer, or why there is not one. */
    private record Answered(String nodeId, String name, String statistics, String reason) {}

    private static Answered ask(
        ServerlessNode serving,
        MetadataPlane metadata,
        org.opensearch.serverless.membership.NodeLease lease,
        String localId
    ) {
        if (lease.nodeId().equals(localId)) {
            // No hop to itself. A node that answered its own question over the transport would be one
            // connection failure away from reporting that it could not reach itself.
            try (XContentBuilder mine = org.opensearch.common.xcontent.XContentFactory.jsonBuilder()) {
                mine.startObject();
                describe(mine, serving, metadata);
                mine.endObject();
                return new Answered(
                    lease.nodeId(),
                    lease.name(),
                    org.opensearch.core.common.bytes.BytesReference.bytes(mine).utf8ToString(),
                    null
                );
            } catch (Exception e) {
                return new Answered(lease.nodeId(), lease.name(), null, describeFailure(e));
            }
        }
        try {
            final var peer = serving.router().peer(lease.nodeId());
            if (peer.isEmpty()) {
                return new Answered(lease.nodeId(), lease.name(), null, "its lease names an address this node could not reach");
            }
            return new Answered(lease.nodeId(), lease.name(), serving.router().forwardStats(peer.get()).statistics(), null);
        } catch (Exception e) {
            return new Answered(lease.nodeId(), lease.name(), null, describeFailure(e));
        }
    }

    private static String describeFailure(Exception e) {
        final String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getSimpleName() : message;
    }

    /**
     * Writes one node's statistics into an object the caller has already opened.
     *
     * <p>Separated from the route so that a node answering for itself and a node answering on behalf of a
     * peer produce byte-identical sections. A fan-out that rendered its own summary of a peer's numbers
     * would be a second account of them, and the two would drift.
     *
     * @param builder the builder, positioned inside an open object
     * @param serving the node to describe
     * @param metadata the metadata plane, or null when there is none
     * @throws IOException if writing fails
     */
    public static void describe(XContentBuilder builder, ServerlessNode serving, MetadataPlane metadata) throws IOException {
        {
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
        }
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
