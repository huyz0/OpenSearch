/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code GET /_cluster/health}, {@code /_cluster/health/{index}} and {@code GET /_cat/health}.
 *
 * <p><b>The colours had to be redefined, and that is the whole design.</b> Classic health's
 * green/yellow/red is an assertion about replica placement: green means every primary and replica is
 * assigned, yellow means replicas are missing, red means a primary is. This architecture has no replicas, so
 * <b>yellow is unreachable by construction</b> — and, more importantly, "primary unassigned" is the
 * <em>normal resting state</em> of a shard nobody is currently writing to. Mapping an unowned shard onto
 * classic's red or yellow would report a fault where there is none, and a client calling
 * {@code wait_for_status=green} would then block until every shard happened to be activated, which without
 * traffic may never happen. A compatibility gesture that deadlocks the caller is worse than a 501.
 *
 * <p>So the colours mean the only thing they can mean here:
 * <ul>
 *   <li><b>green</b> — every shard asked about can be served. It has a live owner, or it can be recovered
 *       from the object store on demand, or it is new and empty. A dormant shard is a healthy shard.</li>
 *   <li><b>red</b> — some shard cannot be served by anybody, which in practice means the control plane
 *       could not be read. That is a genuinely useful red: it is the failure that actually matters here.</li>
 *   <li><b>yellow</b> — never returned, and said so in the response rather than left as an unexplained
 *       absence.</li>
 * </ul>
 *
 * <p><b>Scope decides cost, and cost decides what is answerable.</b> An index-scoped question is one
 * descriptor read plus one head read per shard — bounded, and the same reads
 * {@code /_serverless/shards/{index}} already makes. A deployment-wide question about shards would have to
 * enumerate every index, which is the inventory operation this design refuses everywhere. So the unscoped
 * form answers the node half and reports {@code complete: false}, borrowing the field search responses
 * already use to say "this answer does not cover everything it might have". It does not report shard
 * counters as zero, because zero is a claim and the honest answer is that none were examined.
 *
 * <p><b>What a client must not read into a green.</b> It does not mean copies exist. Redundancy here is the
 * object store, not a replica, and {@code replication} says so on every response — an additive field a
 * client that does not know it will ignore and a person debugging will see.
 */
public final class ClusterHealthHandler extends BaseRestHandler {

    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30);
    private static final long POLL_MILLIS = 200L;

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node
     */
    public ClusterHealthHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_cluster_health_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_cluster/health"),
            new Route(RestRequest.Method.GET, "/_cluster/health/{index}"),
            new Route(RestRequest.Method.GET, "/_cat/health")
        );
    }

    /** What a health request asked for, after reading it. */
    private record Ask(List<String> indices, String waitForStatus, Integer waitForNodes, TimeValue timeout, boolean perShard) {
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final boolean cat = request.path().startsWith("/_cat");
        // Every parameter is read before any early return, because BaseRestHandler rejects a request whose
        // parameters were not all consumed -- which would turn a deliberate refusal into a 400.
        final String indexParam = request.param("index");
        final String waitForStatus = request.param("wait_for_status");
        final String waitForNodes = request.param("wait_for_nodes");
        final String level = request.param("level", "cluster");
        final String waitForActiveShards = request.param("wait_for_active_shards");
        final String waitForEvents = request.param("wait_for_events");
        // Nothing here ever relocates or initialises, so a caller waiting for neither is already satisfied.
        // Reading them and answering immediately is truthful; refusing them would be pedantry.
        request.param("wait_for_no_relocating_shards");
        request.param("wait_for_no_initializing_shards");
        // Hints: no cluster manager to time out against or to be local to, no awareness attributes, no
        // closed indices for expand_wildcards. _cat/health's ts, time and help shape a table that is
        // rendered one way here.
        for (String hint : new String[] {
            "local",
            "master_timeout",
            "cluster_manager_timeout",
            "expand_wildcards",
            "awareness_attribute",
            "ensure_node_weighed_in",
            "ts",
            "time",
            "help" }) {
            request.param(hint);
        }
        final TimeValue timeout = request.paramAsTime("timeout", DEFAULT_TIMEOUT);
        final String catUnsupported = cat ? CatTable.unsupported(request) : null;
        request.param("format");
        request.paramAsBoolean("v", false);

        final String refusal = refuse(cat, catUnsupported, waitForStatus, waitForActiveShards, waitForEvents, level);
        if (refusal != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_health_parameter", refusal)
            );
        }

        Integer nodesWanted = null;
        if (waitForNodes != null) {
            // ">=3", "3" and "ge(3)" all mean the same thing to a client waiting for a fleet to come up.
            final String digits = waitForNodes.replaceAll("[^0-9]", "");
            if (digits.isEmpty()) {
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "invalid_parameter", "wait_for_nodes must contain a number")
                );
            }
            nodesWanted = Integer.parseInt(digits);
        }

        final MetadataPlane metadata = plane.get();
        if (metadata == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        final List<String> indices = new ArrayList<>();
        if (indexParam != null) {
            for (String name : indexParam.split(",")) {
                if (name.isBlank() == false) {
                    indices.add(name.trim());
                }
            }
        }
        final Ask ask = new Ask(indices, waitForStatus, nodesWanted, timeout, "shards".equals(level));

        return channel -> dispatch(channel, () -> {
            final Health health = await(metadata, ask);
            if (health.missing != null) {
                channel.sendResponse(
                    IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + health.missing)
                );
                return;
            }
            if (cat) {
                final CatTable table = new CatTable("cluster", "status", "node.total", "node.data", "shards", "dormant");
                table.row(
                    metadata == null ? "serverless" : clusterName(),
                    health.status,
                    health.nodes,
                    health.nodes,
                    health.indices.isEmpty() ? "-" : Integer.toString(health.activeShards),
                    health.indices.isEmpty() ? "-" : Integer.toString(health.dormantShards)
                );
                table.send(channel, request);
                return;
            }
            render(channel, health, ask);
        });
    }

    private static String refuse(
        boolean cat,
        String catUnsupported,
        String waitForStatus,
        String waitForActiveShards,
        String waitForEvents,
        String level
    ) {
        if (cat && catUnsupported != null) {
            return catUnsupported;
        }
        if (waitForStatus != null && "green".equals(waitForStatus) == false && "yellow".equals(waitForStatus) == false) {
            return "wait_for_status must be 'green' or 'yellow'; 'red' is not a state to wait for, and yellow is "
                + "never returned here because it means 'replicas are missing' and there are no replicas -- a "
                + "caller waiting for yellow is satisfied by green";
        }
        if (waitForActiveShards != null) {
            return "wait_for_active_shards counts shards an allocator has placed, and there is no allocator here: "
                + "a shard is activated by the write that needs it, not by a placement decision. Wait for the "
                + "write instead, or use wait_for_status";
        }
        if (waitForEvents != null) {
            return "wait_for_events waits on a cluster manager's pending-task queue; there is no cluster manager "
                + "and no queue, so there is no event to wait for";
        }
        if ("cluster".equals(level) == false && "indices".equals(level) == false && "shards".equals(level) == false) {
            return "level must be 'cluster', 'indices' or 'shards'";
        }
        return null;
    }

    /** One computed answer. */
    private static final class Health {
        private String status = "green";
        private int nodes;
        private int activeShards;
        private int dormantShards;
        private int unservableShards;
        private String missing;
        private final Map<String, IndexHealth> indices = new LinkedHashMap<>();
    }

    /** One index's shards. */
    private static final class IndexHealth {
        private String status = "green";
        private int shards;
        private int active;
        private int dormant;
        private int unservable;
        private final Map<Integer, String> perShard = new LinkedHashMap<>();
    }

    /**
     * Computes health, retrying until the wait conditions hold or the timeout runs out.
     *
     * <p>Retrying is meaningful rather than ceremonial: status here is computed from a live read of the
     * control plane, and the fleet size from a live listing of leases, so both can genuinely change between
     * polls. A caller gating startup on {@code wait_for_nodes=3} is waiting for something that will happen.
     */
    private Health await(MetadataPlane metadata, Ask ask) throws Exception {
        final long deadline = System.nanoTime() + ask.timeout().nanos();
        Health health = compute(metadata, ask);
        while (satisfied(health, ask) == false && System.nanoTime() < deadline) {
            Thread.sleep(POLL_MILLIS);
            health = compute(metadata, ask);
        }
        return health;
    }

    private static boolean satisfied(Health health, Ask ask) {
        if (health.missing != null) {
            return true;
        }
        if (ask.waitForNodes() != null && health.nodes < ask.waitForNodes()) {
            return false;
        }
        // green satisfies a wait for yellow as well as for green, because green is strictly better and
        // yellow is never reached.
        return ask.waitForStatus() == null || "green".equals(health.status);
    }

    private Health compute(MetadataPlane metadata, Ask ask) throws IOException {
        final Health health = new Health();
        try {
            metadata.membership().refresh();
            health.nodes = metadata.membership().current().size();
        } catch (IOException e) {
            // The control plane could not be read. That is the red that matters here, and it is reported
            // rather than thrown, because a health endpoint that fails with a 500 when things are unhealthy
            // is the one shape a health endpoint must not have.
            health.status = "red";
            return health;
        }

        final long now = metadata.clock().getAsLong();
        for (String index : ask.indices()) {
            final Optional<IndexDescriptor> descriptor = metadata.describe(index);
            if (descriptor.isEmpty()) {
                health.missing = index;
                return health;
            }
            final IndexHealth each = new IndexHealth();
            each.shards = descriptor.get().numberOfShards();
            for (int shard = 0; shard < descriptor.get().numberOfShards(); shard++) {
                String state;
                try {
                    final var head = metadata.heads().read(index, shard);
                    if (head.isEmpty() || head.get().ownerNodeId() == null || head.get().leaseExpiresAtMillis() <= now) {
                        // Dormant, not unassigned. "Unassigned" in classic means allocation failed and
                        // nobody can serve this; here it means nobody is writing to it right now, and the
                        // next write activates it. Reporting it as unassigned would be the inverse lie --
                        // a fault where there is none.
                        state = "dormant";
                        each.dormant++;
                    } else {
                        state = "owned";
                    }
                    each.active++;
                } catch (Exception e) {
                    state = "unservable";
                    each.unservable++;
                    each.status = "red";
                }
                if (ask.perShard()) {
                    each.perShard.put(shard, state);
                }
            }
            health.indices.put(index, each);
            health.activeShards += each.active;
            health.dormantShards += each.dormant;
            health.unservableShards += each.unservable;
            if ("red".equals(each.status)) {
                health.status = "red";
            }
        }
        return health;
    }

    private void render(org.opensearch.rest.RestChannel channel, Health health, Ask ask) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("cluster_name", clusterName());
            builder.field("status", health.status);
            builder.field("timed_out", satisfied(health, ask) == false);
            builder.field("number_of_nodes", health.nodes);
            // There is no cluster manager to discover; core's field, with the value that is true here.
            builder.field("discovered_cluster_manager", false);
            // Every node here holds shards -- an ingest node writes them and a search node serves them --
            // so there is no data/non-data split to report and the two counts are the same number.
            builder.field("number_of_data_nodes", health.nodes);

            if (health.indices.isEmpty()) {
                // The field search responses already use to say "this answer does not cover everything it
                // might have". Shard counters are omitted rather than reported as zero, because zero is a
                // claim about how many shards exist and the true answer is that none were examined.
                builder.field("complete", false);
                builder.field("shard_counts", "not reported without an index; ask /_cluster/health/{index}");
            } else {
                builder.field("complete", true);
                builder.field("active_primary_shards", health.activeShards);
                builder.field("active_shards", health.activeShards);
                builder.field("relocating_shards", 0);
                builder.field("initializing_shards", 0);
                builder.field("unassigned_shards", health.unservableShards);
                builder.field("delayed_unassigned_shards", 0);
                builder.field("dormant_shards", health.dormantShards);
                final int total = health.activeShards + health.unservableShards;
                builder.field("active_shards_percent_as_number", total == 0 ? 100.0d : (100.0d * health.activeShards) / total);
                builder.field(
                    "active_shards_percent",
                    String.format(java.util.Locale.ROOT, "%.1f%%", total == 0 ? 100.0d : (100.0d * health.activeShards) / total)
                );
            }

            builder.field("number_of_pending_tasks", 0);
            builder.field("number_of_in_flight_fetch", 0);
            builder.field("task_max_waiting_in_queue_millis", 0);
            // Said on every response rather than left to be inferred: green here does not mean copies exist.
            builder.field("replication", "object-store");
            builder.field("yellow_reachable", false);

            if (health.indices.isEmpty() == false) {
                builder.startObject("indices");
                for (Map.Entry<String, IndexHealth> entry : health.indices.entrySet()) {
                    final IndexHealth each = entry.getValue();
                    builder.startObject(entry.getKey());
                    builder.field("status", each.status);
                    builder.field("number_of_shards", each.shards);
                    builder.field("number_of_replicas", 0);
                    builder.field("active_primary_shards", each.active);
                    builder.field("active_shards", each.active);
                    builder.field("relocating_shards", 0);
                    builder.field("initializing_shards", 0);
                    builder.field("unassigned_shards", each.unservable);
                    builder.field("dormant_shards", each.dormant);
                    if (ask.perShard()) {
                        builder.startObject("shards");
                        for (Map.Entry<Integer, String> shard : each.perShard.entrySet()) {
                            builder.startObject(Integer.toString(shard.getKey()));
                            builder.field("status", "unservable".equals(shard.getValue()) ? "red" : "green");
                            builder.field("state", shard.getValue());
                            builder.field("number_of_replicas", 0);
                            builder.endObject();
                        }
                        builder.endObject();
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    private String clusterName() {
        final var serving = node.get();
        return serving == null ? "serverless" : serving.clusterName();
    }

    private void dispatch(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        final var serving = node.get();
        if (serving == null) {
            run(channel, work);
            return;
        }
        serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> run(channel, work));
    }

    private void run(org.opensearch.rest.RestChannel channel, org.opensearch.common.CheckedRunnable<Exception> work) {
        try {
            IndexAdminHandler.gate(
                node.get(),
                org.opensearch.action.admin.cluster.health.ClusterHealthAction.NAME,
                new org.opensearch.action.admin.cluster.health.ClusterHealthRequest(),
                () -> {
                    work.run();
                    return null;
                }
            );
        } catch (Exception e) {
            try {
                channel.sendResponse(IndexAdminHandler.failure(channel, e));
            } catch (IOException nested) {
                logger.error("failed to report a health failure", nested);
            }
        }
    }
}
