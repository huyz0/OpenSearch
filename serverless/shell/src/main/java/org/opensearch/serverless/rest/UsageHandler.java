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
import org.opensearch.serverless.store.IndexUsage;
import org.opensearch.serverless.store.StoreUsage;
import org.opensearch.serverless.usage.Guardrails;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@code GET /_serverless/usage} -- what each index has cost on this node since it started, and each tenant's level.
 *
 * <p><b>Totals, not deltas.</b> Every counter is this node's total since it started; a collector keeps the last answer
 * and subtracts. {@code started_millis} tells it the node restarted, its totals beginning again from zero, so nothing
 * is kept on the node for whoever asks and two collectors asking do not take each other's share.
 *
 * <p>{@code ?top=N} keeps the N indices that cost the most, and {@code _node} and {@code _other}; the default is every
 * index. Requests are in whole requests, to three decimals: a request shared between indices is split by weight.
 */
public final class UsageHandler extends BaseRestHandler {

    private final Supplier<Guardrails> guardrails;
    private final Supplier<StoreUsage> usage;
    private final Supplier<String> nodeId;
    private final long startedMillis;

    /**
     * Creates the handler.
     *
     * @param guardrails the node's guardrails, or a supplier of null
     * @param usage the store's per-index usage, or a supplier of null when metering is off
     * @param nodeId this node's id
     * @param startedMillis when the node started, so a collector can tell its totals began again
     */
    public UsageHandler(Supplier<Guardrails> guardrails, Supplier<StoreUsage> usage, Supplier<String> nodeId, long startedMillis) {
        this.guardrails = guardrails;
        this.usage = usage;
        this.nodeId = nodeId;
        this.startedMillis = startedMillis;
    }

    @Override
    public String getName() {
        return "serverless_usage_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_serverless/usage"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final int top = request.paramAsInt("top", Integer.MAX_VALUE);
        if (top < 0) {
            throw new IllegalArgumentException("[top] must not be negative, got [" + top + "]");
        }
        return channel -> {
            try (XContentBuilder builder = channel.newBuilder()) {
                describe(builder, top);
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
            }
        };
    }

    private void describe(XContentBuilder builder, int top) throws IOException {
        final StoreUsage store = usage.get();
        final Guardrails rails = guardrails.get();
        builder.startObject();
        builder.field("node_id", nodeId.get());
        builder.field("started_millis", startedMillis);
        builder.field("now_millis", System.currentTimeMillis());
        builder.field("metered", store != null);
        builder.field("guardrails", rails == null ? "off" : rails.mode().name().toLowerCase(Locale.ROOT));
        if (store != null) {
            builder.startObject("indices");
            for (Map.Entry<String, IndexUsage> line : costliest(store.lines(), top)) {
                index(builder, line.getKey(), line.getValue(), rails);
            }
            builder.endObject();
        }
        if (rails != null) {
            builder.field("tenants", rails.tenants());
        }
        builder.endObject();
    }

    // The costliest indices first, then the node's own lines, which are always kept.
    static List<Map.Entry<String, IndexUsage>> costliest(Map<String, IndexUsage> lines, int top) {
        final List<Map.Entry<String, IndexUsage>> indices = new ArrayList<>();
        final List<Map.Entry<String, IndexUsage>> own = new ArrayList<>();
        for (Map.Entry<String, IndexUsage> line : lines.entrySet()) {
            (line.getKey().equals(StoreUsage.NODE) || line.getKey().equals(StoreUsage.OTHER) ? own : indices).add(line);
        }
        indices.sort(Comparator.comparingLong((Map.Entry<String, IndexUsage> line) -> line.getValue().costMilliUnits()).reversed());
        final List<Map.Entry<String, IndexUsage>> kept = new ArrayList<>(indices.subList(0, Math.min(top, indices.size())));
        kept.addAll(own);
        return kept;
    }

    private static void index(XContentBuilder builder, String name, IndexUsage usage, Guardrails rails) throws IOException {
        builder.startObject(name);
        if (rails != null && name.startsWith("_") == false) {
            final Guardrails.IndexInfo info = rails.info(name);
            builder.field("tenant", info.tenant());
            builder.field("tier", info.tier().name().toLowerCase(Locale.ROOT));
            builder.field("budget_per_minute", info.budgetPerMinute());
        }
        builder.field("cost_units", usage.costMilliUnits() / 1000d);
        builder.startObject("requests");
        for (IndexUsage.Kind kind : IndexUsage.Kind.values()) {
            if (kind.isRequest()) {
                builder.field(kind.name().toLowerCase(Locale.ROOT), usage.get(kind) / (double) IndexUsage.ONE_REQUEST);
            }
        }
        builder.endObject();
        builder.field("bytes_read", usage.get(IndexUsage.Kind.BYTES_READ));
        builder.field("bytes_written", usage.get(IndexUsage.Kind.BYTES_WRITTEN));
        builder.field("docs", usage.get(IndexUsage.Kind.DOCS));
        builder.field("queries", usage.get(IndexUsage.Kind.QUERIES));
        builder.field("search_millis", usage.get(IndexUsage.Kind.SEARCH_NANOS) / 1_000_000L);
        builder.field("push_bytes", usage.get(IndexUsage.Kind.PUSH_BYTES));
        builder.endObject();
    }
}
