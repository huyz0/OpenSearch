/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@code POST /{alias}/_rollover}: a new index behind a name, for an alias or a data stream.
 *
 * <p>The refusal this replaces said the alias move was the part this design could not do, since each
 * alias is its own compare-and-swap. That is exactly why it can: moving one alias from one index to
 * another is one swap, and M59 already served {@code POST /_aliases} for that case. What is not atomic is
 * the pair -- create the index, then move the name -- and that is reported rather than hidden: a swap lost
 * to a concurrent rollover answers 409 and names the index that was created and not adopted.
 *
 * <p>Conditions are evaluated against what this deployment records: {@code max_age} against the index's
 * creation time, {@code max_docs} against a count fanned out over its shards. Sizes are not reported here,
 * so a size condition is refused rather than answered as never met.
 */
public final class RolloverHandler extends BaseRestHandler {

    private final Supplier<ServerlessNode> node;
    private final Supplier<MetadataPlane> plane;

    /**
     * Creates the handler.
     *
     * @param node supplies the node
     * @param plane supplies the metadata plane
     */
    public RolloverHandler(Supplier<ServerlessNode> node, Supplier<MetadataPlane> plane) {
        this.node = node;
        this.plane = plane;
    }

    @Override
    public String getName() {
        return "serverless_rollover_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.POST, "/{index}/_rollover"),
            new Route(RestRequest.Method.POST, "/{index}/_rollover/{new_index}")
        );
    }

    /** What the request asked for. */
    private record Ask(String target, String newIndex, boolean dryRun, Map<String, Object> conditions,
        IndexAdminHandler.CreateRequest create) {
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String target = request.param("index");
        final String newIndexParam = request.param("new_index");
        final boolean dryRun = request.paramAsBoolean("dry_run", false);
        // Hints; see IndexAdminHandler.
        for (String hint : new String[] { "timeout", "master_timeout", "cluster_manager_timeout", "wait_for_active_shards" }) {
            request.param(hint);
        }
        final MetadataPlane metadata = plane.get();
        final ServerlessNode serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }
        final Map<String, Object> body;
        try {
            body = request.hasContent()
                ? org.opensearch.common.xcontent.XContentHelper.convertToMap(
                    request.content(),
                    false,
                    org.opensearch.common.xcontent.XContentType.JSON
                ).v2()
                : Map.of();
        } catch (Exception e) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "malformed_body", "the body is not a JSON object")
            );
        }
        final Map<String, Object> conditions = body.get("conditions") instanceof Map<?, ?> given ? castMap(given) : Map.of();
        for (String key : conditions.keySet()) {
            if ("max_age".equals(key) == false && "max_docs".equals(key) == false) {
                final String reason = key.startsWith("max_size") || key.startsWith("max_primary_shard_size")
                    ? "store sizes are not reported here, so a size condition cannot be evaluated"
                    : "unknown condition [" + key + "]";
                return channel -> channel.sendResponse(
                    IndexAdminHandler.error(
                        channel,
                        key.startsWith("max_size") || key.startsWith("max_primary_shard_size")
                            ? RestStatus.NOT_IMPLEMENTED
                            : RestStatus.BAD_REQUEST,
                        "unsupported_condition",
                        reason
                    )
                );
            }
        }
        final IndexAdminHandler.CreateRequest create;
        try {
            final Map<String, Object> creation = new LinkedHashMap<>();
            if (body.get("settings") != null) {
                creation.put("settings", body.get("settings"));
            }
            if (body.get("mappings") != null) {
                creation.put("mappings", body.get("mappings"));
            }
            if (body.get("aliases") != null) {
                creation.put("aliases", body.get("aliases"));
            }
            create = creation.isEmpty()
                ? new IndexAdminHandler.CreateRequest(1, null, Settings.EMPTY)
                : IndexAdminHandler.CreateRequest.parse(creation, null, 1);
        } catch (IndexAdminHandler.RefusedException e) {
            return channel -> channel.sendResponse(IndexAdminHandler.error(channel, e.status, e.type, e.getMessage()));
        }
        final Ask ask = new Ask(target, newIndexParam, dryRun, conditions, create);
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                IndexAdminHandler.gate(
                    serving,
                    org.opensearch.action.admin.indices.rollover.RolloverAction.NAME,
                    new org.opensearch.action.admin.indices.rollover.RolloverRequest(ask.target(), ask.newIndex()),
                    () -> {
                        rollover(channel, serving, metadata, ask);
                        return null;
                    }
                );
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a rollover failure", nested);
                }
            }
        });
    }

    private void rollover(org.opensearch.rest.RestChannel channel, ServerlessNode serving, MetadataPlane metadata, Ask ask)
        throws Exception {
        final DescriptorStore.Resolution resolved = metadata.resolve(ask.target());
        if (resolved.alias() == null) {
            sendQuietly(
                channel,
                resolved.index() != null ? RestStatus.BAD_REQUEST : RestStatus.NOT_FOUND,
                resolved.index() != null ? "illegal_argument_exception" : "index_not_found_exception",
                resolved.index() != null
                    ? "rollover target [" + ask.target() + "] is an index; roll over an alias or a data stream"
                    : "no such alias or data stream: " + ask.target()
            );
            return;
        }
        final AliasRecord record = resolved.alias();
        final long generation = metadata.aliasGeneration(ask.target());
        if (record.dataStream() == false && record.indices().size() != 1) {
            sendQuietly(
                channel,
                RestStatus.BAD_REQUEST,
                "illegal_argument_exception",
                "alias [" + ask.target() + "] names " + record.indices().size() + " indices, so there is no single index to roll over"
            );
            return;
        }
        final String oldName = record.dataStream() ? record.writeIndex() : record.indices().get(0);
        final Optional<IndexDescriptor> old = metadata.describe(oldName);
        if (old.isEmpty()) {
            sendQuietly(channel, RestStatus.NOT_FOUND, "index_not_found", "the current index [" + oldName + "] does not exist");
            return;
        }
        final String newName;
        if (ask.newIndex() != null) {
            newName = ask.newIndex();
        } else if (record.dataStream()) {
            newName = DataStreamHandler.backingIndexName(ask.target(), record.generation() + 1);
        } else {
            newName = nextName(oldName);
            if (newName == null) {
                sendQuietly(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "illegal_argument_exception",
                    "index name [" + oldName + "] does not match pattern '^.*-\\\\d+$'; name the new index in the path"
                );
                return;
            }
        }

        // Conditions, each against what is recorded.
        final Map<String, Boolean> met = new LinkedHashMap<>();
        for (Map.Entry<String, Object> condition : ask.conditions().entrySet()) {
            if ("max_age".equals(condition.getKey())) {
                if (old.get().createdAtMillis() == 0L) {
                    sendQuietly(
                        channel,
                        RestStatus.NOT_IMPLEMENTED,
                        "unsupported_condition",
                        "max_age cannot be evaluated: [" + oldName + "] predates creation times being recorded"
                    );
                    return;
                }
                final TimeValue age = TimeValue.parseTimeValue(String.valueOf(condition.getValue()), "max_age");
                met.put("[max_age: " + age + "]", metadata.clock().getAsLong() - old.get().createdAtMillis() >= age.millis());
            } else {
                final long maxDocs = Long.parseLong(String.valueOf(condition.getValue()));
                final var outcome = SearchFanout.run(
                    serving,
                    metadata,
                    Map.of(oldName, old.get()),
                    new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(0).trackTotalHits(true)
                );
                if (outcome.complete() == false) {
                    sendQuietly(
                        channel,
                        RestStatus.SERVICE_UNAVAILABLE,
                        "count_incomplete",
                        "max_docs cannot be evaluated: only "
                            + outcome.answered()
                            + " of "
                            + outcome.shards()
                            + " shards of ["
                            + oldName
                            + "] answered"
                    );
                    return;
                }
                met.put("[max_docs: " + maxDocs + "]", outcome.total() >= maxDocs);
            }
        }
        final boolean shouldRoll = ask.conditions().isEmpty() || met.containsValue(true);
        if (ask.dryRun() || shouldRoll == false) {
            respond(channel, oldName, newName, false, ask.dryRun(), met);
            return;
        }

        // The index first, then the name. A rollover the swap loses is reported with the index it made.
        final IndexAdminHandler.CreateRequest create = record.dataStream()
            ? DataStreamHandler.withTimestamp(ask.create(), record.timestampField())
            : ask.create();
        try {
            IndexAdminHandler.createIndex(metadata, newName, create);
        } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
            sendQuietly(channel, RestStatus.BAD_REQUEST, "resource_already_exists_exception", "index [" + newName + "] already exists");
            return;
        }
        final AliasRecord moved;
        if (record.dataStream()) {
            final List<String> backing = new ArrayList<>(record.indices());
            backing.add(newName);
            moved = new AliasRecord(ask.target(), backing, true, record.generation() + 1, record.timestampField());
        } else {
            moved = new AliasRecord(ask.target(), List.of(newName));
        }
        if (metadata.updateAlias(moved, generation).isEmpty()) {
            // Undo the half that landed. Left in place, the next rollover would compute the same name and
            // find it taken, and the stream could never roll again.
            try {
                metadata.deleteIndex(newName);
            } catch (Exception e) {
                logger.warn("could not remove the index [" + newName + "] created by a rollover that lost its swap", e);
            }
            sendQuietly(
                channel,
                RestStatus.CONFLICT,
                "version_conflict_engine_exception",
                "[" + ask.target() + "] was changed concurrently; nothing was rolled over, retry"
            );
            return;
        }
        respond(channel, oldName, newName, true, false, met);
    }

    /** Core's naming rule: a trailing number, incremented, padded to six digits. */
    static String nextName(String name) {
        final int dash = name.lastIndexOf('-');
        if (dash < 0 || dash == name.length() - 1) {
            return null;
        }
        final String digits = name.substring(dash + 1);
        if (digits.chars().allMatch(Character::isDigit) == false) {
            return null;
        }
        return name.substring(0, dash + 1) + String.format(Locale.ROOT, "%06d", Long.parseLong(digits) + 1);
    }

    private void respond(
        org.opensearch.rest.RestChannel channel,
        String oldName,
        String newName,
        boolean rolledOver,
        boolean dryRun,
        Map<String, Boolean> conditions
    ) throws IOException {
        // Core's RolloverResponse shape.
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", rolledOver);
            builder.field("shards_acknowledged", rolledOver);
            builder.field("old_index", oldName);
            builder.field("new_index", newName);
            builder.field("rolled_over", rolledOver);
            builder.field("dry_run", dryRun);
            builder.startObject("conditions");
            for (Map.Entry<String, Boolean> each : conditions.entrySet()) {
                builder.field(each.getKey(), each.getValue());
            }
            builder.endObject();
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> castMap(Map<?, ?> map) {
        return (Map<String, Object>) map;
    }

    private void sendQuietly(org.opensearch.rest.RestChannel channel, RestStatus status, String type, String reason) {
        try {
            channel.sendResponse(IndexAdminHandler.error(channel, status, type, reason));
        } catch (IOException e) {
            logger.error("failed to report a rollover refusal", e);
        }
    }
}
