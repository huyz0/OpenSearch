/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.action.admin.indices.rollover.RolloverAction;
import org.opensearch.action.admin.indices.rollover.RolloverRequest;
import org.opensearch.action.admin.indices.rollover.RolloverResponse;
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
 * to a concurrent rollover removes the index it created, answers 409, and says so; if the removal itself
 * fails, the 409 names the index that was created and not adopted, so nothing is left behind unmentioned.
 * The record and the generation the swap is made under come from one read (see {@link AliasHandler}).
 *
 * <p>Conditions are evaluated against what this deployment records: {@code max_age} against the index's
 * creation time, {@code max_docs} against a count fanned out over its shards. Sizes are not reported here,
 * so a size condition is refused rather than answered as never met.
 */
public final class RolloverHandler extends BaseRestHandler {

    private static final java.util.Set<String> BODY_FIELDS = java.util.Set.of("conditions", "settings", "mappings", "aliases");

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
        IndexAdminHandler.CreateRequest create, boolean bodyShapesIndex) {
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String target = request.param("index");
        final String newIndex = request.param("new_index");
        final boolean dryRun = request.paramAsBoolean("dry_run", false);
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
        final var content = request.hasContent() ? request.content() : null;
        final var mediaType = request.getMediaType();
        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                final var nativeRequest = new RolloverRequest(target, newIndex);
                nativeRequest.dryRun(dryRun);
                if (content != null) {
                    // Keep malformed conditions from silently becoming an unconditional rollover, and
                    // preserve the explicit refusal of size conditions this deployment cannot evaluate.
                    final Map<String, Object> body = org.opensearch.common.xcontent.XContentHelper.convertToMap(content, false, mediaType)
                        .v2();
                    validateBody(body);
                    try (
                        var parser = mediaType.xContent()
                            .createParser(
                                serving.searchXContentRegistry(),
                                org.opensearch.core.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                                content.streamInput()
                            )
                    ) {
                        nativeRequest.fromXContent(parser);
                    }
                }
                final var response = serving.actionGate()
                    .run(
                        RolloverAction.NAME,
                        nativeRequest,
                        admitted -> rollover(serving, metadata, admittedAsk((RolloverRequest) admitted)),
                        org.opensearch.serverless.shell.ActionGate.identity(RolloverResponse.class)
                    );
                try (XContentBuilder builder = channel.newBuilder()) {
                    response.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
                    channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
                }
            } catch (Exception e) {
                try {
                    if (e instanceof IndexAdminHandler.RefusedException refused) {
                        channel.sendResponse(IndexAdminHandler.error(channel, refused.status, refused.type, refused.getMessage()));
                    } else {
                        channel.sendResponse(IndexAdminHandler.failure(channel, e));
                    }
                } catch (IOException nested) {
                    logger.error("failed to report a rollover failure", nested);
                }
            }
        });
    }

    private static void validateBody(Map<String, Object> body) throws IndexAdminHandler.RefusedException {
        for (Map.Entry<String, Object> field : body.entrySet()) {
            if (BODY_FIELDS.contains(field.getKey()) == false || field.getValue() instanceof Map<?, ?> == false) {
                throw new IndexAdminHandler.RefusedException(
                    RestStatus.BAD_REQUEST,
                    "malformed_body",
                    "rollover field [" + field.getKey() + "] must be a supported object-valued field"
                );
            }
        }
        if (body.get("conditions") instanceof Map<?, ?> conditions) {
            for (Object key : conditions.keySet()) {
                validateCondition(String.valueOf(key));
            }
        }
    }

    private static void validateCondition(String name) throws IndexAdminHandler.RefusedException {
        if ("max_age".equals(name) || "max_docs".equals(name)) {
            return;
        }
        final boolean size = name.startsWith("max_size") || name.startsWith("max_primary_shard_size");
        throw new IndexAdminHandler.RefusedException(
            size ? RestStatus.NOT_IMPLEMENTED : RestStatus.BAD_REQUEST,
            "unsupported_condition",
            size ? "store sizes are not reported here, so a size condition cannot be evaluated" : "unknown condition [" + name + "]"
        );
    }

    private static Ask admittedAsk(RolloverRequest request) throws Exception {
        final var invalid = request.validate();
        if (invalid != null) {
            throw invalid;
        }
        final Map<String, Object> conditions = new LinkedHashMap<>();
        for (var condition : request.getConditions().entrySet()) {
            validateCondition(condition.getKey());
            conditions.put(condition.getKey(), condition.getValue().value());
        }
        final var nativeCreate = request.getCreateIndexRequest();
        final var create = IndexAdminHandler.fromNative(nativeCreate);
        // Core uses {} for an absent mapping. Structural settings are consumed by fromNative, but
        // still count as an explicit settings override, which a data stream must refuse.
        final String mapping = "{}".equals(create.mapping) ? null : create.mapping;
        return new Ask(
            request.getRolloverTarget(),
            request.getNewIndexName(),
            request.isDryRun(),
            conditions,
            create.withMapping(mapping),
            mapping != null || nativeCreate.settings().isEmpty() == false
        );
    }

    private RolloverResponse rollover(ServerlessNode serving, MetadataPlane metadata, Ask ask) throws Exception {
        final DescriptorStore.Resolution resolved = metadata.resolveForAliasMutation(ask.target());
        if (resolved.alias() == null) {
            throw new IndexAdminHandler.RefusedException(
                resolved.index() != null ? RestStatus.BAD_REQUEST : RestStatus.NOT_FOUND,
                resolved.index() != null ? "illegal_argument_exception" : "index_not_found_exception",
                resolved.index() != null
                    ? "rollover target [" + ask.target() + "] is an index; roll over an alias or a data stream"
                    : "no such alias or data stream: " + ask.target()
            );
        }
        final AliasRecord record = resolved.alias();
        if (record.mutationPending()) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.CONFLICT,
                "version_conflict_engine_exception",
                "alias is being changed: " + ask.target()
            );
        }
        final long generation = resolved.generation();
        final boolean bodyShapesIndex = ask.bodyShapesIndex();
        if (record.dataStream() && (ask.newIndex() != null || bodyShapesIndex)) {
            // Core's rules, in core's words: a stream's backing index is named by its generation and shaped
            // by its template, and a rollover may not override either.
            throw new IndexAdminHandler.RefusedException(
                RestStatus.BAD_REQUEST,
                "illegal_argument_exception",
                ask.newIndex() != null
                    ? "new index name may not be specified when rolling over a data stream"
                    : "aliases, mappings, and index settings may not be specified when rolling over a data stream"
            );
        }
        if (record.dataStream() == false && record.indices().size() != 1) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.BAD_REQUEST,
                "illegal_argument_exception",
                "alias [" + ask.target() + "] names " + record.indices().size() + " indices, so there is no single index to roll over"
            );
        }
        final String oldName = record.dataStream() ? record.writeIndex() : record.indices().get(0);
        final Optional<IndexDescriptor> old = metadata.describe(oldName);
        if (old.isEmpty()) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.NOT_FOUND,
                "index_not_found",
                "the current index [" + oldName + "] does not exist"
            );
        }
        String newName;
        if (ask.newIndex() != null) {
            newName = ask.newIndex();
        } else if (record.dataStream()) {
            newName = DataStreamHandler.backingIndexName(ask.target(), incrementCounter(record.generation(), "rollover generation"));
        } else {
            newName = nextName(oldName);
            if (newName == null) {
                throw new IndexAdminHandler.RefusedException(
                    RestStatus.BAD_REQUEST,
                    "illegal_argument_exception",
                    "index name [" + oldName + "] does not match pattern '^.*-\\\\d+$'; name the new index in the path"
                );
            }
        }

        // Conditions, each against what is recorded.
        final Map<String, Boolean> met = new LinkedHashMap<>();
        for (Map.Entry<String, Object> condition : ask.conditions().entrySet()) {
            if ("max_age".equals(condition.getKey())) {
                if (old.get().createdAtMillis() == 0L) {
                    throw new IndexAdminHandler.RefusedException(
                        RestStatus.NOT_IMPLEMENTED,
                        "unsupported_condition",
                        "max_age cannot be evaluated: [" + oldName + "] predates creation times being recorded"
                    );
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
                    throw new IndexAdminHandler.RefusedException(
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
                }
                met.put("[max_docs: " + maxDocs + "]", outcome.total() >= maxDocs);
            }
        }
        final boolean shouldRoll = ask.conditions().isEmpty() || met.containsValue(true);
        if (ask.dryRun() || shouldRoll == false) {
            return new RolloverResponse(oldName, newName, met, ask.dryRun(), false, false, false);
        }

        if (serving.isSystemIndex(newName)) {
            // Reserved on every path that mints a name, not only on PUT /{index}.
            throw new IndexAdminHandler.RefusedException(
                RestStatus.FORBIDDEN,
                "system_index",
                "[" + newName + "] belongs to a plugin and cannot be created here"
            );
        }

        // The index first, then the name. A rollover the swap loses is reported with what became of the
        // index it made. A stream's backing index inherits what the stream's own name matches, which is the
        // template that declared the stream -- see DataStreamHandler.create for why it cannot be resolved
        // against the backing name.
        final IndexAdminHandler.CreateRequest create = ask.create();
        long newGeneration = incrementCounter(record.generation(), "rollover generation");
        String createdUuid;
        try {
            if (record.dataStream()) {
                final var inherited = org.opensearch.serverless.metadata.TemplateResolver.resolveFromStores(
                    metadata.indexTemplates(),
                    metadata::componentTemplates,
                    ask.target()
                );
                final IndexAdminHandler.CreateRequest streamCreate = DataStreamHandler.withTimestamp(
                    create,
                    record.timestampField(),
                    inherited
                );
                // A taken name is skipped by bumping the generation, as core does, rather than wedging the
                // stream on a name something else holds.
                for (int attempt = 0;; attempt++) {
                    try {
                        createdUuid = IndexAdminHandler.createIndex(serving, metadata, newName, streamCreate, inherited).uuid();
                        break;
                    } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
                        if (attempt >= 8) {
                            throw e;
                        }
                        newGeneration = incrementCounter(newGeneration, "data stream generation");
                        newName = DataStreamHandler.backingIndexName(ask.target(), newGeneration);
                    }
                }
            } else {
                createdUuid = IndexAdminHandler.createIndex(serving, metadata, newName, create).uuid();
            }
        } catch (org.opensearch.serverless.metadata.IndexAlreadyExistsException e) {
            throw new IndexAdminHandler.RefusedException(
                RestStatus.BAD_REQUEST,
                "resource_already_exists_exception",
                "index [" + newName + "] already exists"
            );
        } catch (IndexAdminHandler.RefusedException e) {
            throw new IndexAdminHandler.RefusedException(e.status, e.type, e.getMessage());
        }
        final AliasRecord moved;
        if (record.dataStream()) {
            final List<String> backing = new ArrayList<>(record.indices());
            backing.add(newName);
            moved = new AliasRecord(ask.target(), backing, true, newGeneration, record.timestampField());
        } else {
            moved = new AliasRecord(ask.target(), List.of(newName));
        }
        if (metadata.updateAlias(record, moved, generation).isEmpty()) {
            // Undo the half that landed. Left in place, the next rollover would compute the same name and
            // find it taken -- and the answer has to say which of the two outcomes this is.
            String left = "the index [" + newName + "] created for it was removed";
            try {
                if (metadata.deleteIndex(newName, createdUuid) == false) {
                    left = "the index [" + newName + "] changed concurrently and was not removed by this rollback";
                }
            } catch (Exception e) {
                logger.warn("could not remove the index [" + newName + "] created by a rollover that lost its swap", e);
                left = "the index [" + newName + "] was created and not adopted, and could not be removed: " + e.getMessage();
            }
            throw new IndexAdminHandler.RefusedException(
                RestStatus.CONFLICT,
                "version_conflict_engine_exception",
                "[" + ask.target() + "] was changed concurrently; nothing was rolled over and " + left + "; retry"
            );
        }
        return new RolloverResponse(oldName, newName, met, false, true, true, true);
    }

    private static long incrementCounter(long current, String counter) {
        try {
            return Math.incrementExact(current);
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(counter + " is exhausted at [" + current + "]", e);
        }
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
        return name.substring(0, dash + 1) + String.format(
            Locale.ROOT,
            "%06d",
            incrementCounter(Long.parseLong(digits), "numeric index suffix")
        );
    }

}
