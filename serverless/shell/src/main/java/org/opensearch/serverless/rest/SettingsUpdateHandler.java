/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.function.Supplier;

/**
 * {@code PUT /{index}/_settings} — the other half of changing an index after it exists.
 *
 * <p>M52 made mappings mutable and left settings write-once. Both compared serverless products ship both, and
 * the shape of the work is the same: compare-and-swap the descriptor, then apply to the shards this node
 * already holds. What is different, and the reason it was not folded into M52, is that settings have a
 * validity question mappings do not.
 *
 * <p><b>Which settings may change, and who decides.</b> Core divides index settings into dynamic ones, which
 * may be changed on a live index, and static ones, which classic OpenSearch will only change on a
 * <em>closed</em> index. This design has no closed state — an index is either there or it is not — so a static
 * setting has no moment at which it could be changed safely, and is refused rather than accepted and quietly
 * not applied. {@code IndexScopedSettings#isDynamicSetting} is what answers the question; nothing here keeps a
 * list of its own, which would be a second account of core's settings registry and would drift from it.
 *
 * <p><b>Two settings are refused for reasons of their own.</b> {@code number_of_shards} is structural: it is
 * fixed when the index is created because the shard list lives in the descriptor and routing is a function of
 * it, so changing it would silently re-route every document that already exists. {@code number_of_replicas}
 * is refused above zero for the same reason it is at creation — a shard here has one writer and its redundancy
 * is the object store, so recording a replica count would report a redundancy this deployment does not provide
 * that way.
 *
 * <p><b>The write is a compare-and-swap</b>, and a lost swap re-reads and retries, so two clients changing
 * different settings at the same time both win rather than one silently overwriting the other.
 */
public final class SettingsUpdateHandler extends BaseRestHandler {

    /** How many times a lost compare-and-swap is retried before the caller is asked to. */
    private static final int ATTEMPTS = 4;

    private final Supplier<MetadataPlane> plane;
    private final Supplier<org.opensearch.serverless.shell.ServerlessNode> node;

    /**
     * Creates the handler.
     *
     * @param plane supplies the metadata plane
     * @param node supplies the serving node
     */
    public SettingsUpdateHandler(Supplier<MetadataPlane> plane, Supplier<org.opensearch.serverless.shell.ServerlessNode> node) {
        this.plane = plane;
        this.node = node;
    }

    @Override
    public String getName() {
        return "serverless_settings_update_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.PUT, "/{index}/_settings"), new Route(RestRequest.Method.POST, "/{index}/_settings"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String index = request.param("index");
        final String body = request.hasContent() ? request.content().utf8ToString() : null;
        final boolean preserveExisting = request.paramAsBoolean("preserve_existing", false);
        // Hints; see IndexAdminHandler.
        for (String hint : new String[] {
            "timeout",
            "master_timeout",
            "cluster_manager_timeout",
            "ignore_unavailable",
            "allow_no_indices",
            "expand_wildcards",
            "flat_settings" }) {
            request.param(hint);
        }

        if (body == null || body.isBlank()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "a settings body is required")
            );
        }

        final Settings requested;
        try {
            @SuppressWarnings("unchecked")
            final Map<String, Object> parsed = org.opensearch.common.xcontent.XContentHelper.convertToMap(
                request.content(),
                false,
                org.opensearch.common.xcontent.XContentType.JSON
            ).v2();
            // Both shapes a client sends: {"index": {...}} and a bare {...}. normalizePrefix is core's own
            // normalisation rather than a guess at one, so "refresh_interval" and "index.refresh_interval"
            // mean the same thing here as they do everywhere else.
            final Object inner = parsed.get("settings");
            @SuppressWarnings("unchecked")
            final Map<String, Object> source = inner instanceof Map ? (Map<String, Object>) inner : parsed;
            requested = Settings.builder().loadFromMap(source).normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX).build();
        } catch (Exception e) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(
                    channel,
                    RestStatus.BAD_REQUEST,
                    "malformed_body",
                    "could not parse the settings body: " + e.getMessage()
                )
            );
        }

        if (requested.isEmpty()) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "missing_body", "no settings were given")
            );
        }

        final String unknown = unknownSettings(requested);
        if (unknown != null) {
            // A typo, not a capability this deployment lacks. 400 with core's own vocabulary, because the
            // fix is in the caller's request; the refusals below are 501 because the fix is not.
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.BAD_REQUEST, "illegal_argument_exception", unknown)
            );
        }
        final String refusal = refuse(requested);
        if (refusal != null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.NOT_IMPLEMENTED, "unsupported_setting", refusal)
            );
        }
        try {
            // Every value parsed by core's own setting before anything is written. The first parse used to
            // be IndexService.updateMetadata on a shard, after the compare-and-swap: a value like
            // "banana" for refresh_interval was committed to the descriptor, rendered by GET _settings,
            // and failed every node that next opened a shard of the index -- with a 400 to the caller that
            // read as if it had been refused.
            IndexAdminHandler.validateIndexSettings(requested, false);
        } catch (IndexAdminHandler.RefusedException e) {
            return channel -> channel.sendResponse(IndexAdminHandler.error(channel, e.status, e.type, e.getMessage()));
        }

        final MetadataPlane metadata = plane.get();
        final var serving = node.get();
        if (metadata == null || serving == null) {
            return channel -> channel.sendResponse(
                IndexAdminHandler.error(channel, RestStatus.SERVICE_UNAVAILABLE, "no_metadata_plane", "no metadata plane configured")
            );
        }

        return channel -> serving.threadPool().executor(org.opensearch.threadpool.ThreadPool.Names.GENERIC).execute(() -> {
            try {
                IndexAdminHandler.gate(
                    serving,
                    org.opensearch.action.admin.indices.settings.put.UpdateSettingsAction.NAME,
                    new org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest(requested, index),
                    () -> {
                        apply(channel, metadata, serving, index, requested, preserveExisting);
                        return null;
                    }
                );
            } catch (Exception e) {
                try {
                    channel.sendResponse(IndexAdminHandler.failure(channel, e));
                } catch (IOException nested) {
                    logger.error("failed to report a settings update failure", nested);
                }
            }
        });
    }

    /**
     * Decides whether every requested setting may be changed on a live index here.
     *
     * @param requested the normalised settings
     * @return a refusal, or null when all of them may change
     */
    private static String unknownSettings(Settings requested) {
        final IndexScopedSettings scoped = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;
        final TreeSet<String> unknown = new TreeSet<>();
        for (String key : requested.keySet()) {
            if (scoped.get(key) == null) {
                unknown.add(key);
            }
        }
        return unknown.isEmpty() ? null : "unknown index setting" + (unknown.size() > 1 ? "s " : " ") + unknown;
    }

    private static String refuse(Settings requested) {
        if (requested.hasValue(IndexMetadata.SETTING_NUMBER_OF_SHARDS)) {
            return "number_of_shards cannot be changed after an index is created: the shard list lives in the "
                + "index descriptor and routing is a function of it, so changing the count would re-route every "
                + "document that already exists. An index that needs a different shard count is a new index";
        }
        final String replicas = requested.get(IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
        if (replicas != null && "0".equals(replicas) == false) {
            return "number_of_replicas is not supported: a shard has one writer and its durability comes from "
                + "the object store, not from replica copies. Readers are added by scaling search nodes, not "
                + "by setting a replica count";
        }
        // The index-level pipeline settings: dynamic in core's registry, so they passed the static check
        // below, and read by nothing on this shell's write or search path, so they were stored and never
        // ran. Refused with the reason rather than acknowledged with "changed": true.
        final String unsupported = IndexAdminHandler.unsupportedIndexSetting(requested);
        if (unsupported != null) {
            return unsupported;
        }

        // Core's own registry decides what is dynamic. Keeping a list here would be a second account of it,
        // and a second account of a registry that grows with every release is one that will drift from it.
        final IndexScopedSettings scoped = IndexScopedSettings.DEFAULT_SCOPED_SETTINGS;
        final TreeSet<String> statics = new TreeSet<>();
        for (String key : requested.keySet()) {
            if (IndexMetadata.SETTING_NUMBER_OF_REPLICAS.equals(key)) {
                continue;
            }
            if (scoped.get(key) != null && scoped.isDynamicSetting(key) == false) {
                statics.add(key);
            }
        }
        if (statics.isEmpty() == false) {
            // Refused rather than stored-and-ignored. Classic OpenSearch changes a static setting only on a
            // closed index; there is no closed state here, so there is no moment at which this could be
            // applied, and accepting it would leave a value in the descriptor that nothing acts on.
            return "static index setting"
                + (statics.size() > 1 ? "s " : " ")
                + statics
                + " cannot be changed on a "
                + "live index. Classic OpenSearch changes these only while an index is closed, and this design "
                + "has no closed state: an index is either being served or it does not exist. Set them at "
                + "creation";
        }
        return null;
    }

    private void apply(
        org.opensearch.rest.RestChannel channel,
        MetadataPlane metadata,
        org.opensearch.serverless.shell.ServerlessNode serving,
        String index,
        Settings requested,
        boolean preserveExisting
    ) throws Exception {
        for (int attempt = 0; attempt < ATTEMPTS; attempt++) {
            final long generation = metadata.descriptorGeneration(index);
            final Optional<IndexDescriptor> current = metadata.describe(index);
            if (current.isEmpty()) {
                channel.sendResponse(IndexAdminHandler.error(channel, RestStatus.NOT_FOUND, "index_not_found", "no such index: " + index));
                return;
            }

            // Layered over what is already there, not replacing it: PUT _settings in OpenSearch merges, so a
            // caller changing one setting does not silently clear the others.
            // preserve_existing, as core means it: a setting already present keeps its value, and only
            // the ones not yet set are taken from the request.
            final Settings.Builder merged = Settings.builder().put(current.get().extraSettings());
            for (String key : requested.keySet()) {
                if (preserveExisting == false || current.get().extraSettings().hasValue(key) == false) {
                    merged.put(key, requested.get(key));
                }
            }
            merged.remove(IndexMetadata.SETTING_NUMBER_OF_REPLICAS);
            final Settings result = merged.build();
            if (result.equals(current.get().extraSettings())) {
                acknowledge(channel, index, false);
                return;
            }

            final IndexDescriptor updated = current.get().withSettings(result);
            if (metadata.updateDescriptor(updated, generation).isPresent()) {
                serving.reconciler().refreshSettings(index, updated);
                acknowledge(channel, index, true);
                return;
            }
        }

        channel.sendResponse(
            IndexAdminHandler.error(
                channel,
                RestStatus.CONFLICT,
                "version_conflict_engine_exception",
                "the settings for " + index + " are being changed concurrently; retry"
            )
        );
    }

    private static void acknowledge(org.opensearch.rest.RestChannel channel, String index, boolean changed) throws IOException {
        try (XContentBuilder builder = channel.newBuilder()) {
            builder.startObject();
            builder.field("acknowledged", true);
            builder.field("index", index);
            builder.field("changed", changed);
            builder.endObject();
            channel.sendResponse(new BytesRestResponse(RestStatus.OK, builder));
        }
    }
}
