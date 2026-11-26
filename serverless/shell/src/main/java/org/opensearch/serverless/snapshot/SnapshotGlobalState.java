/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.snapshot;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.node.NodeContext;
import org.opensearch.serverless.rest.StoredScriptHandler;
import org.opensearch.serverless.rest.TemplateHandler;
import org.opensearch.serverless.script.StoredScripts;
import org.opensearch.serverless.template.StoredScriptStore;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The cluster-wide state a snapshot may carry: the persistent settings and the five document sets (index and component
 * templates, ingest and search pipelines, stored scripts). A take reads it, sequentially, into the member the record
 * embeds; a restore reads that member strictly and puts it back by name through the same checks as the PUT routes.
 */
final class SnapshotGlobalState {

    private SnapshotGlobalState() {}

    /** The action a take or restore that touches global state is also authorised for; outside the snapshot action prefix. */
    static final String ACTION = "cluster:admin/global_state/snapshot";

    /** The most bytes of state one record embeds. */
    static final int MAX_BYTES = 4 * 1024 * 1024;

    /** How long a foreground restore spends putting documents back before the rest go to the failed list. */
    private static final long RESTORE_BUDGET_NANOS = java.util.concurrent.TimeUnit.MINUTES.toNanos(2);

    private static final String SETTINGS = "persistent_settings";
    private static final String INDEX_TEMPLATES = "index_templates";
    private static final String COMPONENT_TEMPLATES = "component_templates";
    private static final String INGEST_PIPELINES = "ingest_pipelines";
    private static final String SEARCH_PIPELINES = "search_pipelines";
    private static final String STORED_SCRIPTS = "stored_scripts";

    /** Thrown by a take whose state is over {@link #MAX_BYTES}; names the set that pushed it over. */
    static final class TooLargeException extends Exception {
        TooLargeException(String set) {
            super("the global state is over " + MAX_BYTES + " bytes once [" + set + "] is added; delete some of it or take without it");
        }
    }

    /** Thrown by a restore whose snapshot's {@code global_state} is not what a take writes. */
    static final class UnreadableException extends IOException {
        UnreadableException(String reason) {
            super(reason);
        }
    }

    /** What a restore of the state did: how many settings and documents it put back, and the ones it could not. */
    record Restored(int applied, List<String> failed) {
    }

    /** A state read strictly: settings and each set's documents by name. */
    record Parsed(Map<String, Object> settings, Map<String, Map<String, Map<String, Object>>> sets) {
    }

    /**
     * Reads the whole state a take embeds: the settings, then each set's documents, one read after another.
     *
     * @param metadata the plane whose stores hold the state
     * @return the member to embed; empty when there is no state
     * @throws TooLargeException when the encoded state would pass {@link #MAX_BYTES}
     * @throws IOException if a store cannot be read
     */
    static Map<String, Object> capture(MetadataPlane metadata) throws IOException, TooLargeException {
        final Map<String, Object> state = new LinkedHashMap<>();
        long bytes = 0;
        final Settings settings = metadata.clusterConfig().read().settings();
        final Map<String, Object> flat = new TreeMap<>();
        for (String key : settings.keySet()) {
            final String value = settings.get(key);
            // The register holds scalars only, so a list-valued setting cannot be here to skip.
            if (value != null) {
                flat.put(key, value);
                bytes += key.getBytes(StandardCharsets.UTF_8).length + value.getBytes(StandardCharsets.UTF_8).length + 8L;
            }
        }
        if (bytes > MAX_BYTES) {
            throw new TooLargeException(SETTINGS);
        }
        if (flat.isEmpty() == false) {
            state.put(SETTINGS, flat);
        }
        bytes = add(state, INDEX_TEMPLATES, metadata.indexTemplates()::forEachDocument, bytes);
        bytes = add(state, COMPONENT_TEMPLATES, metadata.componentTemplates()::forEachDocument, bytes);
        bytes = add(state, INGEST_PIPELINES, metadata.pipelines()::forEachDocument, bytes);
        bytes = add(state, SEARCH_PIPELINES, metadata.searchPipelines()::forEachDocument, bytes);
        add(state, STORED_SCRIPTS, metadata.scripts()::forEachDocument, bytes);
        return state;
    }

    /** A walk over one set: hands each document to the visitor, one body at a time. */
    private interface Walk {
        void over(org.opensearch.common.CheckedBiConsumer<String, String, IOException> visitor) throws IOException;
    }

    /** Thrown inside a walk to stop it; unwrapped to {@link TooLargeException}. */
    private static final class Stop extends IOException {
        Stop(String set) {
            super(set);
        }
    }

    // ALLOC: one document body (at most 1 MiB, bounded by the store's put) at a time, plus the parsed documents kept, at most MAX_BYTES.
    private static long add(Map<String, Object> state, String set, Walk walk, long bytes) throws TooLargeException, IOException {
        final Map<String, Object> parsed = new TreeMap<>();
        final long[] total = { bytes };
        try {
            walk.over((name, source) -> {
                total[0] += name.getBytes(StandardCharsets.UTF_8).length + source.getBytes(StandardCharsets.UTF_8).length + 8L;
                if (total[0] > MAX_BYTES) {
                    throw new Stop(set);
                }
                parsed.put(name, XContentHelper.convertToMap(new BytesArray(source), false, XContentType.JSON).v2());
            });
        } catch (Stop stop) {
            throw new TooLargeException(set);
        }
        if (parsed.isEmpty() == false) {
            state.put(set, parsed);
        }
        return total[0];
    }

    /**
     * Reads a record's {@code global_state}, strictly: every member an object, every document an object, every
     * setting a scalar.
     *
     * @param raw the member as the record parse left it, or null when the snapshot has none
     * @return what to apply; empty when there is none
     * @throws UnreadableException if it is not what a take writes
     */
    @SuppressWarnings("unchecked")
    static Parsed read(Object raw) throws UnreadableException {
        final Map<String, Map<String, Map<String, Object>>> sets = new LinkedHashMap<>();
        if (raw == null) {
            return new Parsed(Map.of(), sets);
        }
        if (raw instanceof Map<?, ?> == false) {
            throw new UnreadableException("global_state is not an object");
        }
        final Map<?, ?> state = (Map<?, ?>) raw;
        final Map<String, Object> settings = new TreeMap<>();
        if (state.get(SETTINGS) != null) {
            if (state.get(SETTINGS) instanceof Map<?, ?> == false) {
                throw new UnreadableException(SETTINGS + " is not an object");
            }
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) state.get(SETTINGS)).entrySet()) {
                final Object value = entry.getValue();
                if (value instanceof String == false && value instanceof Number == false && value instanceof Boolean == false) {
                    throw new UnreadableException("setting [" + entry.getKey() + "] is not a scalar");
                }
                settings.put(String.valueOf(entry.getKey()), value);
            }
        }
        for (String set : List.of(INDEX_TEMPLATES, COMPONENT_TEMPLATES, INGEST_PIPELINES, SEARCH_PIPELINES, STORED_SCRIPTS)) {
            final Object documents = state.get(set);
            if (documents == null) {
                continue;
            }
            if (documents instanceof Map<?, ?> == false) {
                throw new UnreadableException(set + " is not an object");
            }
            final Map<String, Map<String, Object>> byName = new TreeMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) documents).entrySet()) {
                if (entry.getValue() instanceof Map<?, ?> == false) {
                    throw new UnreadableException(set + " document [" + entry.getKey() + "] is not an object");
                }
                byName.put(String.valueOf(entry.getKey()), (Map<String, Object>) entry.getValue());
            }
            sets.put(set, byName);
        }
        return new Parsed(settings, sets);
    }

    /**
     * Puts the state back: the settings merged key by key with the snapshot's value winning, then each document put by
     * name through the checks its PUT route makes. Nothing is deleted, and nothing here fails the restore.
     *
     * @param metadata the plane whose stores take the state
     * @param serving the node whose validators the PUT routes use, or null to store without them
     * @param state the state to apply, from {@link #read}
     * @return how many entries were applied and which were not
     */
    static Restored restore(MetadataPlane metadata, NodeContext serving, Parsed state) {
        final long deadline = System.nanoTime() + RESTORE_BUDGET_NANOS;
        final List<String> failed = new ArrayList<>();
        int applied = 0;
        if (state.settings().isEmpty() == false) {
            try {
                metadata.clusterConfig().update(new LinkedHashMap<>(state.settings()));
                applied += state.settings().size();
            } catch (Exception e) {
                LOGGER.warn("global state could not merge its persistent settings", e);
                failed.add(SETTINGS);
            }
        }
        for (Map.Entry<String, Map<String, Map<String, Object>>> set : state.sets().entrySet()) {
            for (Map.Entry<String, Map<String, Object>> document : set.getValue().entrySet()) {
                final String label = label(set.getKey()) + "/" + document.getKey();
                if (System.nanoTime() - deadline > 0) {
                    failed.add(label);
                    continue;
                }
                try {
                    put(metadata, serving, set.getKey(), document.getKey(), json(document.getValue()));
                    applied++;
                } catch (Exception e) {
                    LOGGER.warn("global state could not put [{}]: {}", label, e.getMessage());
                    failed.add(label);
                }
            }
        }
        return new Restored(applied, failed);
    }

    private static void put(MetadataPlane metadata, NodeContext serving, String set, String name, String body) throws Exception {
        switch (set) {
            case INDEX_TEMPLATES -> {
                if (serving != null) {
                    TemplateHandler.validateBody(serving, false, body);
                }
                metadata.indexTemplates().put(name, body);
            }
            case COMPONENT_TEMPLATES -> {
                if (serving != null) {
                    TemplateHandler.validateBody(serving, true, body);
                }
                metadata.componentTemplates().put(name, body);
            }
            case INGEST_PIPELINES -> {
                if (serving != null) {
                    serving.services().ingestPipelines().compile(name, body);
                }
                metadata.pipelines().put(name, body);
            }
            case SEARCH_PIPELINES -> {
                if (serving != null) {
                    serving.services().searchPipelines().validate(name, body);
                }
                metadata.searchPipelines().put(name, body);
            }
            default -> {
                if (name.startsWith("_")) {
                    throw new IllegalArgumentException("stored script ids beginning with an underscore are reserved for APIs");
                }
                if (serving != null) {
                    StoredScriptHandler.admit(serving, null, StoredScripts.parse(body));
                }
                final StoredScriptStore scripts = metadata.scripts();
                scripts.put(name, body);
                if (serving != null) {
                    serving.services().storedScripts().refresh(true);
                }
            }
        }
    }

    private static String label(String set) {
        return switch (set) {
            case INDEX_TEMPLATES -> "index_template";
            case COMPONENT_TEMPLATES -> "component_template";
            case INGEST_PIPELINES -> "ingest_pipeline";
            case SEARCH_PIPELINES -> "search_pipeline";
            default -> "stored_script";
        };
    }

    private static String json(Map<String, Object> document) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(document);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    private static final org.apache.logging.log4j.Logger LOGGER = org.apache.logging.log4j.LogManager.getLogger(SnapshotGlobalState.class);
}
