/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * §9.3's {@code /cluster/config}: settings an operator sets, one register, CAS on conflict.
 *
 * <p><b>A store, not a settings service.</b> Core's {@code ClusterSettings} validates a key against a
 * registry of definitions plugins and modules contribute — types, defaults, validators, which are dynamic
 * and which need a restart. Building that here would mean either faking a registry this shell does not
 * have or reaching into core's, and either way inventing a guarantee — "this key is known and this value
 * is legal" — that nobody has actually checked. This register holds whatever an operator puts in it and
 * hands back whatever is there. Validating it is a plugin's job, or an operator's, same as an index's
 * mapping JSON is stored without this shell checking it against Lucene's own field-type rules.
 *
 * <p><b>Only {@code persistent}, never {@code transient}.</b> Not an oversight: transient cluster settings
 * are a footgun even where they exist — silently reset on a full restart, and the source of the exact
 * "why did this change back" confusion operators learn to avoid. Newer OpenSearch versions are moving
 * away from them for that reason. This register only ever had one way to be honest about "no cluster
 * manager, no restart to reset anything on," and it is to not offer the half that was never well-defined
 * here to begin with.
 *
 * <p><b>Scalar values only.</b> A list-valued setting cannot be told apart from an explicit removal
 * through {@link Settings}'s public surface without reaching into its internals — {@code getAsList}
 * returns a non-null answer for a plain scalar too, by design, so it cannot be used to detect a genuine
 * list. Rather than risk silently dropping a list on the next update that touches any key, list values are
 * refused at the door. Nothing here needs one yet.
 */
public final class ClusterConfig {

    private final BlobContainer container;

    /**
     * Creates the register.
     *
     * @param container the {@code cluster/config} container
     */
    public ClusterConfig(BlobContainer container) {
        this.container = container;
    }

    /** The register's current value and the generation it was read at. */
    public record Value(long generation, Settings settings) {
    }

    /**
     * Reads the current settings.
     *
     * @return the settings, empty if nothing has ever been set
     * @throws IOException if the register cannot be read
     */
    public Value read() throws IOException {
        final var register = container.readRegister(RegisterMap.CLUSTER_CONFIG_BLOB);
        if (register.isEmpty()) {
            return new Value(BlobRegister.ABSENT_GENERATION, Settings.EMPTY);
        }
        try (InputStream in = register.get().value().streamInput()) {
            return new Value(register.get().generation(), parse(in));
        }
    }

    /**
     * Applies changes to the persistent settings, merging with what is there.
     *
     * <p>A key mapped to {@code null} removes that setting rather than storing a null — the same meaning
     * classic {@code _cluster/settings} gives a null value, and the reason this rebuilds the merged result
     * from scratch rather than layering {@link Settings.Builder#put(Settings)} once: core's own merge
     * leaves a null-valued tombstone in the map instead of dropping the key, and a register that only ever
     * merged would grow one dead entry per removal, forever.
     *
     * <p><b>Retried under contention</b> rather than failing on the first lost race, because §9.3's whole
     * argument for this register is that the writer population is operators at human-scale — a retry here
     * costs one extra read-and-recompute, not a storm.
     *
     * @param changes dotted setting name to new value (never a {@link List} — the caller must have
     *     refused those with {@link #firstListValue} first), or null to remove
     * @return the settings as written
     * @throws IOException if the register cannot be read or written, or contention never clears
     */
    public Settings update(Map<String, Object> changes) throws IOException {
        for (int attempt = 0; attempt < 10; attempt++) {
            final Value current = read();
            final Settings.Builder merging = Settings.builder().put(current.settings());
            for (Map.Entry<String, Object> change : changes.entrySet()) {
                if (change.getValue() == null) {
                    merging.putNull(change.getKey());
                } else {
                    merging.put(change.getKey(), String.valueOf(change.getValue()));
                }
            }
            final Settings merged = merging.build();
            // Rebuilt clean: a key merged.get() answers null for is a removal now, not a value to keep.
            final Settings.Builder clean = Settings.builder();
            for (String key : merged.keySet()) {
                final String value = merged.get(key);
                if (value != null) {
                    clean.put(key, value);
                }
            }
            final Settings next = clean.build();

            final BytesReference bytes = toBytes(next);
            final BlobRegisterCasResult result = current.generation() == BlobRegister.ABSENT_GENERATION
                ? container.createRegisterIfAbsent(RegisterMap.CLUSTER_CONFIG_BLOB, bytes)
                : container.compareAndSwapRegister(RegisterMap.CLUSTER_CONFIG_BLOB, current.generation(), bytes);
            if (result.applied()) {
                return next;
            }
            // Someone else wrote between the read and this write. Re-read and reapply the same changes
            // against the generation actually stored, rather than assuming this attempt's base was current.
        }
        throw new IOException("could not update cluster config after 10 attempts; contention never cleared");
    }

    /**
     * Refuses a change whose value is a list, before it ever reaches {@link #update}.
     *
     * <p>Called from the REST layer, against the raw parsed request body, because that is the only point
     * a list value can still be told apart from a scalar with certainty — see the class javadoc.
     *
     * @param flat a flattened, dotted-key view of the requested changes
     * @return the name of the first list-valued key found, or null if none
     */
    public static String firstListValue(Map<String, Object> flat) {
        for (Map.Entry<String, Object> entry : flat.entrySet()) {
            if (entry.getValue() instanceof List) {
                return entry.getKey();
            }
        }
        return null;
    }

    private static Settings parse(InputStream in) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, in)
        ) {
            return Settings.fromXContent(parser);
        }
    }

    private static BytesReference toBytes(Settings settings) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            settings.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Flattens a nested JSON object into dotted-key form, the shape {@link #update} and core's own
     * {@code Settings} both use.
     *
     * @param nested the parsed request body's {@code persistent} object
     * @return dotted key to value, with a list value kept as a {@code List} rather than flattened, so
     *     {@link #firstListValue} can still find it
     */
    public static Map<String, Object> flatten(Map<String, Object> nested) {
        final Map<String, Object> flat = new LinkedHashMap<>();
        flattenInto(nested, "", flat);
        return flat;
    }

    @SuppressWarnings("unchecked")
    private static void flattenInto(Map<String, Object> nested, String prefix, Map<String, Object> flat) {
        for (Map.Entry<String, Object> entry : nested.entrySet()) {
            final String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
            if (entry.getValue() instanceof Map<?, ?> child) {
                flattenInto((Map<String, Object>) child, key, flat);
            } else {
                flat.put(key, entry.getValue());
            }
        }
    }
}
