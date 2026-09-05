/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The truth record for one repository: a name, a {@code type}, and settings — the same three keys
 * {@code PUT _snapshot/{repo}} accepts on real OpenSearch, kept for API compatibility even though only one
 * of them changes behaviour here.
 *
 * <p><b>A repository here is a namespace, not a storage backend.</b> Classic OpenSearch's repository
 * abstraction exists to let a snapshot land on storage distinct from the cluster's own data — a different
 * bucket, a different provider, a different account. This shell has exactly one configured object store
 * (D3: one register implementation, not a pluggable set of backends), and a snapshot's data already lives
 * in it — the same store, the same durability, the same R11 conformance question. {@code type} is accepted
 * and stored for a client that sends it, but only the value meaning "this deployment's own store" is
 * functional; a genuinely distinct backend is refused at the REST layer rather than silently ignored (see
 * {@link org.opensearch.serverless.rest.RepositoryHandler}). A caller asking for real cross-store
 * durability wants object-store replication, not a second repository type here.
 *
 * <p><b>{@code settings.remote_store_index_shallow_copy}</b> is the one setting that does something: it
 * chooses, for every snapshot later taken against this repository, whether capture is shallow (references
 * the live shard's own blobs, free to take, depends on that shard's storage) or standard (copies blobs
 * into the repository's own storage at capture time, costs the copy, depends on nothing else). The default
 * is {@code false} — standard — matching real OpenSearch's own default for this setting.
 */
public final class RepositoryDescriptor {

    /** The only functional {@code type}: this deployment's own configured object store. */
    public static final String TYPE_NATIVE = "fs";

    /** The repository setting choosing shallow capture. Read once per repository, not per snapshot. */
    public static final String SHALLOW_SETTING = "remote_store_index_shallow_copy";

    private final String name;
    private final String type;
    private final Map<String, Object> settings;
    private final long createdAtMillis;

    /**
     * Creates a descriptor.
     *
     * @param name the repository name
     * @param type the repository type, as the client named it
     * @param settings the repository settings, as the client sent them
     * @param createdAtMillis when it was registered, in the plane's clock
     */
    public RepositoryDescriptor(String name, String type, Map<String, Object> settings, long createdAtMillis) {
        this.name = Objects.requireNonNull(name);
        this.type = type == null ? TYPE_NATIVE : type;
        this.settings = settings == null ? Map.of() : Map.copyOf(settings);
        this.createdAtMillis = createdAtMillis;
    }

    /**
     * Returns the repository name.
     *
     * @return the name
     */
    public String name() {
        return name;
    }

    /**
     * Returns the repository type, as registered.
     *
     * @return the type
     */
    public String type() {
        return type;
    }

    /**
     * Returns the repository's settings, as registered.
     *
     * @return the settings
     */
    public Map<String, Object> settings() {
        return settings;
    }

    /**
     * Reports whether a snapshot taken against this repository, absent any other instruction, captures
     * shallow rather than standard.
     *
     * @return true if {@link #SHALLOW_SETTING} is set and true
     */
    public boolean shallowByDefault() {
        return Boolean.TRUE.equals(settings.get(SHALLOW_SETTING)) || "true".equals(String.valueOf(settings.get(SHALLOW_SETTING)));
    }

    /**
     * Returns when this repository was registered.
     *
     * @return the creation time, in the plane's clock
     */
    public long createdAtMillis() {
        return createdAtMillis;
    }

    /**
     * Renders the record for its register.
     *
     * @return the bytes to store
     * @throws IOException if it cannot be written
     */
    public BytesReference toBytes() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("name", name);
            builder.field("type", type);
            builder.field("created_at", createdAtMillis);
            builder.startObject("settings");
            for (Map.Entry<String, Object> setting : settings.entrySet()) {
                builder.field(setting.getKey(), String.valueOf(setting.getValue()));
            }
            builder.endObject();
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Parses a record from its register bytes.
     *
     * @param input the stored bytes
     * @return the record
     * @throws IOException if the bytes are not a well-formed record
     */
    public static RepositoryDescriptor fromStream(InputStream input) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)
        ) {
            final var body = parser.map();
            final Object name = body.get("name");
            final Object createdAt = body.get("created_at");
            if (name == null || createdAt == null) {
                throw new IOException("malformed repository record: missing a required field");
            }
            final Object type = body.get("type");
            final Map<String, Object> settings = new LinkedHashMap<>();
            if (body.get("settings") instanceof Map<?, ?> settingsMap) {
                for (Map.Entry<?, ?> entry : settingsMap.entrySet()) {
                    settings.put(String.valueOf(entry.getKey()), entry.getValue());
                }
            }
            return new RepositoryDescriptor(
                name.toString(),
                type == null ? null : type.toString(),
                settings,
                Long.parseLong(String.valueOf(createdAt))
            );
        }
    }

    @Override
    public String toString() {
        return "RepositoryDescriptor[" + name + ", type=" + type + "]";
    }
}
