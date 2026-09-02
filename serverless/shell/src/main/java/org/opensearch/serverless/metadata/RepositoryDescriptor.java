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
import java.util.Objects;

/**
 * The truth record for one repository: a name, and nothing else.
 *
 * <p><b>A repository here is a namespace, not a storage backend.</b> Classic OpenSearch's repository
 * abstraction exists to let a snapshot land on storage distinct from the cluster's own data — a different
 * bucket, a different provider, a different account. This shell has exactly one configured object store
 * (D3: one register implementation, not a pluggable set of backends), and a snapshot's data already lives
 * in it — the same store, the same durability, the same R11 conformance question. Registering a repository
 * records a name to snapshot under and nothing to configure, because there is nothing this deployment's
 * own {@code serverless.store.*} settings do not already fix. A caller asking for genuine cross-store
 * durability wants object-store replication, not a second repository type here.
 */
public final class RepositoryDescriptor {

    private final String name;
    private final long createdAtMillis;

    /**
     * Creates a descriptor.
     *
     * @param name the repository name
     * @param createdAtMillis when it was registered, in the plane's clock
     */
    public RepositoryDescriptor(String name, long createdAtMillis) {
        this.name = Objects.requireNonNull(name);
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
            builder.field("created_at", createdAtMillis);
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
            return new RepositoryDescriptor(name.toString(), Long.parseLong(String.valueOf(createdAt)));
        }
    }

    @Override
    public String toString() {
        return "RepositoryDescriptor[" + name + "]";
    }
}
