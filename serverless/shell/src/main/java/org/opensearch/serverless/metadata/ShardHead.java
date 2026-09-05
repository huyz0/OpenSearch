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
 * The register that says who owns a shard, and at what term.
 *
 * <p>This is the only object in the system that arbitrates ownership. Membership does not; a projected
 * view does not; gossip does not. Section 10.1 puts it plainly: a node existing is not a node owning
 * anything, and only this answers the second question.
 *
 * <p><b>The term is not decoration.</b> S0 found that the data plane refuses to activate a primary at
 * term 0, and S1 found that feeding a per-node counter where a term belongs causes updates to be
 * silently ignored. The generation of this register is the one monotonic number every node that touches
 * the shard agrees on, which is exactly what {@code updateShardState} needs and what a projection
 * counter cannot be.
 *
 * <p><b>What this alone does not do</b> is fence a zombie. A writer whose JVM paused past its lease can
 * still hold a file handle and keep writing bytes; CAS here establishes who <em>should</em> own the
 * shard, not what a stale owner is physically able to do. That is R9, and the mechanism for it is
 * term-scoped key paths (section 9.6) — not implemented in this phase, and deliberately not implied by
 * this class.
 */
public final class ShardHead {

    private final String indexName;
    private final String indexUuid;
    private final int shardId;
    private final long term;
    private final String ownerNodeId;
    private final String ownerEphemeralId;
    private final long leaseExpiresAtMillis;

    /**
     * Creates a head.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @param term the term ownership was acquired at; positive
     * @param ownerNodeId the owning node, or null when unowned
     * @param ownerEphemeralId the owner's ephemeral id, or null
     * @param leaseExpiresAtMillis when the owner's claim lapses
     */
    public ShardHead(String indexName, int shardId, long term, String ownerNodeId, String ownerEphemeralId, long leaseExpiresAtMillis) {
        this(indexName, shardId, term, ownerNodeId, ownerEphemeralId, leaseExpiresAtMillis, null);
    }

    /**
     * Creates a head bound to one incarnation of its index.
     *
     * <p>Heads are keyed by index name, and a name outlives an index: a delete and a recreate keep the
     * name and change the uuid. A head that carries the uuid it was acquired for can be told from a head
     * left behind by the previous incarnation, which is how a recreated index stops inheriting a writer
     * that is still serving the deleted one.
     *
     * @param indexName the index name
     * @param shardId the shard number
     * @param term the term
     * @param ownerNodeId the owner, or null when released
     * @param ownerEphemeralId the owner's ephemeral id
     * @param leaseExpiresAtMillis the acquisition stamp
     * @param indexUuid the index uuid this head was acquired for, or null for a head written before this
     */
    public ShardHead(
        String indexName,
        int shardId,
        long term,
        String ownerNodeId,
        String ownerEphemeralId,
        long leaseExpiresAtMillis,
        String indexUuid
    ) {
        this.indexName = Objects.requireNonNull(indexName);
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        if (term < 1) {
            throw new IllegalArgumentException("shard-head term must be positive, got " + term);
        }
        this.term = term;
        this.ownerNodeId = ownerNodeId;
        this.ownerEphemeralId = ownerEphemeralId;
        this.leaseExpiresAtMillis = leaseExpiresAtMillis;
    }

    /**
     * Returns the index name.
     *
     * @return the index
     */
    /**
     * Returns the uuid of the index incarnation this head was acquired for.
     *
     * @return the uuid, or null for a head written before uuids were recorded
     */
    public String indexUuid() {
        return indexUuid;
    }

    public String indexName() {
        return indexName;
    }

    /**
     * Returns the shard number.
     *
     * @return the shard id
     */
    public int shardId() {
        return shardId;
    }

    /**
     * Returns the term.
     *
     * @return the term
     */
    public long term() {
        return term;
    }

    /**
     * Returns the owning node id, or null when unowned.
     *
     * @return the owner
     */
    public String ownerNodeId() {
        return ownerNodeId;
    }

    /**
     * Returns the owner's ephemeral id, or null.
     *
     * @return the owner's ephemeral id
     */
    public String ownerEphemeralId() {
        return ownerEphemeralId;
    }

    /**
     * Returns when the owner's claim lapses.
     *
     * @return expiry in wall-clock millis
     */
    public long leaseExpiresAtMillis() {
        return leaseExpiresAtMillis;
    }

    /**
     * Reports whether the current owner's claim is still live.
     *
     * @param nowMillis the observer's clock
     * @return true when owned and unexpired
     */
    public boolean isHeldAt(long nowMillis) {
        return ownerNodeId != null && nowMillis < leaseExpiresAtMillis;
    }

    /**
     * Serializes this head.
     *
     * @return the register bytes
     * @throws IOException if serialization fails
     */
    public BytesReference toBytes() throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("index", indexName);
            builder.field("shard", shardId);
            builder.field("term", term);
            builder.field("owner_node_id", ownerNodeId);
            builder.field("owner_ephemeral_id", ownerEphemeralId);
            builder.field("lease_expires_at_millis", leaseExpiresAtMillis);
            if (indexUuid != null) {
                builder.field("index_uuid", indexUuid);
            }
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }

    /**
     * Parses a head from its register bytes.
     *
     * @param input the serialized head
     * @return the parsed head
     * @throws IOException if the bytes are not a well-formed head
     */
    public static ShardHead fromStream(InputStream input) throws IOException {
        try (
            XContentParser parser = XContentType.JSON.xContent()
                .createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, input)
        ) {
            String index = null;
            String uuid = null;
            String owner = null;
            String ephemeral = null;
            int shard = -1;
            long term = 0;
            long expiry = 0;
            String field = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != null && token != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    field = parser.currentName();
                } else if (token.isValue() || token == XContentParser.Token.VALUE_NULL) {
                    switch (field == null ? "" : field) {
                        case "index" -> index = parser.textOrNull();
                        case "index_uuid" -> uuid = parser.textOrNull();
                        case "shard" -> shard = parser.intValue();
                        case "term" -> term = parser.longValue();
                        case "owner_node_id" -> owner = parser.textOrNull();
                        case "owner_ephemeral_id" -> ephemeral = parser.textOrNull();
                        case "lease_expires_at_millis" -> expiry = parser.longValue();
                        default -> {
                            // forward compatibility
                        }
                    }
                }
            }
            if (index == null || shard < 0 || term < 1) {
                throw new IOException("malformed shard head: missing a required field");
            }
            return new ShardHead(index, shard, term, owner, ephemeral, expiry, uuid);
        }
    }

    @Override
    public String toString() {
        return "ShardHead[" + indexName + "][" + shardId + "] term=" + term + " owner=" + ownerNodeId + " expires=" + leaseExpiresAtMillis;
    }
}
