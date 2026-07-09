/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * One (indexUuid, shardId) pair known to have mirrored at least one operation into a shared WAL
 * container, as tracked by {@link WalShardRegistry} (rfc-serverless-opensearch.md &sect;6.4).
 */
public final class RegisteredShard implements Writeable {

    private final String indexUuid;
    private final int shardId;

    /**
     * Identifies one shard.
     *
     * @param indexUuid the shard's owning index UUID.
     * @param shardId the shard number within that index.
     */
    public RegisteredShard(String indexUuid, int shardId) {
        this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid");
        this.shardId = shardId;
    }

    /**
     * Deserializes a shard identity.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link RegisteredShard}.
     */
    public RegisteredShard(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt());
    }

    /** @param out stream to write this shard's {@code (indexUuid, shardId)} pair to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
    }

    /** The shard's owning index UUID. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** @param o the object to compare against. */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RegisteredShard)) return false;
        RegisteredShard that = (RegisteredShard) o;
        return shardId == that.shardId && indexUuid.equals(that.indexUuid);
    }

    /** Consistent with {@link #equals}. */
    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId);
    }

    /** {@code indexUuid/shardId}. */
    @Override
    public String toString() {
        return indexUuid + "/" + shardId;
    }
}
