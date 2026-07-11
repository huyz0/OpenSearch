/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/** A shard identified by (indexUuid, shardId) -- the repeated element of {@link ShardShrinkRequest#sources()}. */
public record ShardRef(String indexUuid, int shardId) implements Writeable {

    /**
     * Creates a shard reference.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     */
    public ShardRef {
    }

    /**
     * Deserializes a shard reference.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardRef}.
     */
    public ShardRef(StreamInput in) throws IOException {
        this(in.readString(), in.readVInt());
    }

    /** @param out stream to write this reference's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
    }

    /** UUID of the index the shard belongs to. */
    @Override
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    @Override
    public int shardId() {
        return shardId;
    }
}
