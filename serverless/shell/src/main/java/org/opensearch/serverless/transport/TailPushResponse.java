/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

/** A search node's answer to a push: the shards it no longer has an overlay for, which its writer stops pushing. */
public final class TailPushResponse extends TransportResponse {

    private final Set<ShardId> unwanted;

    /**
     * Creates a response.
     *
     * @param unwanted the shards not wanted any more
     */
    public TailPushResponse(Set<ShardId> unwanted) {
        this.unwanted = unwanted;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public TailPushResponse(StreamInput in) throws IOException {
        final int count = in.readVInt();
        this.unwanted = new LinkedHashSet<>();
        for (int i = 0; i < count; i++) {
            unwanted.add(new ShardId(new Index(in.readString(), in.readString()), in.readVInt()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(unwanted.size());
        for (ShardId shardId : unwanted) {
            out.writeString(shardId.getIndexName());
            out.writeString(shardId.getIndex().getUUID());
            out.writeVInt(shardId.id());
        }
    }

    /** @return the shards not wanted any more */
    public Set<ShardId> unwanted() {
        return unwanted;
    }
}
