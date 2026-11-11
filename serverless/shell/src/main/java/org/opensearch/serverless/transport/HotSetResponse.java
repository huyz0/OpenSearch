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
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.serverless.shell.CacheHotSet.HotShard;

import java.io.IOException;
import java.util.List;

/** A peer's answer to a {@link HotSetRequest}: the joiner's share of its readers, with their warm block keys. */
public final class HotSetResponse extends TransportResponse {

    private final List<HotShard> shards;

    /**
     * Creates a response.
     *
     * @param shards the shards and their keys
     */
    public HotSetResponse(List<HotShard> shards) {
        this.shards = List.copyOf(shards);
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public HotSetResponse(StreamInput in) throws IOException {
        this.shards = in.readList(i -> new HotShard(i.readString(), i.readString(), i.readVInt(), i.readStringList()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(shards, (o, shard) -> {
            o.writeString(shard.indexName());
            o.writeString(shard.indexUuid());
            o.writeVInt(shard.shard());
            o.writeStringCollection(shard.keys());
        });
    }

    /**
     * Returns the shards.
     *
     * @return the joiner's share, each with its keys hottest first
     */
    public List<HotShard> shards() {
        return shards;
    }
}
