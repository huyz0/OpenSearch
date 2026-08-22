/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/** The result of a {@link NodeIdleShardsAction} request: every idle-shard entry this node has tracked. */
public class NodeIdleShardsResponse extends ActionResponse implements ToXContentObject {

    private final List<IdleShardEntry> entries;

    /**
     * Creates a response.
     *
     * @param entries every writer shard's idle time currently tracked on the receiving node --
     *                see {@link org.opensearch.serverless.storage.writerengine.ShardActivityRegistry#snapshotAll}
     *                for exactly which shards that is (and isn't).
     */
    public NodeIdleShardsResponse(List<IdleShardEntry> entries) {
        this.entries = entries;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeIdleShardsResponse}.
     */
    public NodeIdleShardsResponse(StreamInput in) throws IOException {
        super(in);
        this.entries = in.readList(IdleShardEntry::new);
    }

    /** @param out stream to write {@link #entries()} to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(entries);
    }

    /** Every writer shard's idle time currently tracked on the node that answered this request. */
    public List<IdleShardEntry> entries() {
        return entries;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().startArray("idle_shards");
        for (IdleShardEntry entry : entries) {
            entry.toXContent(builder, params);
        }
        return builder.endArray().endObject();
    }
}
