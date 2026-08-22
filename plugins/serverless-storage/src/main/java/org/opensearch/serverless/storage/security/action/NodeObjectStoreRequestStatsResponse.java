/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/** The result of a {@link NodeObjectStoreRequestStatsAction} request: this node's real object-store request counts, by shape. */
public class NodeObjectStoreRequestStatsResponse extends ActionResponse implements ToXContentObject {

    private final long getCount;
    private final long putCount;
    private final long deleteCount;
    private final long listCount;

    /**
     * Creates a response.
     *
     * @param getCount total GET-shaped requests recorded so far on the receiving node.
     * @param putCount total PUT-shaped requests recorded so far on the receiving node.
     * @param deleteCount total DELETE-shaped requests recorded so far on the receiving node.
     * @param listCount total LIST-shaped requests recorded so far on the receiving node.
     */
    public NodeObjectStoreRequestStatsResponse(long getCount, long putCount, long deleteCount, long listCount) {
        this.getCount = getCount;
        this.putCount = putCount;
        this.deleteCount = deleteCount;
        this.listCount = listCount;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeObjectStoreRequestStatsResponse}.
     */
    public NodeObjectStoreRequestStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.getCount = in.readVLong();
        this.putCount = in.readVLong();
        this.deleteCount = in.readVLong();
        this.listCount = in.readVLong();
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(getCount);
        out.writeVLong(putCount);
        out.writeVLong(deleteCount);
        out.writeVLong(listCount);
    }

    /** Total GET-shaped requests recorded so far on the node that answered this request. */
    public long getCount() {
        return getCount;
    }

    /** Total PUT-shaped requests recorded so far on the node that answered this request. */
    public long putCount() {
        return putCount;
    }

    /** Total DELETE-shaped requests recorded so far on the node that answered this request. */
    public long deleteCount() {
        return deleteCount;
    }

    /** Total LIST-shaped requests recorded so far on the node that answered this request. */
    public long listCount() {
        return listCount;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("get_count", getCount)
            .field("put_count", putCount)
            .field("delete_count", deleteCount)
            .field("list_count", listCount)
            .endObject();
    }
}
