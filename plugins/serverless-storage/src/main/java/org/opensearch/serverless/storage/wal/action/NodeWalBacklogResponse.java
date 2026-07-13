/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * The result of a {@link NodeWalBacklogAction} request: the node-shared WAL chunk service's
 * currently-buffered (not yet flushed) record count and total payload bytes.
 */
public class NodeWalBacklogResponse extends ActionResponse implements ToXContentObject {

    private final int bufferedRecordCount;
    private final long totalBufferedBytes;

    /**
     * Creates a response.
     *
     * @param bufferedRecordCount records currently buffered (since the last flush) on the receiving node.
     * @param totalBufferedBytes total payload bytes currently buffered (since the last flush) on the receiving node.
     */
    public NodeWalBacklogResponse(int bufferedRecordCount, long totalBufferedBytes) {
        this.bufferedRecordCount = bufferedRecordCount;
        this.totalBufferedBytes = totalBufferedBytes;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeWalBacklogResponse}.
     */
    public NodeWalBacklogResponse(StreamInput in) throws IOException {
        super(in);
        this.bufferedRecordCount = in.readVInt();
        this.totalBufferedBytes = in.readVLong();
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(bufferedRecordCount);
        out.writeVLong(totalBufferedBytes);
    }

    /** Records currently buffered (since the last flush) on the node that answered this request. */
    public int bufferedRecordCount() {
        return bufferedRecordCount;
    }

    /** Total payload bytes currently buffered (since the last flush) on the node that answered this request. */
    public long totalBufferedBytes() {
        return totalBufferedBytes;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("buffered_record_count", bufferedRecordCount)
            .field("total_buffered_bytes", totalBufferedBytes)
            .endObject();
    }
}
