/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/** The result of a {@link CompactionTriggerAction} request. */
public class CompactionTriggerResponse extends ActionResponse implements ToXContentObject {

    private final boolean attempted;

    /**
     * Creates a response.
     *
     * @param attempted whether the shard was a compaction candidate and a publish attempt was
     *                  actually made -- {@code false} means the shard was never activated, has
     *                  never published anything, or {@link org.opensearch.serverless.storage.compaction.CompactionPolicy}
     *                  decided it isn't worth compacting right now; either way, this call is
     *                  always a safe no-op, never an error, when nothing needed doing.
     */
    public CompactionTriggerResponse(boolean attempted) {
        this.attempted = attempted;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link CompactionTriggerResponse}.
     */
    public CompactionTriggerResponse(StreamInput in) throws IOException {
        super(in);
        this.attempted = in.readBoolean();
    }

    /** @param out stream to write {@link #attempted()} to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(attempted);
    }

    /** Whether the shard was a compaction candidate and a publish attempt was actually made. */
    public boolean attempted() {
        return attempted;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("attempted", attempted).endObject();
    }
}
