/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/** The result of a {@link SnapshotRestoreAction} request. */
public class SnapshotRestoreResponse extends ActionResponse implements ToXContentObject {

    private final long primaryTerm;
    private final long generation;

    /**
     * Creates a response reporting the exact manifest generation the shard's head was restored to.
     *
     * @param primaryTerm the primary term the restored head now carries (unchanged from before the restore).
     * @param generation the manifest generation the shard's head was restored to.
     */
    public SnapshotRestoreResponse(long primaryTerm, long generation) {
        this.primaryTerm = primaryTerm;
        this.generation = generation;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link SnapshotRestoreResponse}.
     */
    public SnapshotRestoreResponse(StreamInput in) throws IOException {
        super(in);
        this.primaryTerm = in.readVLong();
        this.generation = in.readVLong();
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(primaryTerm);
        out.writeVLong(generation);
    }

    /** The primary term the restored head now carries (unchanged from before the restore). */
    public long primaryTerm() {
        return primaryTerm;
    }

    /** The manifest generation the shard's head was restored to. */
    public long generation() {
        return generation;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("primary_term", primaryTerm).field("generation", generation).endObject();
    }
}
