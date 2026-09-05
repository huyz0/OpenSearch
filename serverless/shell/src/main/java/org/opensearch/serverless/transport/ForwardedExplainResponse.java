/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.apache.lucene.search.Explanation;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;

import static org.opensearch.common.lucene.Lucene.readExplanation;
import static org.opensearch.common.lucene.Lucene.writeExplanation;

/**
 * What the owning node found when it explained a document.
 *
 * <p>The explanation travels whole, through core's own {@code Lucene.writeExplanation}, rather than as
 * rendered JSON. A nested explanation is a tree of values and descriptions and flattening it to text on one
 * node so the other can hand it back would make a forwarded explain read differently from a local one.
 */
public final class ForwardedExplainResponse extends TransportResponse {

    private final String ownerNodeId;
    private final boolean exists;
    private final Explanation explanation;

    /**
     * Creates a response.
     *
     * @param ownerNodeId the node that did the explaining
     * @param exists whether the document is in that shard
     * @param explanation the account of the score, or null when the document does not exist
     */
    public ForwardedExplainResponse(String ownerNodeId, boolean exists, Explanation explanation) {
        this.ownerNodeId = ownerNodeId;
        this.exists = exists;
        this.explanation = explanation;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedExplainResponse(StreamInput in) throws IOException {
        this.ownerNodeId = in.readString();
        this.exists = in.readBoolean();
        this.explanation = in.readBoolean() ? readExplanation(in) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(ownerNodeId);
        out.writeBoolean(exists);
        out.writeBoolean(explanation != null);
        if (explanation != null) {
            writeExplanation(out, explanation);
        }
    }

    /**
     * Returns the node that did the explaining.
     *
     * @return the node id
     */
    public String ownerNodeId() {
        return ownerNodeId;
    }

    /**
     * Reports whether the document is in that shard.
     *
     * @return true when it exists
     */
    public boolean exists() {
        return exists;
    }

    /**
     * Returns the explanation.
     *
     * @return the explanation, or null
     */
    public Explanation explanation() {
        return explanation;
    }
}
