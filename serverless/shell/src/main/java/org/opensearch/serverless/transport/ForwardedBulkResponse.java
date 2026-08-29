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
import org.opensearch.serverless.shell.ServerlessNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * What the owning node made of a forwarded batch, one outcome per operation.
 *
 * <p>Per-item rather than a single acknowledgement, because a bulk is a batch and not a transaction: one
 * document can be refused for a mapping conflict while the rest of the batch stands, and a response that
 * could only say "the whole thing worked" would have to fail all of them to stay honest.
 */
public final class ForwardedBulkResponse extends TransportResponse {

    private final String ownerNodeId;
    private final List<ServerlessNode.BulkOutcome> outcomes;

    /**
     * Creates a response.
     *
     * @param ownerNodeId the node that performed the batch
     * @param outcomes one outcome per operation, in request order
     */
    public ForwardedBulkResponse(String ownerNodeId, List<ServerlessNode.BulkOutcome> outcomes) {
        this.ownerNodeId = ownerNodeId;
        this.outcomes = List.copyOf(outcomes);
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedBulkResponse(StreamInput in) throws IOException {
        this.ownerNodeId = in.readString();
        final int count = in.readVInt();
        final List<ServerlessNode.BulkOutcome> read = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final String id = in.readString();
            final String failure = in.readOptionalString();
            final boolean deletion = in.readBoolean();
            final boolean found = in.readBoolean();
            if (failure != null) {
                read.add(ServerlessNode.BulkOutcome.failed(id, failure));
            } else if (deletion) {
                read.add(ServerlessNode.BulkOutcome.deleted(id, found));
            } else {
                read.add(ServerlessNode.BulkOutcome.indexed(id));
            }
        }
        this.outcomes = List.copyOf(read);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(ownerNodeId);
        out.writeVInt(outcomes.size());
        for (ServerlessNode.BulkOutcome outcome : outcomes) {
            out.writeString(outcome.id());
            out.writeOptionalString(outcome.failure());
            out.writeBoolean(outcome.isDeletion());
            out.writeBoolean(outcome.found());
        }
    }

    /**
     * Returns the node that performed the batch.
     *
     * @return the owning node id
     */
    public String ownerNodeId() {
        return ownerNodeId;
    }

    /**
     * Returns one outcome per operation, in request order.
     *
     * @return the outcomes
     */
    public List<ServerlessNode.BulkOutcome> outcomes() {
        return outcomes;
    }
}
