/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Fences {@code sourceIndexName} -- see {@link FenceSplitSourceAction}'s own javadoc for the full
 * design rationale. A {@link ClusterManagerNodeRequest}, the same shape {@link
 * org.opensearch.serverless.storage.resharding.action.EnableWritePartitionRoutingRequest} uses.
 */
public class FenceSplitSourceRequest extends ClusterManagerNodeRequest<FenceSplitSourceRequest> {

    private final String sourceIndexName;
    private final String supersedingAliasName;

    /**
     * Creates a request.
     *
     * @param sourceIndexName the split source index to fence; must be non-empty.
     * @param supersedingAliasName the alias that now serves as the real entry point in its place; must be non-empty.
     */
    public FenceSplitSourceRequest(String sourceIndexName, String supersedingAliasName) {
        this.sourceIndexName = sourceIndexName;
        this.supersedingAliasName = supersedingAliasName;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link FenceSplitSourceRequest}.
     */
    public FenceSplitSourceRequest(StreamInput in) throws IOException {
        super(in);
        this.sourceIndexName = in.readString();
        this.supersedingAliasName = in.readString();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceIndexName);
        out.writeString(supersedingAliasName);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndexName == null || sourceIndexName.isEmpty()) {
            validationException = addValidationError("sourceIndexName must be non-empty", validationException);
        }
        if (supersedingAliasName == null || supersedingAliasName.isEmpty()) {
            validationException = addValidationError("supersedingAliasName must be non-empty", validationException);
        }
        return validationException;
    }

    /** The split source index to fence. */
    public String sourceIndexName() {
        return sourceIndexName;
    }

    /** The alias that now serves as the real entry point in the fenced source's place. */
    public String supersedingAliasName() {
        return supersedingAliasName;
    }
}
