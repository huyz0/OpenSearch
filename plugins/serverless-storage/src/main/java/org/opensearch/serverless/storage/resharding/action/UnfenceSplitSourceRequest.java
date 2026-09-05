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
 * Unfences {@code sourceIndexName} -- see {@link UnfenceSplitSourceAction}'s own javadoc for the
 * design rationale. Carries no alias name, unlike {@link FenceSplitSourceRequest}: removing a fence
 * needs no knowledge of what superseded the index, and requiring the caller to name the alias
 * correctly would be one more way for a recovery action to refuse to run when it is needed most.
 */
public class UnfenceSplitSourceRequest extends ClusterManagerNodeRequest<UnfenceSplitSourceRequest> {

    private final String sourceIndexName;

    /**
     * Creates a request.
     *
     * @param sourceIndexName the fenced split source index to unfence; must be non-empty.
     */
    public UnfenceSplitSourceRequest(String sourceIndexName) {
        this.sourceIndexName = sourceIndexName;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link UnfenceSplitSourceRequest}.
     */
    public UnfenceSplitSourceRequest(StreamInput in) throws IOException {
        super(in);
        this.sourceIndexName = in.readString();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceIndexName);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndexName == null || sourceIndexName.isEmpty()) {
            validationException = addValidationError("sourceIndexName must be non-empty", validationException);
        }
        return validationException;
    }

    /** The fenced split source index to unfence. */
    public String sourceIndexName() {
        return sourceIndexName;
    }
}
