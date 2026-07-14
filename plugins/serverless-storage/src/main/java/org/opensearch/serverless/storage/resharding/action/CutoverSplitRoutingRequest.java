/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Names a real, operator-chosen alias and the real split-target indices it should point at --
 * {@link CutoverSplitRoutingAction}'s own scoped-first-version answer to
 * rfc-serverless-opensearch.md &sect;16 Phase 4's own "real routing cutover remains undesigned"
 * gap. Deliberately additive, not destructive: this never touches the source index a split came
 * from, only creates or updates a separate alias -- see {@link CutoverSplitRoutingAction}'s own
 * javadoc for why fully reclaiming the source's own original name is a distinct, separate,
 * already-existing follow-up ({@link RetireShrinkSourceAction}'s own "verify then delete" pattern,
 * generalized), not something this action does itself.
 */
public class CutoverSplitRoutingRequest extends ActionRequest {

    private final String aliasName;
    private final List<String> targetIndexNames;

    /**
     * Creates a request.
     *
     * @param aliasName the alias to create or update, pointing at every one of {@code targetIndexNames}.
     * @param targetIndexNames the real split-target indices this alias should resolve to; must be non-empty.
     */
    public CutoverSplitRoutingRequest(String aliasName, List<String> targetIndexNames) {
        this.aliasName = aliasName;
        this.targetIndexNames = targetIndexNames;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link CutoverSplitRoutingRequest}.
     */
    public CutoverSplitRoutingRequest(StreamInput in) throws IOException {
        super(in);
        this.aliasName = in.readString();
        this.targetIndexNames = in.readStringList();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(aliasName);
        out.writeStringCollection(targetIndexNames);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (aliasName == null || aliasName.isEmpty()) {
            validationException = addValidationError("aliasName is required", validationException);
        }
        if (targetIndexNames == null || targetIndexNames.isEmpty()) {
            validationException = addValidationError("targetIndexNames must be non-empty", validationException);
        }
        return validationException;
    }

    /** The alias to create or update, pointing at every one of {@link #targetIndexNames()}. */
    public String aliasName() {
        return aliasName;
    }

    /** The real split-target indices this alias should resolve to. */
    public List<String> targetIndexNames() {
        return targetIndexNames;
    }
}
