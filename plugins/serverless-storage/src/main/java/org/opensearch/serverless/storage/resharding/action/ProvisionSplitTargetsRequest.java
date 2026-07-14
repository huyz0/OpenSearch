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
 * Names the source index and the exact target index names to create for it -- see {@link
 * ProvisionSplitTargetsAction}'s own javadoc for why the caller, not this plugin, always supplies
 * the target names, mirroring core's own {@code POST /{index}/_split/{target}} contract exactly.
 */
public class ProvisionSplitTargetsRequest extends ActionRequest {

    private final String sourceIndexName;
    private final List<String> targetIndexNames;

    /**
     * Creates a request.
     *
     * @param sourceIndexName the index being split.
     * @param targetIndexNames the real target index names to create, in partition order; must name
     *                         at least 2 partitions, caller-supplied same as core's own {@code
     *                         _split} target name.
     */
    public ProvisionSplitTargetsRequest(String sourceIndexName, List<String> targetIndexNames) {
        this.sourceIndexName = sourceIndexName;
        this.targetIndexNames = targetIndexNames;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ProvisionSplitTargetsRequest}.
     */
    public ProvisionSplitTargetsRequest(StreamInput in) throws IOException {
        super(in);
        this.sourceIndexName = in.readString();
        this.targetIndexNames = in.readStringList();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceIndexName);
        out.writeStringCollection(targetIndexNames);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndexName == null || sourceIndexName.isEmpty()) {
            validationException = addValidationError("sourceIndexName is required", validationException);
        }
        if (targetIndexNames == null || targetIndexNames.size() < 2) {
            validationException = addValidationError("targetIndexNames must name at least 2 partitions", validationException);
        }
        return validationException;
    }

    /** The index being split. */
    public String sourceIndexName() {
        return sourceIndexName;
    }

    /** The real target index names to create, in partition order. */
    public List<String> targetIndexNames() {
        return targetIndexNames;
    }
}
