/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.security.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Requests the real object-store request counts {@link NodeObjectStoreRequestStatsAction} finds
 * tallied on the node that receives it -- no fields, same "what do you have" shape as {@code
 * NodeCacheStatsRequest}.
 */
public class NodeObjectStoreRequestStatsRequest extends ActionRequest {

    /** Creates a request. */
    public NodeObjectStoreRequestStatsRequest() {}

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeObjectStoreRequestStatsRequest}.
     */
    public NodeObjectStoreRequestStatsRequest(StreamInput in) throws IOException {
        super(in);
    }

    /** @param out stream to write this request to -- no fields of its own beyond the superclass. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    /** @return always {@code null} -- this request has no fields to validate. */
    @Override
    public ActionRequestValidationException validate() {
        return null;
    }
}
