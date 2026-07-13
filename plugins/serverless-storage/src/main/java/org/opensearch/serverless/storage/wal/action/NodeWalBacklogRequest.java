/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Requests the node-shared WAL chunk service's currently-buffered record count and total payload
 * bytes {@link NodeWalBacklogAction} finds on the node that receives it -- no fields, same "what do
 * you have" shape as {@code NodeCacheStatsRequest}.
 */
public class NodeWalBacklogRequest extends ActionRequest {

    /** Creates a request. */
    public NodeWalBacklogRequest() {}

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeWalBacklogRequest}.
     */
    public NodeWalBacklogRequest(StreamInput in) throws IOException {
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
