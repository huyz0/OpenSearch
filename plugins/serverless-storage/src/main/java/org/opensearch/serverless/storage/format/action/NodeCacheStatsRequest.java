/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Requests the node-shared in-memory bundle cache's hit/miss counts plus every reader shard's
 * local disk cache stats {@link NodeCacheStatsAction} finds tracked on the node that receives it
 * -- no fields, same "what do you have" shape as {@code
 * org.opensearch.serverless.storage.readerengine.action.NodeManifestLagRequest}.
 */
public class NodeCacheStatsRequest extends ActionRequest {

    /** Creates a request. */
    public NodeCacheStatsRequest() {}

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeCacheStatsRequest}.
     */
    public NodeCacheStatsRequest(StreamInput in) throws IOException {
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
