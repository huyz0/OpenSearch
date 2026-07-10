/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Requests every idle-shard entry {@link NodeIdleShardsAction} finds tracked on the node that
 * receives it -- no fields, since (unlike {@link ShardIdleTimeRequest}) this asks "what do you
 * have", not "what do you have for this one shard".
 */
public class NodeIdleShardsRequest extends ActionRequest {

    /** Creates a request. */
    public NodeIdleShardsRequest() {}

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeIdleShardsRequest}.
     */
    public NodeIdleShardsRequest(StreamInput in) throws IOException {
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
