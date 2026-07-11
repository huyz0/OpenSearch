/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Reactivates every suspended shard of one index, either the writer or the reader (search-only)
 * copies (rfc-serverless-opensearch.md &sect;7.3) -- see {@code
 * org.opensearch.serverless.storage.scaletozero.ShardReactivationActionFilter}, this request's only
 * caller. A {@link ClusterManagerNodeRequest} specifically because {@code
 * ShardReactivationActionFilter} runs on whichever node happens to receive the triggering write or
 * search request, which is frequently not the elected cluster-manager node -- {@link
 * org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction} is what
 * transparently forwards this request to whichever node actually is, rather than every caller
 * needing to know or care.
 */
public final class ReactivateShardsRequest extends ClusterManagerNodeRequest<ReactivateShardsRequest> {

    private final String indexName;
    private final boolean reader;

    /**
     * Creates a request.
     *
     * @param indexName the index whose suspended shards (if any) should be reactivated.
     * @param reader {@code true} to reactivate reader (search-only) copies, {@code false} for writer copies.
     */
    public ReactivateShardsRequest(String indexName, boolean reader) {
        this.indexName = indexName;
        this.reader = reader;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ReactivateShardsRequest}.
     */
    public ReactivateShardsRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.reader = in.readBoolean();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeBoolean(reader);
    }

    /** The index whose suspended shards (if any) should be reactivated. */
    public String indexName() {
        return indexName;
    }

    /** {@code true} to reactivate reader (search-only) copies, {@code false} for writer copies. */
    public boolean reader() {
        return reader;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexName == null || indexName.isEmpty()) {
            validationException = addValidationError("indexName must not be empty", null);
        }
        return validationException;
    }
}
