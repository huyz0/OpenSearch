/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.opensearch.core.index.shard.ShardId;

/** What an update did. */
public final class UpdateOutcome {

    private final org.opensearch.action.update.UpdateResponse response;
    private final String result;
    private final String servedBy;
    private final long seqNo;
    private final long primaryTerm;
    private final long version;

    UpdateOutcome(String result, String servedBy, long seqNo, long primaryTerm, long version, ShardId shardId, String id, boolean refresh) {
        this(
            new org.opensearch.action.update.UpdateResponse(
                shardId,
                id,
                seqNo,
                primaryTerm,
                version,
                org.opensearch.action.DocWriteResponse.Result.valueOf(result.toUpperCase(java.util.Locale.ROOT))
            ),
            servedBy
        );
        if ("noop".equals(result) == false) {
            ShardOperations.completeNativeWriteResponse(response, refresh);
        }
    }

    UpdateOutcome(org.opensearch.action.update.UpdateResponse response, String servedBy) {
        this.response = response;
        this.result = response.getResult().getLowercase();
        this.servedBy = servedBy;
        this.seqNo = response.getSeqNo();
        this.primaryTerm = response.getPrimaryTerm();
        this.version = response.getVersion();
    }

    /**
     * Returns the native response as the action filters left it.
     * @return the native result, including its actual index and document id
     */
    public org.opensearch.action.update.UpdateResponse response() {
        return response;
    }

    /**
     * Returns the sequence number the engine assigned the write this update performed.
     *
     * <p>For a noop it is the sequence number the document already had: nothing was written, so the
     * document's identity is unchanged, and a caller conditioning a later write on it is right to.
     *
     * @return the sequence number
     */
    public long seqNo() {
        return seqNo;
    }

    /**
     * Returns the primary term that operation ran at.
     *
     * @return the primary term
     */
    public long primaryTerm() {
        return primaryTerm;
    }

    /**
     * Returns the document's version after this update.
     *
     * @return the version
     */
    public long version() {
        return version;
    }

    /**
     * Returns what happened: {@code created}, {@code updated}, {@code deleted} or {@code noop}.
     *
     * @return the result
     */
    public String result() {
        return result;
    }

    /**
     * Returns the node that performed the write, or answered the read for a noop.
     *
     * @return the node id
     */
    public String servedBy() {
        return servedBy;
    }
}
