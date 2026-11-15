/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.serverless.cluster.IndexDescriptor;

/**
 * What resolving the names in a request produced: the indices to fan out over, or the refusal.
 *
 * @param indices each index and its shard count, in request order
 * @param skipped names {@code ignore_unavailable} dropped, so the answer can say so
 * @param status the refusal's status, or null when the names resolved
 * @param type the refusal's error type, or null
 * @param reason the refusal's reason, or null
 * @param prunedShards shards of pattern matches the digest rollups ruled out, which are not in {@code indices}
 * @param judged indices the rollups already found may match, which the fan-out need not check shard by shard
 */
public record IndexResolution(java.util.LinkedHashMap<String, IndexDescriptor> indices, java.util.List<String> skipped, RestStatus status,
    String type, String reason, int prunedShards, java.util.Set<String> judged) {

    /**
     * A resolution with nothing ruled out by the rollups.
     *
     * @param indices each index and its shard count, in request order
     * @param skipped names {@code ignore_unavailable} dropped
     * @param status the refusal's status, or null
     * @param type the refusal's error type, or null
     * @param reason the refusal's reason, or null
     */
    public IndexResolution(
        java.util.LinkedHashMap<String, IndexDescriptor> indices,
        java.util.List<String> skipped,
        RestStatus status,
        String type,
        String reason
    ) {
        this(indices, skipped, status, type, reason, 0, java.util.Set.of());
    }

    public static IndexResolution refuse(RestStatus status, String type, String reason) {
        return new IndexResolution(null, null, status, type, reason);
    }

    /**
     * Reports whether the names could not be resolved.
     *
     * @return true when this carries a refusal rather than indices
     */
    public boolean refused() {
        return status != null;
    }
}
