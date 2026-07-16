/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.split;

import org.opensearch.cluster.ack.ClusterStateUpdateRequest;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Cluster state update request for in-place shard merge -- the reverse of
 * {@link InPlaceSplitShardClusterStateUpdateRequest}. Reverses an earlier split by merging its
 * children back into the single parent shard identified by {@code parentShardId}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class InPlaceMergeShardClusterStateUpdateRequest extends ClusterStateUpdateRequest<InPlaceMergeShardClusterStateUpdateRequest> {

    private final String index;
    private final int parentShardId;
    private final String cause;

    public InPlaceMergeShardClusterStateUpdateRequest(String cause, String index, int parentShardId) {
        this.index = index;
        this.parentShardId = parentShardId;
        this.cause = cause;
    }

    public String getIndex() {
        return index;
    }

    public int getParentShardId() {
        return parentShardId;
    }

    public String cause() {
        return cause;
    }
}
