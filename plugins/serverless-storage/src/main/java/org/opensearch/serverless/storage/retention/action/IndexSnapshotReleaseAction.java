/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.action.ActionType;

/**
 * The index-wide counterpart to {@link IndexSnapshotPinAction}: releases {@code snapshotId} from
 * every shard of an index by calling {@link SnapshotReleaseAction} once per shard. Unlike {@link
 * IndexSnapshotPinAction}, this needs no compensating rollback on partial failure -- {@link
 * SnapshotReleaseAction} is itself idempotent (removing a pin that's already gone is a no-op), so
 * a caller that sees this fail partway can simply retry the whole request; the shards already
 * released are unaffected by retrying.
 */
public class IndexSnapshotReleaseAction extends ActionType<IndexSnapshotReleaseResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final IndexSnapshotReleaseAction INSTANCE = new IndexSnapshotReleaseAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/index/snapshot_release";

    private IndexSnapshotReleaseAction() {
        super(NAME, IndexSnapshotReleaseResponse::new);
    }
}
