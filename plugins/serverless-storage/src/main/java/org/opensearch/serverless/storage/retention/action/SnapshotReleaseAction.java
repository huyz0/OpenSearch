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
 * The other half of the "snapshot = pinned manifest set" feature (rfc-serverless-opensearch.md
 * &sect;14): releases the pin {@link SnapshotPinAction} added, so the pinned generation becomes
 * deletable again by {@link org.opensearch.serverless.storage.gc.ManifestRetentionPolicy} once no
 * other pin or lease still protects it. Without this half, a pinned snapshot would be a permanent,
 * unbounded storage leak -- the feature is only actually usable with both directions.
 * {@link SnapshotReleaseRequest} names a shard by (indexUuid, shardId) plus the snapshot name;
 * {@link TransportSnapshotReleaseAction} does the actual (object-store-only, no specific-node
 * routing needed) work.
 */
public class SnapshotReleaseAction extends ActionType<SnapshotReleaseResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final SnapshotReleaseAction INSTANCE = new SnapshotReleaseAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/shard/snapshot_release";

    private SnapshotReleaseAction() {
        super(NAME, SnapshotReleaseResponse::new);
    }
}
