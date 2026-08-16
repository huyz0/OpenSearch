/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.deepsnapshot.action;

import org.opensearch.action.ActionType;

/**
 * Round 006 item 5, shipped: an independent, byte-copying snapshot of a whole serverless index into
 * a standard repository, restorable by a cluster that has never heard of this plugin -- proven by
 * {@code DeepSnapshotOrchestrationIT} before this action existed, and by {@code
 * IndexDeepSnapshotActionIT} through it.
 *
 * <p>Fans {@link ShardDeepSnapshotAction} out across every shard (see {@link
 * TransportIndexDeepSnapshotAction}), each pinned and released independently, then finalizes once on
 * the cluster manager -- one cluster-manager operation per snapshot with all of the byte movement
 * distributable off it, which is the shape docs/rounds/006-durability/plan.md's own item 5 write-up
 * calls out as "the right shape for a branch whose whole project was removing per-index
 * cluster-manager work."
 *
 * <p>Distinct from every action in {@link org.opensearch.serverless.storage.retention.action}: those
 * are shallow (a pointer to a manifest generation, no bytes copied, restorable only while the source
 * object store survives). This is deep -- see docs-site/src/content/docs/design/durability-posture.md
 * for the three-tier distinction this action is the "independent copy" row of.
 *
 * <p>Resolves the index name through {@code AbsentIndexDescriptorSuppliers.metadataOrDescriptor}, the
 * same gated-index resolution item 1 of this round built for the shallow actions, so a gated index
 * can be deep-copied exactly as an ordinary serverless index can.
 */
public class IndexDeepSnapshotAction extends ActionType<IndexDeepSnapshotResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final IndexDeepSnapshotAction INSTANCE = new IndexDeepSnapshotAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/index/deep_snapshot";

    private IndexDeepSnapshotAction() {
        super(NAME, IndexDeepSnapshotResponse::new);
    }
}
