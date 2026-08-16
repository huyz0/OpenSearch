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
 * One shard's half of round 006 item 5: an independent, byte-copying snapshot of a serverless
 * shard into a standard {@link org.opensearch.repositories.Repository}, proven end to end by
 * {@code BundleBackedCommitIsCopyableTests} and {@code DeepSnapshotOrchestrationIT} before this
 * action existed to wire it (rfc-serverless-opensearch.md &sect;14, docs/rounds/006-durability).
 *
 * <p>Unlike {@link org.opensearch.serverless.storage.retention.action.SnapshotPinAction} and its
 * siblings, this is not shallow: no pointer is written, no manifest generation number is what gets
 * handed back. The shard's currently-pinned commit is read out of the object store by this node,
 * byte for byte, and written into the target repository in the repository's own standard format --
 * the same {@link org.opensearch.repositories.Repository#snapshotShard} core's own {@code
 * SnapshotsService} calls for an ordinary index, given a {@link org.opensearch.index.store.Store}
 * built over a {@link org.opensearch.serverless.storage.readerengine.lazydirectory.LazyBundleDirectory}
 * instead of a real shard's local directory.
 *
 * <p>Pins the generation being copied for the duration of the copy (see {@link
 * TransportShardDeepSnapshotAction}), so GC cannot reclaim it mid-read, and releases that pin on
 * both the success and failure paths -- the plan's own "release the pin, on both paths" instruction,
 * taken literally.
 *
 * <p>Requires no routing to a specific data node, the same reasoning {@link
 * org.opensearch.serverless.storage.retention.action.TransportSnapshotPinAction}'s own javadoc
 * gives: this operates purely against the shared object store and the target repository, never
 * against node-local shard state, so whichever node receives this request can execute it -- which is
 * what lets a deep snapshot's byte movement be distributed across nodes rather than serialized
 * through the cluster manager the way {@link
 * org.opensearch.serverless.storage.deepsnapshot.action.IndexDeepSnapshotAction}'s own {@code
 * finalizeSnapshot} call must be.
 */
public class ShardDeepSnapshotAction extends ActionType<ShardDeepSnapshotResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final ShardDeepSnapshotAction INSTANCE = new ShardDeepSnapshotAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/shard/deep_snapshot";

    private ShardDeepSnapshotAction() {
        super(NAME, ShardDeepSnapshotResponse::new);
    }
}
