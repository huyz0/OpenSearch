/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone.action;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.storage.util.IndexMetadataUuidIndex;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The provisioning precondition on shard clone, which -- unlike the read-mostly retention actions
 * -- <em>writes</em> to whatever location the target uuid resolves to, so an unvalidated uuid there
 * is a write the caller aims rather than a shard the caller owns. Both sides are checked, and the
 * error names which one failed. See {@code TransportSnapshotPinActionTests} for the full reasoning
 * on resolving through cluster metadata rather than character-checking.
 */
public class TransportShardCloneActionTests extends OpenSearchTestCase {

    private static final String SOURCE_UUID = "aReallyRealSourceUuid";
    private static final String TARGET_UUID = "aReallyRealTargetUuid";

    /** Source has 2 shards, target has 1. */
    private static Metadata twoIndices() {
        return Metadata.builder().put(index("source-idx", SOURCE_UUID, 2), false).put(index("target-idx", TARGET_UUID, 1), false).build();
    }

    private static IndexMetadata index(String name, String uuid, int numberOfShards) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).put(IndexMetadata.SETTING_INDEX_UUID, uuid)
            )
            .numberOfShards(numberOfShards)
            .numberOfReplicas(0)
            .build();
    }

    public void testRealSourceAndTargetShardsAreAccepted() {
        Metadata metadata = twoIndices();
        IndexMetadataUuidIndex uuidIndex = new IndexMetadataUuidIndex();
        TransportShardCloneAction.requireRealShard(uuidIndex, metadata, "source", SOURCE_UUID, 1);
        TransportShardCloneAction.requireRealShard(uuidIndex, metadata, "target", TARGET_UUID, 0);
    }

    public void testATraversalShapedTargetUuidIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportShardCloneAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), "target", "../../../etc", 0)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("target index"));
        assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
    }

    public void testAWellFormedButUnknownSourceUuidIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportShardCloneAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), "source", "notAnIndexUuid", 0)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("source index"));
        assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
    }

    /** The unrelated-but-real case: a genuine uuid, but shard 1 belongs to the other index. */
    public void testARealTargetUuidWithAShardIdBelongingToADifferentIndexIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportShardCloneAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), "target", TARGET_UUID, 1)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("out of bounds"));
        assertTrue("the error must name the index it actually resolved to", e.getMessage().contains("target-idx"));
    }
}
