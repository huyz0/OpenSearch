/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction.action;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.storage.util.IndexMetadataUuidIndex;
import org.opensearch.test.OpenSearchTestCase;

/**
 * The shard-existence precondition on the compaction trigger. See {@code
 * TransportSnapshotPinActionTests} for the full reasoning on why the uuid is resolved through
 * cluster metadata rather than merely character-checked -- the short version is that the uuid
 * becomes a blob-path segment, so accepting any string made the path, not the shard, the caller's
 * to choose.
 */
public class TransportCompactionTriggerActionTests extends OpenSearchTestCase {

    private static final String REAL_UUID = "aReallyRealIndexUuid1";
    private static final String OTHER_UUID = "anotherRealIndexUuid2";

    private static Metadata twoIndices() {
        return Metadata.builder().put(index("real-idx", REAL_UUID, 2), false).put(index("other-idx", OTHER_UUID, 1), false).build();
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

    public void testARealIndexAndShardIsAccepted() {
        TransportCompactionTriggerAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), REAL_UUID, 1);
    }

    public void testATraversalShapedUuidIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportCompactionTriggerAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), "../../../etc", 0)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
    }

    public void testAWellFormedButUnknownUuidIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportCompactionTriggerAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), "notAnIndexUuidAtAll", 0)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("does not exist"));
    }

    /** The unrelated-but-real case: a genuine uuid, but shard 1 belongs to the other index. */
    public void testARealUuidWithAShardIdBelongingToADifferentIndexIsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportCompactionTriggerAction.requireRealShard(new IndexMetadataUuidIndex(), twoIndices(), OTHER_UUID, 1)
        );
        assertTrue(e.getMessage(), e.getMessage().contains("out of bounds"));
    }
}
