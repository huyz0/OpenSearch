/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.allocation;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class SuspendedShardsMetadataTests extends OpenSearchTestCase {

    private static IndexMetadata freshIndex() {
        return IndexMetadata.builder("suspend-metadata-test-idx")
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(3)
            .numberOfReplicas(0)
            .build();
    }

    public void testFreshIndexHasNoSuspendedShards() {
        IndexMetadata index = freshIndex();
        assertFalse(SuspendedShardsMetadata.isSuspended(index, 0));
        assertEquals(Set.of(), SuspendedShardsMetadata.suspendedShardIds(index));
    }

    public void testWithShardSuspendedMarksOnlyThatShard() {
        IndexMetadata index = freshIndex();
        IndexMetadata suspended = SuspendedShardsMetadata.withShardSuspended(index, 1);

        assertTrue(SuspendedShardsMetadata.isSuspended(suspended, 1));
        assertFalse(SuspendedShardsMetadata.isSuspended(suspended, 0));
        assertFalse(SuspendedShardsMetadata.isSuspended(suspended, 2));
        assertEquals(Set.of(1), SuspendedShardsMetadata.suspendedShardIds(suspended));
    }

    public void testWithShardSuspendedIsIdempotentAndReturnsSameInstanceIfAlreadySuspended() {
        IndexMetadata index = freshIndex();
        IndexMetadata suspendedOnce = SuspendedShardsMetadata.withShardSuspended(index, 1);
        IndexMetadata suspendedTwice = SuspendedShardsMetadata.withShardSuspended(suspendedOnce, 1);

        assertSame("suspending an already-suspended shard must be a true no-op", suspendedOnce, suspendedTwice);
    }

    public void testMultipleShardsCanBeSuspendedIndependently() {
        IndexMetadata index = freshIndex();
        IndexMetadata suspended = SuspendedShardsMetadata.withShardSuspended(SuspendedShardsMetadata.withShardSuspended(index, 0), 2);

        assertEquals(Set.of(0, 2), SuspendedShardsMetadata.suspendedShardIds(suspended));
    }

    public void testWithAllShardsReactivatedClearsEveryEntry() {
        IndexMetadata index = freshIndex();
        IndexMetadata suspended = SuspendedShardsMetadata.withShardSuspended(SuspendedShardsMetadata.withShardSuspended(index, 0), 2);

        IndexMetadata reactivated = SuspendedShardsMetadata.withAllShardsReactivated(suspended);

        assertEquals(Set.of(), SuspendedShardsMetadata.suspendedShardIds(reactivated));
    }

    public void testWithAllShardsReactivatedIsANoOpWhenNothingIsSuspended() {
        IndexMetadata index = freshIndex();
        assertSame(index, SuspendedShardsMetadata.withAllShardsReactivated(index));
    }
}
