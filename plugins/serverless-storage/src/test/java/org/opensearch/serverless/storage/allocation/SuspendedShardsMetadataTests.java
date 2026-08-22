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

        IndexMetadata reactivated = SuspendedShardsMetadata.withAllShardsReactivated(suspended, 1000L);

        assertEquals(Set.of(), SuspendedShardsMetadata.suspendedShardIds(reactivated));
    }

    public void testWithAllShardsReactivatedIsANoOpWhenNothingIsSuspended() {
        IndexMetadata index = freshIndex();
        assertSame(index, SuspendedShardsMetadata.withAllShardsReactivated(index, 1000L));
    }

    public void testWithAllShardsReactivatedStampsTheReactivationTimeForExactlyTheClearedShards() {
        IndexMetadata index = freshIndex();
        IndexMetadata suspended = SuspendedShardsMetadata.withShardSuspended(SuspendedShardsMetadata.withShardSuspended(index, 0), 2);

        IndexMetadata reactivated = SuspendedShardsMetadata.withAllShardsReactivated(suspended, 5000L);

        assertEquals(5000L, SuspendedShardsMetadata.lastReactivatedAtMillis(reactivated, 0, false));
        assertEquals(5000L, SuspendedShardsMetadata.lastReactivatedAtMillis(reactivated, 2, false));
        assertEquals(
            "shard 1 was never suspended, so it must have no reactivation time",
            -1L,
            SuspendedShardsMetadata.lastReactivatedAtMillis(reactivated, 1, false)
        );
    }

    public void testWriterAndReaderSuspensionAreTrackedCompletelyIndependently() {
        IndexMetadata index = freshIndex();
        IndexMetadata writerSuspended = SuspendedShardsMetadata.withShardSuspended(index, 1);

        assertTrue(SuspendedShardsMetadata.isSuspended(writerSuspended, 1));
        assertFalse(
            "suspending the writer copy must not suspend the reader copy of the same shard",
            SuspendedShardsMetadata.isReaderSuspended(writerSuspended, 1)
        );
        assertEquals(Set.of(), SuspendedShardsMetadata.suspendedReaderShardIds(writerSuspended));

        IndexMetadata bothSuspended = SuspendedShardsMetadata.withReaderShardSuspended(writerSuspended, 1);
        assertTrue(SuspendedShardsMetadata.isSuspended(bothSuspended, 1));
        assertTrue(SuspendedShardsMetadata.isReaderSuspended(bothSuspended, 1));

        IndexMetadata writerReactivated = SuspendedShardsMetadata.withAllShardsReactivated(bothSuspended, 1000L);
        assertFalse(
            "reactivating writer shards must not affect reader suspension",
            SuspendedShardsMetadata.isSuspended(writerReactivated, 1)
        );
        assertTrue(
            "reactivating writer shards must not affect reader suspension",
            SuspendedShardsMetadata.isReaderSuspended(writerReactivated, 1)
        );

        IndexMetadata readerReactivated = SuspendedShardsMetadata.withAllReaderShardsReactivated(writerReactivated, 2000L);
        assertFalse(SuspendedShardsMetadata.isReaderSuspended(readerReactivated, 1));

        assertEquals(1000L, SuspendedShardsMetadata.lastReactivatedAtMillis(readerReactivated, 1, false));
        assertEquals(2000L, SuspendedShardsMetadata.lastReactivatedAtMillis(readerReactivated, 1, true));
    }

    public void testWithReaderShardSuspendedIsIdempotentAndReturnsSameInstanceIfAlreadySuspended() {
        IndexMetadata index = freshIndex();
        IndexMetadata suspendedOnce = SuspendedShardsMetadata.withReaderShardSuspended(index, 1);
        IndexMetadata suspendedTwice = SuspendedShardsMetadata.withReaderShardSuspended(suspendedOnce, 1);

        assertSame("suspending an already-suspended reader shard must be a true no-op", suspendedOnce, suspendedTwice);
    }

    public void testWithAllReaderShardsReactivatedIsANoOpWhenNothingIsSuspended() {
        IndexMetadata index = freshIndex();
        assertSame(index, SuspendedShardsMetadata.withAllReaderShardsReactivated(index, 1000L));
    }

    public void testIsSuspensionAllowedWithNoCooldownConfigured() {
        IndexMetadata index = freshIndex();
        assertTrue(SuspendedShardsMetadata.isSuspensionAllowed(index, 0, false, 1_000_000L, 0L));
    }

    public void testIsSuspensionAllowedForAShardNeverReactivated() {
        IndexMetadata index = freshIndex();
        assertTrue(SuspendedShardsMetadata.isSuspensionAllowed(index, 0, false, 1_000_000L, 60_000L));
    }

    public void testIsSuspensionAllowedRespectsTheCooldownWindow() {
        IndexMetadata index = freshIndex();
        IndexMetadata suspended = SuspendedShardsMetadata.withShardSuspended(index, 0);
        IndexMetadata reactivated = SuspendedShardsMetadata.withAllShardsReactivated(suspended, 10_000L);

        assertFalse(
            "reactivated 5s ago with a 60s cooldown must still be disallowed",
            SuspendedShardsMetadata.isSuspensionAllowed(reactivated, 0, false, 15_000L, 60_000L)
        );
        assertTrue(
            "reactivated 61s ago with a 60s cooldown must be allowed again",
            SuspendedShardsMetadata.isSuspensionAllowed(reactivated, 0, false, 71_000L, 60_000L)
        );
    }

    public void testIsSuspensionAllowedForReaderIsIndependentOfWriter() {
        IndexMetadata index = freshIndex();
        IndexMetadata writerSuspended = SuspendedShardsMetadata.withShardSuspended(index, 0);
        IndexMetadata writerReactivated = SuspendedShardsMetadata.withAllShardsReactivated(writerSuspended, 10_000L);

        // The writer was just reactivated (within cooldown), but the reader has never been
        // suspended at all -- its own suspension must still be allowed.
        assertTrue(SuspendedShardsMetadata.isSuspensionAllowed(writerReactivated, 0, true, 15_000L, 60_000L));
        assertFalse(SuspendedShardsMetadata.isSuspensionAllowed(writerReactivated, 0, false, 15_000L, 60_000L));
    }
}
