/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/** Proves {@link SourceSplitFenceMetadata}'s round-trip persistence contract in isolation. */
public class SourceSplitFenceMetadataTests extends OpenSearchTestCase {

    private static IndexMetadata newIndexMetadata(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
            .build();
    }

    public void testUnfencedIndexReportsNotFenced() {
        IndexMetadata metadata = newIndexMetadata("source-idx");
        assertFalse(SourceSplitFenceMetadata.isFencedSource(metadata));
        assertNull(SourceSplitFenceMetadata.supersedingAlias(metadata));
    }

    public void testWithFenceMarksTheIndexAndRecordsTheSupersedingAlias() {
        IndexMetadata metadata = newIndexMetadata("source-idx");
        IndexMetadata fenced = SourceSplitFenceMetadata.withFence(metadata, "split-alias");
        assertTrue(SourceSplitFenceMetadata.isFencedSource(fenced));
        assertEquals("split-alias", SourceSplitFenceMetadata.supersedingAlias(fenced));
        // The original instance must be untouched -- IndexMetadata is immutable/builder-based.
        assertFalse(SourceSplitFenceMetadata.isFencedSource(metadata));
    }

    /**
     * Finding R-7: {@code SourceSplitFenceMetadata} shipped with no unfence primitive, on the stated
     * reasoning that a fence only ever follows a successful cutover. The consequence was that an
     * operator holding an unintended fence -- because a later orchestration stage failed, or because
     * they fenced the wrong index -- could only recover by deleting the index or hand-editing cluster
     * state. A permanent write block on real data whose only escape is data loss is not a safe
     * mechanism.
     */
    public void testWithoutFenceRestoresAWritableIndex() {
        IndexMetadata metadata = newIndexMetadata("source-idx");
        IndexMetadata fenced = SourceSplitFenceMetadata.withFence(metadata, "split-alias");
        assertTrue(SourceSplitFenceMetadata.isFencedSource(fenced));

        IndexMetadata unfenced = SourceSplitFenceMetadata.withoutFence(fenced);
        assertFalse(
            "the fence must be gone, so WritePartitionRoutingActionFilter accepts writes again",
            SourceSplitFenceMetadata.isFencedSource(unfenced)
        );
        assertNull(SourceSplitFenceMetadata.supersedingAlias(unfenced));
        assertTrue("the original fenced instance must be untouched", SourceSplitFenceMetadata.isFencedSource(fenced));
    }

    public void testWithoutFenceOnAnUnfencedIndexReturnsTheSameReference() {
        // Reference identity is load-bearing: TransportUnfenceSplitSourceAction uses it to decide
        // whether to publish a cluster state at all, and MasterService publishes on reference
        // inequality -- so returning an equal-but-new instance would publish a no-op state.
        IndexMetadata metadata = newIndexMetadata("source-idx");
        assertSame(metadata, SourceSplitFenceMetadata.withoutFence(metadata));
    }

    public void testFenceUnfenceFenceRoundTrips() {
        IndexMetadata metadata = newIndexMetadata("source-idx");
        IndexMetadata fenced = SourceSplitFenceMetadata.withFence(metadata, "alias-a");
        IndexMetadata unfenced = SourceSplitFenceMetadata.withoutFence(fenced);
        IndexMetadata refenced = SourceSplitFenceMetadata.withFence(unfenced, "alias-b");
        assertTrue(SourceSplitFenceMetadata.isFencedSource(refenced));
        assertEquals("alias-b", SourceSplitFenceMetadata.supersedingAlias(refenced));
    }

    public void testWithFenceOverwritesAPriorFence() {
        IndexMetadata metadata = newIndexMetadata("source-idx");
        IndexMetadata firstFence = SourceSplitFenceMetadata.withFence(metadata, "alias-a");
        IndexMetadata secondFence = SourceSplitFenceMetadata.withFence(firstFence, "alias-b");
        assertTrue(SourceSplitFenceMetadata.isFencedSource(secondFence));
        assertEquals("alias-b", SourceSplitFenceMetadata.supersedingAlias(secondFence));
    }
}
