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

    public void testWithFenceOverwritesAPriorFence() {
        IndexMetadata metadata = newIndexMetadata("source-idx");
        IndexMetadata firstFence = SourceSplitFenceMetadata.withFence(metadata, "alias-a");
        IndexMetadata secondFence = SourceSplitFenceMetadata.withFence(firstFence, "alias-b");
        assertTrue(SourceSplitFenceMetadata.isFencedSource(secondFence));
        assertEquals("alias-b", SourceSplitFenceMetadata.supersedingAlias(secondFence));
    }
}
