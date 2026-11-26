/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.serverless.repository.SnapshotRecord;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/** An index entry with failed shards is a finished capture, not the in-progress marker that pins the whole index. */
public class SnapshotPinTests extends OpenSearchTestCase {

    private static SnapshotRecord record(Map<Integer, CommitManifest> shards, List<Integer> failed) {
        return new SnapshotRecord(
            "repo",
            "view",
            "uuid",
            true,
            1L,
            2L,
            Map.of("index", new SnapshotRecord.SnapshottedIndex("uuid-index", 2, null, Map.of(), shards, null, null, failed))
        );
    }

    public void testAMarkerPinsEveryShardAndAFinishedAllFailedIndexPinsNone() {
        final var marker = record(Map.of(), List.of());
        final var allFailed = record(Map.of(), List.of(0, 1));
        assertTrue(IndexLifecycle.pinnedBy(List.of(marker), "uuid-index", 1));
        assertFalse(IndexLifecycle.pinnedBy(List.of(allFailed), "uuid-index", 0));
        assertFalse(IndexLifecycle.pinnedBy(List.of(allFailed), "uuid-index", 1));
    }

    public void testAShallowPartialSnapshotPinsOnlyTheShardsItCaptured() {
        final var partial = record(Map.of(0, new CommitManifest(1L, Map.of("f", "t=1"), null, Map.of())), List.of(1));
        assertTrue(IndexLifecycle.pinnedBy(List.of(partial), "uuid-index", 0));
        assertFalse(IndexLifecycle.pinnedBy(List.of(partial), "uuid-index", 1));
    }
}
