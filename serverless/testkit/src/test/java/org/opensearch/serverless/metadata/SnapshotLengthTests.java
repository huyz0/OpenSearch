/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/** Stored lengths are needed to copy a captured commit without listing unrelated historical files. */
public class SnapshotLengthTests extends OpenSearchTestCase {
    private static SnapshotRecord record(Map<String, Long> lengths) {
        return new SnapshotRecord(
            "repo",
            "snapshot",
            "uuid",
            false,
            1000L,
            2000L,
            Map.of(
                "index",
                new SnapshotRecord.SnapshottedIndex(
                    "uuid-index",
                    1,
                    null,
                    Map.of(0, new CommitManifest(1L, Map.of("file", "t=1"), null, lengths))
                )
            )
        );
    }

    public void testFileLengthsSurviveTheStoredSnapshotRoundTrip() throws Exception {
        final var original = record(Map.of("file", 17L));
        final var decoded = SnapshotRecord.fromStream(original.toBytes().streamInput());
        assertEquals(Map.of("file", 17L), decoded.indices().get("index").shards().get(0).lengths());
    }

    public void testLegacySnapshotsNeedNotHaveFileLengths() throws Exception {
        final var original = record(Map.of());
        assertFalse(original.toBytes().utf8ToString().contains("lengths"));
        final var decoded = SnapshotRecord.fromStream(original.toBytes().streamInput());
        assertTrue(decoded.indices().get("index").shards().get(0).lengths().isEmpty());
    }

    public void testMalformedLengthsCannotReachAnUpload() throws Exception {
        final String valid = record(Map.of()).toBytes().utf8ToString();
        for (String malformed : new String[] {
            "[]",
            "null",
            "true",
            "{\"file\":-1}",
            "{\"file\":1.5}",
            "{\"file\":true}",
            "{\"absent\":1}" }) {
            final String json = valid.replace("\"files\":", "\"lengths\":" + malformed + ",\"files\":");
            expectThrows(IOException.class, () -> SnapshotRecord.fromStream(new BytesArray(json).streamInput()));
        }
    }
}
