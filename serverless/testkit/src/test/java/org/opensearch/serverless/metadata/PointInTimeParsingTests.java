/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/** A damaged pin must never silently lose the file references it protects. */
public class PointInTimeParsingTests extends OpenSearchTestCase {
    private static final String VALID = "{\"id\":\"abcdefghijklmnopqrstuv\",\"index\":\"index\",\"expires_at\":10000,"
        + "\"shards\":[{\"shard\":0,\"term\":1,\"files\":{\"file\":\"t=1\"}}]}";

    private static PointInTime parse(String json) throws IOException {
        return PointInTime.fromStream(new BytesArray(json).streamInput());
    }

    public void testMalformedFileMapsAreNotEmptyPins() {
        expectThrows(IOException.class, () -> parse(VALID.replace("{\"file\":\"t=1\"}", "[]")));
        expectThrows(IOException.class, () -> parse(VALID.replace("\"t=1\"", "true")));
        expectThrows(IOException.class, () -> parse(VALID.replace("\"t=1\"", "null")));
    }

    public void testMalformedIdentityAndCaptureFlagsAreRejected() {
        expectThrows(IOException.class, () -> parse(VALID.replace("\"index\":\"index\"", "\"index\":42")));
        expectThrows(IOException.class, () -> parse(VALID.replace("\"expires_at\":10000", "\"expires_at\":10000,\"capturing\":\"false\"")));
        expectThrows(IOException.class, () -> parse(VALID.replace("\"index\":\"index\"", "\"index\":\"\"")));
    }

    public void testInvalidAndDuplicateShardNumbersAreRejected() {
        expectThrows(IOException.class, () -> parse(VALID.replace("\"shard\":0", "\"shard\":-1")));
        expectThrows(IOException.class, () -> parse(VALID.replace("\"term\":1", "\"term\":-1")));
        final String duplicate = VALID.replace("]}", ",{\"shard\":0,\"term\":2,\"files\":{}}]}");
        expectThrows(IOException.class, () -> parse(duplicate));
    }

    public void testTrailingContentIsRejected() {
        expectThrows(IOException.class, () -> parse(VALID + " {}"));
    }

    public void testMalformedLengthsAreRejected() {
        expectThrows(IOException.class, () -> parse(VALID.replace("\"term\":1", "\"term\":1,\"lengths\":[]")));
        expectThrows(IOException.class, () -> parse(VALID.replace("\"term\":1", "\"term\":1,\"lengths\":{\"file\":-1}")));
        expectThrows(IOException.class, () -> parse(VALID.replace("\"term\":1", "\"term\":1,\"lengths\":{\"absent\":1}")));
    }

    public void testMalformedDiscoveredPinProtectsAllFiles() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30000L);
            final var bytes = new BytesArray(VALID.replace("{\"file\":\"t=1\"}", "[]"));
            final var inventory = store.blobContainer(RegisterMap.pointsInTime(BlobPath.cleanPath()));
            try (var input = bytes.streamInput()) {
                inventory.writeBlobAtomic("abcdefghijklmnopqrstuv", input, bytes.length(), true);
            }
            final var live = plane.livePointsInTime(1000L);
            assertEquals(1, live.size());
            assertTrue("unreadable references must stop collection", live.get(0).isPlaceholder());
        }
    }

    public void testLegacyFinishedAndCaptureRecordsStillParse() throws Exception {
        assertEquals("index", parse(VALID).index());
        assertNull(parse(VALID).indexUuid());
        final String capture = "{\"id\":\"abcdefghijklmnopqrstuv\",\"index\":\"index\",\"expires_at\":10000,"
            + "\"capturing\":true,\"shards\":[]}";
        assertTrue(parse(capture).isCapturing());
    }
}
