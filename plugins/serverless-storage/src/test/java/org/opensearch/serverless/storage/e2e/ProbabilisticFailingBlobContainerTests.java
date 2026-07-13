/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.e2e;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.util.List;

public class ProbabilisticFailingBlobContainerTests extends OpenSearchTestCase {

    private BlobContainer newRawContainer() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    public void testZeroProbabilityNeverFails() throws Exception {
        BlobContainer container = new ProbabilisticFailingBlobContainer(newRawContainer(), random(), 0.0);
        for (int i = 0; i < 50; i++) {
            byte[] bytes = ("v" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            container.writeBlob("blob-" + i, new ByteArrayInputStream(bytes), bytes.length, true);
        }
        // No exception thrown across 50 real writes -- proves 0.0 genuinely means "never," not "rarely."
    }

    public void testOneProbabilityAlwaysFails() throws Exception {
        BlobContainer container = new ProbabilisticFailingBlobContainer(newRawContainer(), random(), 1.0);
        byte[] bytes = "v".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        expectThrows(java.io.IOException.class, () -> container.writeBlob("blob", new ByteArrayInputStream(bytes), bytes.length, true));
    }

    public void testFailsRepeatedlyAcrossManyCallsNotJustOnce() throws Exception {
        // Unlike a one-shot fixture, a container configured to always fail must keep failing on
        // every subsequent call, not just the first.
        BlobContainer container = new ProbabilisticFailingBlobContainer(newRawContainer(), random(), 1.0);
        byte[] bytes = "v".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (int i = 0; i < 5; i++) {
            int index = i;
            expectThrows(
                java.io.IOException.class,
                () -> container.writeBlob("blob-" + index, new ByteArrayInputStream(bytes), bytes.length, true)
            );
        }
    }

    public void testReadsAndListsAreNeverFaulted() throws Exception {
        BlobContainer raw = newRawContainer();
        byte[] bytes = "v".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        raw.writeBlob("blob", new ByteArrayInputStream(bytes), bytes.length, true);
        BlobContainer container = new ProbabilisticFailingBlobContainer(raw, random(), 1.0);

        try (var in = container.readBlob("blob")) {
            assertArrayEquals(bytes, in.readAllBytes());
        }
        // Not assertEquals(1, ...size()): createTempDir() deliberately, randomly salts the
        // directory it returns with extra junk entries (e.g. "extra0") to catch code that wrongly
        // assumes a pristine directory -- confirmed by a real flake this test's own first version
        // hit (listBlobs().size() was 2, not 1, purely from that injected junk file, not from any
        // fault the container actually introduced). listBlobs() succeeding at all (no exception)
        // and containing exactly the one blob this test wrote is the real property under test.
        assertTrue(container.listBlobs().containsKey("blob"));
    }

    public void testRejectsAnOutOfRangeProbability() throws Exception {
        BlobContainer raw = newRawContainer();
        expectThrows(IllegalArgumentException.class, () -> new ProbabilisticFailingBlobContainer(raw, random(), 1.5));
        expectThrows(IllegalArgumentException.class, () -> new ProbabilisticFailingBlobContainer(raw, random(), -0.1));
    }

    public void testCompareAndSwapRegisterAndDeleteAreAlsoFaultable() throws Exception {
        BlobContainer raw = newRawContainer();
        BlobContainer container = new ProbabilisticFailingBlobContainer(raw, random(), 1.0);

        expectThrows(java.io.IOException.class, () -> container.compareAndSwapRegister("register", 0L, new BytesArray("v")));
        expectThrows(java.io.IOException.class, () -> container.deleteBlobsIgnoringIfNotExists(List.of("blob")));
    }
}
