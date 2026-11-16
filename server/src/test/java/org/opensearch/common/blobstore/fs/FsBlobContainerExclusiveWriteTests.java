/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.fs;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.PathUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that a put-if-absent write to {@link FsBlobContainer} is published whole.
 */
public class FsBlobContainerExclusiveWriteTests extends OpenSearchTestCase {

    /**
     * A put-if-absent write is never visible before its content is whole. A listing taken while the write is
     * blocked mid-stream must not show the name at all -- an object store never shows a partly written object, and
     * a log fence detected by a blob's length read a half-written record as a fence.
     */
    public void testAnExclusiveWriteIsNeverListedIncomplete() throws Exception {
        final FsBlobContainer container = newContainer();
        final byte[] content = randomByteArrayOfLength(64 * 1024);
        final java.util.concurrent.CountDownLatch halfway = new java.util.concurrent.CountDownLatch(1);
        final java.util.concurrent.CountDownLatch resume = new java.util.concurrent.CountDownLatch(1);
        final InputStream blocking = new FilterInputStream(new ByteArrayInputStream(content)) {
            private boolean first = true;

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (first == false) {
                    halfway.countDown();
                    try {
                        assertTrue(resume.await(30, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new IOException(e);
                    }
                }
                first = false;
                return super.read(b, off, Math.min(len, 1024));
            }
        };
        final AtomicInteger failures = new AtomicInteger();
        final Thread writer = new Thread(() -> {
            try {
                container.writeBlob("00000000000000000007", blocking, content.length, true);
            } catch (IOException e) {
                failures.incrementAndGet();
            }
        });
        writer.start();
        try {
            assertTrue(halfway.await(30, TimeUnit.SECONDS));
            assertFalse(
                "a blob being written must not be listed under its name before it is whole",
                container.listBlobs().containsKey("00000000000000000007")
            );
        } finally {
            resume.countDown();
            writer.join();
        }
        assertThat(failures.get(), equalTo(0));
        assertThat(container.listBlobs().get("00000000000000000007").length(), equalTo((long) content.length));
        assertThat(
            "and no temporary blob is left behind",
            container.listBlobsByPrefix("pending-").keySet(),
            equalTo(Collections.emptySet())
        );
        expectThrows(
            java.nio.file.FileAlreadyExistsException.class,
            () -> container.writeBlob("00000000000000000007", new ByteArrayInputStream(content), content.length, true)
        );
        assertThat(
            "a refused write leaves no temporary blob either",
            container.listBlobsByPrefix("pending-").keySet(),
            equalTo(Collections.emptySet())
        );
    }

    private FsBlobContainer newContainer() throws IOException {
        final Path path = PathUtils.get(createTempDir().toString());
        return new FsBlobContainer(new FsBlobStore(randomIntBetween(1, 8) * 1024, path, false), BlobPath.cleanPath(), path);
    }
}
