/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Does the conformance suite actually catch a backend that does not conform?
 *
 * <p>It will be pointed at implementations nobody here controls, so the one thing that must not be
 * true of it is that it passes regardless. These plant the two failures that matter — a
 * compare-and-swap that always claims success, and a ranged read that ignores the range — and confirm
 * the properties the suite asserts on would come out differently.
 */
public class ConformanceSuiteSelfTests extends OpenSearchTestCase {

    private BlobContainer real() throws Exception {
        return new FsBlobStore(1024, createTempDir(), false).blobContainer(BlobPath.cleanPath());
    }

    /** A backend whose compare-and-swap always claims to have applied. */
    private static final class AlwaysWinsContainer extends DelegatingBlobContainer {
        AlwaysWinsContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue) {
            return BlobRegisterCasResult.applied(expectedGeneration + 1);
        }
    }

    /** A backend that ignores the requested range and returns the whole blob. */
    private static final class IgnoresRangeContainer extends DelegatingBlobContainer {
        IgnoresRangeContainer(BlobContainer delegate) {
            super(delegate);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            return super.readBlob(blobName);
        }
    }

    public void testTheSuiteWouldCatchACompareAndSwapThatAlwaysWins() throws Exception {
        final BlobContainer container = new AlwaysWinsContainer(real());
        container.createRegisterIfAbsent("head", new BytesArray("initial".getBytes(StandardCharsets.UTF_8)));
        final long generation = container.readRegister("head").orElseThrow().generation();

        final CountDownLatch start = new CountDownLatch(1);
        final AtomicInteger winners = new AtomicInteger();
        final Thread[] threads = new Thread[8];
        for (int i = 0; i < threads.length; i++) {
            final int id = i;
            threads[i] = new Thread(() -> {
                try {
                    start.await();
                    if (container.compareAndSwapRegister(
                        "head",
                        generation,
                        new BytesArray(("owner-" + id).getBytes(StandardCharsets.UTF_8))
                    ).applied()) {
                        winners.incrementAndGet();
                    }
                } catch (Exception e) {
                    // counted as a non-win
                }
            });
            threads[i].start();
        }
        start.countDown();
        for (Thread t : threads) {
            t.join(60_000);
        }

        // The suite asserts exactly one. A backend like this would give eight, and two nodes would
        // both believe they owned the same shard.
        assertTrue("the suite's winner count must differ on a broken backend, but was " + winners.get(), winners.get() > 1);
    }

    public void testTheSuiteWouldCatchARangedReadThatIgnoresItsRange() throws Exception {
        final BlobContainer container = new IgnoresRangeContainer(real());
        final byte[] content = new byte[4096];
        for (int i = 0; i < content.length; i++) {
            content[i] = (byte) (i % 251);
        }
        container.writeBlob("data", new java.io.ByteArrayInputStream(content), content.length, false);

        boolean detected = false;
        final int offset = 100;
        final int length = 16;
        final byte[] actual = new byte[length];
        try (InputStream in = container.readBlob("data", offset, length)) {
            int read = 0;
            while (read < length) {
                final int n = in.read(actual, read, length - read);
                if (n <= 0) {
                    break;
                }
                read += n;
            }
            // The suite checks both that the bytes are right and that the stream ends where it should.
            if (in.read() != -1) {
                detected = true;
            }
        }
        for (int i = 0; i < length && detected == false; i++) {
            if (actual[i] != content[offset + i]) {
                detected = true;
            }
        }
        assertTrue("a backend ignoring the range must not pass the suite's range checks", detected);
    }
}
