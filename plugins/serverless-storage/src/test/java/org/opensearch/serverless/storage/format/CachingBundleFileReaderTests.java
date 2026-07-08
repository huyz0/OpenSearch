/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class CachingBundleFileReaderTests extends OpenSearchTestCase {

    private static final class CountingBundleFileReader implements BundleFileReader {
        private final BundleFileReader delegate;
        final AtomicInteger callCount = new AtomicInteger();

        CountingBundleFileReader(BundleFileReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
            callCount.incrementAndGet();
            return delegate.readFile(bundleName, entry);
        }
    }

    public void testTwoShardsSharingOneCacheEachGetTheirOwnHitsAndMisses() throws Exception {
        SegmentBundle bundleA = BundleWriter.write(List.of(new BundleFileContent("a.bin", "shard-a".getBytes(StandardCharsets.UTF_8))));
        SegmentBundle bundleB = BundleWriter.write(List.of(new BundleFileContent("a.bin", "shard-b".getBytes(StandardCharsets.UTF_8))));
        CountingBundleFileReader missDelegateA = new CountingBundleFileReader(
            (bundleName, entry) -> BundleReader.extractFile(bundleA.bytes(), entry)
        );
        CountingBundleFileReader missDelegateB = new CountingBundleFileReader(
            (bundleName, entry) -> BundleReader.extractFile(bundleB.bytes(), entry)
        );

        InMemoryPlaintextBundleCache sharedCache = new InMemoryPlaintextBundleCache(1024);
        CachingBundleFileReader readerA = new CachingBundleFileReader(sharedCache, missDelegateA);
        CachingBundleFileReader readerB = new CachingBundleFileReader(sharedCache, missDelegateB);

        BundleFileEntry entryA = bundleA.entries().get("a.bin");
        BundleFileEntry entryB = bundleB.entries().get("a.bin");

        byte[] firstA = readerA.readFile("bundle-shard-a", entryA);
        byte[] firstB = readerB.readFile("bundle-shard-b", entryB);
        byte[] secondA = readerA.readFile("bundle-shard-a", entryA);
        byte[] secondB = readerB.readFile("bundle-shard-b", entryB);

        assertEquals("shard-a", new String(firstA, StandardCharsets.UTF_8));
        assertEquals("shard-b", new String(firstB, StandardCharsets.UTF_8));
        assertArrayEquals(firstA, secondA);
        assertArrayEquals(firstB, secondB);
        assertEquals("shard A's second read must be a shared-cache hit, not a re-fetch", 1, missDelegateA.callCount.get());
        assertEquals("shard B's second read must be a shared-cache hit, not a re-fetch", 1, missDelegateB.callCount.get());
        assertEquals(2, sharedCache.hitCount());
        assertEquals(2, sharedCache.missCount());
    }
}
