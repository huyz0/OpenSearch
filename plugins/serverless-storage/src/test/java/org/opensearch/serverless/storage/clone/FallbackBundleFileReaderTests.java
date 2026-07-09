/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone;

import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.Map;

public class FallbackBundleFileReaderTests extends OpenSearchTestCase {

    private static final BundleFileEntry ENTRY = new BundleFileEntry("segments_1", 0, 4, 0L);

    private static final class MapBundleFileReader implements BundleFileReader {
        private final Map<String, byte[]> files;

        MapBundleFileReader(Map<String, byte[]> files) {
            this.files = files;
        }

        @Override
        public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
            byte[] content = files.get(bundleName);
            if (content == null) {
                throw new NoSuchFileException("[" + bundleName + "] blob not found");
            }
            return content;
        }
    }

    public void testReadsFromPrimaryWhenPresent() throws IOException {
        BundleFileReader primary = new MapBundleFileReader(Map.of("own-bundle", "primary".getBytes("UTF-8")));
        BundleFileReader fallback = new MapBundleFileReader(Map.of("own-bundle", "fallback".getBytes("UTF-8")));
        FallbackBundleFileReader reader = new FallbackBundleFileReader(primary, fallback);
        assertArrayEquals("primary".getBytes("UTF-8"), reader.readFile("own-bundle", ENTRY));
    }

    public void testFallsBackWhenAbsentFromPrimary() throws IOException {
        BundleFileReader primary = new MapBundleFileReader(Map.of());
        BundleFileReader fallback = new MapBundleFileReader(Map.of("inherited-bundle", "fallback".getBytes("UTF-8")));
        FallbackBundleFileReader reader = new FallbackBundleFileReader(primary, fallback);
        assertArrayEquals("fallback".getBytes("UTF-8"), reader.readFile("inherited-bundle", ENTRY));
    }

    public void testPropagatesNonMissingIOExceptionsFromPrimaryWithoutFallingBack() {
        BundleFileReader primary = new BundleFileReader() {
            @Override
            public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
                throw new IOException("simulated transient failure, not a missing-file condition");
            }
        };
        BundleFileReader fallback = new MapBundleFileReader(Map.of("bundle", "fallback".getBytes()));
        FallbackBundleFileReader reader = new FallbackBundleFileReader(primary, fallback);
        expectThrows(IOException.class, () -> reader.readFile("bundle", ENTRY));
    }

    public void testThrowsWhenAbsentFromBoth() {
        BundleFileReader primary = new MapBundleFileReader(Map.of());
        BundleFileReader fallback = new MapBundleFileReader(Map.of());
        FallbackBundleFileReader reader = new FallbackBundleFileReader(primary, fallback);
        expectThrows(NoSuchFileException.class, () -> reader.readFile("nowhere-bundle", ENTRY));
    }
}
