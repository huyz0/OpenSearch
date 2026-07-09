/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone;

import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.Map;

public class FallbackStreamReaderTests extends OpenSearchTestCase {

    private static TransferManager.StreamReader ofMap(Map<String, byte[]> contents) {
        return (name, position, length) -> {
            byte[] bytes = contents.get(name);
            if (bytes == null) {
                throw new NoSuchFileException("[" + name + "] blob not found");
            }
            return new ByteArrayInputStream(bytes);
        };
    }

    public void testReadsFromPrimaryWhenPresent() throws IOException {
        TransferManager.StreamReader primary = ofMap(Map.of("bundle", "primary".getBytes("UTF-8")));
        TransferManager.StreamReader fallback = ofMap(Map.of("bundle", "fallback".getBytes("UTF-8")));
        FallbackStreamReader reader = new FallbackStreamReader(primary, fallback);
        try (InputStream in = reader.read("bundle", 0, 7)) {
            assertArrayEquals("primary".getBytes("UTF-8"), in.readAllBytes());
        }
    }

    public void testFallsBackWhenAbsentFromPrimary() throws IOException {
        TransferManager.StreamReader primary = ofMap(Map.of());
        TransferManager.StreamReader fallback = ofMap(Map.of("inherited-bundle", "fallback".getBytes("UTF-8")));
        FallbackStreamReader reader = new FallbackStreamReader(primary, fallback);
        try (InputStream in = reader.read("inherited-bundle", 0, 8)) {
            assertArrayEquals("fallback".getBytes("UTF-8"), in.readAllBytes());
        }
    }

    public void testThrowsWhenAbsentFromBoth() {
        FallbackStreamReader reader = new FallbackStreamReader(ofMap(Map.of()), ofMap(Map.of()));
        expectThrows(NoSuchFileException.class, () -> reader.read("nowhere", 0, 1));
    }
}
