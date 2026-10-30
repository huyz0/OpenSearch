/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.testkit.CountingBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class WalReplayReadTests extends OpenSearchTestCase {
    public void testCorruptionStopsBeforeFetchingTheUnboundedTail() throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var root = BlobPath.cleanPath().add("shard");
            final var wal = new WalStore(store, root).readingWith(Runnable::run);
            for (int i = 0; i < 30; i++) {
                wal.append(1L, new WalRecord("id-" + i, "{}"));
            }
            final var term = store.blobContainer(root.add("wal").add(SegmentPublisher.termSegment(1L)));
            final String first = term.listBlobs()
                .values()
                .stream()
                .filter(blob -> blob.name().matches("[0-9]{20}") && blob.length() > 0L)
                .map(org.opensearch.common.blobstore.BlobMetadata::name)
                .min(String::compareTo)
                .orElseThrow();
            final byte[] corrupt = "invalid WAL".getBytes(java.nio.charset.StandardCharsets.UTF_8);
            term.writeBlob(first, new ByteArrayInputStream(corrupt), corrupt.length, false);
            store.reset();
            final IOException failure = expectThrows(IOException.class, wal::replayable);
            assertTrue(failure.getMessage(), failure.getMessage().contains(first));
            assertTrue(
                "decode each bounded window before fetching the rest: " + store.blobReads(),
                store.blobReads() <= WalStore.READ_WIDTH
            );
        }
    }

    public void testReplayRetainsTermBatchAndRecordOrderAcrossWindows() throws Exception {
        try (
            var store = new FsBlobStore(1024, createTempDir(), false);
            var workers = java.util.concurrent.Executors.newFixedThreadPool(2)
        ) {
            final var root = BlobPath.cleanPath().add("shard");
            final var wal = new WalStore(store, root).readingWith(workers);
            final java.util.List<String> expected = new java.util.ArrayList<>();
            for (long term : new long[] { 2L, 10L }) {
                for (int i = 0; i < 25; i++) {
                    final String id = term + "-" + i;
                    wal.append(term, List.of(new WalRecord(id + "-a", "{}"), new WalRecord(id + "-b", "{}")));
                    expected.add(id + "-a");
                    expected.add(id + "-b");
                }
            }
            assertEquals(expected, wal.replayable().stream().map(WalRecord::id).toList());
            final var term2 = store.blobContainer(root.add("wal").add(SegmentPublisher.termSegment(2L)));
            final List<String> names = term2.listBlobs()
                .values()
                .stream()
                .filter(blob -> blob.name().matches("[0-9]{20}") && blob.length() > 0L)
                .map(org.opensearch.common.blobstore.BlobMetadata::name)
                .sorted()
                .toList();
            assertEquals(expected.subList(0, 18), wal.replayable(Map.of(2L, names.get(8))).stream().map(WalRecord::id).toList());
        }
    }

    public void testReadWindowsRespectTheByteBudget() throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var root = BlobPath.cleanPath().add("shard");
            final var wal = new WalStore(store, root).readingWith(Runnable::run);
            final String source = "{\"value\":\"" + "x".repeat(9 * 1024 * 1024) + "\"}";
            for (int i = 0; i < 3; i++) {
                wal.append(1L, new WalRecord("id-" + i, source));
            }
            final var term = store.blobContainer(root.add("wal").add(SegmentPublisher.termSegment(1L)));
            final String first = term.listBlobs()
                .values()
                .stream()
                .filter(blob -> blob.name().matches("[0-9]{20}") && blob.length() > 0L)
                .map(org.opensearch.common.blobstore.BlobMetadata::name)
                .min(String::compareTo)
                .orElseThrow();
            final byte[] corrupt = new byte[9 * 1024 * 1024];
            java.util.Arrays.fill(corrupt, (byte) 'x');
            term.writeBlob(first, new ByteArrayInputStream(corrupt), corrupt.length, false);
            store.reset();
            expectThrows(IOException.class, wal::replayable);
            assertEquals("two large bodies must not occupy the same raw read window", 1L, store.blobReads());
        }
    }
}
