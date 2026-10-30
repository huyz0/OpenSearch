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
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.metadata.TemplateStore;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class TemplateMigrationPublicationTests extends OpenSearchTestCase {
    public void testAcknowledgedMigrationNeverExposesAnotherUploadsPartialBytes() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false); var executor = Executors.newFixedThreadPool(2)) {
            final var raw = store.blobContainer(BlobPath.cleanPath().add("templates"));
            final String legacy = "{\"index_patterns\":[\"legacy-*\"],\"priority\":7}";
            final byte[] bytes = legacy.getBytes(StandardCharsets.UTF_8);
            raw.writeBlob("template-legacy", new ByteArrayInputStream(bytes), bytes.length, true);
            raw.createRegisterIfAbsent("_version", new BytesArray("1"));
            final String adoptedName = "blob-" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
            final CountDownLatch firstReading = new CountDownLatch(1);
            final CountDownLatch secondReading = new CountDownLatch(1);
            final CountDownLatch releaseFirst = new CountDownLatch(1);
            final CountDownLatch releaseSecond = new CountDownLatch(1);
            final AtomicInteger uploads = new AtomicInteger();
            final BlobContainer container = new DelegatingBlobContainer(raw) {
                @Override
                public void writeBlob(String name, InputStream input, long size, boolean exclusive) throws IOException {
                    if (name.equals(adoptedName)) {
                        final boolean first = uploads.incrementAndGet() == 1;
                        final CountDownLatch reached = first ? firstReading : secondReading;
                        final CountDownLatch release = first ? releaseFirst : releaseSecond;
                        final AtomicBoolean paused = new AtomicBoolean();
                        input = new FilterInputStream(input) {
                            @Override
                            public int read(byte[] buffer, int offset, int length) throws IOException {
                                if (paused.compareAndSet(false, true)) {
                                    reached.countDown();
                                    try {
                                        if (release.await(30, TimeUnit.SECONDS) == false) {
                                            throw new IOException("timed out waiting to complete migration upload");
                                        }
                                    } catch (InterruptedException e) {
                                        Thread.currentThread().interrupt();
                                        throw new IOException(e);
                                    }
                                }
                                return super.read(buffer, offset, length);
                            }
                        };
                    }
                    super.writeBlob(name, input, size, exclusive);
                }
            };
            final var first = executor.submit(() -> put(new TemplateStore(container), "new-a"));
            try {
                assertTrue(firstReading.await(30, TimeUnit.SECONDS));
                final var second = executor.submit(() -> put(new TemplateStore(container), "new-b"));
                assertTrue(secondReading.await(30, TimeUnit.SECONDS));
                releaseFirst.countDown();
                first.get(30, TimeUnit.SECONDS);
                // One mutation has acknowledged the indexed marker while the other upload is still
                // paused after opening its output. A cold reader must see the complete adopted body.
                assertEquals(legacy, new TemplateStore(raw).get("legacy").orElseThrow());
                releaseSecond.countDown();
                second.get(30, TimeUnit.SECONDS);
                assertEquals(Map.of("legacy", legacy, "new-a", "{}", "new-b", "{}"), new TemplateStore(raw).all());
            } finally {
                releaseFirst.countDown();
                releaseSecond.countDown();
            }
        }
    }

    public void testPartialUnindexedContentFromAnOldCrashDoesNotPoisonMigration() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var container = store.blobContainer(BlobPath.cleanPath());
            final String legacy = "{\"index_patterns\":[\"legacy-*\"]}";
            final byte[] bytes = legacy.getBytes(StandardCharsets.UTF_8);
            container.writeBlob("template-legacy", new ByteArrayInputStream(bytes), bytes.length, true);
            container.createRegisterIfAbsent("_version", new BytesArray("1"));
            final String key = "blob-" + HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
            container.writeBlob(key, new ByteArrayInputStream(new byte[0]), 0, true);
            new TemplateStore(container).put("new", "{}");
            assertEquals(legacy, new TemplateStore(container).get("legacy").orElseThrow());
        }
    }

    public void testIdenticalLegacyBodiesNeedOnlyOneSharedPublication() throws Exception {
        try (var store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var container = store.blobContainer(BlobPath.cleanPath());
            final byte[] bytes = "{\"priority\":7}".getBytes(StandardCharsets.UTF_8);
            for (String name : java.util.List.of("first", "second")) {
                container.writeBlob("template-" + name, new ByteArrayInputStream(bytes), bytes.length, true);
            }
            container.createRegisterIfAbsent("_version", new BytesArray("1"));
            store.reset();
            new TemplateStore(container).put("new", "{}");
            assertEquals("one shared adopted body plus the new revision", 2L, store.blobWrites());
            assertEquals(3, new TemplateStore(container).all().size());
        }
    }

    private static void put(TemplateStore templates, String name) {
        try {
            templates.put(name, "{}");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
