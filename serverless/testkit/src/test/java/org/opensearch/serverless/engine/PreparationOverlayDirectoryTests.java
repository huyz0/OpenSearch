/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PreparationOverlayDirectoryTests extends OpenSearchTestCase {
    private static final PreparationOverlayDirectory.Limits LIMITS = new PreparationOverlayDirectory.Limits(
        10_000,
        4 * 1024 * 1024,
        1L << 30
    );

    public void testNativePrivateCommitsAndMergesNeverChangeTheSource() throws Exception {
        final var policy = new SnapshotDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
        try (var source = newDirectory(); var writer = new IndexWriter(source, newIndexWriterConfig().setIndexDeletionPolicy(policy))) {
            writer.addDocument(document("original"));
            writer.commit();
            final var commit = policy.snapshot();
            final var releases = new AtomicInteger();
            final var overlay = new PreparationOverlayDirectory(
                new MockDirectoryWrapper(random(), new ByteBuffersDirectory()),
                commit,
                () -> {
                    policy.release(commit);
                    releases.incrementAndGet();
                },
                LIMITS
            );
            try (
                overlay;
                var privateWriter = new IndexWriter(overlay, newIndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND))
            ) {
                writer.addDocument(document("public-later"));
                writer.commit();
                privateWriter.deleteDocuments(new Term("id", "original"));
                privateWriter.addDocument(document("private"));
                privateWriter.forceMerge(1);
                privateWriter.commit();
                try (var reader = DirectoryReader.open(overlay)) {
                    assertEquals(1, reader.numDocs());
                    assertEquals("private", reader.storedFields().document(0).get("id"));
                }
                try (var reader = DirectoryReader.open(source)) {
                    assertEquals(2, reader.numDocs());
                }
                assertEquals(0, releases.get());
            }
            overlay.close();
            assertEquals(1, releases.get());
            expectThrows(AlreadyClosedException.class, overlay::listAll);
            writer.addDocument(document("source-still-open"));
            writer.commit();
            try (var reader = DirectoryReader.open(source)) {
                assertEquals(3, reader.numDocs());
            }
        }
    }

    public void testPrivateDeletesAndReplacementsNeverRevealOrRemoveBaseFiles() throws Exception {
        try (var source = newDirectory(); var writer = new IndexWriter(source, newIndexWriterConfig())) {
            writer.addDocument(document("base"));
            writer.commit();
            final var commit = DirectoryReader.listCommits(source).get(0);
            final String name = commit.getSegmentsFileName();
            final byte[] original = read(source, name);
            final var releases = new AtomicInteger();
            try (var overlay = new PreparationOverlayDirectory(new ByteBuffersDirectory(), commit, releases::incrementAndGet, LIMITS)) {
                assertArrayEquals(original, read(overlay, name));
                overlay.sync(List.of(name));
                overlay.deleteFile(name);
                expectThrows(IOException.class, () -> overlay.openInput(name, IOContext.READONCE));
                expectThrows(IOException.class, () -> overlay.deleteFile(name));
                assertFalse(Arrays.asList(overlay.listAll()).contains(name));
                try (var output = overlay.createOutput(name, IOContext.DEFAULT)) {
                    output.writeByte((byte) 7);
                }
                assertArrayEquals(new byte[] { 7 }, read(overlay, name));
                overlay.sync(List.of(name));
                overlay.deleteFile(name);
                expectThrows(IOException.class, () -> overlay.fileLength(name));
                assertArrayEquals(original, read(source, name));
                try (var temp = overlay.createTempOutput("pending", "tmp", IOContext.DEFAULT)) {
                    temp.writeByte((byte) 9);
                }
                try (var output = overlay.createOutput("local", IOContext.DEFAULT)) {
                    output.writeByte((byte) 11);
                }
                overlay.rename("local", name);
                assertArrayEquals(new byte[] { 11 }, read(overlay, name));
                expectThrows(IOException.class, () -> overlay.rename("not-a-private-file", "other"));
                overlay.syncMetaData();
                assertArrayEquals(original, read(source, name));
                writer.addDocument(document("future"));
                writer.commit();
                final String futureCommit = DirectoryReader.listCommits(source).get(0).getSegmentsFileName();
                expectThrows(IOException.class, () -> overlay.openInput(futureCommit, IOContext.READONCE));
                assertFalse(Arrays.asList(overlay.listAll()).contains(futureCommit));
            }
            assertEquals(1, releases.get());
        }
    }

    public void testLimitsAndFailedConstructionRetainCallerOwnership() throws Exception {
        try (
            var source = newDirectory();
            var writer = new IndexWriter(source, newIndexWriterConfig());
            var local = new ByteBuffersDirectory()
        ) {
            writer.addDocument(document("base"));
            writer.commit();
            final var commit = DirectoryReader.listCommits(source).get(0);
            final var releases = new AtomicInteger();
            for (var limits : List.of(
                new PreparationOverlayDirectory.Limits(1, LIMITS.maximumMetadataBytes(), LIMITS.maximumPinnedBytes()),
                new PreparationOverlayDirectory.Limits(LIMITS.maximumFiles(), 128, LIMITS.maximumPinnedBytes()),
                new PreparationOverlayDirectory.Limits(LIMITS.maximumFiles(), LIMITS.maximumMetadataBytes(), 1)
            )) {
                expectThrows(IOException.class, () -> new PreparationOverlayDirectory(local, commit, releases::incrementAndGet, limits));
                assertEquals(0, releases.get());
                assertEquals(0, local.listAll().length);
            }
            expectThrows(
                IllegalArgumentException.class,
                () -> new PreparationOverlayDirectory(source, commit, releases::incrementAndGet, LIMITS)
            );
            try (var output = local.createOutput("unowned", IOContext.DEFAULT)) {
                output.writeByte((byte) 1);
            }
            expectThrows(IOException.class, () -> new PreparationOverlayDirectory(local, commit, releases::incrementAndGet, LIMITS));
            assertEquals(0, releases.get());
            assertArrayEquals(new byte[] { 1 }, read(local, "unowned"));
        }
    }

    private static Document document(String id) {
        final var doc = new Document();
        doc.add(new StringField("id", id, Field.Store.YES));
        return doc;
    }

    private static byte[] read(Directory directory, String name) throws IOException {
        try (var input = directory.openInput(name, IOContext.READONCE)) {
            final byte[] bytes = new byte[Math.toIntExact(input.length())];
            input.readBytes(bytes, 0, bytes.length);
            return bytes;
        }
    }
}
