/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.serverless.store.LocalSegmentTier;
import org.opensearch.serverless.store.TieredSegmentDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Freshly flushed segment files held in the node's memory tier: indexing, searching, merging and committing read them as
 * if they were on disk; a merge writes to disk; a file that does not fit moves to disk while being written, and the oldest
 * held files move there when the budget runs low; deleting a held file never touches the disk; and every byte written to
 * disk is counted by what wrote it.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*") // compares against the real files on disk
public class TieredSegmentDirectoryTests extends OpenSearchTestCase {

    private static final long MB = 1024 * 1024;

    private static List<String> onDisk(Path path) throws Exception {
        final List<String> names = new ArrayList<>();
        try (var listing = Files.list(path)) {
            listing.forEach(p -> names.add(p.getFileName().toString()));
        }
        names.remove("write.lock");
        return names;
    }

    private static Document doc(int i) {
        final Document doc = new Document();
        doc.add(new StringField("id", "d" + i, Field.Store.YES));
        doc.add(new TextField("msg", "order " + i + " shipped to customer " + (i % 97), Field.Store.YES));
        return doc;
    }

    private static IndexWriterConfig config() {
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setUseCompoundFile(true);
        return config;
    }

    private static int count(Directory directory) throws Exception {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            return new IndexSearcher(reader).count(new MatchAllDocsQuery());
        }
    }

    private static void checkIndex(Directory directory) throws Exception {
        try (CheckIndex check = new CheckIndex(directory)) {
            check.setLevel(CheckIndex.Level.MIN_LEVEL_FOR_INTEGRITY_CHECKS);
            assertTrue(check.checkIndex().clean);
        }
    }

    public void testFlushedSegmentsStayInMemoryAndMergesGoToDisk() throws Exception {
        final Path path = createTempDir();
        final LocalSegmentTier tier = new LocalSegmentTier(true, 64 * MB, 16 * MB, null);
        try (TieredSegmentDirectory directory = new TieredSegmentDirectory(FSDirectory.open(path), tier)) {
            try (IndexWriter writer = new IndexWriter(directory, config())) {
                for (int round = 0; round < 3; round++) {
                    for (int i = 0; i < 100; i++) {
                        writer.addDocument(doc(round * 100 + i));
                    }
                    writer.commit();
                }
                assertEquals("flushes and commit points never reached the disk", List.of(), onDisk(path));
                assertEquals(300, count(directory));
                assertTrue(tier.stats().memoryFlush() > 0);
                assertEquals(0, tier.stats().diskFlush());
                assertTrue(tier.stats().deletedInMemory() > 0); // a compound file's parts, and superseded commit points

                writer.forceMerge(1);
                writer.commit();
                assertEquals(300, count(directory));
                assertTrue("a merge writes to disk", tier.stats().diskMerge() > 0);
                assertTrue(onDisk(path).stream().anyMatch(name -> name.endsWith(".cfs") || name.endsWith(".si")));
            }
            checkIndex(directory);
            assertTrue(tier.stats().memoryOpens() > 0);
        }
        assertEquals("closing the directory gives its memory back", 0, tier.stats().heldBytes());
        assertEquals(0, tier.stats().heldFiles());
    }

    public void testAFileTooBigForItsShareMovesToDiskWhileBeingWritten() throws Exception {
        final Path path = createTempDir();
        final LocalSegmentTier tier = new LocalSegmentTier(true, 4 * MB, 256 * 1024, null);
        try (TieredSegmentDirectory directory = new TieredSegmentDirectory(FSDirectory.open(path), tier)) {
            final byte[] bytes = randomByteArrayOfLength(700 * 1024);
            final long checksum;
            try (IndexOutput output = directory.createOutput("_0.big", IOContext.DEFAULT)) {
                output.writeBytes(bytes, 0, 1000);
                output.writeBytes(bytes, 1000, bytes.length - 1000);
                checksum = output.getChecksum();
            }
            assertEquals(List.of("_0.big"), onDisk(path));
            assertTrue(tier.stats().spilledWhileWriting() > 0);
            assertEquals(bytes.length, tier.stats().diskOther());
            assertEquals(0, tier.stats().heldBytes());
            final java.util.zip.CRC32 crc = new java.util.zip.CRC32();
            crc.update(bytes);
            assertEquals("the checksum covers what was written before the move too", crc.getValue(), checksum);
            try (IndexInput input = directory.openInput("_0.big", IOContext.DEFAULT)) {
                final byte[] read = new byte[bytes.length];
                input.readBytes(read, 0, read.length);
                assertArrayEquals(bytes, read);
            }
        }
    }

    public void testTheOldestFilesMoveToDiskWhenTheBudgetRunsLowButCommitPointsStay() throws Exception {
        final Path path = createTempDir();
        final LocalSegmentTier tier = new LocalSegmentTier(true, MB, MB, null);
        try (TieredSegmentDirectory directory = new TieredSegmentDirectory(FSDirectory.open(path), tier)) {
            final byte[] chunk = randomByteArrayOfLength(200 * 1024);
            for (int i = 0; i < 8; i++) {
                try (IndexOutput output = directory.createOutput(String.format(Locale.ROOT, "_%d.dat", i), IOContext.DEFAULT)) {
                    output.writeBytes(chunk, 0, chunk.length);
                }
            }
            try (IndexOutput output = directory.createOutput("segments_1", IOContext.DEFAULT)) {
                output.writeBytes(chunk, 0, 100);
            }
            final LocalSegmentTier.Stats stats = tier.stats();
            assertTrue("held stays within the budget: " + stats, stats.heldBytes() <= MB);
            assertTrue("some files were moved to disk: " + stats, stats.evicted() > 0);
            assertTrue(onDisk(path).contains("_0.dat"));
            assertFalse("the commit point is never moved on its own", onDisk(path).contains("segments_1"));
            assertTrue(directory.heldInMemory().contains("segments_1"));
            for (int i = 0; i < 8; i++) {
                assertEquals(chunk.length, directory.fileLength(String.format(Locale.ROOT, "_%d.dat", i)));
                try (IndexInput input = directory.openInput(String.format(Locale.ROOT, "_%d.dat", i), IOContext.DEFAULT)) {
                    final byte[] read = new byte[chunk.length];
                    input.readBytes(read, 0, read.length);
                    assertArrayEquals(chunk, read);
                }
            }
            assertEquals(stats.evicted(), stats.diskOther() + stats.diskFlush());
        }
    }

    public void testDeletingAndRenamingAHeldFileNeverTouchesTheDisk() throws Exception {
        final Path path = createTempDir();
        final LocalSegmentTier tier = new LocalSegmentTier(true, 8 * MB, MB, null);
        try (TieredSegmentDirectory directory = new TieredSegmentDirectory(FSDirectory.open(path), tier)) {
            try (IndexOutput output = directory.createOutput("pending_segments_2", IOContext.DEFAULT)) {
                output.writeInt(42);
            }
            final IndexInput openBefore = directory.openInput("pending_segments_2", IOContext.DEFAULT);
            directory.rename("pending_segments_2", "segments_2");
            assertArrayEquals(new String[] { "segments_2" }, directory.listAll());
            expectThrows(java.nio.file.FileAlreadyExistsException.class, () -> directory.createOutput("segments_2", IOContext.DEFAULT));
            directory.deleteFile("segments_2");
            assertEquals(0, directory.listAll().length);
            assertEquals("a reader open before the delete still reads it", 42, openBefore.readInt());
            openBefore.close();
            expectThrows(java.nio.file.NoSuchFileException.class, () -> directory.openInput("segments_2", IOContext.DEFAULT));
            assertEquals(List.of(), onDisk(path));
            assertEquals(4, tier.stats().deletedInMemory());
            assertEquals(0, tier.stats().heldBytes());
        }
    }

    public void testTemporaryFilesAreHeldInMemoryUnderNamesOfTheirOwn() throws Exception {
        final Path path = createTempDir();
        final LocalSegmentTier tier = new LocalSegmentTier(true, 8 * MB, MB, null);
        try (TieredSegmentDirectory directory = new TieredSegmentDirectory(FSDirectory.open(path), tier)) {
            final IndexOutput first = directory.createTempOutput("_0", "doc_ids", IOContext.DEFAULT);
            final IndexOutput second = directory.createTempOutput("_0", "doc_ids", IOContext.DEFAULT);
            assertNotEquals(first.getName(), second.getName());
            first.writeLong(7);
            first.close();
            second.close();
            try (IndexInput input = directory.openInput(first.getName(), IOContext.READONCE)) {
                assertEquals(7, input.readLong());
            }
            directory.deleteFile(first.getName());
            directory.deleteFile(second.getName());
            assertEquals(List.of(), onDisk(path));
            assertEquals(0, tier.stats().diskTotal());
        }
    }

    public void testWithTheTierOffEverythingIsOnDiskAndCountedByWhatWroteIt() throws Exception {
        final Path path = createTempDir();
        final LocalSegmentTier tier = new LocalSegmentTier(false, 64 * MB, 16 * MB, null);
        try (TieredSegmentDirectory directory = new TieredSegmentDirectory(FSDirectory.open(path), tier)) {
            try (IndexWriter writer = new IndexWriter(directory, config())) {
                for (int i = 0; i < 200; i++) {
                    writer.addDocument(doc(i));
                    if (i % 50 == 49) {
                        writer.commit();
                    }
                }
                writer.forceMerge(1);
                writer.commit();
            }
            assertEquals(200, count(directory));
            assertEquals(0, directory.heldInMemory().size());
        }
        final LocalSegmentTier.Stats stats = tier.stats();
        assertTrue(stats.toString(), stats.diskFlush() > 0);
        assertTrue(stats.toString(), stats.diskMerge() > 0);
        assertEquals(0, stats.memoryFlush());
        assertEquals(0, stats.peakBytes());
    }

    public void testManyShardsShareOneSmallBudgetUnderConcurrentIndexingAndRefresh() throws Exception {
        final LocalSegmentTier tier = new LocalSegmentTier(true, 2 * MB, 512 * 1024, null);
        final int shards = 4;
        final List<TieredSegmentDirectory> directories = new ArrayList<>();
        final List<IndexWriter> writers = new ArrayList<>();
        for (int s = 0; s < shards; s++) {
            final TieredSegmentDirectory directory = new TieredSegmentDirectory(FSDirectory.open(createTempDir()), tier);
            directories.add(directory);
            final IndexWriterConfig config = config();
            config.setRAMBufferSizeMB(0.5);
            writers.add(new IndexWriter(directory, config));
        }
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final int perShard = 3000;
        final List<Thread> threads = new ArrayList<>();
        for (int s = 0; s < shards; s++) {
            final IndexWriter writer = writers.get(s);
            final Thread thread = new Thread(() -> {
                try {
                    DirectoryReader reader = DirectoryReader.open(writer);
                    for (int i = 0; i < perShard; i++) {
                        writer.updateDocument(new Term("id", "d" + i), doc(i));
                        if (i % 97 == 0) {
                            final DirectoryReader newer = DirectoryReader.openIfChanged(reader, writer);
                            if (newer != null) {
                                reader.close();
                                reader = newer;
                            }
                        }
                        if (i % 701 == 0) {
                            writer.commit();
                        }
                    }
                    reader.close();
                    writer.commit();
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                }
            });
            threads.add(thread);
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        if (failure.get() != null) {
            throw new AssertionError(failure.get());
        }
        final LocalSegmentTier.Stats stats = tier.stats();
        assertTrue("the budget held: " + stats, stats.peakBytes() <= 2 * MB);
        for (int s = 0; s < shards; s++) {
            writers.get(s).close();
            assertEquals(perShard, count(directories.get(s)));
            checkIndex(directories.get(s));
            directories.get(s).close();
        }
        assertEquals(0, tier.stats().heldBytes());
        assertEquals(0, tier.stats().heldFiles());
    }
}
