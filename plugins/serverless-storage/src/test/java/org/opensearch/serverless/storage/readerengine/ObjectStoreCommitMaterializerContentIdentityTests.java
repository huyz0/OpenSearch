/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32C;

/**
 * The reader-side half of the compaction correctness bug, and the ordering and pruning fixes that
 * came with it. Every test here fails against the length-only "already present" check and the
 * unordered, purely additive write loop the materializer used to have.
 */
public class ObjectStoreCommitMaterializerContentIdentityTests extends OpenSearchTestCase {

    private static long crc(byte[] content) {
        CRC32C crc = new CRC32C();
        crc.update(content);
        return crc.getValue();
    }

    /** A bundle store that serves exactly the content registered under each {@code (bundle, offset)}. */
    private static final class ContentBundleStore implements BundleFileReader {
        private final Map<String, byte[]> contentByLocation = new HashMap<>();
        private final List<String> fetched = new ArrayList<>();
        private final Set<String> streamed = new HashSet<>();

        FileReference register(String bundleName, long offset, String name, byte[] content) {
            contentByLocation.put(bundleName + "@" + offset, content);
            return new FileReference(bundleName, offset, content.length, crc(content));
        }

        @Override
        public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
            byte[] content = contentByLocation.get(bundleName + "@" + entry.offset());
            if (content == null) {
                throw new IOException("no such file in " + bundleName + " at " + entry.offset());
            }
            fetched.add(entry.name());
            return content.clone();
        }

        @Override
        public InputStream openFile(String bundleName, BundleFileEntry entry) throws IOException {
            streamed.add(entry.name());
            byte[] content = contentByLocation.get(bundleName + "@" + entry.offset());
            if (content == null) {
                throw new IOException("no such file in " + bundleName + " at " + entry.offset());
            }
            fetched.add(entry.name());
            return new ByteArrayInputStream(content);
        }
    }

    private static CommitManifest manifest(long generation, String segmentsFileName, Map<String, FileReference> files) {
        return new CommitManifest(
            "AbCdEfGhIjKlMnOpQrStUv",
            0,
            1,
            generation,
            segmentsFileName,
            files,
            0,
            0,
            null,
            0,
            new PruningStats(0, null, null, Map.of()),
            System.currentTimeMillis()
        );
    }

    /** A cheap deterministic payload; a 16 MiB random array is needless work for a size-threshold test. */
    private static byte[] filled(int length, byte value) {
        byte[] bytes = new byte[length];
        Arrays.fill(bytes, value);
        // Vary the tail so two same-length payloads in different tests cannot hash alike by accident.
        for (int i = 0; i < Math.min(64, length); i++) {
            bytes[length - 1 - i] = (byte) (value + i);
        }
        return bytes;
    }

    private static byte[] readAll(Directory directory, String name) throws IOException {
        try (IndexInput in = directory.openInput(name, IOContext.DEFAULT)) {
            byte[] bytes = new byte[(int) in.length()];
            in.readBytes(bytes, 0, bytes.length);
            return bytes;
        }
    }

    /**
     * The core R1(b) regression, in the exact shape a real compaction produces: the second manifest
     * binds the SAME file name, at the SAME length, to different bytes in a different bundle. That
     * is not a hypothetical -- a real Lucene 10.5 experiment shows a compaction's {@code segments_1}
     * is byte-for-byte a different 155-byte file from the writer's own {@code segments_1}. Under the
     * old length-only check the fetch was skipped outright and the reader went on serving the old
     * commit while reporting itself caught up.
     */
    public void testAFileRebindingToDifferentBytesOfTheSameLengthIsRefetched() throws Exception {
        ContentBundleStore store = new ContentBundleStore();
        Directory directory = new ByteBuffersDirectory();
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(store);

        byte[] writerSegments = "AAAAAAAAAAAAAAAA".getBytes(StandardCharsets.UTF_8);
        byte[] compactedSegments = "BBBBBBBBBBBBBBBB".getBytes(StandardCharsets.UTF_8);
        assertEquals(
            "the whole point of this test is that the two differ only in content",
            writerSegments.length,
            compactedSegments.length
        );

        FileReference writerRef = store.register("bundle-writer", 0, "segments_1", writerSegments);
        materializer.materialize(manifest(1, "segments_1", Map.of("segments_1", writerRef)), directory);
        assertArrayEquals(writerSegments, readAll(directory, "segments_1"));

        FileReference compactedRef = store.register("bundle-compacted", 0, "segments_1", compactedSegments);
        materializer.materialize(manifest(2, "segments_1", Map.of("segments_1", compactedRef)), directory);

        assertArrayEquals(
            "a same-length rebind must be re-fetched; skipping it is what made compaction invisible to readers",
            compactedSegments,
            readAll(directory, "segments_1")
        );
        assertEquals(List.of("segments_1", "segments_1"), store.fetched);
    }

    /** The common, unchanged-segment case must stay free: an identical FileReference is never re-fetched or re-hashed. */
    public void testAnUnchangedFileIsNeverRefetched() throws Exception {
        ContentBundleStore store = new ContentBundleStore();
        Directory directory = new ByteBuffersDirectory();
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(store);

        FileReference segments = store.register("bundle-0", 0, "segments_1", "segments".getBytes(StandardCharsets.UTF_8));
        FileReference si = store.register("bundle-0", 100, "_0.si", "si-content".getBytes(StandardCharsets.UTF_8));
        materializer.materialize(manifest(1, "segments_1", Map.of("segments_1", segments, "_0.si", si)), directory);
        assertEquals(2, store.fetched.size());

        // A later generation still referencing _0.si unchanged, with a new segments file.
        FileReference segments2 = store.register("bundle-1", 0, "segments_2", "segments-two".getBytes(StandardCharsets.UTF_8));
        materializer.materialize(manifest(2, "segments_2", Map.of("segments_2", segments2, "_0.si", si)), directory);

        // Order-insensitive: the non-segments files are iterated in map order, which CommitManifest
        // deliberately does not guarantee. What matters is that _0.si was fetched exactly once.
        assertEquals("only the new segments file may be fetched the second time", 3, store.fetched.size());
        assertEquals(Set.of("segments_1", "_0.si", "segments_2"), new HashSet<>(store.fetched));
    }

    /**
     * A directory populated by a previous incarnation of the process has no sidecar record, so the
     * content check falls back to a real re-hash -- which must accept a genuinely-identical file and
     * reject a same-length impostor. This is the restart case, where trusting length alone would
     * carry a wrong commit's file across a node restart.
     */
    public void testAFileThisProcessDidNotWriteIsVerifiedByRehashing() throws Exception {
        ContentBundleStore store = new ContentBundleStore();
        Directory directory = new ByteBuffersDirectory();
        byte[] real = "the-real-content".getBytes(StandardCharsets.UTF_8);
        FileReference ref = store.register("bundle-0", 0, "segments_1", real);

        // Pre-populate the directory behind the materializer's back, as a previous process would have.
        try (IndexOutput out = directory.createOutput("segments_1", IOContext.DEFAULT)) {
            out.writeBytes(real, real.length);
        }
        new ObjectStoreCommitMaterializer(store).materialize(manifest(1, "segments_1", Map.of("segments_1", ref)), directory);
        assertEquals("a byte-identical pre-existing file must be accepted, not re-fetched", List.of(), store.fetched);

        // Same length, different bytes: must be rejected and re-fetched.
        Directory impostorDirectory = new ByteBuffersDirectory();
        byte[] impostor = "the-fake-content".getBytes(StandardCharsets.UTF_8);
        assertEquals(real.length, impostor.length);
        try (IndexOutput out = impostorDirectory.createOutput("segments_1", IOContext.DEFAULT)) {
            out.writeBytes(impostor, impostor.length);
        }
        ContentBundleStore impostorStore = new ContentBundleStore();
        FileReference impostorRef = impostorStore.register("bundle-0", 0, "segments_1", real);
        new ObjectStoreCommitMaterializer(impostorStore).materialize(
            manifest(1, "segments_1", Map.of("segments_1", impostorRef)),
            impostorDirectory
        );
        assertArrayEquals(real, readAll(impostorDirectory, "segments_1"));
        assertEquals(List.of("segments_1"), impostorStore.fetched);
    }

    /**
     * Commit-visibility ordering: the segments file must be the last thing written, because
     * {@code manifest.files()} iterates a map whose order derives from a {@code HashSet} and
     * therefore guaranteed nothing. A refresh landing inside a materialization -- and core's own
     * one-second scheduled refresh reaches this engine independently of the poll -- would otherwise
     * read a commit pointer whose segment data has not arrived.
     */
    public void testTheSegmentsFileIsWrittenAfterEveryOtherFile() throws Exception {
        ContentBundleStore store = new ContentBundleStore();
        Map<String, FileReference> files = new HashMap<>();
        files.put("segments_9", store.register("bundle-0", 0, "segments_9", "s".getBytes(StandardCharsets.UTF_8)));
        for (int i = 0; i < 12; i++) {
            String name = "_" + i + ".cfs";
            files.put(name, store.register("bundle-0", 100L + i, name, ("data-" + i).getBytes(StandardCharsets.UTF_8)));
        }
        new ObjectStoreCommitMaterializer(store).materialize(manifest(1, "segments_9", files), new ByteBuffersDirectory());

        assertEquals("the commit pointer must be fetched and written last", "segments_9", store.fetched.get(store.fetched.size() - 1));
        assertEquals(files.size(), store.fetched.size());
    }

    /**
     * A superseded commit pointer left in the directory is what let "the highest-generation
     * segments_N present" and "the commit the manifest names" be two different things -- the exact
     * mechanism by which a compaction's {@code segments_1} was materialized, recorded as current,
     * and then never actually served because {@code segments_3} was still on disk.
     */
    public void testSupersededCommitPointersAreRemovedSoTheManifestDecidesWhatIsOpen() throws Exception {
        ContentBundleStore store = new ContentBundleStore();
        Directory directory = new ByteBuffersDirectory();
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(store);

        for (int generation = 1; generation <= 3; generation++) {
            String name = "segments_" + generation;
            FileReference ref = store.register("bundle-" + generation, 0, name, ("commit-" + generation).getBytes(StandardCharsets.UTF_8));
            materializer.materialize(manifest(generation, name, Map.of(name, ref)), directory);
        }
        assertEquals("only the newest commit pointer may remain", Set.of("segments_3"), new HashSet<>(Arrays.asList(directory.listAll())));

        // Now a compaction: its merged commit is at a LOWER Lucene generation than what is on disk.
        byte[] compacted = "compacted-commit".getBytes(StandardCharsets.UTF_8);
        FileReference compactedRef = store.register("bundle-compacted", 0, "segments_1", compacted);
        materializer.materialize(manifest(4, "segments_1", Map.of("segments_1", compactedRef)), directory);

        assertEquals(
            "after applying the compacted manifest, its commit pointer must be the only one present",
            Set.of("segments_1"),
            new HashSet<>(Arrays.asList(directory.listAll()))
        );
        assertArrayEquals(compacted, readAll(directory, "segments_1"));
    }

    /**
     * Files above the streaming threshold must go through the streaming path, which is what removes
     * the 2 GiB {@code byte[]} ceiling that a default-configured compaction could walk a shard into
     * permanently. Asserted by observing which method the bundle store was asked for, since the
     * ceiling itself cannot be exercised in a unit test.
     */
    public void testLargeFilesAreStreamedRatherThanFetchedWholeIntoHeap() throws Exception {
        ContentBundleStore store = new ContentBundleStore();
        byte[] big = filled((int) ObjectStoreCommitMaterializer.STREAMING_THRESHOLD_BYTES + 4096, (byte) 0x5A);
        byte[] small = filled(1024, (byte) 0x11);
        Map<String, FileReference> files = new HashMap<>();
        files.put("segments_1", store.register("bundle-0", 0, "segments_1", small));
        files.put("_0.fdt", store.register("bundle-0", 1_000_000, "_0.fdt", big));

        Directory directory = new ByteBuffersDirectory();
        new ObjectStoreCommitMaterializer(store).materialize(manifest(1, "segments_1", files), directory);

        assertEquals("only the large file may take the streaming path", Set.of("_0.fdt"), store.streamed);
        assertArrayEquals(big, readAll(directory, "_0.fdt"));
        assertArrayEquals(small, readAll(directory, "segments_1"));
    }

    /** A streamed file whose bytes do not hash to what the manifest recorded must fail loudly and leave nothing behind. */
    public void testAStreamedFileThatFailsItsChecksumIsRejectedAndNotLeftOnDisk() throws Exception {
        ContentBundleStore store = new ContentBundleStore();
        byte[] big = filled((int) ObjectStoreCommitMaterializer.STREAMING_THRESHOLD_BYTES + 4096, (byte) 0x33);
        FileReference honest = store.register("bundle-0", 0, "_0.fdt", big);
        FileReference lying = new FileReference(honest.bundleName(), honest.offset(), honest.length(), honest.checksum() ^ 0xFFFFL);
        FileReference segments = store.register("bundle-0", 9_000_000, "segments_1", "s".getBytes(StandardCharsets.UTF_8));

        Directory directory = new ByteBuffersDirectory();
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(store);
        IOException mismatch = expectThrows(
            IOException.class,
            () -> materializer.materialize(manifest(1, "segments_1", Map.of("_0.fdt", lying, "segments_1", segments)), directory)
        );
        assertTrue(mismatch.getMessage(), mismatch.getMessage().contains("checksum mismatch"));
        assertFalse(
            "a failed streaming write must not leave a file behind claiming to be this manifest's",
            Arrays.asList(directory.listAll()).contains("_0.fdt")
        );
    }

    /**
     * A reader shard has no {@code IndexWriter} and so no {@code IndexFileDeleter}: without an
     * explicit prune, every superseded segment file the shard ever published accumulates on local
     * disk, bounded by nothing (neither the per-shard nor the node-wide cache budget covers the
     * shard's Lucene store directory).
     */
    public void testUnreferencedFilesArePrunedWhileTheRetainedGenerationsSurvive() throws Exception {
        ContentBundleStore store = new ContentBundleStore();
        Directory directory = new ByteBuffersDirectory();
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(store);

        Map<String, FileReference> first = new HashMap<>();
        first.put("segments_1", store.register("bundle-0", 0, "segments_1", "one".getBytes(StandardCharsets.UTF_8)));
        first.put("_0.cfs", store.register("bundle-0", 10, "_0.cfs", "zero".getBytes(StandardCharsets.UTF_8)));
        CommitManifest generationOne = manifest(1, "segments_1", first);
        materializer.materialize(generationOne, directory);

        Map<String, FileReference> second = new HashMap<>();
        second.put("segments_2", store.register("bundle-1", 0, "segments_2", "two".getBytes(StandardCharsets.UTF_8)));
        second.put("_1.cfs", store.register("bundle-1", 10, "_1.cfs", "onefile".getBytes(StandardCharsets.UTF_8)));
        CommitManifest generationTwo = manifest(2, "segments_2", second);
        materializer.materialize(generationTwo, directory);

        // _0.cfs is still on disk even though generation 2 does not reference it -- that is the leak.
        assertTrue(Arrays.asList(directory.listAll()).contains("_0.cfs"));

        // Retaining both generations keeps everything; retaining only the current one drops _0.cfs.
        assertEquals(0, materializer.pruneUnreferencedFiles(directory, List.of(generationTwo, generationOne)));
        assertTrue(Arrays.asList(directory.listAll()).contains("_0.cfs"));

        assertEquals(1, materializer.pruneUnreferencedFiles(directory, List.of(generationTwo)));
        assertEquals(Set.of("segments_2", "_1.cfs"), new HashSet<>(Arrays.asList(directory.listAll())));
    }
}
