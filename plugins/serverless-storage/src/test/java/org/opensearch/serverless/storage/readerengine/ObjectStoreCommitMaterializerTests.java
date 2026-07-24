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
import org.apache.lucene.store.IndexInput;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kill-mid-refresh (rfc-serverless-opensearch.md &sect;17): {@link ObjectStoreCommitMaterializer#materialize}
 * is the reader engine's own multi-file analogue of {@code ObjectStoreCommitPublisher#publishCommit}'s
 * two-step write and {@code WalChunkService#writeChunk}'s claim-then-write -- a kill partway through
 * fetching a manifest's files leaves some already written to the target {@link Directory} and others
 * not, and {@link ObjectStoreReaderEngine#pollForNewerManifest} only ever advances {@code
 * currentManifestGeneration} after a full, successful {@code materialize} call returns, so a reader
 * killed mid-refresh must keep serving its last-good generation and a retry must converge cleanly.
 */
public class ObjectStoreCommitMaterializerTests extends OpenSearchTestCase {

    private CommitManifest manifestWithThreeFiles() {
        // Lengths must actually match contentFor()'s real output now that materialize() verifies
        // an already-present file's on-disk length against this field -- these were previously
        // fabricated placeholders since nothing checked them before that fix existed.
        Map<String, FileReference> files = new LinkedHashMap<>();
        files.put("segments_3", new FileReference("bundle-0", 0, contentFor("segments_3").length, 1L));
        files.put("_0.si", new FileReference("bundle-0", 10, contentFor("_0.si").length, 2L));
        files.put("_0.cfs", new FileReference("bundle-0", 20, contentFor("_0.cfs").length, 3L));
        return new CommitManifest(
            "index-uuid-abc",
            0,
            1,
            5,
            "segments_3",
            files,
            0,
            0,
            null,
            0,
            new PruningStats(0, null, null, Map.of()),
            System.currentTimeMillis()
        );
    }

    private static byte[] contentFor(String fileName) {
        return ("content-of-" + fileName).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] readAll(Directory directory, String fileName) throws IOException {
        try (IndexInput in = directory.openInput(fileName, org.apache.lucene.store.IOContext.DEFAULT)) {
            byte[] bytes = new byte[(int) in.length()];
            in.readBytes(bytes, 0, bytes.length);
            return bytes;
        }
    }

    public void testMaterializeSucceedsWhenNothingFails() throws Exception {
        CommitManifest manifest = manifestWithThreeFiles();
        Directory directory = new ByteBuffersDirectory();
        new ObjectStoreCommitMaterializer(new FakeBundleFileReader(-1)).materialize(manifest, directory);

        for (String fileName : manifest.files().keySet()) {
            assertArrayEquals(contentFor(fileName), readAll(directory, fileName));
        }
    }

    /**
     * The kill-mid-refresh case itself: a fault on the second of three files leaves the first
     * already written and the remaining two entirely absent -- proving a killed materialize call
     * never leaves the target directory in a state where some files silently never arrive, and
     * never partially writes the file it was killed on (the fault fires before any bytes for that
     * file are written, matching a real "process died before this file's fetch completed" kill).
     */
    public void testAKilledMaterializeLeavesOnlyThePriorFilesWrittenAndARetryCompletesTheRest() throws Exception {
        CommitManifest manifest = manifestWithThreeFiles();
        Directory directory = new ByteBuffersDirectory();
        // Fails on the 2nd readFile call (0-indexed: call #1) -- iteration order over
        // manifest.files() is not insertion order (CommitManifest wraps it in Map.copyOf, which
        // makes no ordering guarantee), so which specific file that lands on varies; the assertions
        // below deliberately don't depend on which one it was, only on the aggregate shape.
        FakeBundleFileReader faulty = new FakeBundleFileReader(1);
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(faulty);

        expectThrows(IOException.class, () -> materializer.materialize(manifest, directory));

        java.util.Set<String> presentAfterKill = new java.util.HashSet<>(Arrays.asList(directory.listAll()));
        assertEquals(
            "exactly the one file fetched before the kill must be present -- never a partial write of "
                + "the file the kill happened on, and never a file past the kill point",
            1,
            presentAfterKill.size()
        );
        assertTrue(
            "the file materialize was killed on must never be partially written",
            manifest.files().keySet().containsAll(presentAfterKill)
        );
        assertEquals("only the file that succeeded and the one that was killed should have been attempted", 2, faulty.callCount());

        // Retry -- matching ObjectStoreReaderEngine#pollForNewerManifest's own "log and retry next
        // tick" behavior on any materialize failure. currentManifestGeneration was never advanced
        // (this test's own scope stops at the materializer, but that ordering is what makes a
        // second call against the same manifest and directory the exact retry shape a real reader
        // takes).
        faulty.stopFailing();
        materializer.materialize(manifest, directory);

        for (String fileName : manifest.files().keySet()) {
            assertArrayEquals(contentFor(fileName), readAll(directory, fileName));
        }
        // The one file already present from the killed attempt must not have been re-fetched --
        // materialize's own documented skip-if-present behavior: 2 calls in the killed attempt (one
        // succeeded, one failed) + 2 more on retry (the two files that were never written) = 4, not
        // 5 (which is what a naive re-fetch-everything retry would produce).
        assertEquals("the already-written file from the killed attempt must be skipped, not re-fetched, on retry", 4, faulty.callCount());
    }

    /**
     * Simulates the specific leftover an interrupted materialize call can produce: a file that was
     * fully {@code close()}d by a prior call but never reached that call's own batch {@link
     * Directory#sync}, because that call was killed on a LATER file before it got there. On a real
     * filesystem across a crash, such a file can end up truncated/incomplete despite {@code
     * listAll()} reporting it present. This directly writes a truncated stand-in for one file (never
     * going through the materializer at all) to model exactly that leftover, then verifies a normal
     * {@code materialize} call detects the length mismatch and re-fetches it rather than trusting it.
     */
    public void testATruncatedLeftoverFileFromAnInterruptedPriorWriteIsRefetchedNotTrusted() throws Exception {
        CommitManifest manifest = manifestWithThreeFiles();
        Directory directory = new ByteBuffersDirectory();

        String truncatedFile = manifest.files().keySet().iterator().next();
        byte[] fullContent = contentFor(truncatedFile);
        byte[] truncatedContent = Arrays.copyOf(fullContent, fullContent.length - 3);
        try (org.apache.lucene.store.IndexOutput out = directory.createOutput(truncatedFile, org.apache.lucene.store.IOContext.DEFAULT)) {
            out.writeBytes(truncatedContent, truncatedContent.length);
        }
        assertEquals(
            "test setup sanity check: the stand-in file must actually be shorter than the manifest expects",
            truncatedContent.length,
            directory.fileLength(truncatedFile)
        );

        FakeBundleFileReader reader = new FakeBundleFileReader(-1);
        new ObjectStoreCommitMaterializer(reader).materialize(manifest, directory);

        for (String fileName : manifest.files().keySet()) {
            assertArrayEquals(
                "every file, including the truncated leftover, must end up with its full correct content",
                contentFor(fileName),
                readAll(directory, fileName)
            );
        }
        assertEquals(
            "the truncated leftover must have been re-fetched (all 3 files fetched), not silently trusted as already present",
            3,
            reader.callCount()
        );
    }

    /** Returns fixed per-file content, failing the {@code failOnCallIndex}-th (0-indexed) {@code readFile} call exactly once. */
    private static final class FakeBundleFileReader implements BundleFileReader {

        private final int failOnCallIndex;
        private final AtomicInteger calls = new AtomicInteger();
        private boolean failingEnabled;

        FakeBundleFileReader(int failOnCallIndex) {
            this.failOnCallIndex = failOnCallIndex;
            this.failingEnabled = failOnCallIndex >= 0;
        }

        void stopFailing() {
            failingEnabled = false;
        }

        int callCount() {
            return calls.get();
        }

        @Override
        public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
            int callIndex = calls.getAndIncrement();
            if (failingEnabled && callIndex == failOnCallIndex) {
                throw new IOException("injected kill-mid-refresh failure fetching " + entry.name());
            }
            return contentFor(entry.name());
        }
    }
}
