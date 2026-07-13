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
        Map<String, FileReference> files = new LinkedHashMap<>();
        files.put("segments_3", new FileReference("bundle-0", 0, 10, 1L));
        files.put("_0.si", new FileReference("bundle-0", 10, 10, 2L));
        files.put("_0.cfs", new FileReference("bundle-0", 20, 10, 3L));
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
