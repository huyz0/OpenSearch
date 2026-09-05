/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Tests for report findings B1 (a publication re-read and re-uploaded the <em>entire</em> commit's
 * file set on every flush, fully materialised in heap) and P1 (a generation collision with a
 * concurrent compaction escaped as an untyped {@code IOException} that killed the primary instead of
 * being retried).
 */
public class ObjectStoreCommitPublisherDeltaBundlingTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "delta-idx";
    private static final int SHARD_ID = 0;

    private BlobContainer blobContainer;
    private ObjectStoreCommitPublisher publisher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
    }

    private CommitManifest publish(Directory directory, SegmentInfos segmentInfos, long generation, CommitManifest deltaBase)
        throws IOException {
        return publisher.publishCommit(
            directory,
            segmentInfos,
            INDEX_UUID,
            SHARD_ID,
            1L,
            generation,
            generation,
            generation,
            new WalPosition("epoch-0", generation),
            0,
            PruningStats.empty(),
            "",
            true,
            deltaBase
        );
    }

    private static void addDocument(IndexWriter writer, String id) throws IOException {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.YES));
        writer.addDocument(doc);
    }

    private long totalBundleBytes() throws IOException {
        long total = 0;
        for (var entry : blobContainer.listBlobsByPrefix(BlobContainerBundleStore.NAME_PREFIX).entrySet()) {
            total += entry.getValue().length();
        }
        return total;
    }

    /**
     * <b>B1.</b> A second publication of the same shard, one document later, must upload only the
     * files that commit newly introduced -- carrying every unchanged file's {@link FileReference}
     * forward, pointing into the bundle it already lives in. Without this, publishing generation 2
     * re-uploaded generation 1's whole file set, so a shard's upload volume per publication was its
     * entire committed size rather than its delta.
     */
    public void testASecondPublicationOnlyBundlesTheFilesTheCommitActuallyAdded() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            // No merges, so the second commit genuinely only ADDS a segment rather than rewriting
            // the first one -- otherwise this would be measuring merge behaviour, not delta bundling.
            IndexWriterConfig config = new IndexWriterConfig().setUseCompoundFile(false).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                // A substantial first commit and a one-document second one, so "uploaded only the
                // delta" is measurable rather than lost in fixed per-file overhead.
                for (int i = 0; i < 500; i++) {
                    addDocument(writer, "first-" + i);
                }
                writer.commit();
                SegmentInfos first = SegmentInfos.readLatestCommit(directory);
                CommitManifest firstManifest = publish(directory, first, 1L, null);
                long bytesAfterFirst = totalBundleBytes();
                Set<String> firstFiles = Set.copyOf(first.files(true));

                addDocument(writer, "2");
                writer.commit();
                SegmentInfos second = SegmentInfos.readLatestCommit(directory);
                CommitManifest secondManifest = publish(directory, second, 2L, firstManifest);

                // Every file of the second commit is described, exactly as before.
                for (String fileName : second.files(true)) {
                    assertTrue("manifest must reference " + fileName, secondManifest.files().containsKey(fileName));
                }

                // The files the first commit already had, and that the second commit still uses, must
                // point at the FIRST bundle -- they were not re-uploaded.
                String firstBundle = firstManifest.files().get(firstManifest.segmentsFileName()).bundleName();
                boolean sawCarriedForward = false;
                for (Map.Entry<String, FileReference> entry : secondManifest.files().entrySet()) {
                    if (firstFiles.contains(entry.getKey())) {
                        assertEquals(
                            "an unchanged file must keep pointing at the bundle it already lives in, not be re-uploaded: " + entry.getKey(),
                            firstBundle,
                            entry.getValue().bundleName()
                        );
                        assertEquals(firstManifest.files().get(entry.getKey()), entry.getValue());
                        sawCarriedForward = true;
                    }
                }
                assertTrue("this test is only meaningful if the second commit reuses at least one file", sawCarriedForward);

                long deltaBytes = totalBundleBytes() - bytesAfterFirst;
                assertTrue(
                    "the second publication must upload far less than a full re-upload of the whole commit ("
                        + deltaBytes
                        + " vs "
                        + bytesAfterFirst
                        + " bytes); before delta bundling these were roughly equal",
                    deltaBytes < bytesAfterFirst / 2
                );
            }
        }
    }

    /**
     * <b>B1, correctness half.</b> Delta bundling must not change what a manifest describes: every
     * file's recorded length and checksum must match what a full-bundle publication of the very same
     * commit records. Otherwise "smaller upload" would have been bought with a manifest that no
     * longer describes the commit.
     */
    public void testADeltaBundledManifestDescribesExactlyTheSameFilesAsAFullBundledOne() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig().setUseCompoundFile(false).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                addDocument(writer, "1");
                writer.commit();
                SegmentInfos first = SegmentInfos.readLatestCommit(directory);
                CommitManifest firstManifest = publish(directory, first, 1L, null);

                addDocument(writer, "2");
                writer.commit();
                SegmentInfos second = SegmentInfos.readLatestCommit(directory);

                CommitManifest deltaManifest = publish(directory, second, 2L, firstManifest);
                CommitManifest fullManifest = publish(directory, second, 3L, null);

                assertEquals(fullManifest.files().keySet(), deltaManifest.files().keySet());
                for (String name : fullManifest.files().keySet()) {
                    FileReference full = fullManifest.files().get(name);
                    FileReference delta = deltaManifest.files().get(name);
                    assertEquals("length must agree for " + name, full.length(), delta.length());
                    assertEquals("checksum must agree for " + name, full.checksum(), delta.checksum());
                }
            }
        }
    }

    /**
     * <b>P1.</b> A foreign actor (in production: a compaction publish, which targets the identical
     * {@code manifest-<term>-<gen>} name) occupying this generation must surface as a typed, retriable
     * {@link ManifestGenerationCollisionException}, not as an anonymous {@code IOException} that the
     * head publisher's retry loop cannot tell apart from a real store failure -- which is exactly how
     * a benign race used to reach {@code failEngine} and kill the primary.
     */
    public void testAForeignManifestAtTheTargetGenerationRaisesATypedRetriableCollision() throws Exception {
        try (Directory foreignDirectory = new ByteBuffersDirectory(); Directory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig().setUseCompoundFile(false);
            try (IndexWriter foreignWriter = new IndexWriter(foreignDirectory, config)) {
                addDocument(foreignWriter, "foreign");
                foreignWriter.commit();
            }
            SegmentInfos foreign = SegmentInfos.readLatestCommit(foreignDirectory);
            publish(foreignDirectory, foreign, 1L, null);

            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setUseCompoundFile(false))) {
                addDocument(writer, "mine-1");
                addDocument(writer, "mine-2");
                addDocument(writer, "mine-3");
                writer.commit();
            }
            SegmentInfos mine = SegmentInfos.readLatestCommit(directory);

            ManifestGenerationCollisionException collision = expectThrows(
                ManifestGenerationCollisionException.class,
                () -> publish(directory, mine, 1L, null)
            );
            assertEquals(1L, collision.primaryTerm());
            assertEquals(1L, collision.generation());
        }
    }

    /** A genuine idempotent retry -- the same commit, the same generation -- still short-circuits rather than colliding. */
    public void testRepublishingTheExactSameCommitAtTheSameGenerationIsStillIdempotent() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig().setUseCompoundFile(false);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                addDocument(writer, "1");
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);

            CommitManifest once = publish(directory, segmentInfos, 1L, null);
            CommitManifest twice = publish(directory, segmentInfos, 1L, null);
            assertEquals(once.files(), twice.files());
            assertEquals(once.generation(), twice.generation());
        }
    }
}
