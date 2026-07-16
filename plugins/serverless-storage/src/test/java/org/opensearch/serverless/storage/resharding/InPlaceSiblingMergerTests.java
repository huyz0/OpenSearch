/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.resharding.InPlaceSiblingMerger.MergeChild;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for {@link InPlaceSiblingMerger} -- the data half of an in-place shard merge. Mirrors
 * {@link ShardShrinkerTests}' fixture style (build a real Lucene commit, publish it to an {@link
 * FsBlobContainer}, then exercise the merge and read the result back through a materializer), but
 * targets the case the whole in-place-merge design revolves around: two children that have taken
 * their own disjoint post-split writes and per-child deletes/updates against a shared base segment.
 */
public class InPlaceSiblingMergerTests extends OpenSearchTestCase {

    private static final String ID_TERM_FIELD = "_id_term";
    private static final String VERSION_FIELD = "v";

    /** Splits the hash space at zero, matching how a real bisecting split partitions it. */
    private static final int RANGE_A_START = Integer.MIN_VALUE;
    private static final int RANGE_A_END = -1;
    private static final int RANGE_B_START = 0;
    private static final int RANGE_B_END = Integer.MAX_VALUE;

    private static boolean inRangeA(String id) {
        return Murmur3HashFunction.hash(id) < 0;
    }

    /** Draws {@code count} ids that hash into range A (if {@code wantA}) or range B, deterministically. */
    private List<String> idsIn(boolean wantA, String prefix, int count) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; ids.size() < count; i++) {
            String id = prefix + "-" + i;
            if (inRangeA(id) == wantA) {
                ids.add(id);
            }
        }
        return ids;
    }

    private static void addDoc(IndexWriter writer, String id, String version) throws Exception {
        writer.addDocument(newDoc(id, version));
    }

    private static Document newDoc(String id, String version) {
        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));
        doc.add(new StringField(ID_TERM_FIELD, id, Field.Store.NO));
        doc.add(new StoredField(VERSION_FIELD, version));
        return doc;
    }

    /**
     * Builds one child: adds every base doc (simulating clone-by-reference -- both children physically
     * hold the full pre-split set), its own post-split docs, and soft-updates the given base ids to a
     * new version (recording the superseded copy as soft-deleted, exactly as a real post-split update
     * does). Publishes the commit to its own container and returns the {@link MergeChild}.
     */
    private MergeChild buildChild(
        int childShardId,
        int rangeStart,
        int rangeEnd,
        List<String> baseIds,
        List<String> postSplitIds,
        Map<String, String> updates
    ) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(container);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(container);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
        String indexUuid = "merge-child-" + childShardId;

        CommitManifest manifest;
        try (Directory dir = new ByteBuffersDirectory()) {
            IndexWriterConfig iwc = new IndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (String id : baseIds) {
                    addDoc(writer, id, "base");
                }
                for (String id : postSplitIds) {
                    addDoc(writer, id, "post");
                }
                for (Map.Entry<String, String> update : updates.entrySet()) {
                    writer.softUpdateDocument(
                        new Term(ID_TERM_FIELD, update.getKey()),
                        newDoc(update.getKey(), update.getValue()),
                        new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1)
                    );
                }
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(dir);
            manifest = publisher.publishCommit(
                dir,
                segmentInfos,
                indexUuid,
                childShardId,
                1,
                1,
                10,
                10,
                new WalPosition("epoch-0", 0),
                1,
                PruningStats.empty()
            );
        }
        return new MergeChild(manifest, bundleStore, new ShardRange(childShardId, rangeStart, rangeEnd));
    }

    private Map<String, String> readMerged(Directory targetDirectory) throws Exception {
        Map<String, String> idToVersion = new HashMap<>();
        try (DirectoryReader reader = DirectoryReader.open(targetDirectory)) {
            for (org.apache.lucene.index.LeafReaderContext leafContext : reader.leaves()) {
                org.apache.lucene.index.LeafReader leaf = leafContext.reader();
                org.apache.lucene.util.Bits liveDocs = leaf.getLiveDocs();
                for (int i = 0; i < leaf.maxDoc(); i++) {
                    if (liveDocs != null && liveDocs.get(i) == false) {
                        continue;
                    }
                    Document doc = leaf.storedFields().document(i);
                    String id = Uid.decodeId(doc.getField(IdFieldMapper.NAME).binaryValue().bytes);
                    idToVersion.put(id, doc.getField(VERSION_FIELD).stringValue());
                }
            }
        }
        return idToVersion;
    }

    public void testMergeRejectsAnEmptyChildList() {
        expectThrows(java.io.IOException.class, () -> InPlaceSiblingMerger.merge(List.of(), new ByteBuffersDirectory()));
    }

    public void testMergeFoldsBothChildrensAuthoritativeSlicesWithoutDoubleCounting() throws Exception {
        // Shared pre-split base: a spread of ids across both ranges, every one physically present in
        // BOTH children (clone-by-reference).
        List<String> baseA = idsIn(true, "base", 4);   // hash into range A
        List<String> baseB = idsIn(false, "base", 4);  // hash into range B
        List<String> baseIds = new ArrayList<>();
        baseIds.addAll(baseA);
        baseIds.addAll(baseB);

        // Disjoint post-split writes: each lands only in the child whose range owns its hash.
        List<String> postA = idsIn(true, "postA", 3);
        List<String> postB = idsIn(false, "postB", 3);

        // Child A owns range A. It updates one of its own base docs (owned, in-range) AND updates a
        // base doc owned by B (out of A's range) -- the latter must NOT corrupt B's authoritative copy.
        String ownedByA = baseA.get(0);
        String ownedByB = baseB.get(0);
        Map<String, String> childAUpdates = new HashMap<>();
        childAUpdates.put(ownedByA, "updated-by-A");
        childAUpdates.put(ownedByB, "wrongly-updated-by-A");

        // Child B owns range B. It updates one of its own base docs.
        String ownedByBUpdated = baseB.get(1);
        Map<String, String> childBUpdates = new HashMap<>();
        childBUpdates.put(ownedByBUpdated, "updated-by-B");

        MergeChild childA = buildChild(1, RANGE_A_START, RANGE_A_END, baseIds, postA, childAUpdates);
        MergeChild childB = buildChild(2, RANGE_B_START, RANGE_B_END, baseIds, postB, childBUpdates);

        Directory target = new ByteBuffersDirectory();
        long mergedMaxSeqNo = InPlaceSiblingMerger.merge(List.of(childA, childB), target);
        assertEquals("merged maxSeqNo is the max across children", 10L, mergedMaxSeqNo);

        Map<String, String> merged = readMerged(target);

        // Every base doc survives exactly once, via the child whose range owns its hash.
        Set<String> expectedIds = new HashSet<>(baseIds);
        expectedIds.addAll(postA);
        expectedIds.addAll(postB);
        assertEquals("no document may be double-counted or dropped", expectedIds, merged.keySet());

        // Child A's update of its own base doc wins.
        assertEquals("updated-by-A", merged.get(ownedByA));
        // Child B's update of its own base doc wins.
        assertEquals("updated-by-B", merged.get(ownedByBUpdated));
        // Child A's out-of-range update of a B-owned doc is filtered out; B's authoritative copy stands.
        assertEquals("a cross-range update must not corrupt the owning child's copy", "base", merged.get(ownedByB));
        // Post-split writes carried through from both children.
        for (String id : postA) {
            assertEquals("post", merged.get(id));
        }
        for (String id : postB) {
            assertEquals("post", merged.get(id));
        }
    }
}
