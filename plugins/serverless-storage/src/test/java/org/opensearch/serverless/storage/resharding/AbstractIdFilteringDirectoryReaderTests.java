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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The behaviour {@link AbstractIdFilteringDirectoryReader} owns for both of its subclasses: nested
 * blocks (finding R-8) and the per-segment membership cache (finding R-1).
 *
 * <p>Every test here fails against the previous implementation, and each one fails for a reason
 * worth stating rather than just a number changing:
 *
 * <ul>
 *   <li>the nested tests failed because a nested child document has its {@code _id} indexed but not
 *       stored, so the filter -- which rebuilt live docs from the <em>stored</em> {@code _id} --
 *       excluded every one of them, calling that "the safe direction". It is not a safe direction:
 *       {@code InPlaceSiblingMerger} runs each child through this same filter before {@code
 *       addIndexes}, so an in-place merge physically and permanently deleted them;</li>
 *   <li>the caching test failed because the O(maxDoc) stored-fields scan ran inside the wrap, and
 *       the in-place filter is installed as an {@code IndexModule} reader wrapper -- which core
 *       applies on every searcher acquisition, i.e. once per search request per shard.</li>
 * </ul>
 */
public class AbstractIdFilteringDirectoryReaderTests extends OpenSearchTestCase {

    /** A root document, shaped the way an OpenSearch writer shapes one. */
    private static Document rootDocument(String id) {
        Document doc = new Document();
        doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE));
        // The field that marks a document a root: exactly what Queries#newNonNestedFilter tests for.
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, 1L));
        return doc;
    }

    /**
     * A nested child document: no stored {@code _id} and no {@code _primary_term}, which is what
     * {@code IdFieldMapper.NESTED_FIELD_TYPE} produces (it calls {@code setStored(true)} and then
     * {@code setStored(false)} on the following line).
     */
    private static Document nestedChildDocument(String parentId, int ordinal) {
        Document doc = new Document();
        doc.add(new StringField("child_of", parentId, Field.Store.YES));
        doc.add(new StringField("child_ordinal", Integer.toString(ordinal), Field.Store.YES));
        return doc;
    }

    /** The whole hash space, split at the midpoint into two disjoint child ranges. */
    private static ShardRange[] midpointSplit() {
        long midpoint = ((long) Integer.MIN_VALUE + Integer.MAX_VALUE) / 2;
        return new ShardRange[] {
            new ShardRange(0, Integer.MIN_VALUE, (int) midpoint),
            new ShardRange(1, (int) midpoint + 1, Integer.MAX_VALUE) };
    }

    private static Directory indexWithNestedBlocks(int parentCount, int childrenPerParent) throws Exception {
        Directory directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < parentCount; i++) {
                String parentId = "parent-" + i;
                List<Document> block = new ArrayList<>();
                for (int c = 0; c < childrenPerParent; c++) {
                    block.add(nestedChildDocument(parentId, c));
                }
                // Children first, then the root: the order Lucene requires for a block.
                block.add(rootDocument(parentId));
                writer.addDocuments(block);
            }
            writer.commit();
        }
        return directory;
    }

    /** Every visible document, split into root ids and a count of visible nested children. */
    private record Visible(Set<String> rootIds, int childCount) {
    }

    private static Visible readVisible(DirectoryReader reader, int limit) throws Exception {
        IndexSearcher searcher = new IndexSearcher(reader);
        ScoreDoc[] hits = searcher.search(new MatchAllDocsQuery(), limit).scoreDocs;
        Set<String> rootIds = new HashSet<>();
        int childCount = 0;
        for (ScoreDoc hit : hits) {
            IndexableField idField = reader.storedFields().document(hit.doc).getField(IdFieldMapper.NAME);
            if (idField == null) {
                childCount++;
            } else {
                rootIds.add(Uid.decodeId(idField.binaryValue().bytes));
            }
        }
        return new Visible(rootIds, childCount);
    }

    public void testNestedChildDocumentsAreCarriedByTheirParentsVerdict() throws Exception {
        final int parentCount = 60;
        final int childrenPerParent = 3;
        try (Directory directory = indexWithNestedBlocks(parentCount, childrenPerParent)) {
            ShardRange[] ranges = midpointSplit();
            Set<String> allRootIds = new HashSet<>();
            int totalChildren = 0;
            for (ShardRange range : ranges) {
                try (
                    DirectoryReader raw = DirectoryReader.open(directory);
                    DirectoryReader filtered = new InPlaceSplitFilteringDirectoryReader(raw, range)
                ) {
                    Visible visible = readVisible(filtered, parentCount * (childrenPerParent + 1) * 2);
                    // The core assertion, and the one that failed before the fix: the number of
                    // visible children is exactly childrenPerParent times the number of visible
                    // parents. Before the fix it was always zero, for every range.
                    assertEquals(
                        "every visible parent must bring exactly its own children, and no child may appear without its parent",
                        visible.rootIds().size() * childrenPerParent,
                        visible.childCount()
                    );
                    allRootIds.addAll(visible.rootIds());
                    totalChildren += visible.childCount();
                }
            }
            assertEquals("the two disjoint ranges together must cover every parent exactly once", parentCount, allRootIds.size());
            assertEquals(
                "and therefore every nested child exactly once -- an in-place merge of these two children unions them, so a "
                    + "child missing here is a child deleted from the revived parent",
                parentCount * childrenPerParent,
                totalChildren
            );
        }
    }

    public void testAnIndexWithNoNestedDocumentsIsUnaffected() throws Exception {
        // The no-nested path must be byte-for-byte the old behaviour: parentDocs() returns null when
        // every document carries _primary_term, and the filter walks every document as before.
        try (Directory directory = indexWithNestedBlocks(40, 0)) {
            ShardRange[] ranges = midpointSplit();
            Set<String> allRootIds = new HashSet<>();
            for (ShardRange range : ranges) {
                try (
                    DirectoryReader raw = DirectoryReader.open(directory);
                    DirectoryReader filtered = new InPlaceSplitFilteringDirectoryReader(raw, range)
                ) {
                    Visible visible = readVisible(filtered, 200);
                    assertEquals("no nested children exist, so none may be reported", 0, visible.childCount());
                    allRootIds.addAll(visible.rootIds());
                }
            }
            assertEquals(40, allRootIds.size());
        }
    }

    public void testSoftDeletedDocumentsStayHiddenThroughTheFilter() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
                .setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (int i = 0; i < 50; i++) {
                    writer.addDocument(rootDocument("doc-" + i));
                }
                writer.softUpdateDocument(
                    new Term(IdFieldMapper.NAME, Uid.encodeId("doc-7")),
                    rootDocument("doc-7"),
                    new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1)
                );
                writer.commit();
            }
            // The engine's read path wraps in the soft-deletes reader before this filter, so the
            // superseded version is already excluded by the live docs the filter starts from. This
            // pins that the filter's own live-docs AND does not resurrect it -- which is precisely
            // what the membership cache must not do, since the cache is deliberately live-docs
            // independent and the AND happens per wrap.
            ShardRange whole = new ShardRange(0, Integer.MIN_VALUE, Integer.MAX_VALUE);
            try (
                DirectoryReader raw = new org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper(
                    DirectoryReader.open(directory),
                    Lucene.SOFT_DELETES_FIELD
                );
                DirectoryReader filtered = new InPlaceSplitFilteringDirectoryReader(raw, whole)
            ) {
                Visible visible = readVisible(filtered, 200);
                assertEquals("the whole hash range keeps every live document", 50, visible.rootIds().size());
                assertEquals("and no more than that -- the soft-deleted previous version must stay hidden", 50, filtered.numDocs());
            }
        }
    }

    public void testTheStoredFieldsScanRunsOncePerSegmentNotOncePerWrap() throws Exception {
        try (Directory directory = indexWithNestedBlocks(30, 2)) {
            ShardRange range = midpointSplit()[0];
            // Deliberately not closed per wrap: FilterDirectoryReader#doClose closes the reader it
            // wraps, so closing each wrapper would close `raw` and invalidate the very segment cores
            // whose cache entries this test is about. The wrappers hold nothing of their own; `raw`
            // is closed once at the end, which is what actually releases the segments.
            DirectoryReader raw = DirectoryReader.open(directory);
            try {
                long before = AbstractIdFilteringDirectoryReader.MEMBERSHIP_COMPUTATIONS.get();
                readVisible(new InPlaceSplitFilteringDirectoryReader(raw, range), 200);
                long afterFirstWrap = AbstractIdFilteringDirectoryReader.MEMBERSHIP_COMPUTATIONS.get();
                assertTrue("the first wrap must actually compute the membership bitset", afterFirstWrap > before);

                // Ten more wraps of the same reader. Core's IndexShard#wrapSearcher applies the
                // reader wrapper on *every* searcher acquisition, so this is what a shard serving
                // ten search requests does -- and before finding R-1 was fixed, each one re-read
                // every stored field in the segment, which on an object-store-backed directory
                // means faulting the whole .fdt/.fdx in per query.
                for (int i = 0; i < 10; i++) {
                    readVisible(new InPlaceSplitFilteringDirectoryReader(raw, range), 200);
                }
                assertEquals(
                    "re-wrapping the same reader generation must reuse the cached membership bitset, not rescan stored fields",
                    afterFirstWrap,
                    AbstractIdFilteringDirectoryReader.MEMBERSHIP_COMPUTATIONS.get()
                );
            } finally {
                raw.close();
            }
        }
    }

    public void testTwoDifferentFiltersOverTheSameSegmentDoNotShareABitset() throws Exception {
        // The membership cache is keyed on (segment core, filter identity). If the filter identity
        // were dropped from the key, the second range would silently reuse the first range's bitset
        // and the two children would return the same documents -- so this asserts they are disjoint.
        try (Directory directory = indexWithNestedBlocks(40, 1)) {
            ShardRange[] ranges = midpointSplit();
            DirectoryReader raw = DirectoryReader.open(directory);
            Set<String> lower;
            Set<String> upper;
            try {
                lower = readVisible(new InPlaceSplitFilteringDirectoryReader(raw, ranges[0]), 200).rootIds();
                upper = readVisible(new InPlaceSplitFilteringDirectoryReader(raw, ranges[1]), 200).rootIds();
            } finally {
                raw.close();
            }
            assertFalse("this test is meaningless if either side is empty", lower.isEmpty() || upper.isEmpty());
            Set<String> intersection = new HashSet<>(lower);
            intersection.retainAll(upper);
            assertTrue("two disjoint hash ranges must never both claim a document: " + intersection, intersection.isEmpty());
            assertEquals(40, lower.size() + upper.size());
        }
    }
}
