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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

public class InPlaceSplitFilteringDirectoryReaderTests extends OpenSearchTestCase {

    private static final int DOC_COUNT = 200;

    private static Directory buildIndexWithRealIds(boolean multiSegment) throws Exception {
        Directory directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        if (multiSegment) {
            // Prevent merges so multiple commits stay as multiple segments, exercising the
            // per-leaf wrapping path (RangeSubReaderWrapper#wrap is called once per segment).
            config.setMergePolicy(NoMergePolicy.INSTANCE);
        }
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < DOC_COUNT; i++) {
                Document doc = new Document();
                doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId("doc-" + i), IdFieldMapper.Defaults.FIELD_TYPE));
                writer.addDocument(doc);
                if (multiSegment && i % 25 == 24) {
                    writer.commit();
                }
            }
            writer.commit();
        }
        return directory;
    }

    private static Set<String> readAllVisibleIds(DirectoryReader reader) throws Exception {
        IndexSearcher searcher = new IndexSearcher(reader);
        ScoreDoc[] hits = searcher.search(new MatchAllDocsQuery(), DOC_COUNT * 2).scoreDocs;
        Set<String> ids = new HashSet<>();
        for (ScoreDoc hit : hits) {
            org.apache.lucene.index.StoredFields storedFields = reader.storedFields();
            String id = storedFields.document(hit.doc).getField(IdFieldMapper.NAME) != null
                ? Uid.decodeId(storedFields.document(hit.doc).getField(IdFieldMapper.NAME).binaryValue().bytes)
                : null;
            assertNotNull("every hit must have a real _id", id);
            ids.add(id);
        }
        return ids;
    }

    public void testWrappingAReaderFiltersOutNonMatchingDocs() throws Exception {
        try (Directory directory = buildIndexWithRealIds(false)) {
            // Split the full hash space into two disjoint child ranges at the midpoint.
            long midpoint = ((long) Integer.MIN_VALUE + Integer.MAX_VALUE) / 2;
            ShardRange lower = new ShardRange(0, Integer.MIN_VALUE, (int) midpoint);
            ShardRange upper = new ShardRange(1, (int) midpoint + 1, Integer.MAX_VALUE);

            Set<String> allSeen = new HashSet<>();
            int totalVisible = 0;
            for (ShardRange range : new ShardRange[] { lower, upper }) {
                try (
                    DirectoryReader raw = DirectoryReader.open(directory);
                    InPlaceSplitFilteringDirectoryReader wrapped = new InPlaceSplitFilteringDirectoryReader(raw, range)
                ) {
                    Set<String> visible = readAllVisibleIds(wrapped);
                    for (String id : visible) {
                        assertTrue(
                            "a document seen from this range must never have been seen from the other range already",
                            allSeen.add(id)
                        );
                        assertTrue(
                            "every visible document must genuinely match this range under InPlaceSplitPartitionFilter directly",
                            InPlaceSplitPartitionFilter.matches(id, range)
                        );
                    }
                    assertEquals("numDocs() must agree with what's actually visible via search", visible.size(), wrapped.numDocs());
                    totalVisible += visible.size();
                }
            }
            assertEquals("every document across both ranges combined must add up to the full original set", DOC_COUNT, totalVisible);
            assertEquals(DOC_COUNT, allSeen.size());
        }
    }

    public void testGetReaderCacheHelperDelegatesToTheWrappedReader() throws Exception {
        try (Directory directory = buildIndexWithRealIds(false)) {
            ShardRange range = new ShardRange(0, Integer.MIN_VALUE, Integer.MAX_VALUE);
            try (DirectoryReader raw = DirectoryReader.open(directory)) {
                try (InPlaceSplitFilteringDirectoryReader wrapped = new InPlaceSplitFilteringDirectoryReader(raw, range)) {
                    // Regression test: IndexShard#wrapSearcher requires the wrapped reader's cache
                    // helper to equal the original's. A prior bug returned a fresh/independent cache
                    // helper (or null) here instead of delegating, which broke that contract.
                    assertNotNull("the reader cache helper must not be null when the wrapped reader has one", wrapped.getReaderCacheHelper());
                    assertSame(
                        "getReaderCacheHelper() must delegate to the wrapped (unfiltered) reader's cache helper, not "
                            + "construct its own",
                        raw.getReaderCacheHelper(),
                        wrapped.getReaderCacheHelper()
                    );
                }
            }
        }
    }

    public void testMultiSegmentIndexFiltersConsistentlyAcrossAllSegments() throws Exception {
        try (Directory directory = buildIndexWithRealIds(true)) {
            try (DirectoryReader raw = DirectoryReader.open(directory)) {
                assertTrue("test setup must actually produce multiple segments", raw.leaves().size() > 1);
            }

            long midpoint = ((long) Integer.MIN_VALUE + Integer.MAX_VALUE) / 2;
            ShardRange lower = new ShardRange(0, Integer.MIN_VALUE, (int) midpoint);

            try (
                DirectoryReader raw = DirectoryReader.open(directory);
                InPlaceSplitFilteringDirectoryReader wrapped = new InPlaceSplitFilteringDirectoryReader(raw, lower)
            ) {
                Set<String> visible = readAllVisibleIds(wrapped);
                for (String id : visible) {
                    assertTrue(InPlaceSplitPartitionFilter.matches(id, lower));
                }
                // Every id in range must be visible regardless of which segment it landed in.
                int expectedCount = 0;
                for (int i = 0; i < DOC_COUNT; i++) {
                    String id = "doc-" + i;
                    if (Murmur3HashFunction.hash(id) <= midpoint) {
                        expectedCount++;
                    }
                }
                assertEquals(expectedCount, visible.size());
                assertEquals(expectedCount, wrapped.numDocs());
            }
        }
    }
}
