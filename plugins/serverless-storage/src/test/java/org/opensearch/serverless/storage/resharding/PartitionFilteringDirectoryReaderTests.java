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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

public class PartitionFilteringDirectoryReaderTests extends OpenSearchTestCase {

    private static final int DOC_COUNT = 200;

    private static Directory buildIndexWithRealIds() throws Exception {
        Directory directory = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            for (int i = 0; i < DOC_COUNT; i++) {
                Document doc = new Document();
                doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId("doc-" + i), IdFieldMapper.Defaults.FIELD_TYPE));
                writer.addDocument(doc);
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

    public void testEveryDocumentIsVisibleFromExactlyOnePartition() throws Exception {
        try (Directory directory = buildIndexWithRealIds()) {
            int numPartitions = 4;
            Set<String> allSeen = new HashSet<>();
            int totalVisible = 0;
            for (int partitionIndex = 0; partitionIndex < numPartitions; partitionIndex++) {
                ShardPartitionDescriptor descriptor = new ShardPartitionDescriptor(partitionIndex, numPartitions);
                // A fresh DirectoryReader per partition, not one shared and re-wrapped: closing a
                // FilterDirectoryReader closes its delegate too (FilterDirectoryReader#doClose),
                // so reusing one raw reader across iterations would break every iteration after the
                // first once its wrapper is closed.
                try (
                    DirectoryReader raw = DirectoryReader.open(directory);
                    PartitionFilteringDirectoryReader wrapped = new PartitionFilteringDirectoryReader(raw, descriptor)
                ) {
                    Set<String> visible = readAllVisibleIds(wrapped);
                    for (String id : visible) {
                        assertTrue(
                            "a document seen from this partition must never have been seen from a different one already -- "
                                + "partitions must be disjoint",
                            allSeen.add(id)
                        );
                        assertTrue(
                            "every visible document must genuinely match this partition under RoutingPartitionFilter directly",
                            RoutingPartitionFilter.matches(id, descriptor)
                        );
                    }
                    assertEquals("numDocs() must agree with what's actually visible via search", visible.size(), wrapped.numDocs());
                    totalVisible += visible.size();
                }
            }
            assertEquals("every document across all partitions combined must add up to the full original set", DOC_COUNT, totalVisible);
            assertEquals(DOC_COUNT, allSeen.size());
        }
    }

    public void testAPartitionFilterSurvivesOpenIfChangedAfterANewDocumentIsAdded() throws Exception {
        try (Directory directory = buildIndexWithRealIds()) {
            ShardPartitionDescriptor descriptor = new ShardPartitionDescriptor(0, 2);
            DirectoryReader raw = DirectoryReader.open(directory);
            // Closing the wrapper transitively closes its delegate (FilterDirectoryReader#doClose),
            // so `raw` itself is deliberately never separately closed here -- doing so would
            // double-close it once `wrapped`/`reopened` closes below.
            PartitionFilteringDirectoryReader wrapped = new PartitionFilteringDirectoryReader(raw, descriptor);
            try {
                Set<String> before = readAllVisibleIds(wrapped);
                assertFalse(before.isEmpty());

                // Add a new document directly to the same underlying directory, then reopen via
                // the standard Lucene incremental-reopen path -- the same mechanism
                // OpenSearchReaderManager#refreshIfNeeded uses in the real engine.
                try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                    Document doc = new Document();
                    doc.add(new Field(IdFieldMapper.NAME, Uid.encodeId("doc-new"), IdFieldMapper.Defaults.FIELD_TYPE));
                    writer.addDocument(doc);
                    writer.commit();
                }

                DirectoryReader reopened = DirectoryReader.openIfChanged(wrapped);
                assertNotNull("a real new commit must produce a non-null reopened reader", reopened);
                try {
                    assertTrue(
                        "openIfChanged on an already-wrapped reader must produce another partition-filtered "
                            + "reader automatically, via doWrapDirectoryReader -- not a raw unwrapped reader",
                        reopened instanceof PartitionFilteringDirectoryReader
                    );
                    Set<String> after = readAllVisibleIds(reopened);
                    assertTrue("everything visible before must still be visible after a reopen", after.containsAll(before));
                    boolean newDocMatchesThisPartition = RoutingPartitionFilter.matches("doc-new", descriptor);
                    assertEquals(newDocMatchesThisPartition, after.contains("doc-new"));
                } finally {
                    reopened.close();
                }
            } finally {
                wrapped.close();
            }
        }
    }
}
