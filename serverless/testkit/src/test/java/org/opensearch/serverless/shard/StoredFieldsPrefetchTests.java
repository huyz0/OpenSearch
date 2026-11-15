/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.search.fetch.StoredFieldsContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** What the fetch phase's listener announces: each hit it is about to load, to its own segment. */
public class StoredFieldsPrefetchTests extends OpenSearchTestCase {

    private static final int SEGMENTS = 3;
    private static final int DOCS_PER_SEGMENT = 10;

    /** Segment ordinal to the segment-local ids announced to it, in order. */
    private final Map<Integer, List<Integer>> announced = new TreeMap<>();

    public void testEachHitIsAnnouncedToItsOwnSegmentAtItsIdThere() throws Exception {
        try (Directory directory = segments(); DirectoryReader reader = recording(DirectoryReader.open(directory))) {
            StoredFieldsPrefetch.announce(reader.leaves(), new int[] { 0, 3, 12, 25, 29 });
            assertEquals(Map.of(0, List.of(0, 3), 1, List.of(2), 2, List.of(5, 9)), announced);
        }
    }

    public void testTheHitsGivenToTheFetchAreTheOnesAnnounced() throws Exception {
        try (Directory directory = segments(); DirectoryReader reader = recording(DirectoryReader.open(directory))) {
            final SearchContext context = context(reader, null, 0);
            // The fetch loads the slice it was given, in any order; ids outside it are not its hits.
            when(context.docIdsToLoad()).thenReturn(new int[] { 1, 27, 4, 15, 2 });
            when(context.docIdsToLoadFrom()).thenReturn(1);
            when(context.docIdsToLoadSize()).thenReturn(3);
            StoredFieldsPrefetch.INSTANCE.onPreFetchPhase(context);
            assertEquals(Map.of(0, List.of(4), 1, List.of(5), 2, List.of(7)), announced);
        }
    }

    public void testAFetchThatFollowsItsQueryAnnouncesTheTopHitsFromTheOffset() throws Exception {
        try (Directory directory = segments(); DirectoryReader reader = recording(DirectoryReader.open(directory))) {
            final SearchContext context = context(reader, topDocs(28, 3, 14), 1);
            StoredFieldsPrefetch.INSTANCE.onPreFetchPhase(context);
            assertEquals(Map.of(0, List.of(3), 1, List.of(4)), announced);
        }
    }

    public void testNothingIsAnnouncedWhenStoredFieldsAreOff() throws Exception {
        try (Directory directory = segments(); DirectoryReader reader = recording(DirectoryReader.open(directory))) {
            final SearchContext context = context(reader, topDocs(28, 3), 0);
            when(context.storedFieldsContext()).thenReturn(StoredFieldsContext.fromList(List.of(StoredFieldsContext._NONE_)));
            StoredFieldsPrefetch.INSTANCE.onPreFetchPhase(context);
            assertEquals(Map.of(), announced);
        }
    }

    public void testNothingIsAnnouncedPastTheTopHitsOrOnceTheyAreConsumed() throws Exception {
        try (Directory directory = segments(); DirectoryReader reader = recording(DirectoryReader.open(directory))) {
            StoredFieldsPrefetch.INSTANCE.onPreFetchPhase(context(reader, topDocs(28, 3), 2));
            final QuerySearchResult consumed = topDocs(28, 3);
            consumed.consumeTopDocs();
            StoredFieldsPrefetch.INSTANCE.onPreFetchPhase(context(reader, consumed, 0));
            StoredFieldsPrefetch.INSTANCE.onPreFetchPhase(context(reader, null, 0));
            assertEquals(Map.of(), announced);
        }
    }

    public void testASegmentThatCannotTakeTheHintDoesNotFailTheFetch() throws Exception {
        try (Directory directory = segments(); DirectoryReader reader = failing(DirectoryReader.open(directory))) {
            StoredFieldsPrefetch.announce(reader.leaves(), new int[] { 0, 12 });
        }
    }

    /** Every shard a node opens runs the listener: a fetch through the shard's own listeners announces its hits. */
    public void testEveryShardANodeOpensAnnouncesTheHitsOfItsFetches() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(
            new FsBlobStore(8192, createTempDir(), false),
            BlobPath.cleanPath(),
            clock::get,
            30_000L
        );
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, "{\"properties\":{\"msg\":{\"type\":\"text\"}}}", null));
        try (
            ServerlessNode node = new ServerlessNode(
                Settings.builder()
                    .put("node.name", "prefetching")
                    .put("cluster.name", "serverless-stored-fields-prefetch")
                    .put("path.home", createTempDir())
                    .put("network.host", "127.0.0.1")
                    .put("http.port", "0")
                    .put("transport.port", "0")
                    .put("serverless.roles", "ingest")
                    .build()
            )
        ) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            final IndexShard shard = node.reconciler().shard(shardId);
            try (Directory directory = segments(); DirectoryReader reader = recording(DirectoryReader.open(directory))) {
                shard.getSearchOperationListener().onPreFetchPhase(context(reader, topDocs(12), 0));
            }
            assertEquals(Map.of(1, List.of(2)), announced);
        }
    }

    private static SearchContext context(DirectoryReader reader, QuerySearchResult queryResult, int from) {
        final ContextIndexSearcher searcher = mock(ContextIndexSearcher.class);
        when(searcher.getIndexReader()).thenReturn(reader);
        final SearchContext context = mock(SearchContext.class);
        when(context.searcher()).thenReturn(searcher);
        when(context.queryResult()).thenReturn(queryResult);
        when(context.from()).thenReturn(from);
        return context;
    }

    private static QuerySearchResult topDocs(int... docs) {
        final ScoreDoc[] scoreDocs = new ScoreDoc[docs.length];
        for (int i = 0; i < docs.length; i++) {
            scoreDocs[i] = new ScoreDoc(docs[i], 1f);
        }
        final QuerySearchResult result = new QuerySearchResult();
        result.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(docs.length, TotalHits.Relation.EQUAL_TO), scoreDocs), 1f), null);
        return result;
    }

    private Directory segments() throws IOException {
        final Directory directory = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int segment = 0; segment < SEGMENTS; segment++) {
                for (int i = 0; i < DOCS_PER_SEGMENT; i++) {
                    final Document doc = new Document();
                    doc.add(new StoredField("n", segment * DOCS_PER_SEGMENT + i));
                    doc.add(new Field("f", "x", org.apache.lucene.document.TextField.TYPE_NOT_STORED));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
        return directory;
    }

    private DirectoryReader recording(DirectoryReader in) throws IOException {
        return wrap(in, (ord, delegate) -> new StoredFields() {
            @Override
            public void prefetch(int docID) {
                announced.computeIfAbsent(ord, k -> new ArrayList<>()).add(docID);
            }

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                delegate.document(docID, visitor);
            }
        });
    }

    private static DirectoryReader failing(DirectoryReader in) throws IOException {
        return wrap(in, (ord, delegate) -> new StoredFields() {
            @Override
            public void prefetch(int docID) throws IOException {
                throw new IOException("no hints here");
            }

            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                delegate.document(docID, visitor);
            }
        });
    }

    /** Wraps a segment's stored fields, given its ordinal. */
    private interface Wrapping {
        StoredFields wrap(int ord, StoredFields delegate);
    }

    private static DirectoryReader wrap(DirectoryReader in, Wrapping wrapping) throws IOException {
        final int[] ord = { 0 };
        return new FilterDirectoryReader(in, new FilterDirectoryReader.SubReaderWrapper() {
            @Override
            public LeafReader wrap(LeafReader reader) {
                final int mine = ord[0]++;
                return new FilterLeafReader(reader) {
                    @Override
                    public StoredFields storedFields() throws IOException {
                        return wrapping.wrap(mine, in.storedFields());
                    }

                    @Override
                    public CacheHelper getCoreCacheHelper() {
                        return in.getCoreCacheHelper();
                    }

                    @Override
                    public CacheHelper getReaderCacheHelper() {
                        return in.getReaderCacheHelper();
                    }
                };
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        };
    }
}
