/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.ScoreDoc;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Announces the stored fields of every hit a fetch phase is about to load, before it loads the first.
 *
 * <p>The fetch phase loads hits one after another. Over the object store, each hit whose stored-field block was cold paid
 * its own round trip in turn: ten hits in ten segments waited ten round trips. Announced first through Lucene's
 * {@link StoredFields#prefetch}, the blocks are fetched together by the object-store input's read prefetch, and each load
 * finds its block held or in flight. The requests are the same ones the loads would have made; only the waiting changes.
 * With read prefetch off the hints are ignored and this costs a pass over the hits.
 *
 * <p>Registered as a search listener, so core's fetch phase is unchanged. A fetch that follows its query in one request
 * has not been given its hits when the listener runs; they are then the query's top hits from the request's offset, which
 * is what that fetch loads.
 */
public final class StoredFieldsPrefetch implements SearchOperationListener {

    private static final Logger logger = LogManager.getLogger(StoredFieldsPrefetch.class);

    /** The listener; it holds no state. */
    public static final StoredFieldsPrefetch INSTANCE = new StoredFieldsPrefetch();

    private StoredFieldsPrefetch() {}

    @Override
    public void onPreFetchPhase(SearchContext context) {
        if (context.storedFieldsContext() != null && context.storedFieldsContext().fetchFields() == false) {
            // Stored fields are off for this request: the fetch loads none, and a hint would fetch blocks nobody reads.
            return;
        }
        final int[] docs = hitsToLoad(context);
        if (docs.length == 0 || context.searcher() == null) {
            return;
        }
        Arrays.sort(docs);
        announce(context.searcher().getIndexReader().leaves(), docs);
    }

    /** The doc ids the fetch is about to load: the ones it was given, or the query's top hits from the offset. */
    static int[] hitsToLoad(SearchContext context) {
        if (context.docIdsToLoadSize() > 0) {
            return Arrays.copyOfRange(
                context.docIdsToLoad(),
                context.docIdsToLoadFrom(),
                context.docIdsToLoadFrom() + context.docIdsToLoadSize()
            );
        }
        if (context.queryResult() == null || context.queryResult().hasConsumedTopDocs() || context.queryResult().topDocs() == null) {
            return new int[0];
        }
        final ScoreDoc[] top = context.queryResult().topDocs().topDocs.scoreDocs;
        final int from = Math.max(0, context.from());
        if (from >= top.length) {
            return new int[0];
        }
        final int[] docs = new int[top.length - from];
        for (int i = from; i < top.length; i++) {
            docs[i - from] = top[i].doc;
        }
        return docs;
    }

    /**
     * Announces each doc to its own segment, at its id within that segment.
     *
     * @param leaves the searcher's leaves
     * @param docs top-level doc ids, sorted
     */
    public static void announce(List<LeafReaderContext> leaves, int[] docs) {
        int leafIndex = -1;
        LeafReaderContext leaf = null;
        StoredFields storedFields = null;
        try {
            for (int doc : docs) {
                final int index = ReaderUtil.subIndex(doc, leaves);
                if (index != leafIndex) {
                    leafIndex = index;
                    leaf = leaves.get(index);
                    storedFields = leaf.reader().storedFields();
                }
                storedFields.prefetch(doc - leaf.docBase);
            }
        } catch (IOException | RuntimeException e) {
            // Only a hint: each load fetches for itself.
            logger.trace("could not announce the stored fields of the hits to fetch", e);
        }
    }
}
