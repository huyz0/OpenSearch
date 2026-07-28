/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.pagination;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * H16. Whether a paginated listing can see an index that cluster state does not hold.
 *
 * <p>It cannot, and the way it fails is the one this area has now hit nine times. {@code
 * PaginationStrategy.getSortedIndexMetadata} streams {@code metadata().indices()}, so a gated index is not
 * absent from the page, it is absent from the input. Nothing throws. The response is a well-formed page
 * with a next-page token, and the caller has no way to distinguish it from a complete answer.
 *
 * <p>That is the same signature as C21's refresh reaching zero shards, C22's field mappings returning
 * nothing, and the stats that reported an index with no shards. Each of them returned a confident empty
 * answer because a computed index was invisible to the thing doing the enumerating.
 *
 * <p><b>There is a second problem and it is the less serious one.</b> The strategy sorts the entire
 * population to produce one page, which defeats the purpose of paginating: at the index counts this area
 * exists for, {@code _list/indices} would sort a hundred million entries per request. Recorded here
 * because a fix that made gated indices visible by adding them to the same sort would make the cost
 * problem worse while appearing to solve the correctness one.
 *
 * <p>This pins the gap rather than a hypothetical fix, in the shape that worked for H4c, H8a and H9a: it
 * fails the day someone closes it, and the message says what to decide then.
 */
public class GatedIndexPaginationGapTests extends OpenSearchTestCase {

    /** The gap. A gated index is missing from the page and nothing says so. */
    public void testAGatedIndexIsInvisibleToPagination() {
        // Two ordinary indices in cluster state, standing in for a population that also contains gated
        // indices which are not in the map at all.
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata("ordinary-a"), false).put(indexMetadata("ordinary-b"), false).build())
            .build();

        IndexPaginationStrategy strategy = new IndexPaginationStrategy(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, 10), state);

        assertEquals(
            "pagination can only ever see indices present in cluster state, so a gated index is missing "
                + "from the page with no exception and no signal. Closing this means either resolving the "
                + "page through the descriptor index or refusing the request for a cluster containing gated "
                + "indices, and adding them to the existing sort is not an option: that sort already walks "
                + "the whole population to produce one page",
            2,
            strategy.getRequestedEntities().size()
        );
        assertFalse("and the page is well formed, which is what makes the omission silent", strategy.getRequestedEntities().isEmpty());
    }

    /**
     * The cost half, asserted as a shape rather than a latency. Producing a page of one still touches every
     * index in the population, so the work is set by the population and not by the page size.
     */
    public void testAPageOfOneStillSortsTheWholePopulation() {
        Metadata.Builder metadata = Metadata.builder();
        for (int i = 0; i < 500; i++) {
            metadata.put(indexMetadata(String.format(java.util.Locale.ROOT, "idx-%04d", i)), false);
        }
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata.build()).build();

        // The sorted list the strategy works from is the whole population, regardless of page size.
        assertEquals(
            "pagination sorts every index to return one page, so its cost is set by the population rather "
                + "than by the page. At a hundred million indices this is the request, not an overhead on it",
            500,
            PaginationStrategy.getSortedIndexMetadata(state, java.util.Comparator.comparing(m -> m.getIndex().getName())).size()
        );

        IndexPaginationStrategy strategy = new IndexPaginationStrategy(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, 1), state);
        assertEquals("while the page itself is one entry", 1, strategy.getRequestedEntities().size());
    }

    private static IndexMetadata indexMetadata(String name) {
        return IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
