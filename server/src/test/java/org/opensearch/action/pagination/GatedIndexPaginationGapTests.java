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
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

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
 * <p><b>Closed by folding one page of gated indices into the page built from cluster state.</b> A merge
 * rather than a bigger sort, because the two problems constrain each other: the cluster state side is
 * bounded by the ordinary index count, which is small by construction since the massive population is
 * exactly the gated part, and the gated side is bounded by the page size because a pager is asked for a
 * page. The hundred million appears in neither term.
 */
public class GatedIndexPaginationGapTests extends OpenSearchTestCase {

    /** With no pager installed, nothing changes, so a cluster that never gated an index is untouched. */
    public void testWithoutAPagerOnlyClusterStateIndicesAppear() {
        // Two ordinary indices in cluster state, standing in for a population that also contains gated
        // indices which are not in the map at all.
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata("ordinary-a"), false).put(indexMetadata("ordinary-b"), false).build())
            .build();

        IndexPaginationStrategy strategy = new IndexPaginationStrategy(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, 10), state);

        assertEquals("only the two cluster state indices", 2, strategy.getRequestedEntities().size());
    }

    /**
     * The gap H16 pinned, now closed. A gated index appears in the page alongside ordinary ones.
     */
    public void testAGatedIndexAppearsInThePage() {
        AbsentIndexDescriptorSuppliers.registerPager((after, afterDate, asc, size) -> List.of(descriptor("gated-a", 5L)));
        ClusterState state = twoOrdinaryIndices();

        IndexPaginationStrategy strategy = new IndexPaginationStrategy(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, 10), state);

        assertEquals("the gated index must be in the page", 3, strategy.getRequestedEntities().size());
        assertTrue("by name", strategy.getRequestedEntities().contains("gated-a"));
        assertTrue("and the ordinary ones must still be there", strategy.getRequestedEntities().contains("ordinary-a"));
    }

    /**
     * The property that rules out the fix anyone reaches for first. The pager is asked for a page, never
     * for the population, so folding gated indices in does not reintroduce the cost H16 recorded.
     */
    public void testThePagerIsAskedForAPageAndNotThePopulation() {
        java.util.concurrent.atomic.AtomicInteger requestedSize = new java.util.concurrent.atomic.AtomicInteger(-1);
        AbsentIndexDescriptorSuppliers.registerPager((after, afterDate, asc, size) -> {
            requestedSize.set(size);
            return List.of(descriptor("gated-a", 5L));
        });

        new IndexPaginationStrategy(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, 7), twoOrdinaryIndices());

        assertEquals(
            "the pager must be asked for the page size and nothing more. Asking it for the population is "
                + "the fix H16 warned against: it would make gated indices visible while making the cost "
                + "problem worse",
            7,
            requestedSize.get()
        );
    }

    /** The page respects its size even when both sides together exceed it. */
    public void testTheMergedPageIsStillBoundedByTheRequestedSize() {
        AbsentIndexDescriptorSuppliers.registerPager(
            (after, afterDate, asc, size) -> List.of(descriptor("gated-a", 1L), descriptor("gated-b", 2L))
        );

        IndexPaginationStrategy strategy = new IndexPaginationStrategy(
            new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, 2),
            twoOrdinaryIndices()
        );

        assertEquals("a page of two must stay a page of two", 2, strategy.getRequestedEntities().size());
    }

    /**
     * A pager that throws must leave the ordinary page intact. A listing missing its gated indices is bad;
     * a listing that fails outright because the descriptor index hiccuped is worse.
     */
    public void testAFailingPagerLeavesTheOrdinaryPageIntact() {
        AbsentIndexDescriptorSuppliers.registerPager((after, afterDate, asc, size) -> { throw new IllegalStateException("pager down"); });

        IndexPaginationStrategy strategy = new IndexPaginationStrategy(
            new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, 10),
            twoOrdinaryIndices()
        );

        assertEquals("the ordinary indices must still be listed", 2, strategy.getRequestedEntities().size());
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

    @org.junit.After
    public void clearPager() {
        AbsentIndexDescriptorSuppliers.registerPager(null);
    }

    private static ClusterState twoOrdinaryIndices() {
        return ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata("ordinary-a"), false).put(indexMetadata("ordinary-b"), false).build())
            .build();
    }

    private static IndexDescriptor descriptor(String name, long creationDate) {
        return new IndexDescriptor(
            name,
            name + "-uuid",
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
            Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            creationDate
        );
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
