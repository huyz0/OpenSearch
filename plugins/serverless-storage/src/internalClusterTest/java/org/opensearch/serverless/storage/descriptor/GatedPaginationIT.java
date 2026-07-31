/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.action.pagination.IndexPaginationStrategy;
import org.opensearch.action.pagination.PageParams;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * T27. Whether listing a gated population can get past its first page.
 *
 * <p>H16 made pagination able to see gated indices by merging one pager page into the cluster state page,
 * and the asymmetry it relies on is sound: the ordinary side is small by construction and the gated side is
 * bounded by the page size, so a hundred million appears in neither term. What was never driven is the
 * <em>loop</em>. Every test of the merge asked for one page.
 *
 * <p>It found four defects, and drove the fix for all of them. Measured before the fix, twelve gated
 * indices in pages of four returned one page of four names in both directions, and a tombstoned index was
 * listed as an index:
 *
 * <ol>
 *   <li>{@code getResponseToken} was computed from the cluster state page alone. With every index gated
 *       that count is zero, zero is not greater than the page size, and the token was therefore null. A
 *       null token is how this API says "that was everything", so a hundred million indices listed as ten.</li>
 *   <li>{@code mergeGatedIndices} orders by {@code (creationDate, name)} and states that both sides arrive
 *       already in page order. {@code DescriptorGate.pagerFor} ordered by {@code name} alone and resumed
 *       with {@code search_after(afterName)}, discarding the {@code afterCreationDate} it was handed, so a
 *       page was selected in one order and returned in another.</li>
 *   <li>Descending was served by fetching ascending and reversing, which reverses the <em>first</em> page
 *       rather than producing the last one, so both walks returned the same names.</li>
 *   <li>Neither prefix search filtered on state, so a deleted index was still listed as an index.</li>
 * </ol>
 *
 * <p>Defect one is the one that hid the others, because a listing that stops after one page never reaches a
 * second page to be wrong about.
 *
 * <p>Creation dates run <b>opposite</b> to name order on purpose, and that is what makes this test able to
 * fail. With both orders agreeing, a pager sorting by name and a merge sorting by creation date produce
 * identical pages and defect two is invisible, which is presumably how it survived H16 and T5. The control
 * pins the harness down: the same walk over ordinary indices must enumerate all of them exactly once.
 */
public class GatedPaginationIT extends OpenSearchIntegTestCase {

    private static final int POPULATION = 12;

    private static final int PAGE_SIZE = 4;

    @After
    public void clearGate() {
        DescriptorGate.uninstall();
    }

    /** The question, ascending. Walk the pages to exhaustion and see how much of the population appears. */
    public void testWalkingEveryPageOfAGatedPopulation() {
        installWithGatedPopulation();

        Walk walk = walk(PageParams.PARAM_ASC_SORT_VALUE);
        logger.warn(
            "T27: ascending walk over {} gated indices in pages of {} took {} pages and yielded {} distinct names {}",
            POPULATION,
            PAGE_SIZE,
            walk.pages,
            walk.seen.size(),
            walk.seen
        );

        assertEquals(
            "paging must enumerate every gated index exactly once, or an operator listing the population is "
                + "shown one page and told it was everything",
            POPULATION,
            walk.seen.size()
        );
    }

    /** The same walk descending, which before the fix reversed a page it had fetched ascending. */
    public void testWalkingEveryPageDescending() {
        installWithGatedPopulation();

        Walk walk = walk(PageParams.PARAM_DESC_SORT_VALUE);
        logger.warn("T27: descending walk yielded {} distinct names over {} pages {}", walk.seen.size(), walk.pages, walk.seen);

        assertEquals("descending must enumerate the same population as ascending", POPULATION, walk.seen.size());
    }

    /**
     * The adjacent question, asked because neither prefix search filters on state.
     *
     * <p>H4 records a deletion as a tombstone rather than an absence, so a deleted gated index remains a
     * document in the descriptor index carrying {@code state=DELETED}. Listing reads those documents and
     * nothing in the query excludes them, which would mean a deleted index is still listed as an index.
     *
     * <p>Kept to a single page on purpose. The walk defects above would otherwise decide the outcome, and
     * this is a different question: not whether paging reaches an index, but whether an index that no longer
     * exists is offered at all.
     */
    public void testADeletedIndexIsNotListed() {
        DescriptorStore store = new DescriptorStore(client(), 1);
        store.create(descriptor("listed-alive", 1_700_000_001_000L));
        store.create(tombstone("listed-deleted", 1_700_000_002_000L));
        client().admin().indices().prepareRefresh(DescriptorStore.DESCRIPTOR_INDEX).get();
        DescriptorGate.install(
            store,
            new IndexBackedMappingStore(client()),
            new IndexBackedMappingStatsAggregator(client()),
            new StoreBackedFieldRefresher(),
            true
        );

        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        List<String> page = new IndexPaginationStrategy(new PageParams(null, PageParams.PARAM_ASC_SORT_VALUE, 10), empty)
            .getRequestedEntities();
        logger.warn("T27: a page over one live and one tombstoned descriptor listed {}", page);

        assertTrue("the premise: a live gated index is listed", page.contains("listed-alive"));
        assertFalse(
            "a deleted index must not be listed, and H4 keeps its descriptor as a tombstone rather than "
                + "removing it, so listing has to exclude it by state rather than by absence",
            page.contains("listed-deleted")
        );
    }

    /**
     * The control. The identical walk over ordinary indices with the identical inverted creation dates,
     * which the strategy has always handled, so a failure here would mean the harness rather than the gate.
     *
     * <p>Creation dates cannot be set directly through the create API, so this asserts against whatever the
     * cluster assigned rather than against the inverted schedule. That is enough for a control: the question
     * is whether the walk terminates having seen everything.
     */
    public void testTheSameWalkOverOrdinaryIndices() {
        for (int i = 0; i < POPULATION; i++) {
            createIndex(name(i));
        }
        ClusterState state = client().admin().cluster().prepareState().get().getState();

        Set<String> seen = new LinkedHashSet<>();
        String token = null;
        for (int page = 0; page < POPULATION * 2; page++) {
            IndexPaginationStrategy strategy = new IndexPaginationStrategy(
                new PageParams(token, PageParams.PARAM_ASC_SORT_VALUE, PAGE_SIZE),
                state
            );
            seen.addAll(strategy.getRequestedEntities());
            token = strategy.getResponseToken().getNextToken();
            if (token == null) {
                break;
            }
        }

        assertEquals("the control: the same walk over ordinary indices enumerates all of them", POPULATION, seen.size());
    }

    private record Walk(Set<String> seen, int pages) {
    }

    /**
     * Pages until the strategy says there is no next page, against a deliberately empty cluster state so
     * every name in the result can only have come from the pager.
     *
     * <p>Bounded at twice the population, so a token that never advances fails as a wrong count rather than
     * hanging the suite.
     */
    private Walk walk(String sort) {
        ClusterState empty = ClusterState.builder(ClusterName.DEFAULT).build();
        Set<String> seen = new LinkedHashSet<>();
        String token = null;
        int pages = 0;
        for (int page = 0; page < POPULATION * 2; page++) {
            IndexPaginationStrategy strategy = new IndexPaginationStrategy(new PageParams(token, sort, PAGE_SIZE), empty);
            List<String> names = strategy.getRequestedEntities();
            pages++;
            seen.addAll(names);
            token = strategy.getResponseToken().getNextToken();
            if (token == null) {
                break;
            }
        }
        return new Walk(seen, pages);
    }

    private void installWithGatedPopulation() {
        DescriptorStore store = new DescriptorStore(client(), 1);
        List<IndexDescriptor> descriptors = new ArrayList<>();
        for (int i = 0; i < POPULATION; i++) {
            descriptors.add(descriptor(name(i), invertedCreationDate(i)));
        }
        for (IndexDescriptor descriptor : descriptors) {
            store.create(descriptor);
        }
        // Paging is a search and a search is refresh-bound, and the descriptor index carries a one second
        // refresh interval by contract (H18). Without this the first run of this test measured an empty
        // search: every page came back with nothing, which looks exactly like the defect being hunted and
        // is not it.
        client().admin().indices().prepareRefresh(DescriptorStore.DESCRIPTOR_INDEX).get();
        DescriptorGate.install(
            store,
            new IndexBackedMappingStore(client()),
            new IndexBackedMappingStatsAggregator(client()),
            new StoreBackedFieldRefresher(),
            true
        );
        // The guard that the earlier run lacked. If the pager itself answers nothing, everything below
        // measures an empty store rather than a pagination walk, and it fails silently because
        // AbsentIndexDescriptorSuppliers.page converts a pager failure into an empty page.
        assertEquals(
            "the premise: the pager must be able to produce a first page at all",
            PAGE_SIZE,
            store.findNamesForPage(null, 0L, true, PAGE_SIZE).size()
        );
    }

    /** Later names get earlier dates, so name order and page order are opposites rather than the same. */
    private static long invertedCreationDate(int i) {
        return 1_700_000_000_000L + (POPULATION - i) * 1_000L;
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "paged-%03d", i);
    }

    /** A deleted index as H4 records it: still a document, distinguished only by its state. */
    private static IndexDescriptor tombstone(String name, long creationDate) {
        return new IndexDescriptor(
            name,
            name + "-uuid",
            1,
            0,
            true,
            IndexDescriptor.State.DELETED,
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
}
