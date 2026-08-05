/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexService;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * A gated index nothing has touched must be given back.
 *
 * <h2>Why this is the property the design rests on</h2>
 *
 * A hundred million indices at three to a hundred shards is up to ten billion shards, which no fleet holds
 * open. The architecture only works if resident shards track the working set rather than the population.
 *
 * <p>Before this, they tracked neither. {@code removeIndices} skips every gated index a node opened on
 * demand, and {@code openedOnDemand} shrank only when an index was deleted or the node stopped, so the
 * resident set counted every distinct tenant the node had ever served. {@code GatedResidencySoakIT} measured
 * what that costs -- 150,888 bytes of heap and 3 file descriptors per open index, 11,470 of them opened with
 * nothing failing, and <b>zero</b> released after ninety seconds of complete inactivity.
 *
 * <p>So residency grew monotonically toward the population, with a delay set by how fast tenants arrived.
 * That is the equality the whole design exists to avoid.
 *
 * <h2>Why the assertion is on the count and not on the timing</h2>
 *
 * The interesting failure is not "eviction was slow", it is "eviction never ran", which is this branch's
 * signature defect: a mechanism built, tested through its own entry point, and reached by nothing. So the
 * test opens indices the ordinary way -- a write, which is what makes the on-demand opener build a shard --
 * and then asserts the node gives them back without anything asking it to.
 *
 * <p>The intervals are set absurdly short here because the alternative is a test that takes half an hour.
 * The production default is thirty minutes; what is being tested is the mechanism, and the mechanism does not
 * know what number it was given.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GatedIdleEvictionIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static final int TENANTS = 12;

    /** Enough tenants that the population is several times the expected resident set, and no more. */
    private static final int PLATEAU_TENANTS = 120;

    /** A pause between creations, so the run spans several idle windows instead of fitting inside one. */
    private static final long PACE_MILLIS = 40;

    /** How long a shard may be untouched before this cluster evicts it. Short, because the test waits for it. */
    private static final TimeValue IDLE_AFTER = TimeValue.timeValueSeconds(2);

    /** How often the sweep runs. Shorter still, so eviction happens promptly once a shard qualifies. */
    private static final TimeValue SWEEP_EVERY = TimeValue.timeValueSeconds(1);

    private volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(org.opensearch.serverless.storage.ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                org.opensearch.serverless.storage.ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(),
                basePath().toString()
            )
            // Without computed placement a gated index has no routing table, the on-demand opener declines,
            // and no shard is ever built -- so there would be nothing to evict and this would pass empty.
            .put(org.opensearch.serverless.storage.ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .put(IndicesClusterStateService.GATED_SHARD_SWEEP_INTERVAL_SETTING.getKey(), SWEEP_EVERY)
            .put(IndicesClusterStateService.GATED_SHARD_IDLE_EVICTION_SETTING.getKey(), IDLE_AFTER)
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    public void testAnIdleGatedIndexIsClosedAndReopensOnTheNextRequest() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        IndicesService indices = internalCluster().getInstance(IndicesService.class, dataNode);

        for (int i = 0; i < TENANTS; i++) {
            String name = tenant(i);
            client().admin().indices().create(new CreateIndexRequest(name).settings(gated())).actionGet();
            client().prepareIndex(name).setId("1").setSource("tenant", name).get();
        }

        assertBusy(
            () -> assertEquals("every tenant written to must be open before anything can be evicted", TENANTS, gatedOpenCount(indices)),
            30,
            TimeUnit.SECONDS
        );

        // Nothing is touched from here. The node has to decide on its own that these are cold.
        assertBusy(() -> {
            int open = gatedOpenCount(indices);
            assertEquals(String.format(Locale.ROOT, "%d of %d gated indices were still resident after going idle", open, TENANTS), 0, open);
        }, 60, TimeUnit.SECONDS);

        // And the eviction is a close, not a loss: the document is still there, served by a shard the
        // on-demand opener rebuilds because a request arrived for it. Without this the test would pass
        // just as happily against an implementation that evicted by deleting.
        client().admin().indices().prepareRefresh(tenant(0)).get();
        assertEquals(
            "the document written before eviction must still be readable afterwards",
            1,
            client().prepareSearch(tenant(0)).setSize(0).get().getHits().getTotalHits().value()
        );
    }

    /**
     * With eviction switched off, the same population stays resident.
     *
     * <p>The control. Without it, a bug that closed gated indices for some unrelated reason -- a failing
     * descriptor read, a sweep misfiring -- would make the test above pass while proving nothing about
     * idleness. It also pins the setting's meaning: zero has to mean never, because that is the behaviour
     * every existing cluster has today and the one an operator falls back to.
     */
    public void testEvictionOffKeepsThemResident() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode(
            Settings.builder().put(IndicesClusterStateService.GATED_SHARD_IDLE_EVICTION_SETTING.getKey(), TimeValue.ZERO).build()
        );
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        IndicesService indices = internalCluster().getInstance(IndicesService.class, dataNode);

        for (int i = 0; i < TENANTS; i++) {
            String name = tenant(i);
            client().admin().indices().create(new CreateIndexRequest(name).settings(gated())).actionGet();
            client().prepareIndex(name).setId("1").setSource("tenant", name).get();
        }
        assertBusy(() -> assertEquals(TENANTS, gatedOpenCount(indices)), 30, TimeUnit.SECONDS);

        // Comfortably longer than the idle threshold the other test evicts on, so a sweep that was going to
        // close these has had several chances.
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        assertEquals(
            "with idle eviction disabled a gated index must stay open, which is what every cluster does today",
            TENANTS,
            gatedOpenCount(indices)
        );
    }

    /**
     * The property the design actually needs: residency is bounded by arrival rate, not by population.
     *
     * <p>{@link #testAnIdleGatedIndexIsClosedAndReopensOnTheNextRequest} shows eviction happens. It does not
     * show it happens fast enough to matter, and those are different claims: an eviction that only runs once
     * the population has already gone resident bounds nothing. What has to hold is that a node steadily
     * serving new tenants keeps roughly {@code rate x idle window} of them open, whatever the total.
     *
     * <p>So this opens a population several times larger than that product and watches the peak. With a two
     * second window and creation paced to a few tens per second, the expected resident count is a few dozen
     * out of {@link #PLATEAU_TENANTS}; the assertion allows half the population, which is far above what the
     * mechanism should produce and far below what no eviction at all would produce. A loose bound on the
     * right side of the interesting line beats a tight one that fails on a slow machine.
     *
     * <p>The pacing is deliberate rather than incidental. Without it a fast machine creates the whole
     * population inside one idle window, every index is legitimately warm, and the test would fail while
     * nothing was wrong -- measuring the machine instead of the mechanism.
     */
    public void testResidencyIsBoundedByArrivalRateRatherThanByPopulation() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        IndicesService indices = internalCluster().getInstance(IndicesService.class, dataNode);

        int peakResident = 0;
        for (int i = 0; i < PLATEAU_TENANTS; i++) {
            String name = plateauTenant(i);
            client().admin().indices().create(new CreateIndexRequest(name).settings(gated())).actionGet();
            client().prepareIndex(name).setId("1").setSource("tenant", name).get();
            // Paced so the run spans several idle windows rather than fitting inside one.
            Thread.sleep(PACE_MILLIS);
            // Every iteration, not every tenth. Sampling sparsely can only ever miss a peak, never invent
            // one, so an under-sampled run passes an assertion the real peak would have failed -- which is
            // the wrong direction for a bound to be wrong in.
            peakResident = Math.max(peakResident, gatedOpenCount(indices));
        }

        logger.warn("plateau: opened {} gated indices, peak resident {}", PLATEAU_TENANTS, peakResident);

        assertTrue(
            "the whole point is that residency does not track the population: "
                + peakResident
                + " of "
                + PLATEAU_TENANTS
                + " were resident at once, so eviction is not keeping up with arrivals and a node serving a "
                + "hundred million tenants would still end up holding all of them",
            peakResident <= PLATEAU_TENANTS / 2
        );
        // And the population really was opened, so the bound above is a bound rather than a count of
        // indices that were never built. Without this the test would pass against a broken opener.
        assertTrue("the run must actually have opened indices for the peak to mean anything", peakResident > 0);
    }

    private static int gatedOpenCount(IndicesService indices) throws Exception {
        int count = 0;
        for (IndexService indexService : indices) {
            if (indexService.getIndexSettings().getSettings().getAsBoolean("index.serverless_storage.enabled", false)) {
                count++;
            }
        }
        return count;
    }

    private static String tenant(int i) throws Exception {
        return String.format(Locale.ROOT, "tenant-%04d", i);
    }

    /** A separate name space, so a plateau run cannot resolve a name an earlier test left behind. */
    private static String plateauTenant(int i) throws Exception {
        return String.format(Locale.ROOT, "plateau-%04d", i);
    }

    private static Settings gated() throws Exception {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
