/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexAction;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Where a gated creation actually runs.
 *
 * <h2>Why this is worth a test of its own</h2>
 *
 * Creation is a {@code TransportClusterManagerNodeAction}, so every request is sent to the elected cluster
 * manager before anything looks at it. T49 took gated creation off that node's state update thread, which
 * removed the serialisation but not the funnel: every creation in the cluster still executed on one
 * machine, so the cluster's creation rate was one machine's rate however many nodes it had. Measuring it
 * made that concrete -- 100M indices in two hours needs about 13,889 per second, and one node sustains a
 * few thousand.
 *
 * <p>Nothing about a gated creation needs the cluster manager. Uniqueness is the descriptor store's
 * register compare-and-swap, and what creation reads from cluster state is a snapshot every node has. So
 * the request is executed where it lands.
 *
 * <h2>What this asserts, and why the control is half of it</h2>
 *
 * That a gated creation sent to a data node is <em>not</em> forwarded, and that an ordinary one still is.
 * The second is not decoration: the whole risk of this change is a bypass that fires too widely, and an
 * ordinary index created without its cluster state update is not a fast creation but a lost one. A test
 * that only asserted the first would pass just as happily in that case.
 *
 * <p>Counted at the transport layer rather than inferred from timing or from which thread ran, because
 * "was this request sent to another node" is exactly what the change is about and is the one thing a
 * correct-looking result cannot tell you: the index is created either way.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GatedCreationOnAnyNodeIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    /**
     * Per test method, not per class, because each method here builds its own cluster and a repository path
     * from a previous one is not an allowed path in the next. A static holder passes the first method and
     * fails the second on "did not resolve to a usable container", which reads as a storage problem rather
     * than as a stale path.
     */
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
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class, MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath().toString())
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    @After
    public void clearGate() throws Exception {
        DescriptorGate.uninstall();
    }

    public void testAGatedCreationRunsOnTheNodeThatReceivedItAndAnOrdinaryOneDoesNot() throws Exception {
        String clusterManager = internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);
        installBlobBackedDescriptorPlane();

        AtomicInteger forwardedToClusterManager = new AtomicInteger();
        MockTransportService fromDataNode = (MockTransportService) internalCluster().getInstance(TransportService.class, dataNode);
        TransportService onClusterManager = internalCluster().getInstance(TransportService.class, clusterManager);
        fromDataNode.addSendBehavior(onClusterManager, (connection, requestId, action, request, options) -> {
            if (CreateIndexAction.NAME.equals(action)) {
                forwardedToClusterManager.incrementAndGet();
            }
            connection.sendRequest(requestId, action, request, options);
        });

        assertTrue(
            internalCluster().client(dataNode)
                .admin()
                .indices()
                .create(new CreateIndexRequest("serverless_gated-here").settings(gated()))
                .actionGet()
                .isAcknowledged()
        );
        assertEquals(
            "a gated creation needs nothing from the cluster manager -- uniqueness is the descriptor "
                + "store's compare-and-swap -- so sending it there is the funnel this removes",
            0,
            forwardedToClusterManager.get()
        );
        // Created, and gated, so this cannot pass by the request having quietly done nothing.
        assertNotNull("the index must exist as a descriptor", AbsentIndexDescriptorSuppliers.supply("serverless_gated-here"));
        assertNull(
            "and must not have a cluster state entry, or it took the ordinary road after all",
            client().admin().cluster().prepareState().get().getState().metadata().index("serverless_gated-here")
        );

        assertTrue(
            internalCluster().client(dataNode)
                .admin()
                .indices()
                .create(new CreateIndexRequest("ordinary-here").settings(ordinary()))
                .actionGet()
                .isAcknowledged()
        );
        assertEquals(
            "an ordinary index needs a cluster state update, which only the cluster manager can publish, "
                + "so this one must still be forwarded",
            1,
            forwardedToClusterManager.get()
        );
        assertNotNull(
            "and must exist in cluster state, which is what being forwarded was for",
            client().admin().cluster().prepareState().get().getState().metadata().index("ordinary-here")
        );
    }

    // The boundary -- an alias, a template's alias, an ordinary index, no gate installed -- is asserted in
    // AdmissionTemplateResolutionTests against MetadataCreateIndexService#certainlyGated directly. Driving
    // it from here would mean creating an index that carries the gated setting and cannot be gated, whose
    // shard then fails to open for reasons that have nothing to do with where the request ran.

    private static Settings gated() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }

    private static Settings ordinary() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
    }
}
