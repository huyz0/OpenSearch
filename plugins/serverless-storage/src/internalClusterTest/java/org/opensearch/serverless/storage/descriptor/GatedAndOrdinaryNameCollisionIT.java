/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;

/**
 * What happens when one name is claimed in both planes.
 *
 * <p>A gated index's name is held by a register compare-and-swap in the descriptor store; an ordinary
 * index's is held by cluster state. Neither authority consults the other at creation time, which raises a
 * question this test answers by measurement rather than by reading: can the same name end up claimed twice,
 * and if so, what does a client see?
 *
 * <p>Asserted rather than argued because the reasoning had already been written down wrong once -- as "the
 * ordinary path consults the descriptor store", which {@code MetadataCreateIndexService#validate} does not
 * do: it checks the routing table, the metadata and the aliases, all of which are cluster state.
 *
 * <h2>What it found</h2>
 *
 * Both claims are granted. Creating a gated {@code collide-me} and then an ordinary {@code collide-me}
 * succeeds twice, sequentially, with no concurrency involved at all -- the descriptor stays live and cluster
 * state gains an index of the same name with a different uuid. Resolution consults metadata before the
 * supplier, so the ordinary index shadows the gated one from that moment: the client that created the gated
 * index was told it exists and can no longer address it. Deleting the ordinary index unshadows the
 * descriptor, so the name comes back as a different index with a different uuid and no data.
 *
 * <h2>Why it is not fixed here</h2>
 *
 * The obvious fix -- have {@code validateIndexName} ask the descriptor store -- cannot go where the check
 * is. That runs inside the cluster state update task, and {@code AbsentIndexDescriptorSuppliers} refuses to
 * resolve on that thread by design, because a blocking descriptor read there is the deadlock W4 already
 * paid for. The check has to move to the request path, before the task is submitted, which is a change to
 * how ordinary creation is sequenced and costs every ordinary creation in a gated cluster one descriptor
 * read. That is a decision about the ordinary path, which R1 protects, rather than something to append to a
 * throughput change.
 *
 * <p>Marked {@code AwaitsFix} rather than deleted or weakened, the same way {@code
 * GatedCreationDurabilityIT} keeps T17 visible: a test that asserted the current behaviour would turn this
 * into a specification.
 */
@org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix(bugUrl = "one name can be claimed in both planes: neither creation path consults the other's authority")
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GatedAndOrdinaryNameCollisionIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

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
        return List.of(ServerlessStoragePlugin.class);
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

    public void testCreatingAnOrdinaryIndexOverAGatedName() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        assertTrue(client().admin().indices().create(new CreateIndexRequest("collide-me").settings(gated())).actionGet().isAcknowledged());
        assertNotNull("the gated index must exist before this asserts anything", AbsentIndexDescriptorSuppliers.supply("collide-me"));

        Exception refused = null;
        try {
            client().admin().indices().create(new CreateIndexRequest("collide-me").settings(ordinary())).actionGet();
        } catch (Exception e) {
            refused = e;
        }

        boolean inClusterState = client().admin().cluster().prepareState().get().getState().metadata().hasIndex("collide-me");
        var descriptor = AbsentIndexDescriptorSuppliers.supply("collide-me");
        logger.warn(
            "COLLISION: ordinary creation over a gated name -> refused={}, in cluster state={}, descriptor still live={}",
            refused == null ? "no" : refused.getClass().getSimpleName(),
            inClusterState,
            descriptor != null && descriptor.exists()
        );

        assertNotNull(
            "one name must not be claimable in both planes: the descriptor store holds [collide-me] and "
                + "cluster state was allowed to claim it as well, so the name now resolves to whichever "
                + "plane the resolver consults first and the other claim is invisible until it is not",
            refused
        );
    }

    /**
     * The other order, which is refused, and which is what makes the case above a finding rather than a
     * property of the design.
     *
     * <p>A gated creation validates its name against cluster state like any other, so an existing ordinary
     * index of that name stops it. Only one of the two orders is open, and it is the one where the
     * authority that already holds the name is the one nobody asks.
     */
    public void testCreatingAGatedIndexOverAnOrdinaryNameIsRefused() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        assertTrue(
            client().admin().indices().create(new CreateIndexRequest("ordinary-first").settings(ordinary())).actionGet().isAcknowledged()
        );

        expectThrows(
            org.opensearch.ResourceAlreadyExistsException.class,
            () -> client().admin().indices().create(new CreateIndexRequest("ordinary-first").settings(gated())).actionGet()
        );
        assertNull(
            "and nothing may have been recorded in the descriptor store for a creation that was refused",
            AbsentIndexDescriptorSuppliers.supply("ordinary-first")
        );
    }

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
