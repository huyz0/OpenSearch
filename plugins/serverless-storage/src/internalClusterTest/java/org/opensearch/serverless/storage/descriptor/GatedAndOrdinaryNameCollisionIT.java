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
 * One name, one plane, and no agreement needed between the two authorities to keep it that way.
 *
 * <h2>What this used to be</h2>
 *
 * An {@code AwaitsFix} recording a defect. A gated index's name was held by a register compare-and-swap in
 * the descriptor store and an ordinary index's by cluster state, gating was decided by a *setting*, so the
 * same name could be claimed in both planes -- and was, sequentially, with no concurrency involved at all.
 * Creating a gated {@code x} and then an ordinary {@code x} succeeded twice. Resolution consults metadata
 * before the descriptor supplier, so the ordinary index shadowed the gated one from that moment: the client
 * that created the gated index was told it existed and could no longer address it. Nothing reconciled the
 * two, so it did not converge; deleting the ordinary index brought the name back as a different index with a
 * different uuid and no data.
 *
 * <p>The fix that was obvious then was to have {@code validateIndexName} ask the descriptor store, and it
 * could not go where the check is: that runs inside the cluster state update task, where a blocking
 * descriptor read is the deadlock W4 paid for.
 *
 * <h2>What closed it</h2>
 *
 * Neither authority asks the other, and neither needs to. The {@code serverless_} namespace partitions the
 * names: {@code DescriptorGate#gatable} refuses a name outside it, so no descriptor can exist for one, and
 * {@code MetadataCreateIndexService#clusterStateCreateIndex} refuses a name inside it, so no cluster state
 * entry can exist for one. Both checks are string comparisons on the name, which is why they can run where
 * the descriptor read could not.
 *
 * <p>This asserts both directions, because a partition that holds in one direction is the defect this file
 * was opened for.
 */
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

    /**
     * The direction that was open: a gated name, then an ordinary creation of it.
     *
     * <p>It is not merely refused now -- it is unaskable. The second creation carries no serverless setting
     * and asks for an ordinary index, and it is still routed into the descriptor plane, because the name is
     * what decides. So it meets the register compare-and-swap that already holds the name and comes back
     * {@link org.opensearch.ResourceAlreadyExistsException}, which is what a duplicate has always been told.
     */
    public void testAnOrdinaryCreationCannotTakeAGatedName() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        assertTrue(
            client().admin()
                .indices()
                .create(new CreateIndexRequest("serverless_collide-me").settings(gated()))
                .actionGet()
                .isAcknowledged()
        );
        assertNotNull(
            "the gated index must exist before this asserts anything",
            AbsentIndexDescriptorSuppliers.supply("serverless_collide-me")
        );

        expectThrows(
            org.opensearch.ResourceAlreadyExistsException.class,
            () -> client().admin().indices().create(new CreateIndexRequest("serverless_collide-me").settings(ordinary())).actionGet()
        );

        assertFalse(
            "and nothing may have reached cluster state under that name, which is what shadowed the "
                + "descriptor and made the name change identity under whoever was writing to it",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex("serverless_collide-me")
        );
        assertNotNull(
            "the descriptor is still the one thing holding the name",
            AbsentIndexDescriptorSuppliers.supply("serverless_collide-me")
        );
    }

    /**
     * The direction that was already closed, kept because a partition needs both halves and because the
     * reason this one closes has changed.
     *
     * <p>It used to close incidentally: a gated creation validated its name against cluster state like any
     * other, so an existing ordinary index stopped it. Now it cannot arise at all -- an ordinary index can
     * never hold a name in the namespace to begin with, which is what the first assertion here checks.
     */
    public void testAGatedNameCanNeverHaveBeenClaimedByAnOrdinaryIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        // An ordinary index outside the namespace, which is the only kind there is.
        assertTrue(
            client().admin().indices().create(new CreateIndexRequest("ordinary-first").settings(ordinary())).actionGet().isAcknowledged()
        );
        assertTrue(
            "an index outside the namespace must not be gated however it was created, and being in cluster "
                + "state is what not being gated means",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex("ordinary-first")
        );

        // And a gated creation of that same ordinary name is refused, because the name is not in the
        // namespace and so the descriptor plane will not take it either -- it is an ordinary duplicate.
        expectThrows(
            org.opensearch.ResourceAlreadyExistsException.class,
            () -> client().admin().indices().create(new CreateIndexRequest("ordinary-first").settings(gated())).actionGet()
        );
        assertTrue(
            "and the refused creation must not have disturbed the index that already held the name",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex("ordinary-first")
        );
    }

    /**
     * The remaining way a namespaced name could have reached cluster state, which is the one the fallback
     * used to take: an index the gate declines. A filtered alias is the original reason
     * {@code DescriptorRepresentable} refuses to gate an index, so it is the case to drive.
     *
     * <p>Refused rather than created ordinary. This is the assertion that makes the partition a partition
     * rather than a convention -- without it, any index the gate declines walks straight into the other
     * plane under a name a descriptor may already hold.
     */
    public void testANamespacedIndexTheGateWouldDeclineIsRefusedOutright() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        Exception refused = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .create(
                    new CreateIndexRequest("serverless_wants-an-alias").settings(gated())
                        .alias(new org.opensearch.action.admin.indices.alias.Alias("an-alias"))
                )
                .actionGet()
        );
        logger.info("namespaced index with an alias refused with: {}", refused.toString());

        assertFalse(
            "an index in the namespace that cannot be gated must not be created ordinary instead",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex("serverless_wants-an-alias")
        );
        assertNull(
            "and it must not exist as a descriptor either -- refused means refused in both planes",
            AbsentIndexDescriptorSuppliers.supply("serverless_wants-an-alias")
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
