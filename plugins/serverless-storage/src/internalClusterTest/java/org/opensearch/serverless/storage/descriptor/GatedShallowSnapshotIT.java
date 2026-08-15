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
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinRequest;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Whether the shallow-snapshot surface reaches a gated index. It does not, and this records why.
 *
 * <h2>What the plugin already has</h2>
 *
 * A shallow snapshot, in the sense the question means: {@code _snapshot_pin} writes a durable pin naming a
 * manifest generation and copies nothing, because the bundles that generation refers to are already in the
 * object store. {@code _snapshot_restore} compare-and-swaps the shard head back to a pinned generation, and
 * {@code _snapshot_release} drops the pin so GC can reclaim what it was holding. There is no data movement
 * anywhere in that, which is what makes it cheap enough to matter at this branch's target population.
 *
 * <h2>What this measures</h2>
 *
 * The index-level orchestration ({@code IndexSnapshotPinAction} and its restore and release siblings) is the
 * layer a user actually calls, and it resolves the index name through {@code
 * clusterService.state().metadata().index(name)} to get a uuid and a shard count. For a gated index that
 * returns null, so the request fails with {@code IndexNotFoundException} for an index that exists and is
 * serving traffic. Exactly the shape the snapshot path had before its refusal was made honest, and exactly
 * the shape that {@code AbsentIndexDescriptorSuppliers} exists to fix.
 *
 * <p>The shard-level actions underneath take a uuid and a shard id directly and never consult cluster state,
 * so the mechanism itself is indifferent to gating. Only the name-to-uuid step is not. That is a small,
 * well-shaped gap rather than a missing feature, which is why it is worth pinning precisely: the difference
 * between "shallow snapshots do not work for gated indices" and "one resolution call reads the wrong
 * authority" is the difference between a project and an afternoon.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GatedShallowSnapshotIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

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

    public void testPinningAGatedIndexFailsBecauseTheNameIsResolvedAgainstClusterStateOnly() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        assertTrue(
            client().admin().indices().create(new CreateIndexRequest("serverless_pin-me").settings(gated())).actionGet().isAcknowledged()
        );
        // Alive and flushed, so it has a manifest generation to pin and the failure below cannot be read as
        // "there was nothing to snapshot".
        assertBusy(() -> {
            try {
                client().prepareIndex("serverless_pin-me").setId("1").setSource("f", "v").get();
            } catch (Exception e) {
                throw new AssertionError("write not yet servable: " + e.getMessage(), e);
            }
        }, 60, TimeUnit.SECONDS);
        client().admin().indices().prepareFlush("serverless_pin-me").get();

        Exception failure = expectThrows(
            Exception.class,
            () -> client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest("serverless_pin-me", "pin-1")).get()
        );
        logger.info("pinning a gated index -> {}", failure.toString());

        assertTrue(
            "the index-level pin resolves its name against cluster state only, so a gated index -- which "
                + "exists, is serving traffic, and has manifests to pin -- is reported missing: "
                + failure.toString(),
            failure.toString().contains("IndexNotFoundException") || failure.toString().contains("no such index")
        );
    }

    /**
     * The control, and the reason the gap is narrow rather than deep: the identical call against an index
     * that uses the same storage engine and is <em>not</em> gated succeeds. Nothing about the pin mechanism
     * cares; only the name resolution in front of it does.
     */
    public void testPinningAnUngatedServerlessIndexSucceeds() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        assertTrue(
            client().admin().indices().create(new CreateIndexRequest("ungated-pin-me").settings(gated())).actionGet().isAcknowledged()
        );
        ensureGreen("ungated-pin-me");
        client().prepareIndex("ungated-pin-me").setId("1").setSource("f", "v").get();
        client().admin().indices().prepareFlush("ungated-pin-me").get();

        assertEquals(
            "one shard pinned, with no data copied anywhere -- the pin names a manifest generation that is "
                + "already in the object store, which is what makes this shallow",
            1,
            client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest("ungated-pin-me", "pin-1")).get().shardCount()
        );
    }

    private static Settings gated() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }
}
