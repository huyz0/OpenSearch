/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * What happens to gated mappings when {@code .opensearch-index-mappings} is deleted under a live cluster.
 *
 * <h2>Why this is its own class</h2>
 *
 * It deletes the mapping index, and an internal cluster test's cluster is shared across the methods of a
 * class. Any later method that creates a mapped gated index then finds the store's cached "the index exists"
 * flag still true, skips the create, and lets auto-creation rebuild the index at cluster defaults -- so a
 * geometry assertion reads 1 shard where the node asked for 3, and the failure lands in a method that did
 * nothing wrong. That is what happened when this test lived beside the others: the mutation run failed two
 * methods, only one of which was the one under test.
 *
 * <p>It also needs the plugin's own install path, which is where the watcher carrying the signal is
 * registered. Every other IT in this package installs the gate by hand with a store whose signal is
 * permanently off, so a test written there would pass while proving nothing.
 *
 * <h2>What it asserts</h2>
 *
 * That the write fails. Not a nice outcome, and the only honest one: the declared fields are gone, this node
 * cannot get them back, and the alternative is silently replacing them with whichever field the next
 * document happens to carry -- which is what the code did before T48, successfully and with an
 * acknowledgement.
 */
public class GatedMappingIndexLossIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private static volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (GatedMappingIndexLossIT.class) {
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
        return List.of(ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath().toString())
            // The plugin installs the gate, and with it the watcher, only when this is on. That is the whole
            // point of this class: the signal has to be the one production wires, not one a test passes in.
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_NODE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    public void testAMappingWriteAfterTheMappingIndexIsDeletedFailsRatherThanRewriting() throws Exception {
        client().admin()
            .indices()
            .create(
                new CreateIndexRequest("gated-lost-mappings").settings(gated())
                    .mapping(Map.of("properties", Map.of("tenant", Map.of("type", "keyword"))))
            )
            .actionGet();

        client().admin().indices().prepareDelete(IndexBackedMappingStore.MAPPING_INDEX).get();
        assertBusy(
            () -> assertFalse(
                "the deletion has to be visible in cluster state before the watcher can have seen it",
                client().admin().cluster().prepareState().get().getState().metadata().hasIndex(IndexBackedMappingStore.MAPPING_INDEX)
            )
        );

        Exception failure = expectThrows(
            Exception.class,
            () -> client().admin().indices().preparePutMapping("gated-lost-mappings").setSource("amount", "type=double").get()
        );

        String message = ExceptionsHelper.unwrapCause(failure).getMessage();
        assertNotNull(message);
        assertTrue(
            "the failure must name the lost store rather than reading as an ordinary mapping error: " + message,
            message.contains("was lost while this node was running")
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
