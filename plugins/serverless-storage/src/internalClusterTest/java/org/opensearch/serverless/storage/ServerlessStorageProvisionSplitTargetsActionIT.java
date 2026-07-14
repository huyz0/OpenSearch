/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.resharding.action.ProvisionSplitTargetsAction;
import org.opensearch.serverless.storage.resharding.action.ProvisionSplitTargetsRequest;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Proves {@link ProvisionSplitTargetsAction} genuinely creates real, caller-named target indices
 * inheriting the source's settings and mapping -- the auto-provisioning primitive
 * rfc-serverless-opensearch.md &sect;16 Phase 4's auto-split-controller gap identified as missing,
 * built to mirror core's own {@code POST /{index}/_split/{target}} contract (caller always names
 * every target, never an algorithm), not the earlier draft that generated names itself.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageProvisionSplitTargetsActionIT extends ServerlessStorageIntegTestCase {

    @Override
    protected java.util.Collection<Class<? extends Plugin>> nodePlugins() {
        return java.util.Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testCreatesCallerNamedTargetsInheritingSourceSettingsAndMapping() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            "provision-source",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        client().admin().indices().preparePutMapping("provision-source").setSource("greeting", "type=keyword").get();
        ensureGreen("provision-source");

        List<String> targetNames = List.of("provision-target-a", "provision-target-b");
        AcknowledgedResponse response = client().execute(
            ProvisionSplitTargetsAction.INSTANCE,
            new ProvisionSplitTargetsRequest("provision-source", targetNames)
        ).get();
        assertTrue("provisioning must be acknowledged", response.isAcknowledged());

        ensureGreen("provision-target-a", "provision-target-b");

        for (String targetName : targetNames) {
            IndexMetadata targetMetadata = client().admin().cluster().prepareState().get().getState().metadata().index(targetName);
            assertEquals(
                "each target must always be exactly one shard, matching ShardSplitter's per-shard model",
                1,
                targetMetadata.getNumberOfShards()
            );
            assertEquals("replicas must be inherited from the source, not defaulted", 0, targetMetadata.getNumberOfReplicas());
            Object mappingProperties = targetMetadata.mapping().sourceAsMap().get("properties");
            assertTrue(
                "the source's own mapping must be inherited onto the target, not left empty",
                mappingProperties instanceof Map && ((Map<?, ?>) mappingProperties).containsKey("greeting")
            );
        }

        // Both targets must be real, independently writable/searchable indices -- not stubs.
        client().prepareIndex("provision-target-a").setId("1").setSource("greeting", "hello").get();
        client().prepareIndex("provision-target-b").setId("1").setSource("greeting", "hi").get();
        refresh("provision-target-a", "provision-target-b");
        assertHitCount(client().prepareSearch("provision-target-a").setSize(0).get(), 1);
        assertHitCount(client().prepareSearch("provision-target-b").setSize(0).get(), 1);
    }

    public void testRefusesWhenATargetIndexAlreadyExists() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            "provision-source-2",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        createIndex(
            "already-exists-target",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen("provision-source-2", "already-exists-target");

        Exception failure = expectThrows(
            Exception.class,
            () -> client().execute(
                ProvisionSplitTargetsAction.INSTANCE,
                new ProvisionSplitTargetsRequest("provision-source-2", List.of("already-exists-target", "brand-new-target"))
            ).get()
        );
        assertTrue(
            "the refusal must name the actual conflicting index, not some other failure: " + failure,
            causedByMessageContaining(failure, "already exists")
        );

        // A refused provisioning must never partially apply.
        assertFalse(
            "the not-yet-existing target must never have been created by a refused call",
            client().admin().cluster().prepareState().get().getState().metadata().hasIndex("brand-new-target")
        );
    }

    private static boolean causedByMessageContaining(Throwable error, String substring) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains(substring)) {
                return true;
            }
        }
        return false;
    }
}
