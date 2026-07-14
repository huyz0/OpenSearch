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
import org.opensearch.serverless.storage.resharding.action.CutoverSplitRoutingAction;
import org.opensearch.serverless.storage.resharding.action.CutoverSplitRoutingRequest;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Proves {@link CutoverSplitRoutingAction} genuinely routes real search traffic across a shard
 * split's real targets over the real transport layer, in a real cluster -- and that it never
 * touches anything but the alias it creates (rfc-serverless-opensearch.md &sect;16 Phase 4's own
 * "additive, not destructive" scoping decision).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageCutoverSplitRoutingActionIT extends ServerlessStorageIntegTestCase {

    @Override
    protected java.util.Collection<Class<? extends Plugin>> nodePlugins() {
        return java.util.Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testCutoverAliasSearchesAcrossBothRealSplitTargets() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            "cutover-target-a",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        createIndex(
            "cutover-target-b",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen("cutover-target-a", "cutover-target-b");

        client().prepareIndex("cutover-target-a").setId("a-1").setSource("field", "value").get();
        client().prepareIndex("cutover-target-b").setId("b-1").setSource("field", "value").get();
        client().prepareIndex("cutover-target-b").setId("b-2").setSource("field", "value").get();
        refresh("cutover-target-a", "cutover-target-b");

        AcknowledgedResponse response = client().execute(
            CutoverSplitRoutingAction.INSTANCE,
            new CutoverSplitRoutingRequest("cutover-alias", List.of("cutover-target-a", "cutover-target-b"))
        ).get();
        assertTrue("the alias cutover must be acknowledged", response.isAcknowledged());

        // Real client-facing traffic against the new alias name must transparently reach both
        // real targets and return the union, not just one of them.
        assertHitCount(client().prepareSearch("cutover-alias").setSize(0).get(), 3);

        // Deliberately additive: both real target indices must still exist and be independently
        // queryable by their own names too -- this action must never have deleted or renamed
        // anything.
        assertTrue(client().admin().cluster().prepareState().get().getState().metadata().hasIndex("cutover-target-a"));
        assertTrue(client().admin().cluster().prepareState().get().getState().metadata().hasIndex("cutover-target-b"));
        assertHitCount(client().prepareSearch("cutover-target-a").setSize(0).get(), 1);
        assertHitCount(client().prepareSearch("cutover-target-b").setSize(0).get(), 2);
    }

    public void testRefusesCutoverToANonExistentTargetIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            "cutover-target-c",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen("cutover-target-c");

        Exception failure = expectThrows(
            Exception.class,
            () -> client().execute(
                CutoverSplitRoutingAction.INSTANCE,
                new CutoverSplitRoutingRequest("cutover-alias-2", List.of("cutover-target-c", "never-created-index"))
            ).get()
        );
        // This plugin's own pre-check must be what actually fired here, not merely core's own
        // generic alias-target-resolution error surfacing coincidentally -- confirmed the hard
        // way: an earlier version of this test passed even with that pre-check deliberately
        // disabled, since core's own IndicesAliasesRequest independently rejects a nonexistent
        // target too. Asserting this plugin's own specific message is what actually distinguishes
        // "this check does real, additional work" from "core would have caught it anyway."
        assertTrue(
            "the plugin's own pre-check message must be present somewhere in the cause chain, not just " + "some other failure: " + failure,
            causedByThisPluginsOwnPreCheck(failure)
        );

        // A refused cutover must never partially apply -- the alias must not exist against even
        // the one real target index named.
        assertFalse(
            "a refused cutover must never partially create the alias",
            client().admin().cluster().prepareState().get().getState().metadata().hasAlias("cutover-alias-2")
        );
    }

    private static boolean causedByThisPluginsOwnPreCheck(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains("refusing to route traffic to it")) {
                return true;
            }
        }
        return false;
    }
}
