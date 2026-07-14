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
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.resharding.action.CutoverSplitRoutingAction;
import org.opensearch.serverless.storage.resharding.action.CutoverSplitRoutingRequest;
import org.opensearch.serverless.storage.resharding.action.EnableWritePartitionRoutingAction;
import org.opensearch.serverless.storage.resharding.action.EnableWritePartitionRoutingRequest;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Proves {@link EnableWritePartitionRoutingAction}/{@code WritePartitionRoutingActionFilter}
 * genuinely route real write traffic against a write-routing-enabled alias to the one correct real
 * target partition over the real transport layer, in a real cluster (rfc-serverless-opensearch.md
 * &sect;16 Phase 4's "real write-side partition routing" gap).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageWritePartitionRoutingActionIT extends ServerlessStorageIntegTestCase {

    @Override
    protected java.util.Collection<Class<? extends Plugin>> nodePlugins() {
        return java.util.Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testWritesAgainstTheAliasLandInTheirOwnCorrectRealPartitionOnly() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            "write-routing-target-a",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        createIndex(
            "write-routing-target-b",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
        );
        ensureGreen("write-routing-target-a", "write-routing-target-b");

        List<String> targets = List.of("write-routing-target-a", "write-routing-target-b");

        AcknowledgedResponse cutoverResponse = client().execute(
            CutoverSplitRoutingAction.INSTANCE,
            new CutoverSplitRoutingRequest("write-routing-alias", targets)
        ).get();
        assertTrue("the search-only cutover must be acknowledged first", cutoverResponse.isAcknowledged());

        AcknowledgedResponse enableResponse = client().execute(
            EnableWritePartitionRoutingAction.INSTANCE,
            new EnableWritePartitionRoutingRequest("write-routing-alias", targets)
        ).get();
        assertTrue("enabling write-partition-routing must be acknowledged", enableResponse.isAcknowledged());

        // Index a batch of documents with distinct explicit ids against the alias -- not either
        // target index by name -- and independently predict, via the exact same Murmur3 hash
        // RoutingPartitionFilter already uses read-side, which real target each one must land in.
        List<String> idsExpectedInA = new ArrayList<>();
        List<String> idsExpectedInB = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            String id = "doc-" + i;
            int partition = Math.floorMod(Murmur3HashFunction.hash(id), 2);
            (partition == 0 ? idsExpectedInA : idsExpectedInB).add(id);
            client().prepareIndex("write-routing-alias").setId(id).setSource("field", "value").get();
        }
        // A real split with only one document landing in a given partition is uninteresting --
        // assert the test itself actually exercises both real targets before trusting its result.
        assertFalse("test setup must produce documents in both partitions", idsExpectedInA.isEmpty());
        assertFalse("test setup must produce documents in both partitions", idsExpectedInB.isEmpty());

        refresh("write-routing-target-a", "write-routing-target-b");

        for (String id : idsExpectedInA) {
            boolean existsInA = client().prepareGet("write-routing-target-a", id).get().isExists();
            assertTrue("doc [" + id + "] must be routed into write-routing-target-a", existsInA);
            assertFalse(
                "doc [" + id + "] must not have also landed in write-routing-target-b",
                client().prepareGet("write-routing-target-b", id).get().isExists()
            );
        }
        for (String id : idsExpectedInB) {
            boolean existsInB = client().prepareGet("write-routing-target-b", id).get().isExists();
            assertTrue("doc [" + id + "] must be routed into write-routing-target-b", existsInB);
            assertFalse(
                "doc [" + id + "] must not have also landed in write-routing-target-a",
                client().prepareGet("write-routing-target-a", id).get().isExists()
            );
        }

        assertHitCount(client().prepareSearch("write-routing-alias").setSize(0).get(), 20);
    }
}
