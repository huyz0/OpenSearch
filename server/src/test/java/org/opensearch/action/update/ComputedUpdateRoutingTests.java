/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.update;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.AutoCreateIndex;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.AbsentIndexRoutingSuppliers;
import org.opensearch.cluster.routing.ComputedShardRouting;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.SystemIndices;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;
import org.junit.After;

import java.util.Set;

import static org.mockito.Mockito.mock;

/**
 * C22. Whether an update that already knows its shard can reach a computed index.
 *
 * <p><b>Why this is a unit test when the rest of C22 is an integration test.</b> The integration suite
 * covers the two branches a client can reach directly, and both pass: an update routed by document id
 * resolves through {@code OperationRouting}, and a bulk update never enters this action at all. The
 * branch tested here is only reachable on a retry. {@code TransportInstanceSingleOperationAction} sets
 * {@code request.shardId} after {@code shards()} has already succeeded once, so a second attempt after a
 * remote failure re-enters with the shard id populated and takes the direct lookup.
 *
 * <p>That window is real but narrow, and no integration test written so far reaches it. Rather than
 * convert the call site on the strength of reading it, which is what has been wrong repeatedly in this
 * area, the branch is exercised directly. Being honest about the difference matters: this proves the
 * branch behaves, not that a user request travels through it.
 *
 * <p>The failure it prevents is a hang rather than an error. An empty iterator is how this action says
 * "not allocated yet, try again", so against a computed index, whose routing will never be published,
 * the caller retries until it gives up.
 */
public class ComputedUpdateRoutingTests extends OpenSearchTestCase {

    private static final String INDEX = "computed-update";

    @After
    public void clearRegistrations() {
        AbsentIndexRoutingSuppliers.register(null);
        AbsentIndexRoutingSuppliers.registerUnpublished(null);
    }

    /** The load-bearing one: a supplied entry must produce a routable primary rather than nothing. */
    public void testRetriedUpdateResolvesComputedRouting() {
        ClusterState state = stateWithoutRouting();
        AbsentIndexRoutingSuppliers.registerUnpublished(metadata -> true);
        AbsentIndexRoutingSuppliers.register((s, meta) -> computedEntry(meta));

        ShardIterator iterator = updateAction().shards(state, requestForShard(new ShardId(state.metadata().index(INDEX).getIndex(), 0)));

        assertEquals("a computed index must offer exactly one primary to an update that knows its shard", 1, iterator.size());
        assertEquals("the update must be routed to the node the placement names", "node-1", iterator.nextOrNull().currentNodeId());
    }

    /**
     * The control. With nothing installed the behaviour Phase A shipped must be untouched, because an
     * empty iterator is a legitimate answer for an index that really has no routing yet and the caller
     * depends on it to wait rather than fail.
     */
    public void testWithoutASupplierTheEmptyIteratorIsUnchanged() {
        ClusterState state = stateWithoutRouting();

        ShardIterator iterator = updateAction().shards(state, requestForShard(new ShardId(state.metadata().index(INDEX).getIndex(), 0)));

        assertEquals("an index with no routing and no supplier must still yield no shards", 0, iterator.size());
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Carries a shard id and a concrete index the way a retried request does. Both are set on the
     * request by machinery a unit test cannot easily drive, so they are supplied by overriding instead.
     */
    private static final class RetriedUpdateRequest extends UpdateRequest {
        private final String concreteIndex;

        RetriedUpdateRequest(String index, String id, ShardId shardId) {
            super(index, id);
            this.shardId = shardId;
            this.concreteIndex = index;
        }

        @Override
        public String concreteIndex() {
            return concreteIndex;
        }
    }

    private static RetriedUpdateRequest requestForShard(ShardId shardId) {
        return new RetriedUpdateRequest(INDEX, "1", shardId);
    }

    private static TransportUpdateAction updateAction() {
        return new TransportUpdateAction(
            mock(ThreadPool.class),
            mock(ClusterService.class),
            mock(TransportService.class),
            mock(UpdateHelper.class),
            new ActionFilters(Set.of()),
            mock(IndexNameExpressionResolver.class),
            mock(IndicesService.class),
            new AutoCreateIndex(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                mock(IndexNameExpressionResolver.class),
                new SystemIndices(java.util.Map.of())
            ),
            mock(NodeClient.class)
        );
    }

    private static IndexRoutingTable computedEntry(IndexMetadata indexMetadata) {
        ShardId shard = new ShardId(indexMetadata.getIndex(), 0);
        return IndexRoutingTable.builder(indexMetadata.getIndex())
            .addIndexShard(
                new IndexShardRoutingTable.Builder(shard).addShard(
                    ComputedShardRouting.started(shard, "node-1", RecoverySource.EmptyStoreRecoverySource.INSTANCE)
                ).build()
            )
            .build();
    }

    private static ClusterState stateWithoutRouting() {
        IndexMetadata metadata = IndexMetadata.builder(INDEX)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, INDEX + "-uuid-0000000000")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(metadata, false).build()).build();
    }
}
