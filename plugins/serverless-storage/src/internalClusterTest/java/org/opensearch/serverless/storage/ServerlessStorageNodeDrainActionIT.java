/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.nodecapacity.action.NodeDrainAction;
import org.opensearch.serverless.storage.nodecapacity.action.NodeDrainRequest;
import org.opensearch.serverless.storage.nodecapacity.action.NodeWarmupAction;
import org.opensearch.serverless.storage.nodecapacity.action.NodeWarmupRequest;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

/**
 * The {@code node_id} these two endpoints take is a raw path parameter, and the drain half is
 * destructive -- it excludes a node from allocation, moving every shard off it. The request's own
 * {@code validate()} can only check the id is non-empty, which says nothing about whether it names
 * anything; the membership check has to happen where cluster state is available, which is the
 * transport action running on the elected cluster-manager. These tests pin that: an id naming no
 * node in the cluster must fail as a clear client error, not be forwarded to the coordinator to
 * silently write an exclusion rule for a node name that will never match.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ServerlessStorageNodeDrainActionIT extends ServerlessStorageIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    public void testDrainRejectsANodeIdThatIsNotInTheCluster() {
        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(NodeDrainAction.INSTANCE, new NodeDrainRequest("no-such-node-id", true)).get()
        );
        assertTrue(
            "an unknown node id must be a client error, not an accepted no-op: " + failure.getCause(),
            failure.getCause() instanceof IllegalArgumentException
        );
        assertTrue(failure.getCause().getMessage(), failure.getCause().getMessage().contains("no such node"));
    }

    /** Cancelling a drain of a node that doesn't exist is just as meaningless as starting one. */
    public void testDrainCancelRejectsANodeIdThatIsNotInTheCluster() {
        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(NodeDrainAction.INSTANCE, new NodeDrainRequest("no-such-node-id", false)).get()
        );
        assertTrue(failure.getCause() instanceof IllegalArgumentException);
        assertTrue(failure.getCause().getMessage(), failure.getCause().getMessage().contains("no such node"));
    }

    public void testWarmupRejectsANodeIdThatIsNotInTheCluster() {
        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(NodeWarmupAction.INSTANCE, new NodeWarmupRequest("no-such-node-id", true)).get()
        );
        assertTrue(failure.getCause() instanceof IllegalArgumentException);
        assertTrue(failure.getCause().getMessage(), failure.getCause().getMessage().contains("no such node"));
    }

    /** An empty id never even reaches the cluster-manager -- the request's own validate() stops it. */
    public void testDrainRejectsAnEmptyNodeId() {
        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(NodeDrainAction.INSTANCE, new NodeDrainRequest("", true)).get()
        );
        assertTrue(failure.getCause().getMessage(), failure.getCause().getMessage().contains("node_id is required"));
    }

    /**
     * A real node id must still be accepted, so the guard isn't simply refusing everything. Uses
     * the warmup half rather than drain: it exercises the identical membership check but only sets
     * and clears a marker, where drain would move real allocation off the cluster's only data node.
     */
    public void testWarmupAcceptsARealNodeId() throws Exception {
        String realNodeId = client().admin().cluster().prepareState().get().getState().nodes().getDataNodes().keySet().iterator().next();

        assertTrue(client().execute(NodeWarmupAction.INSTANCE, new NodeWarmupRequest(realNodeId, true)).get().isAcknowledged());
        assertTrue(client().execute(NodeWarmupAction.INSTANCE, new NodeWarmupRequest(realNodeId, false)).get().isAcknowledged());
    }
}
