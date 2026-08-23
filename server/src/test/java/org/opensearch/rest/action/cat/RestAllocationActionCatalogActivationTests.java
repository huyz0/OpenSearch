/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.cluster.metadata.IndexCatalog;
import org.opensearch.cluster.metadata.IndexCatalogRegistry;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.junit.After;

import java.util.concurrent.atomic.AtomicReference;

/**
 * {@code _cat/allocation} decides <em>what to request</em> before any {@link
 * org.opensearch.cluster.ClusterState} exists: an index whose routing is computed lives only in metadata,
 * and metadata is the expensive part of a cluster state response, so it is asked for only when computed
 * placement is actually on. This is one of the two call sites that made {@link IndexCatalog#isActive()}
 * mandatory -- there is no cluster state here to carry an attached resolver, so nothing but a node-scoped
 * question can be asked at this point.
 *
 * <p>The other request-shaping site, {@code TransportCatShardsAction}, makes the identical decision from the
 * identical predicate; it is not exercised here because constructing a transport action needs a transport
 * service, a task and a cancellation-timeout wrapper, none of which this decision touches.
 */
public class RestAllocationActionCatalogActivationTests extends OpenSearchTestCase {

    @After
    public void clearRegistry() {
        IndexCatalogRegistry.register(null);
    }

    public void testMetadataIsNotRequestedWhenNoCatalogIsRegistered() throws Exception {
        assertFalse("an ordinary cluster must keep the smaller response it has always had", capturedRequest().metadata());
    }

    /**
     * The distinction the whole seam turns on: a plugin supplies its catalog unconditionally at node
     * startup, so "a catalog is registered" would make every such node pay for metadata forever. Only
     * {@code isActive()} says whether the feature is on right now.
     */
    public void testMetadataIsNotRequestedForARegisteredButInactiveCatalog() throws Exception {
        IndexCatalogRegistry.register(catalog(false));

        assertFalse("a registered-but-inactive catalog must not widen the request", capturedRequest().metadata());
    }

    public void testMetadataIsRequestedWhenTheCatalogIsActive() throws Exception {
        IndexCatalogRegistry.register(catalog(true));

        assertTrue("without metadata the listing silently omits every computed shard", capturedRequest().metadata());
    }

    /** Drives the real handler and captures the {@link ClusterStateRequest} it would have sent. */
    private ClusterStateRequest capturedRequest() throws Exception {
        AtomicReference<ClusterStateRequest> captured = new AtomicReference<>();
        try (NoOpNodeClient client = new NoOpNodeClient("cat-allocation-catalog-activation") {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                captured.set((ClusterStateRequest) request);
                // Deliberately no onResponse: the request itself is the whole assertion here, and
                // completing it would drag the table-rendering path in for no added coverage.
            }
        }) {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
            // Driven through the public entry point rather than doCatRequest(...).accept(...): the
            // RestChannelConsumer that returns is a protected nested interface of BaseRestHandler and is
            // not accessible from this package. handleRequest is also the path a real request takes.
            new RestAllocationAction().handleRequest(request, new FakeRestChannel(request, true, 1), client);
        }
        ClusterStateRequest clusterStateRequest = captured.get();
        assertNotNull("the handler must have issued a cluster state request", clusterStateRequest);
        return clusterStateRequest;
    }

    private static IndexCatalog catalog(boolean active) {
        return () -> active;
    }
}
