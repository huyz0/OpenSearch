/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.ReaderPlacement;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.SearchReadiness;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A search node that joins to warm stays out of reader placement until it is ready, by leaving the search role off its
 * lease, and leaves it again when it drains. With warm-up off, nothing changes.
 */
public class SearchReadinessTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private Settings nodeSettings(String name, boolean warmOnJoin) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-readiness")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "search")
            .put(SearchReadiness.WARM_ON_JOIN, warmOnJoin)
            .build();
    }

    private static Set<String> advertised(MetadataPlane plane, String nodeId) throws Exception {
        plane.membership().refresh();
        return plane.membership().current().stream().filter(l -> l.nodeId().equals(nodeId)).findFirst().map(NodeLease::roles).orElseThrow();
    }

    public void testAWarmingNodeIsNotAReaderUntilReadyAndStopsBeingOneWhenItDrains() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            try (ServerlessNode node = new ServerlessNode(nodeSettings("warming", true))) {
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
                final String id = node.localNode().getId();
                final SearchReadiness readiness = SearchReadiness.of(node);

                assertEquals(SearchReadiness.State.WARMING, readiness.state());
                assertEquals("a member, not a reader", Set.of(), advertised(plane, id));
                assertEquals(
                    "no shard ranks it",
                    java.util.List.of(),
                    ReaderPlacement.candidatesFor("alpha", 0, plane.membership().current(), ServerlessNode.ROLE_SEARCH, 2)
                );

                readiness.markReady();
                clock.addAndGet(TTL / 3);
                node.leases().renewLease(plane);
                assertEquals(Set.of(ServerlessNode.ROLE_SEARCH), advertised(plane, id));
                assertEquals(
                    java.util.List.of(id),
                    ReaderPlacement.candidatesFor("alpha", 0, plane.membership().current(), ServerlessNode.ROLE_SEARCH, 2)
                );

                readiness.startDraining();
                readiness.markReady();
                assertEquals("a draining node does not turn ready again", SearchReadiness.State.DRAINING, readiness.state());
                clock.addAndGet(TTL / 3);
                node.leases().renewLease(plane);
                assertEquals(Set.of(), advertised(plane, id));
            }
        }
    }

    public void testWithWarmUpOffASearchNodeIsReadyFromTheStart() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        try (FsBlobStore store = new FsBlobStore(8192, createTempDir(), false)) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            try (ServerlessNode node = new ServerlessNode(nodeSettings("plain", false))) {
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
                assertTrue(SearchReadiness.of(node).ready());
                assertEquals(Set.of(ServerlessNode.ROLE_SEARCH), advertised(plane, node.localNode().getId()));
            }
        }
    }
}
