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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.ReaderPlacement;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A search for a shard whose placement-preferred nodes are full is answered by a search node with room, not refused.
 *
 * <p>Placement is a cache affinity, not a requirement. A fleet's stale-read check searched thousands of indices in a
 * burst; the preferred nodes filled, refused at their shard cap, and the searches failed outright while other nodes had
 * slots free.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessSearchSpillTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings settings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-spill")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .build();
    }

    public void testASearchSpillsPastFullPreferredNodes() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("spill", "uuid-spill-000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("filler", "uuid-filler-00000000", 1, MAPPING, null));

        try (ServerlessNode writer = new ServerlessNode(settings("spill-writer", "ingest"))) {
            writer.start();
            writer.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
            loop.want("spill", 0);
            loop.want("filler", 0);
            loop.tick(clock.get());
            assertEquals(201, send(writer, "PUT", "/spill/_doc/1", "{\"msg\":\"needle\"}").status());
            assertEquals(201, send(writer, "PUT", "/filler/_doc/1", "{\"msg\":\"hay\"}").status());
            loop.publishAll();
        }

        final List<ServerlessNode> searchers = new ArrayList<>();
        try {
            for (String name : List.of("spill-a", "spill-b", "spill-c")) {
                final ServerlessNode node = new ServerlessNode(settings(name, "search"));
                searchers.add(node);
                node.start();
                node.setMetadataPlane(plane);
                node.heartbeat(plane);
            }
            final List<String> ranked = ReaderPlacement.candidatesFor(
                "spill",
                0,
                plane.membership().refresh(),
                ServerlessNode.ROLE_SEARCH,
                3
            );
            assertEquals(3, ranked.size());
            // The two preferred nodes hold one shard each, the cap, and will not evict it.
            for (String full : ranked.subList(0, 2)) {
                final ServerlessNode node = byId(searchers, full);
                new BackgroundReconciler(node, plane).setMaxShardsHeld(1).setEvictAfterMillis(0L);
                node.serveAsReader(plane, "filler", 0);
            }
            final ServerlessNode coordinator = byId(searchers, ranked.get(0));

            final Response found = send(coordinator, "POST", "/spill/_search", "{\"query\":{\"match\":{\"msg\":\"needle\"}}}");
            assertEquals(found.body(), 200, found.status());
            assertTrue("answered by the node with room: " + found.body(), found.body().contains("\"value\":1"));
            assertTrue(
                "which holds it now",
                byId(searchers, ranked.get(2)).reconciler().openShards().stream().anyMatch(s -> s.getIndexName().equals("spill"))
            );
        } finally {
            for (ServerlessNode node : searchers) {
                node.close();
            }
        }
    }

    private static ServerlessNode byId(List<ServerlessNode> nodes, String id) {
        return nodes.stream().filter(n -> n.localNode().getId().equals(id)).findFirst().orElseThrow();
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8)
                )
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
