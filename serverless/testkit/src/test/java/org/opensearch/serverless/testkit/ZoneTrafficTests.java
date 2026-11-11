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
import org.opensearch.serverless.cluster.ZonePreference;
import org.opensearch.serverless.cluster.Zones;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.transport.ForwardedSearchRequest;
import org.opensearch.serverless.transport.ZoneTraffic;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A node's zone is its stock awareness attribute, it travels in the lease, and the bytes a node exchanges with peers in
 * other zones are counted.
 *
 * <p>Runs against {@code FsBlobContainer} only.
 */
public class ZoneTrafficTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    public void testTheZoneIsTheAwarenessAttribute() {
        assertEquals("us-east-1a", Zones.localZone(Settings.builder().put("node.attr.zone", "us-east-1a").build()));
        assertNull("no attribute, no zone", Zones.localZone(Settings.EMPTY));
        assertNull("blank is unknown", Zones.localZone(Settings.builder().put("node.attr.zone", " ").build()));
        // A deployment that names its awareness attribute differently is read by that name.
        final Settings renamed = Settings.builder()
            .putList(Zones.AWARENESS_ATTRIBUTES, "az", "rack")
            .put("node.attr.az", "europe-west1-b")
            .put("node.attr.zone", "ignored")
            .build();
        assertEquals("az", Zones.attribute(renamed));
        assertEquals("europe-west1-b", Zones.localZone(renamed));
        assertTrue(Zones.same("a", "a"));
        assertFalse("unknown is never the same zone", Zones.same(null, null));
        assertFalse("nor a crossing", Zones.crosses(null, "a"));
        assertTrue(Zones.crosses("a", "b"));
    }

    public void testTheLeaseCarriesTheZone() throws Exception {
        final NodeLease lease = new NodeLease("n", "e", "127.0.0.1:9300", Set.of("search"), 10L, "name", "v").withLoad(1, 2)
            .withZone("us-east-1b");
        final NodeLease parsed = NodeLease.fromStream(new ByteArrayInputStream(lease.toBytes().toBytesRef().bytes));
        assertEquals("us-east-1b", parsed.zone());
        assertEquals("survives a renewal", "us-east-1b", parsed.renewedUntil(20L).zone());
        assertEquals("survives a revocation", "us-east-1b", parsed.revokedCopy().zone());
        assertEquals(2, parsed.withZone("us-east-1b").cap());
        final NodeLease old = new NodeLease("n", "e", "127.0.0.1:9300", Set.of("search"), 10L);
        assertNull(
            "a lease from before the field has no zone",
            NodeLease.fromStream(new ByteArrayInputStream(old.toBytes().toBytesRef().bytes)).zone()
        );
        assertFalse("and writes none", old.toBytes().utf8ToString().contains("zone"));
    }

    public void testCrossZoneForwardsAreCounted() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("zoned", "uuid-zoned-000000000", 1, MAPPING, null));
        try (ServerlessNode writer = new ServerlessNode(settings("zone-writer", "ingest", "zone-a"))) {
            writer.startup().start();
            writer.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
            loop.want("zoned", 0);
            loop.tick(clock.get());
            assertEquals(201, send(writer, "PUT", "/zoned/_doc/1", "{\"msg\":\"needle\"}").status());
            loop.publishAll();
        }
        final List<ServerlessNode> searchers = new ArrayList<>();
        try {
            for (String[] node : new String[][] { { "zone-s1", "zone-a" }, { "zone-s2", "zone-b" } }) {
                final ServerlessNode searcher = new ServerlessNode(settings(node[0], "search", node[1]));
                searchers.add(searcher);
                searcher.startup().start();
                searcher.setMetadataPlane(plane);
                searcher.leases().heartbeat(plane);
            }
            final var members = plane.membership().refresh();
            for (ServerlessNode searcher : searchers) {
                assertEquals(
                    "the lease advertises the zone",
                    searcher.router().localZone(),
                    Zones.zoneOf(members, searcher.localNode().getId())
                );
            }
            // Coordinated by the node placement ranks second, so the shard is forwarded to the first, in the other zone.
            final List<String> ranked = ReaderPlacement.candidatesFor("zoned", 0, members, ServerlessNode.ROLE_SEARCH, 2);
            final ServerlessNode coordinator = byId(searchers, ranked.get(1));
            final Response found = send(coordinator, "POST", "/zoned/_search", "{\"query\":{\"match\":{\"msg\":\"needle\"}}}");
            assertEquals(found.body(), 200, found.status());
            assertTrue(found.body(), found.body().contains("\"value\":1"));

            final ZoneTraffic meter = coordinator.router().zoneTraffic();
            final long crossed = meter.total(ZoneTraffic.Path.CROSS_ZONE);
            assertTrue("the forward and its answer crossed zones: " + crossed, crossed > 0);
            assertTrue(meter.byAction(ZoneTraffic.Path.CROSS_ZONE).containsKey(ForwardedSearchRequest.ACTION));
            assertEquals("nothing went to its own zone", 0, meter.total(ZoneTraffic.Path.SAME_ZONE));

            final Response stats = send(coordinator, "GET", "/_serverless/stats", null);
            assertEquals(stats.body(), 200, stats.status());
            assertTrue(stats.body(), stats.body().contains("\"zone_traffic\""));
            assertTrue(stats.body(), stats.body().contains("\"cross_zone_bytes\":" + meter.total(ZoneTraffic.Path.CROSS_ZONE)));
        } finally {
            for (ServerlessNode node : searchers) {
                node.close();
            }
        }
    }

    public void testAHotShardIsAnsweredInsideTheCoordinatorsZone() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("hot", "uuid-hot-0000000000", 1, MAPPING, null));
        try (ServerlessNode writer = new ServerlessNode(settings("hot-writer", "ingest", "zone-a"))) {
            writer.startup().start();
            writer.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
            loop.want("hot", 0);
            loop.tick(clock.get());
            assertEquals(201, send(writer, "PUT", "/hot/_doc/1", "{\"msg\":\"needle\"}").status());
            loop.publishAll();
        }
        final List<ServerlessNode> searchers = new ArrayList<>();
        try {
            for (String[] node : new String[][] { { "hot-s1", "zone-a" }, { "hot-s2", "zone-b" }, { "hot-s3", "zone-c" } }) {
                // Every shard counts as hot here, so the first search already prefers the zone.
                final Settings hot = Settings.builder()
                    .put(settings(node[0], "search", node[1]))
                    .put(ZonePreference.MIN_PER_SECOND, 0.0)
                    .build();
                final ServerlessNode searcher = new ServerlessNode(hot);
                searchers.add(searcher);
                searcher.startup().start();
                searcher.setMetadataPlane(plane);
                searcher.leases().heartbeat(plane);
            }
            final var members = plane.membership().refresh();
            final List<String> ranked = ReaderPlacement.candidatesFor("hot", 0, members, ServerlessNode.ROLE_SEARCH, 3);
            // Placement would send this coordinator's search to another zone; its zone's reader is itself.
            final ServerlessNode coordinator = byId(searchers, ranked.get(2));
            for (int i = 0; i < 3; i++) {
                final Response found = send(coordinator, "POST", "/hot/_search", "{\"query\":{\"match\":{\"msg\":\"needle\"}}}");
                assertEquals(found.body(), 200, found.status());
                assertTrue(found.body(), found.body().contains("\"value\":1"));
            }
            assertEquals(
                "nothing crossed a zone: " + coordinator.router().zoneTraffic().byAction(ZoneTraffic.Path.CROSS_ZONE),
                0,
                coordinator.router().zoneTraffic().total(ZoneTraffic.Path.CROSS_ZONE)
            );
            assertTrue(
                "the coordinator answers from its own reader",
                coordinator.reconciler().openShards().stream().anyMatch(s -> s.getIndexName().equals("hot"))
            );
        } finally {
            for (ServerlessNode node : searchers) {
                node.close();
            }
        }
    }

    private Settings settings(String name, String roles, String zone) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-zones")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("node.attr.zone", zone)
            .build();
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
