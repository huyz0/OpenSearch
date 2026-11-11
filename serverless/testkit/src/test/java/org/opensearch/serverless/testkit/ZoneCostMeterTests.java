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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.ZonePreference;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.transport.ForwardedBulkRequest;
import org.opensearch.serverless.transport.ForwardedSearchRequest;
import org.opensearch.serverless.transport.ShardRouter;
import org.opensearch.serverless.transport.ZoneTraffic;
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
 * Measures the bytes a small three-zone fleet moves between zones for the same writes and searches, with the zone
 * preferences off (the fleet before zones) and on.
 *
 * <p>Three writers, one per zone, own six shards between them; six search nodes, two per zone, take the writes and
 * coordinate the searches, so every write is forwarded to its owner and every search fans out over six shards. Results
 * are logged as {@code ZONEMETER|...} lines.
 *
 * <p>Runs against {@code FsBlobContainer} only.
 */
public class ZoneCostMeterTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final int SHARDS = 6;
    private static final String[] ZONES = { "zone-a", "zone-b", "zone-c" };
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"host\":{\"type\":\"keyword\"}}}";

    private record Measured(long writeCross, long searchCross, long cross, long same, long wire) {
    }

    public void testZonePreferencesCutCrossZoneBytes() throws Exception {
        final Measured before = run(
            Settings.builder().put(ZonePreference.ENABLED, false).put(ShardRouter.COMPRESS_CROSS_ZONE, false).build()
        );
        final Measured after = run(Settings.EMPTY);
        logger.info("ZONEMETER|before|{}", before);
        logger.info("ZONEMETER|after|{}", after);
        logger.info(
            "ZONEMETER|search cross-zone payload {} -> {} ({}% less); write cross-zone payload {} -> {}; wire {} -> {} ({}% less)",
            before.searchCross(),
            after.searchCross(),
            100 - 100 * after.searchCross() / Math.max(1, before.searchCross()),
            before.writeCross(),
            after.writeCross(),
            before.wire(),
            after.wire(),
            100 - 100 * after.wire() / Math.max(1, before.wire())
        );
        assertTrue("searches cross zones less: " + before + " vs " + after, after.searchCross() < before.searchCross());
        assertTrue("fewer bytes on the wire: " + before + " vs " + after, after.wire() < before.wire());
    }

    private Measured run(Settings extra) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("meter", "uuid-meter-000000000", SHARDS, MAPPING, null));
        final List<ServerlessNode> writers = new ArrayList<>();
        final List<ServerlessNode> searchers = new ArrayList<>();
        try {
            for (int z = 0; z < ZONES.length; z++) {
                final ServerlessNode writer = new ServerlessNode(settings("w" + z, "ingest", ZONES[z], extra));
                writers.add(writer);
                writer.startup().start();
                writer.setMetadataPlane(plane);
                writer.leases().renewLease(plane);
            }
            final List<ShardId> owned = new ArrayList<>();
            for (int s = 0; s < SHARDS; s++) {
                owned.add(writers.get(s % ZONES.length).activation().activateWriter(plane, "meter", s).orElseThrow());
            }
            for (int i = 0; i < 2 * ZONES.length; i++) {
                final ServerlessNode searcher = new ServerlessNode(settings("s" + i, "search", ZONES[i % ZONES.length], extra));
                searchers.add(searcher);
                searcher.startup().start();
                searcher.setMetadataPlane(plane);
                searcher.leases().renewLease(plane);
            }
            for (ServerlessNode node : writers) {
                node.leases().renewLease(plane);
            }
            plane.membership().refresh();

            // Writes through every search node: each document is forwarded to its shard's owner.
            int id = 0;
            for (ServerlessNode via : searchers) {
                final StringBuilder bulk = new StringBuilder();
                for (int i = 0; i < 120; i++, id++) {
                    bulk.append("{\"index\":{\"_index\":\"meter\",\"_id\":\"d").append(id).append("\"}}\n");
                    bulk.append("{\"msg\":\"GET /api/v1/orders/")
                        .append(id)
                        .append(" returned 200 in ")
                        .append(id % 97)
                        .append(" ms for tenant ")
                        .append(id % 13)
                        .append("\",\"host\":\"web-")
                        .append(id % 7)
                        .append(".example.internal\"}\n");
                }
                final Response response = send(via, "POST", "/_bulk", bulk.toString());
                assertEquals(response.body(), 200, response.status());
                assertFalse(response.body(), response.body().contains("\"errors\":true"));
            }
            for (int s = 0; s < SHARDS; s++) {
                final ServerlessNode owner = writers.get(s % ZONES.length);
                owner.reconciler().shard(owned.get(s)).refresh("meter");
                owner.shards().publishShard(owned.get(s), plane.heads().read("meter", s).orElseThrow().term());
            }

            // Searches from every search node: each fans out over all six shards.
            for (int round = 0; round < 20; round++) {
                for (ServerlessNode via : searchers) {
                    final Response found = send(via, "POST", "/meter/_search", "{\"size\":10,\"query\":{\"match\":{\"msg\":\"orders\"}}}");
                    assertEquals(found.body(), 200, found.status());
                }
            }

            long writeCross = 0;
            long searchCross = 0;
            long cross = 0;
            long same = 0;
            long wire = 0;
            for (ServerlessNode node : searchers) {
                final ZoneTraffic meter = node.router().zoneTraffic();
                writeCross += meter.byAction(ZoneTraffic.Path.CROSS_ZONE).getOrDefault(ForwardedBulkRequest.ACTION, 0L);
                searchCross += meter.byAction(ZoneTraffic.Path.CROSS_ZONE).getOrDefault(ForwardedSearchRequest.ACTION, 0L);
                cross += meter.total(ZoneTraffic.Path.CROSS_ZONE);
                same += meter.total(ZoneTraffic.Path.SAME_ZONE);
                wire += node.router().transportBytesSent();
            }
            return new Measured(writeCross, searchCross, cross, same, wire);
        } finally {
            for (ServerlessNode node : searchers) {
                node.close();
            }
            for (ServerlessNode node : writers) {
                node.close();
            }
        }
    }

    private Settings settings(String name, String roles, String zone, Settings extra) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-zone-meter")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("node.attr.zone", zone)
            .put(extra)
            .build();
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", path.endsWith("_bulk") ? "application/x-ndjson" : "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
