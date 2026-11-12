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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.transport.CommitPush;
import org.opensearch.serverless.transport.RefreshVisibility;
import org.opensearch.serverless.transport.TailFetchRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * What {@code everywhere} mode costs a search, against {@code serving}: search latency on a search node serving a shard
 * that takes writes, a quarter of them refreshing, while it is searched back to back (busy) and once every
 * {@value #IDLE_GAP_MILLIS} ms, longer than a grant (idle); the round trips to the writer per search, the searches
 * refused and answered by another copy, refreshing-write latency, and store requests. Fetches and renewals cross a
 * link that adds {@value #LINK_MILLIS} ms each way, as one across zones might.
 *
 * <p>A measurement, not a pin: it logs one {@code EVERYWHEREMETER} line per variant. Run with
 * {@code -Dtests.meter.refresh=true}.
 */
public class RefreshEverywhereMeterTests extends OpenSearchTestCase {

    private static final long TTL = 120_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"}}}";
    private static final String UUID = "uuid-hot-everywhere00";
    private static final int BUSY_SEARCHES = Integer.parseInt(System.getProperty("meter.searches", "400"));
    private static final int IDLE_SEARCHES = Integer.parseInt(System.getProperty("meter.idle", "20"));
    private static final long IDLE_GAP_MILLIS = 700L;
    private static final long LINK_MILLIS = 1L;

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-everywhere-meter")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put(CommitPush.ENABLED, true)
            .build();
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        plane.membership().refresh();
        return node;
    }

    private static String summary(List<Long> micros) {
        final List<Long> sorted = new ArrayList<>(micros);
        Collections.sort(sorted);
        if (sorted.isEmpty()) {
            return "n=0";
        }
        final java.util.function.DoubleFunction<Double> at = p -> sorted.get(
            Math.max(0, (int) Math.min(sorted.size() - 1, Math.ceil(p * sorted.size()) - 1))
        ) / 1000.0;
        return String.format(
            Locale.ROOT,
            "p50=%.2f p99=%.2f max=%.2f ms (n=%d)",
            at.apply(0.5),
            at.apply(0.99),
            at.apply(1.0),
            sorted.size()
        );
    }

    private static void setMode(RefreshVisibility.Mode mode, ServerlessNode... nodes) {
        final Settings cluster = Settings.builder().put(RefreshVisibility.CLUSTER_SETTING, mode.settingValue()).build();
        for (ServerlessNode node : nodes) {
            node.router().refreshVisibility().applyCluster(cluster);
        }
    }

    public void testWhatEverywhereCostsASearch() throws Exception {
        assumeTrue(
            "a refresh meter runs only when -Dtests.meter.refresh is set",
            System.getProperty("tests.meter.refresh", "").isEmpty() == false
        );
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
            plane.createIndex(new IndexDescriptor("hot", UUID, 1, MAPPING, Settings.builder().put("index.refresh_interval", "-1").build()));
            try (
                ServerlessNode writer = joined(new ServerlessNode(nodeSettings("meter-writer", "ingest")), plane);
                ServerlessNode reader = joined(new ServerlessNode(nodeSettings("meter-search", "search")), plane)
            ) {
                writer.activation().activateWriter(plane, "hot", 0).orElseThrow();
                send(writer, "PUT", "/hot/_doc/seed", "{\"msg\":\"seed\",\"n\":0}");
                writer.shards().publishShard(new ShardId("hot", UUID, 0), plane.heads().read("hot", 0).orElseThrow().term());
                assertBusy(() -> assertTrue(send(reader, "GET", "/hot/_search?q=msg:seed", null).body().contains("\"value\":1")));
                // The reader's asks for grants cross a slower link.
                reader.router().injectSendFault((action, target) -> TailFetchRequest.ACTION.equals(action) ? LINK_MILLIS * 2 : 0L);
                final List<String> lines = new ArrayList<>();
                for (int round = 0; round < 2; round++) {
                    for (RefreshVisibility.Mode mode : List.of(RefreshVisibility.Mode.SERVING, RefreshVisibility.Mode.EVERYWHERE)) {
                        setMode(mode, writer, reader);
                        for (String load : List.of("busy", "idle")) {
                            writer.leases().renewLease(plane);
                            reader.leases().renewLease(plane);
                            final List<Long> searches = new CopyOnWriteArrayList<>();
                            final List<Long> refreshAcks = new CopyOnWriteArrayList<>();
                            final AtomicInteger missed = new AtomicInteger();
                            final AtomicBoolean writing = new AtomicBoolean(true);
                            final long[] checksBefore = reader.router().tailOverlays().grantCounts();
                            final long[] servedBefore = writer.router().tailPush().counts();
                            final long[] waitsBefore = writer.router().tailPush().grantCounts();
                            store.reset();
                            final String tag = mode.settingValue() + "-" + load + "-" + round;
                            final Thread writes = new Thread(() -> {
                                int n = 0;
                                while (writing.get()) {
                                    final String id = tag + "-" + n;
                                    final boolean refresh = n % 4 == 0;
                                    try {
                                        final long started = System.nanoTime();
                                        final Response put = send(
                                            writer,
                                            "PUT",
                                            "/hot/_doc/" + id + (refresh ? "?refresh=true" : ""),
                                            "{\"msg\":\"" + id + "\",\"n\":" + n + "}"
                                        );
                                        if (refresh) {
                                            refreshAcks.add((System.nanoTime() - started) / 1_000L);
                                            if (put.body().contains("\"forced_refresh\":true")
                                                && send(reader, "GET", "/hot/_search?q=msg:" + id, null).body()
                                                    .contains("\"value\":1") == false) {
                                                missed.incrementAndGet();
                                            }
                                        }
                                        Thread.sleep("busy".equals(load) ? 5 : 100);
                                    } catch (Exception e) {
                                        throw new AssertionError(e);
                                    }
                                    n++;
                                }
                            }, "meter-writes");
                            writes.start();
                            final int count = "busy".equals(load) ? BUSY_SEARCHES : IDLE_SEARCHES;
                            for (int i = 0; i < count; i++) {
                                final long started = System.nanoTime();
                                final Response found = send(reader, "GET", "/hot/_search?size=0", null);
                                searches.add((System.nanoTime() - started) / 1_000L);
                                assertEquals(found.body(), 200, found.status());
                                if ("idle".equals(load)) {
                                    Thread.sleep(IDLE_GAP_MILLIS);
                                }
                            }
                            writing.set(false);
                            writes.join();
                            final long[] checksAfter = reader.router().tailOverlays().grantCounts();
                            final long[] servedAfter = writer.router().tailPush().counts();
                            final long[] waitsAfter = writer.router().tailPush().grantCounts();
                            lines.add(
                                String.format(
                                    Locale.ROOT,
                                    "EVERYWHEREMETER|%s|%s|round %d|search %s|asks of the writer per search %.3f|refused %d|"
                                        + "fetches served %d|refresh=true ack %s|grant waits %d|claimed and missed %d|"
                                        + "store PUT %d GET %d LIST %d",
                                    mode.settingValue(),
                                    load,
                                    round,
                                    summary(searches),
                                    (double) (checksAfter[0] - checksBefore[0]) / count,
                                    checksAfter[1] - checksBefore[1],
                                    servedAfter[3] - servedBefore[3],
                                    summary(refreshAcks),
                                    waitsAfter[0] - waitsBefore[0],
                                    missed.get(),
                                    store.blobWrites() + store.registerWrites(),
                                    store.blobReads() + store.registerReads(),
                                    store.listings()
                                )
                            );
                            if (mode == RefreshVisibility.Mode.EVERYWHERE) {
                                assertEquals("no write answered refreshed is missed", 0, missed.get());
                            }
                        }
                    }
                }
                reader.router().injectSendFault(null);
                for (String line : lines) {
                    logger.info(line);
                }
            }
        }
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

    private record Response(int status, String body) {
    }
}
