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
import org.opensearch.serverless.transport.TailOverlays;
import org.opensearch.serverless.transport.TailPush;
import org.opensearch.serverless.transport.TailPushRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.FutureTask;

/**
 * In {@code everywhere} mode, a search that starts after a write answered with {@code forced_refresh:true} finds the
 * write on any search node, or reports the shard failed: one serving the shard, one that joins with nothing open, one
 * that joins while the write waits, one whose pushes were lost, and one whose overlay could not take the write. Both
 * {@code refresh=true} and {@code refresh=wait_for}, single writes and batches.
 *
 * <p>The writer's pushes linger a second here, so a search node can only have a write in time if the refreshing write
 * waited for it or the search node asked for it.
 */
public class RefreshEverywhereTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"}}}";
    private static final String INDEX = "everywhere";
    private static final String UUID = "uuid-everywhere-000000";
    private static final String[] REFRESHES = { "true", "wait_for" };

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-refresh-everywhere")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put(CommitPush.ENABLED, true)
            .put(TailPush.LINGER_SETTING, 1_000L)
            .put(RefreshVisibility.NODE_SETTING, RefreshVisibility.Mode.EVERYWHERE.settingValue())
            .build();
    }

    private static ServerlessNode joined(ServerlessNode node, MetadataPlane plane) throws Exception {
        node.startup().start();
        node.setMetadataPlane(plane);
        node.leases().renewLease(plane);
        plane.membership().refresh();
        return node;
    }

    private interface Scenario {
        void run(MetadataPlane plane, ServerlessNode writer, ServerlessNode reader, Joiner join) throws Exception;
    }

    /** Starts another search node while a scenario runs. */
    private interface Joiner {
        ServerlessNode join(String name) throws Exception;
    }

    private void withNodes(Settings indexSettings, Scenario scenario) throws Exception {
        final MetadataPlane plane = new MetadataPlane(
            new FsBlobStore(8192, createTempDir(), false),
            BlobPath.cleanPath(),
            System::currentTimeMillis,
            TTL
        );
        final Settings settings = Settings.builder().put("index.refresh_interval", "-1").put(indexSettings).build();
        plane.createIndex(new IndexDescriptor(INDEX, UUID, 1, MAPPING, settings));
        final List<ServerlessNode> joined = new ArrayList<>();
        try (
            ServerlessNode writer = joined(new ServerlessNode(nodeSettings("everywhere-writer", "ingest")), plane);
            ServerlessNode reader = joined(new ServerlessNode(nodeSettings("everywhere-reader", "search")), plane)
        ) {
            writer.activation().activateWriter(plane, INDEX, 0).orElseThrow();
            assertEquals(201, send(writer, "PUT", "/" + INDEX + "/_doc/seed", doc("seed")).status());
            writer.shards().publishShard(new ShardId(INDEX, UUID, 0), plane.heads().read(INDEX, 0).orElseThrow().term());
            assertBusy(() -> assertEquals(1L, hits(reader, "seed")));
            scenario.run(plane, writer, reader, name -> {
                final ServerlessNode node = joined(new ServerlessNode(nodeSettings(name, "search")), plane);
                joined.add(node);
                return node;
            });
        } finally {
            for (ServerlessNode node : joined) {
                node.close();
            }
        }
    }

    public void testARefreshingWriteIsFoundOnTheSearchNodeServingTheShard() throws Exception {
        withNodes(Settings.EMPTY, (plane, writer, reader, join) -> {
            for (String refresh : REFRESHES) {
                for (ServerlessNode via : new ServerlessNode[] { writer, reader }) {
                    final String id = "single-" + refresh + "-" + via.localNode().getName();
                    final Response put = send(via, "PUT", "/" + INDEX + "/_doc/" + id + "?refresh=" + refresh, doc(id));
                    assertEquals(put.body(), 201, put.status());
                    assertTrue(put.body(), put.body().contains("\"forced_refresh\":true"));
                    assertEquals(id, 1L, hits(reader, id));

                    final String prefix = "bulk-" + refresh + "-" + via.localNode().getName() + "-";
                    final Response bulk = send(via, "POST", "/_bulk?refresh=" + refresh, bulk(prefix, 5));
                    assertEquals(bulk.body(), 200, bulk.status());
                    assertFalse(bulk.body(), bulk.body().contains("\"errors\":true"));
                    assertTrue(bulk.body(), bulk.body().contains("\"forced_refresh\":true"));
                    for (int i = 0; i < 5; i++) {
                        assertEquals(prefix + i, 1L, hits(reader, prefix + i));
                    }
                }
            }
        });
    }

    public void testANodeThatJoinsAfterTheAnswerFindsTheWrite() throws Exception {
        withNodes(Settings.EMPTY, (plane, writer, reader, join) -> {
            for (String refresh : REFRESHES) {
                final String id = "after-" + refresh;
                final Response put = send(writer, "PUT", "/" + INDEX + "/_doc/" + id + "?refresh=" + refresh, doc(id));
                assertTrue(put.body(), put.body().contains("\"forced_refresh\":true"));
                final ServerlessNode late = join.join("everywhere-after-" + refresh.replace('_', '-'));
                assertEquals("a node with nothing open finds it on its first search", 1L, hits(late, id));
                final String prefix = "after-bulk-" + refresh + "-";
                final Response bulk = send(writer, "POST", "/_bulk?refresh=" + refresh, bulk(prefix, 3));
                assertTrue(bulk.body(), bulk.body().contains("\"forced_refresh\":true"));
                final ServerlessNode later = join.join("everywhere-after-bulk-" + refresh.replace('_', '-'));
                for (int i = 0; i < 3; i++) {
                    assertEquals(prefix + i, 1L, hits(later, prefix + i));
                }
            }
        });
    }

    public void testANodeThatJoinsWhileTheWriteWaitsFindsIt() throws Exception {
        withNodes(Settings.EMPTY, (plane, writer, reader, join) -> {
            // Pushes take 300 ms to leave: the write is still waiting for the reader when the next node joins and opens.
            writer.router().injectSendFault((action, target) -> TailPushRequest.ACTION.equals(action) ? 300L : 0L);
            for (String refresh : REFRESHES) {
                final String id = "midwait-" + refresh;
                // On a thread of its own that the test joins, not the common pool: a pool thread outlives the test and
                // fails it as a leak.
                final FutureTask<Response> put = new FutureTask<>(
                    () -> send(writer, "PUT", "/" + INDEX + "/_doc/" + id + "?refresh=" + refresh, doc(id))
                );
                final Thread putting = new Thread(put, "everywhere-midwait-put");
                putting.start();
                final ServerlessNode mid = join.join("everywhere-mid-" + refresh.replace('_', '-'));
                hits(mid, "seed");
                final Response answered = put.get();
                putting.join();
                assertEquals(answered.body(), 201, answered.status());
                if (answered.body().contains("\"forced_refresh\":true")) {
                    assertEquals("joined while it waited", 1L, hits(mid, id));
                    assertEquals("served throughout", 1L, hits(reader, id));
                }
            }
            writer.router().injectSendFault(null);
        });
    }

    public void testANodeWhosePushesWereLostIsWaitedOut() throws Exception {
        withNodes(Settings.EMPTY, (plane, writer, reader, join) -> {
            final String lost = reader.localNode().getId();
            for (String refresh : REFRESHES) {
                // A search just now renewed the reader's grant, which had run out; then its push is lost.
                Thread.sleep(RefreshVisibility.GRANT_MILLIS + 50);
                final long renewedBefore = System.nanoTime();
                assertEquals(1L, hits(reader, "seed"));
                writer.router()
                    .injectSendFault((action, target) -> TailPushRequest.ACTION.equals(action) && lost.equals(target) ? -1L : 0L);
                final String dropped = "dropped-" + refresh;
                final Response first = send(writer, "PUT", "/" + INDEX + "/_doc/" + dropped + "?refresh=" + refresh, doc(dropped));
                assertEquals(201, first.status());
                writer.router().injectSendFault(null);
                // Answered refreshed only if the reader took the write some other way, by opening a pushed commit.
                final boolean firstClaimed = first.body().contains("\"forced_refresh\":true");
                // The writer no longer pushes to the reader, which may still be answering under its grant: the next
                // refreshing write is answered once that grant has run out, and the reader then asks again.
                final String next = "next-" + refresh;
                final Response second = send(writer, "PUT", "/" + INDEX + "/_doc/" + next + "?refresh=" + refresh, doc(next));
                assertTrue(second.body(), second.body().contains("\"forced_refresh\":true"));
                assertEquals(1L, hits(reader, next));
                assertEquals("what was lost is fetched again too", 1L, hits(reader, dropped));
                final long sinceRenewal = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - renewedBefore);
                assertTrue(
                    "answered refreshed only after the reader's grant ran out: " + sinceRenewal + " ms",
                    firstClaimed || sinceRenewal >= RefreshVisibility.GRANT_MILLIS
                );
            }
        });
    }

    public void testAWriteTheOverlayCannotTakeIsAnsweredByAnotherCopy() throws Exception {
        withNodes(Settings.EMPTY, (plane, writer, reader, join) -> {
            for (String refresh : REFRESHES) {
                // A field the reader's mapping does not have: its overlay leaves the document out, so it refuses the
                // search, and the writer's own copy answers.
                final String id = "unmapped-" + refresh;
                final String body = "{\"msg\":\"" + id + "\",\"n\":1,\"fresh_field_" + refresh.replace('_', 'x') + "\":\"x\"}";
                final Response put = send(writer, "PUT", "/" + INDEX + "/_doc/" + id + "?refresh=" + refresh, body);
                assertEquals(put.body(), 201, put.status());
                if (put.body().contains("\"forced_refresh\":true")) {
                    assertEquals(1L, hits(reader, id));
                }
            }
        });
    }

    public void testWithoutTheOverlayAWriteIsNotClaimed() throws Exception {
        withNodes(Settings.builder().put(TailOverlays.INDEX_SETTING, "false").build(), (plane, writer, reader, join) -> {
            for (String refresh : REFRESHES) {
                final String id = "commit-only-" + refresh;
                final Response put = send(writer, "PUT", "/" + INDEX + "/_doc/" + id + "?refresh=" + refresh, doc(id));
                assertEquals(201, put.status());
                assertFalse("search nodes see only commits: " + put.body(), put.body().contains("\"forced_refresh\":true"));
                // Nothing was claimed, so the reader answers from its commit, which may not hold it yet.
                assertTrue(hits(reader, id) <= 1L);
            }
        });
    }

    public void testWritesAnsweredRefreshedAreNeverMissed() throws Exception {
        withNodes(Settings.EMPTY, (plane, writer, reader, join) -> {
            final ServerlessNode second = join.join("everywhere-second");
            hits(second, "seed");
            final List<ServerlessNode> readers = new ArrayList<>(List.of(reader, second));
            int claimed = 0;
            for (int i = 0; i < 30; i++) {
                final int fault = randomIntBetween(0, 9);
                final String target = randomFrom(readers).localNode().getId();
                // Now and then a push to one reader is lost or late.
                writer.router()
                    .injectSendFault(
                        fault == 0 ? (action, to) -> TailPushRequest.ACTION.equals(action) && target.equals(to) ? -1L : 0L
                            : fault == 1 ? (action, to) -> TailPushRequest.ACTION.equals(action) ? (long) randomIntBetween(50, 700) : 0L
                            : null
                    );
                if (i == 15) {
                    readers.add(join.join("everywhere-third"));
                }
                final String refresh = randomFrom(REFRESHES);
                final String id = "random-" + i;
                final ServerlessNode via = randomBoolean() ? writer : randomFrom(readers);
                final Response put = send(via, "PUT", "/" + INDEX + "/_doc/" + id + "?refresh=" + refresh, doc(id));
                assertEquals(put.body(), 201, put.status());
                writer.router().injectSendFault(null);
                if (put.body().contains("\"forced_refresh\":true")) {
                    claimed++;
                    for (ServerlessNode node : readers) {
                        assertEquals(id + " on " + node.localNode().getName(), 1L, hits(node, id));
                    }
                }
            }
            assertTrue("most writes are answered refreshed: " + claimed, claimed >= 10);
        });
    }

    private static String doc(String id) {
        return "{\"msg\":\"" + id + "\",\"n\":1}";
    }

    private static String bulk(String prefix, int count) {
        final StringBuilder body = new StringBuilder();
        for (int i = 0; i < count; i++) {
            body.append("{\"index\":{\"_index\":\"").append(INDEX).append("\",\"_id\":\"").append(prefix).append(i).append("\"}}\n");
            body.append(doc(prefix + i)).append('\n');
        }
        return body.toString();
    }

    /** Hits for a message through a node's search endpoint. */
    private static long hits(ServerlessNode node, String msg) throws Exception {
        final Response response = send(node, "GET", "/" + INDEX + "/_search?q=msg:" + msg, null);
        assertEquals(response.body(), 200, response.status());
        assertFalse("no shard failed: " + response.body(), response.body().contains("\"failed\":1"));
        final int at = response.body().indexOf("\"total\":{\"value\":");
        assertTrue(response.body(), at >= 0);
        final int from = at + "\"total\":{\"value\":".length();
        int to = from;
        while (Character.isDigit(response.body().charAt(to))) {
            to++;
        }
        return Long.parseLong(response.body().substring(from, to));
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/x-ndjson")
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
