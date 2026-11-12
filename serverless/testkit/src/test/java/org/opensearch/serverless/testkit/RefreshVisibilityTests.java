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
import java.util.concurrent.atomic.AtomicLong;

/**
 * A write answered with {@code refresh=true} or {@code refresh=wait_for} is found by a search on a search node that
 * starts after the answer: one that already serves the shard, and one that opens it for that search. Writes go to the
 * shard's writer directly and through a search node that forwards them; single writes and batches.
 *
 * <p>The writer's pushes linger a second here, far longer than a search takes to follow a write, so a search node can
 * only have a write in time if the refreshing write waited for it. The {@code owner} mode, which does not wait, shows
 * the gap this closes.
 */
public class RefreshVisibilityTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"keyword\"},\"n\":{\"type\":\"long\"}}}";
    private static final String INDEX = "fresh";

    private Settings nodeSettings(String name, String roles, String mode) {
        final Settings.Builder builder = Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-refresh-visibility")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put(CommitPush.ENABLED, true)
            .put(TailPush.LINGER_SETTING, 1_000L);
        if (mode != null) {
            builder.put(RefreshVisibility.NODE_SETTING, mode);
        }
        return builder.build();
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

    private void withNodes(String mode, Settings indexSettings, Scenario scenario) throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final Settings settings = Settings.builder().put("index.refresh_interval", "-1").put(indexSettings).build();
        plane.createIndex(new IndexDescriptor(INDEX, "uuid-fresh-0000000000", 1, MAPPING, settings));
        final List<ServerlessNode> joined = new ArrayList<>();
        try (
            ServerlessNode writer = joined(new ServerlessNode(nodeSettings("fresh-writer", "ingest", mode)), plane);
            ServerlessNode reader = joined(new ServerlessNode(nodeSettings("fresh-reader", "search", mode)), plane)
        ) {
            writer.activation().activateWriter(plane, INDEX, 0).orElseThrow();
            // One write the first publish holds, and a reader open on that commit, subscribed to the writer.
            assertEquals(201, send(writer, "PUT", "/" + INDEX + "/_doc/seed", "{\"msg\":\"seed\",\"n\":0}").status());
            writer.shards()
                .publishShard(
                    writer.reconciler()
                        .writerShard(new org.opensearch.core.index.shard.ShardId(INDEX, "uuid-fresh-0000000000", 0))
                        .shardId(),
                    plane.heads().read(INDEX, 0).orElseThrow().term()
                );
            assertBusy(() -> assertEquals(1L, hits(reader, "seed")));
            scenario.run(plane, writer, reader, name -> {
                final ServerlessNode node = joined(new ServerlessNode(nodeSettings(name, "search", mode)), plane);
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
        withNodes(RefreshVisibility.Mode.SERVING.settingValue(), Settings.EMPTY, (plane, writer, reader, join) -> {
            for (String refresh : new String[] { "true", "wait_for" }) {
                final String id = "direct-" + refresh;
                final Response put = send(writer, "PUT", "/" + INDEX + "/_doc/" + id + "?refresh=" + refresh, doc(id));
                assertEquals(put.body(), 201, put.status());
                assertTrue(put.body(), put.body().contains("\"forced_refresh\":true"));
                assertEquals("refresh=" + refresh + " is found on the search node at once", 1L, hits(reader, id));

                final String forwarded = "forwarded-" + refresh;
                final Response through = send(reader, "PUT", "/" + INDEX + "/_doc/" + forwarded + "?refresh=" + refresh, doc(forwarded));
                assertEquals(through.body(), 201, through.status());
                assertTrue(through.body(), through.body().contains("\"forced_refresh\":true"));
                assertEquals("a write forwarded to the writer is found too", 1L, hits(reader, forwarded));
            }
        });
    }

    public void testARefreshingBatchIsFoundOnTheSearchNodeServingTheShard() throws Exception {
        withNodes(RefreshVisibility.Mode.SERVING.settingValue(), Settings.EMPTY, (plane, writer, reader, join) -> {
            for (String refresh : new String[] { "true", "wait_for" }) {
                for (ServerlessNode via : new ServerlessNode[] { writer, reader }) {
                    final String prefix = "bulk-" + refresh + "-" + via.localNode().getName();
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

    public void testASearchNodeOpeningTheShardAfterTheWriteFindsIt() throws Exception {
        withNodes(RefreshVisibility.Mode.SERVING.settingValue(), Settings.EMPTY, (plane, writer, reader, join) -> {
            for (String refresh : new String[] { "true", "wait_for" }) {
                final String id = "late-" + refresh;
                assertEquals(201, send(writer, "PUT", "/" + INDEX + "/_doc/" + id + "?refresh=" + refresh, doc(id)).status());
                // Joined after the answer, with nothing open: its first search opens the shard.
                final ServerlessNode late = join.join("fresh-late-" + refresh.replace('_', '-'));
                assertEquals("a node that joined after the write finds it on its first search", 1L, hitsHere(late, id));
                assertEquals(1L, hitsHere(late, "seed"));
            }
        });
    }

    public void testWithoutWaitingTheSearchNodeMayNotHaveTheWriteYet() throws Exception {
        withNodes(RefreshVisibility.Mode.OWNER.settingValue(), Settings.EMPTY, (plane, writer, reader, join) -> {
            final Response put = send(writer, "PUT", "/" + INDEX + "/_doc/unseen?refresh=true", doc("unseen"));
            assertEquals(201, put.status());
            assertTrue(put.body(), put.body().contains("\"forced_refresh\":true"));
            assertEquals("the push has not left the writer yet", 0L, hits(reader, "unseen"));
            assertBusy(() -> assertEquals(1L, hits(reader, "unseen")));
        });
    }

    public void testANodeThePushDoesNotReachIsNotClaimedRefreshed() throws Exception {
        withNodes(RefreshVisibility.Mode.SERVING.settingValue(), Settings.EMPTY, (plane, writer, reader, join) -> {
            final String lost = reader.localNode().getId();
            writer.router().injectSendFault((action, target) -> TailPushRequest.ACTION.equals(action) && lost.equals(target) ? -1L : 0L);
            final Response put = send(writer, "PUT", "/" + INDEX + "/_doc/dropped?refresh=true", doc("dropped"));
            assertEquals(201, put.status());
            assertFalse("not claimed refreshed: " + put.body(), put.body().contains("\"forced_refresh\":true"));
            writer.router().injectSendFault(null);
        });
    }

    public void testWithoutTheOverlayARefreshIsNotClaimedOnSearchNodes() throws Exception {
        withNodes(
            RefreshVisibility.Mode.SERVING.settingValue(),
            Settings.builder().put(TailOverlays.INDEX_SETTING, "false").build(),
            (plane, writer, reader, join) -> {
                final Response put = send(writer, "PUT", "/" + INDEX + "/_doc/commit-only?refresh=true", doc("commit-only"));
                assertEquals(201, put.status());
                assertFalse("search nodes see only commits: " + put.body(), put.body().contains("\"forced_refresh\":true"));
            }
        );
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

    /** Hits for a message on a search node's own copy, opening it there if it is not open. */
    private static long hits(ServerlessNode reader, String msg) throws Exception {
        return hitsHere(reader, msg);
    }

    /** Hits for a message through a node's search endpoint. */
    private static long hitsHere(ServerlessNode node, String msg) throws Exception {
        final Response response = send(node, "GET", "/" + INDEX + "/_search?q=msg:" + msg, null);
        assertEquals(response.body(), 200, response.status());
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
