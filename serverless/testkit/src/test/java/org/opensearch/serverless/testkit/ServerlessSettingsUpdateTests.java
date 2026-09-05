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
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * M54: changing an index's settings after it exists — the other half of M52.
 *
 * <p>M52 made mappings mutable and left settings write-once, which both compared serverless products ship. The
 * shape of the work is the same — compare-and-swap the descriptor, apply to shards already open — and the
 * difference is a validity question mappings do not have: core divides index settings into dynamic ones,
 * changeable on a live index, and static ones, which classic OpenSearch changes only on a <em>closed</em>
 * index. This design has no closed state, so a static setting has no moment at which it could be applied and
 * is refused rather than stored and quietly ignored.
 *
 * <p>Nothing here keeps its own list of which settings are which. {@code IndexScopedSettings} answers that,
 * and a second account of a registry that grows every release is one that would drift from it.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessSettingsUpdateTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private int port;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-settings-update")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private record Answer(int status, String body) {
        boolean has(String fragment) {
            return body.contains(fragment);
        }
    }

    private Answer call(String method, String path, String body) throws Exception {
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                    .header("Content-Type", "application/json")
                    .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(30))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
            return new Answer(response.statusCode(), response.body());
        }
    }

    private static MetadataPlane plane(AtomicLong clock, Path store) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, store, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private ServerlessNode running(MetadataPlane plane, AtomicLong clock, String name) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        port = node.boundHttpAddress().publishAddress().getPort();
        call("PUT", "/alpha", "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}");
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        loop.want("alpha", 0);
        loop.tick(clock.get());
        return node;
    }

    /**
     * A dynamic setting changed on a live index reaches the shard that is already open.
     *
     * <p><b>The assertion is the shard's own view, not the descriptor's.</b> Storing a setting and reading it
     * back through {@code GET /_settings} proves a string round-tripped through the object store. What has to
     * be true is that the shard already serving traffic picked it up — so this reads
     * {@code IndexService#getIndexSettings}, which is the object the shard actually consults.
     */
    public void testADynamicSettingReachesAShardAlreadyOpen() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "settings-live")) {
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            final var indexService = node.indicesService().indexService(shardId.getIndex());
            assertNotNull("the shard must be open before the change", indexService);

            final Answer updated = call("PUT", "/alpha/_settings", "{\"index\":{\"refresh_interval\":\"37s\"}}");
            assertEquals(updated.body(), 200, updated.status());
            assertTrue("a change says so: " + updated.body(), updated.has("\"changed\":true"));

            assertEquals(
                "the open shard's own settings must carry the new value, not just the descriptor",
                org.opensearch.common.unit.TimeValue.timeValueSeconds(37),
                indexService.getIndexSettings().getRefreshInterval()
            );

            final Answer read = call("GET", "/alpha/_settings", null);
            assertTrue("and it must be readable back: " + read.body(), read.has("\"refresh_interval\":\"37s\""));
        }
    }

    /** Both body shapes a client sends mean the same thing, and a change merges rather than replaces. */
    public void testBothBodyShapesWorkAndSettingsMerge() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "settings-merge")) {
            assertNotNull(node);
            assertEquals(200, call("PUT", "/alpha/_settings", "{\"index\":{\"refresh_interval\":\"30s\"}}").status());
            // The bare form, without the "index" wrapper, normalised by core's own prefixing.
            assertEquals(200, call("PUT", "/alpha/_settings", "{\"max_result_window\":5000}").status());

            final Answer read = call("GET", "/alpha/_settings", null);
            assertTrue("the second change must not clear the first: " + read.body(), read.has("\"refresh_interval\":\"30s\""));
            assertTrue("and must itself be stored: " + read.body(), read.has("\"max_result_window\":\"5000\""));

            final Answer replay = call("PUT", "/alpha/_settings", "{\"max_result_window\":5000}");
            assertTrue("a replay is a no-op that says so: " + replay.body(), replay.has("\"changed\":false"));
        }
    }

    /**
     * A static setting is refused, because there is no state in which it could be applied.
     *
     * <p>Classic OpenSearch changes a static setting only while an index is closed. There is no closed state
     * here, so accepting one would leave a value in the descriptor that nothing acts on — the quiet kind of
     * wrong answer this surface refuses.
     */
    public void testAStaticSettingIsRefusedRatherThanStoredAndIgnored() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "settings-static")) {
            assertNotNull(node);
            final Answer refused = call("PUT", "/alpha/_settings", "{\"codec\":\"best_compression\"}");
            assertEquals(refused.body(), 501, refused.status());
            assertTrue("naming the setting: " + refused.body(), refused.has("index.codec"));
            assertTrue("and why there is no moment for it: " + refused.body(), refused.has("no closed state"));

            // Refused means refused: nothing was stored.
            assertFalse("the setting must not be in the descriptor: ", call("GET", "/alpha/_settings", null).has("codec"));
        }
    }

    /** The two settings refused for reasons of their own, and a typo told apart from a capability. */
    public void testStructuralSettingsAndTyposAreToldApart() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        try (ServerlessNode node = running(plane, clock, "settings-refused")) {
            assertNotNull(node);

            final Answer shards = call("PUT", "/alpha/_settings", "{\"number_of_shards\":4}");
            assertEquals(shards.body(), 501, shards.status());
            assertTrue("routing is why: " + shards.body(), shards.has("routing is a function of it"));

            final Answer replicas = call("PUT", "/alpha/_settings", "{\"number_of_replicas\":2}");
            assertEquals(replicas.body(), 501, replicas.status());
            assertTrue("durability is why: " + replicas.body(), replicas.has("durability comes from"));

            // Zero is not a refusal: a client spelling out what this deployment already provides is asking
            // for nothing it cannot have.
            assertEquals(200, call("PUT", "/alpha/_settings", "{\"number_of_replicas\":0}").status());

            // A typo is the caller's to fix, so it is a 400 rather than a 501.
            final Answer typo = call("PUT", "/alpha/_settings", "{\"not_a_setting\":true}");
            assertEquals("an unknown setting is a bad request, not an unimplemented one: " + typo.body(), 400, typo.status());
            assertTrue(typo.body(), typo.has("unknown index setting"));

            assertEquals(
                "and an index that is not there is a 404",
                404,
                call("PUT", "/ghost/_settings", "{\"refresh_interval\":\"1s\"}").status()
            );
        }
    }

    /** A settings change survives the node that made it, because it lives in the descriptor. */
    public void testASettingsChangeSurvivesTheNodeThatMadeIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock, createTempDir());
        final ServerlessNode first = running(plane, clock, "settings-a");
        assertEquals(200, call("PUT", "/alpha/_settings", "{\"refresh_interval\":\"41s\"}").status());
        first.close();
        clock.set(clock.get() + TTL + 1);

        try (ServerlessNode second = new ServerlessNode(nodeSettings("settings-b"))) {
            second.start();
            second.setMetadataPlane(plane);
            port = second.boundHttpAddress().publishAddress().getPort();
            final BackgroundReconciler loop = new BackgroundReconciler(second, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            final ShardId shardId = second.reconciler().openShards().iterator().next();
            assertEquals(
                "the successor opens the shard with the changed setting, not the one it was created with",
                org.opensearch.common.unit.TimeValue.timeValueSeconds(41),
                second.indicesService().indexService(shardId.getIndex()).getIndexSettings().getRefreshInterval()
            );
        }
    }
}
