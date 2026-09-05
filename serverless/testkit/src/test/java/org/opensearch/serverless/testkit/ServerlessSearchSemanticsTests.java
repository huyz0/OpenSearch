/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.serverless.cluster.AliasRecord;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What a search says about itself, and what it covers.
 *
 * <p>Three ways a search used to look right while being wrong. A shard that ran out of time was reported
 * as a failed shard, so a caller asking for whatever could be collected in ten milliseconds got a 500
 * instead. A plugin's {@code Client} searched the first index it was given and reported the others as
 * covered. And a pattern refused the whole search the moment a plugin's index matched it, naming the
 * index in the refusal.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessSearchSemanticsTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"rank\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-search-semantics")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane plane(AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private static BackgroundReconciler hold(ServerlessNode node, MetadataPlane plane, AtomicLong clock, String index, int shards)
        throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < shards; shard++) {
            loop.want(index, shard);
        }
        loop.tick(clock.get());
        return loop;
    }

    /**
     * A shard that hits the request's timeout answers with what it collected and says so.
     *
     * <p>Core's contract: {@code timeout} bounds the work and {@code timed_out: true} reports the bound
     * was hit, with {@code _shards.failed: 0}. Every shard here was asked with partial results disallowed,
     * so a timed-out query phase threw instead of stopping, and the request answered 500 "no shard could
     * answer" -- the caller's own {@code allow_partial_search_results} is applied by the coordinator, and
     * the per-shard flag bought nothing but the wrong answer.
     *
     * <p>The query is slow on purpose: a script that spins per document, over enough documents that the
     * time estimator ticks while a leaf is being scored. Two shards, so the merge of two timed-out
     * answers is what is asserted rather than one shard's flag travelling alone.
     */
    public void testAShardThatRunsOutOfTimeSaysSoRatherThanFailing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("slow", "uuid-slow-0000000000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("search-timeout"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "slow", 2);

            final StringBuilder bulk = new StringBuilder();
            for (int i = 0; i < 3_000; i++) {
                bulk.append("{\"index\":{\"_index\":\"slow\",\"_id\":\"d").append(i).append("\"}}\n");
                bulk.append("{\"msg\":\"doc\",\"rank\":").append(i).append("}\n");
            }
            final Response loaded = send(node, "POST", "/_bulk?refresh=true", bulk.toString());
            assertEquals(loaded.body(), 200, loaded.status());
            assertFalse("the load must succeed: " + loaded.body(), loaded.body().contains("\"errors\":true"));

            final String slow = "{\"query\":{\"script\":{\"script\":{\"source\":"
                + "\"long x = 0; for (int i = 0; i < 300000; i++) { x += i; } return x > 0 && doc['rank'].value >= 0;\"}}}}";
            final Response answer = send(node, "POST", "/slow/_search?timeout=1ms&size=5", slow);
            assertEquals("a timed-out search is an answer, not a failure: " + answer.body(), 200, answer.status());
            assertTrue("and it says it timed out: " + answer.body(), answer.body().contains("\"timed_out\":true"));
            assertTrue("with no shard reported as failed: " + answer.body(), answer.body().contains("\"failed\":0"));
            assertTrue("over both shards: " + answer.body(), answer.body().contains("\"successful\":2"));
        }
    }

    /**
     * A plugin's search covers every index it names, and an alias resolves.
     *
     * <p>The client used to take {@code indices()[0]} and search that alone, so a plugin asking for two
     * indices got one with {@code _shards.total} agreeing with it, and a plugin asking through an alias
     * got "no such index". A privilege evaluator loading policy from several indices would have loaded
     * a subset and believed it complete -- the failure that must never succeed.
     */
    public void testAPluginSearchCoversEveryIndexNamedAndAnAlias() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("policy-a", "uuid-policy-a-00000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("policy-b", "uuid-policy-b-00000", 1, MAPPING, null));
        plane.createAlias(new AliasRecord("policy", List.of("policy-a", "policy-b")));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("client-coverage"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("policy-a", 0);
            loop.want("policy-b", 0);
            loop.tick(clock.get());

            final Client client = node.client();
            for (String index : List.of("policy-a", "policy-b")) {
                client.index(
                    new IndexRequest(index).id("rule")
                        .source("{\"msg\":\"rule\",\"rank\":1}", XContentType.JSON)
                        .setRefreshPolicy(org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE)
                ).actionGet();
            }

            final var both = client.search(
                new SearchRequest("policy-a", "policy-b").source(new org.opensearch.search.builder.SearchSourceBuilder().size(10))
            ).actionGet();
            assertEquals("every index named must be searched: " + both, 2L, both.getHits().getTotalHits().value());
            assertEquals("and the coverage must say so", 2, both.getTotalShards());
            assertEquals(2, both.getSuccessfulShards());

            final var viaAlias = client.search(
                new SearchRequest("policy").source(new org.opensearch.search.builder.SearchSourceBuilder().size(10))
            ).actionGet();
            assertEquals(
                "an alias resolves for a plugin as it does for a user: " + viaAlias,
                2L,
                viaAlias.getHits().getTotalHits().value()
            );
            assertEquals(2, viaAlias.getTotalShards());

            // A name that is not there is still refused in the vocabulary a plugin catches.
            expectThrows(
                org.opensearch.index.IndexNotFoundException.class,
                () -> client.search(new SearchRequest("policy-a", "policy-missing")).actionGet()
            );
        }
    }

    /** The plugin under test declares one index as its own. */
    public static final class HiddenStatePlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(new SystemIndexDescriptor(".hidden_state", "a plugin's own index"));
        }
    }

    /**
     * A pattern leaves a plugin's index out rather than refusing the whole search, and never names it.
     *
     * <p>{@code GET /*&#47;_search} failed for everyone, with a 403 spelling the plugin's index, from the
     * moment the plugin created it. The listing endpoints already leave such an index out of a pattern's
     * matches; a search does the same now. Naming the index directly is still refused, since the name
     * was the caller's to begin with.
     */
    public void testAPatternSearchLeavesAPluginsIndexOutRatherThanRefusing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("visible", "uuid-visible-0000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor(".hidden_state", "uuid-hidden-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("pattern-system"), List.of(new HiddenStatePlugin()))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("visible", 0);
            loop.want(".hidden_state", 0);
            loop.tick(clock.get());
            assertEquals(201, send(node, "PUT", "/visible/_doc/1?refresh=true", "{\"msg\":\"shown\",\"rank\":1}").status());

            final Response everything = send(node, "GET", "/*/_search", null);
            assertEquals(
                "a pattern must not be refused because a plugin's index matched it: " + everything.body(),
                200,
                everything.status()
            );
            assertTrue("and it covers what the caller may see: " + everything.body(), everything.body().contains("\"value\":1"));
            assertTrue("completely: " + everything.body(), everything.body().contains("\"complete\":true"));
            assertFalse("without naming what it left out: " + everything.body(), everything.body().contains(".hidden_state"));

            final Response onlyHidden = send(node, "GET", "/.hidden*/_search", null);
            assertEquals("a pattern matching only a plugin's index matched nothing: " + onlyHidden.body(), 404, onlyHidden.status());
            assertFalse("and still does not name it: " + onlyHidden.body(), onlyHidden.body().contains(".hidden_state"));

            final Response named = send(node, "GET", "/visible,.hidden_state/_search", null);
            assertEquals("naming it directly is still refused: " + named.body(), 403, named.status());
        }
    }

    /**
     * A lower-bound total is capped at what the caller asked to count, as core's reduce caps it.
     *
     * <p>Three shards each stop at two and report "at least two"; the sum is a floor of six, and the
     * caller who asked for {@code track_total_hits: 2} was told "at least six" -- true, and more than
     * they asked for, and a client that renders "more than N" from the N it sent is surprised.
     */
    public void testALowerBoundTotalIsCappedAtWhatWasAskedFor() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(new IndexDescriptor("counted", "uuid-counted-0000000", 3, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("total-ceiling"))) {
            node.start();
            node.setMetadataPlane(plane);
            hold(node, plane, clock, "counted", 3);
            for (int i = 0; i < 30; i++) {
                assertEquals(
                    201,
                    send(node, "PUT", "/counted/_doc/c" + i + "?refresh=true", "{\"msg\":\"doc\",\"rank\":" + i + "}").status()
                );
            }
            final Response capped = send(
                node,
                "POST",
                "/counted/_search",
                // A query core cannot count without collecting: match_all and a single term are
                // answered exactly from index statistics whatever track_total_hits says, so a script
                // query is what makes each shard stop at the threshold and report a lower bound.
                "{\"size\":0,\"track_total_hits\":2,\"query\":{\"script\":{\"script\":{\"source\":\"doc['rank'].value >= 0\"}}}}"
            );
            assertEquals(capped.body(), 200, capped.status());
            assertTrue(
                "the total is the ceiling the caller set: " + capped.body(),
                capped.body().contains("\"total\":{\"value\":2,\"relation\":\"gte\"}")
            );

            final Response exact = send(
                node,
                "POST",
                "/counted/_search",
                "{\"size\":0,\"track_total_hits\":true,\"query\":{\"script\":{\"script\":{\"source\":\"doc['rank'].value >= 0\"}}}}"
            );
            assertTrue(
                "and an exact count is left alone: " + exact.body(),
                exact.body().contains("\"total\":{\"value\":30,\"relation\":\"eq\"}")
            );
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(120))
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
