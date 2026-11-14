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
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@code POST /{index}/_delete_by_query} — every document a query matches, removed.
 *
 * <p>See {@link org.opensearch.serverless.shard.ShardOperations#deleteByQuery} for how it matches what a search sees
 * when it starts, one shard at a time in sequence-number order.
 *
 * <p>Runs against {@code FsBlobContainer} only.
 */
public class ServerlessDeleteByQueryTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"},"
        + "\"keep\":{\"type\":\"boolean\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-delete-by-query")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            // Each write here waits for the one before it, and the node log's window would add its wait to every one.
            .put("serverless.wal.node_log.enabled", false)
            .build();
    }

    private MetadataPlane planeOver(java.nio.file.Path dir, AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    private record Response(int status, String body) {
    }

    private static Response send(TransportAddress address, String method, String path, String body) throws Exception {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(30))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    private ServerlessNode started(MetadataPlane plane, String name) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.startup().start();
        node.setMetadataPlane(plane);
        return node;
    }

    /**
     * Publishes every open shard of an index. Delete-by-query matches what a search finds, published or not; tests publish
     * where they also check what survives a restart of the shard.
     */
    private void publish(ServerlessNode node, MetadataPlane plane, String index, int shardCount) throws Exception {
        for (int shard = 0; shard < shardCount; shard++) {
            final int number = shard;
            final var shardId = node.reconciler()
                .openShards()
                .stream()
                .filter(s -> s.getIndexName().equals(index) && s.id() == number)
                .findFirst()
                .orElseThrow();
            final long term = plane.heads().read(index, shard).orElseThrow().term();
            node.shards().publishShard(shardId, term);
        }
    }

    public static final class DeleteSelectionPlugin extends org.opensearch.plugins.Plugin implements org.opensearch.plugins.ActionPlugin {
        private final boolean replace;

        DeleteSelectionPlugin(boolean replace) {
            this.replace = replace;
        }

        @Override
        public java.util.List<org.opensearch.action.support.ActionFilter> getActionFilters() {
            return java.util.List.of(new org.opensearch.action.support.ActionFilter() {
                @Override
                public int order() {
                    return 0;
                }

                @Override
                @SuppressWarnings("unchecked")
                public <
                    Request extends org.opensearch.action.ActionRequest,
                    Response extends org.opensearch.core.action.ActionResponse> void apply(
                        org.opensearch.tasks.Task task,
                        String action,
                        Request request,
                        org.opensearch.action.support.ActionRequestMetadata<Request, Response> metadata,
                        org.opensearch.core.action.ActionListener<Response> listener,
                        org.opensearch.action.support.ActionFilterChain<Request, Response> chain
                    ) {
                    Request admitted = request;
                    if (request instanceof org.opensearch.index.reindex.DeleteByQueryRequest original) {
                        // A security filter must see the selection and limits actually being executed.
                        if (original.getSearchRequest().source().query() == null
                            || original.isRefresh() == false
                            || original.getMaxDocs() != 2) {
                            listener.onFailure(new IllegalStateException("delete request lost its query, refresh or maximum"));
                            return;
                        }
                        final var restricted = replace
                            ? new org.opensearch.index.reindex.DeleteByQueryRequest(original.indices())
                            : original;
                        restricted.setQuery(org.opensearch.index.query.QueryBuilders.termQuery("keep", false));
                        restricted.setMaxDocs(1);
                        restricted.setRefresh(true);
                        admitted = (Request) restricted;
                    }
                    chain.proceed(task, action, admitted, listener);
                }
            });
        }
    }

    public void testDeleteFiltersCanRestrictTheAdmittedSelectionAndLimits() throws Exception {
        for (boolean replace : new boolean[] { false, true }) {
            final var clock = new AtomicLong(1_000L);
            final var plane = planeOver(createTempDir(), clock);
            try (
                var node = new ServerlessNode(nodeSettings("dbq-filter-" + replace), java.util.List.of(new DeleteSelectionPlugin(replace)))
            ) {
                node.startup().start();
                node.setMetadataPlane(plane);
                prepareSelectionDocuments(node, plane);
                final var http = node.boundHttpAddress().publishAddress();
                final var response = send(
                    http,
                    "POST",
                    "/alpha/_delete_by_query?max_docs=2&refresh=true",
                    "{\"query\":{\"match_all\":{}}}"
                );
                assertEquals(response.body(), 200, response.status());
                assertTrue(response.body(), response.body().contains("\"deleted\":1"));
                assertEquals(404, send(http, "GET", "/alpha/_doc/both", null).status());
                assertEquals(200, send(http, "GET", "/alpha/_doc/red", null).status());
                assertEquals(200, send(http, "GET", "/alpha/_doc/blue", null).status());
            }
        }
    }

    public void testInvalidMaximumsAreRejectedWithoutDeletingDocuments() throws Exception {
        final var clock = new AtomicLong(1_000L);
        final var metadata = planeOver(createTempDir(), clock);
        try (var node = started(metadata, "dbq-invalid-maximum")) {
            prepareSelectionDocuments(node, metadata);
            final var http = node.boundHttpAddress().publishAddress();
            for (String maximum : java.util.List.of("0", "-1", "-9223372036854775808", "2147483648")) {
                final var response = send(http, "POST", "/alpha/_delete_by_query?max_docs=" + maximum, "{\"query\":{\"match_all\":{}}}");
                assertEquals(response.body(), 400, response.status());
                assertEquals(200, send(http, "GET", "/alpha/_doc/both", null).status());
            }
        }
    }

    public void testUrlQueryOptionsCannotBroadenDeletion() throws Exception {
        final var clock = new AtomicLong(1_000L);
        final var plane = planeOver(createTempDir(), clock);
        try (var node = started(plane, "dbq-url-options")) {
            final var http = node.boundHttpAddress().publishAddress();
            prepareSelectionDocuments(node, plane);
            final var response = send(http, "POST", "/alpha/_delete_by_query?q=red%20blue&df=msg&default_operator=AND&refresh=true", null);
            assertEquals(response.body(), 200, response.status());
            assertTrue(response.body(), response.body().contains("\"deleted\":1"));
            assertEquals(404, send(http, "GET", "/alpha/_doc/both", null).status());
            assertEquals(200, send(http, "GET", "/alpha/_doc/red", null).status());
            assertEquals(200, send(http, "GET", "/alpha/_doc/blue", null).status());
        }
    }

    public void testUrlQueryOverridesTheBodyQueryBeforeDeletion() throws Exception {
        final var clock = new AtomicLong(1_000L);
        final var plane = planeOver(createTempDir(), clock);
        try (var node = started(plane, "dbq-url-precedence")) {
            final var http = node.boundHttpAddress().publishAddress();
            prepareSelectionDocuments(node, plane);
            final var response = send(http, "POST", "/alpha/_delete_by_query?q=keep:false&refresh=true", "{\"query\":{\"match_all\":{}}}");
            assertEquals(response.body(), 200, response.status());
            assertTrue(response.body(), response.body().contains("\"deleted\":1"));
            assertEquals(200, send(http, "GET", "/alpha/_doc/red", null).status());
            assertEquals(200, send(http, "GET", "/alpha/_doc/blue", null).status());
        }
    }

    public void testUnsupportedUrlSelectionOptionsAreRefusedBeforeAnyDeletion() throws Exception {
        final var clock = new AtomicLong(1_000L);
        final var plane = planeOver(createTempDir(), clock);
        try (var node = started(plane, "dbq-url-selection")) {
            final var http = node.boundHttpAddress().publishAddress();
            prepareSelectionDocuments(node, plane);
            for (String option : new String[] { "size=1", "from=1", "sort=n:asc", "terminate_after=1", "min_score=1000" }) {
                final var response = send(http, "POST", "/alpha/_delete_by_query?refresh=true&" + option, "{\"query\":{\"match_all\":{}}}");
                assertEquals(response.body(), 501, response.status());
                assertEquals(200, send(http, "GET", "/alpha/_doc/both", null).status());
                assertEquals(200, send(http, "GET", "/alpha/_doc/red", null).status());
                assertEquals(200, send(http, "GET", "/alpha/_doc/blue", null).status());
            }
        }
    }

    public void testUnsupportedBodyOptionsAreRefusedBeforeAnyDeletion() throws Exception {
        final var clock = new AtomicLong(1_000L);
        final var plane = planeOver(createTempDir(), clock);
        try (var node = started(plane, "dbq-body-selection")) {
            final var http = node.boundHttpAddress().publishAddress();
            prepareSelectionDocuments(node, plane);
            for (String option : new String[] {
                "\"min_score\":1000",
                "\"terminate_after\":1",
                "\"timeout\":\"1ms\"",
                "\"explain\":true" }) {
                final var response = send(
                    http,
                    "POST",
                    "/alpha/_delete_by_query?refresh=true",
                    "{\"query\":{\"match_all\":{}}," + option + "}"
                );
                assertEquals(response.body(), 501, response.status());
                assertEquals(200, send(http, "GET", "/alpha/_doc/both", null).status());
                assertEquals(200, send(http, "GET", "/alpha/_doc/red", null).status());
                assertEquals(200, send(http, "GET", "/alpha/_doc/blue", null).status());
            }
        }
    }

    private void prepareSelectionDocuments(ServerlessNode node, MetadataPlane plane) throws Exception {
        final var http = node.boundHttpAddress().publishAddress();
        assertEquals(200, send(http, "PUT", "/alpha?shards=1", MAPPING).status());
        node.activation().activateWriter(plane, "alpha", 0);
        assertEquals(201, send(http, "PUT", "/alpha/_doc/both?refresh=true", "{\"msg\":\"red blue\",\"keep\":false}").status());
        assertEquals(201, send(http, "PUT", "/alpha/_doc/red?refresh=true", "{\"msg\":\"red\",\"keep\":true}").status());
        assertEquals(201, send(http, "PUT", "/alpha/_doc/blue?refresh=true", "{\"msg\":\"blue\",\"keep\":true}").status());
        publish(node, plane, "alpha", 1);
    }

    /**
     * Matching documents are removed, and everything else survives -- checked over several shards, which
     * is the case that actually exercises the per-shard walk rather than one shard answering everything.
     */
    public void testMatchingDocumentsAreDeletedAcrossSeveralShards() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-basic")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=3", MAPPING);
            node.activation().activateWriter(plane, "alpha", 0);
            node.activation().activateWriter(plane, "alpha", 1);
            node.activation().activateWriter(plane, "alpha", 2);

            for (int i = 1; i <= 30; i++) {
                final boolean keep = i % 3 == 0;
                assertEquals(
                    201,
                    send(http, "PUT", "/alpha/_doc/d" + i + "?refresh=true", "{\"msg\":\"x\",\"n\":" + i + ",\"keep\":" + keep + "}")
                        .status()
                );
            }
            publish(node, plane, "alpha", 3);

            final Response result = send(http, "POST", "/alpha/_delete_by_query?refresh=true", "{\"query\":{\"term\":{\"keep\":false}}}");
            assertEquals(result.body(), 200, result.status());
            assertTrue("20 of 30 must have matched: " + result.body(), result.body().contains("\"matched\":20"));
            assertTrue("and all 20 deleted: " + result.body(), result.body().contains("\"deleted\":20"));

            final Response search = send(http, "POST", "/alpha/_search", "{\"size\":30,\"query\":{\"match_all\":{}}}");
            assertTrue("only the kept 10 must remain: " + search.body(), search.body().contains("\"value\":10"));
            for (int i = 1; i <= 30; i++) {
                final boolean shouldRemain = i % 3 == 0;
                assertEquals(
                    "d" + i + " must " + (shouldRemain ? "remain" : "be gone"),
                    shouldRemain ? 200 : 404,
                    send(http, "GET", "/alpha/_doc/d" + i, null).status()
                );
            }
        }
    }

    /**
     * More matches than one page, on one shard -- this is the assertion the per-shard walk stands on.
     *
     * <p>The handler's own batch size is 1,000 and is package-private; 2,137 is deliberately more than
     * twice that rather than a number derived from it, so this does not need to track the constant to stay
     * meaningful. One page's worth of matches would already prove the walk fetches at least one page; well
     * over two proves it actually continues with {@code search_after} rather than silently stopping at the
     * first page.
     */
    public void testMoreThanOnePageOfMatchesAreAllDeleted() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-paged")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activation().activateWriter(plane, "alpha", 0);

            final int documents = 2137;
            for (int i = 1; i <= documents; i++) {
                node.writes()
                    .index(node.reconciler().openShards().iterator().next(), "d" + i, "{\"msg\":\"x\",\"n\":" + i + ",\"keep\":false}");
            }
            node.reconciler().shard(node.reconciler().openShards().iterator().next()).refresh("dbq-paged");
            publish(node, plane, "alpha", 1);

            final Response result = send(http, "POST", "/alpha/_delete_by_query?refresh=true", "{\"query\":{\"match_all\":{}}}");
            assertEquals(result.body(), 200, result.status());
            assertTrue(
                "every one of " + documents + " matches must be walked, not just the first page: " + result.body(),
                result.body().contains("\"matched\":" + documents)
            );
            assertTrue(result.body(), result.body().contains("\"deleted\":" + documents));
            assertTrue("all three nonempty pages must be reported: " + result.body(), result.body().contains("\"batches\":3"));

            final Response search = send(http, "POST", "/alpha/_search", "{\"query\":{\"match_all\":{}}}");
            assertTrue("nothing must remain: " + search.body(), search.body().contains("\"value\":0"));
        }
    }

    /** A query matching nothing deletes nothing, and says so rather than erroring. */
    public void testAQueryMatchingNothingDeletesNothing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-nothing")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activation().activateWriter(plane, "alpha", 0);
            send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"x\",\"n\":1,\"keep\":true}");
            publish(node, plane, "alpha", 1);

            final Response result = send(http, "POST", "/alpha/_delete_by_query", "{\"query\":{\"term\":{\"keep\":false}}}");
            assertEquals(result.body(), 200, result.status());
            assertTrue(result.body(), result.body().contains("\"matched\":0"));
            assertEquals(200, send(http, "GET", "/alpha/_doc/1", null).status());
        }
    }

    /**
     * A write refreshed but not yet published is matched: delete_by_query removes what a search finds now, as in core, not
     * only what the writer has published.
     */
    public void testARefreshedUnpublishedWriteIsMatched() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-live")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activation().activateWriter(plane, "alpha", 0);

            send(http, "PUT", "/alpha/_doc/published?refresh=true", "{\"msg\":\"x\",\"n\":1,\"keep\":false}");
            publish(node, plane, "alpha", 1);
            // Refreshed, so a search finds it, and never published.
            send(http, "PUT", "/alpha/_doc/unpublished?refresh=true", "{\"msg\":\"x\",\"n\":2,\"keep\":false}");
            // Written and not refreshed: core does not match such a document either.
            send(http, "PUT", "/alpha/_doc/unrefreshed", "{\"msg\":\"x\",\"n\":3,\"keep\":false}");

            final Response result = send(http, "POST", "/alpha/_delete_by_query", "{\"query\":{\"term\":{\"keep\":false}}}");
            assertEquals(result.body(), 200, result.status());
            assertTrue("both refreshed documents are matched: " + result.body(), result.body().contains("\"deleted\":2"));
            assertEquals(404, send(http, "GET", "/alpha/_doc/published", null).status());
            assertEquals(404, send(http, "GET", "/alpha/_doc/unpublished", null).status());
            assertEquals("a write no search could see yet is left", 200, send(http, "GET", "/alpha/_doc/unrefreshed", null).status());
        }
    }

    /** {@code max_docs} bounds how many matches are visited, as a safety valve rather than an all-or-nothing. */
    public void testMaxDocsBoundsHowManyAreDeleted() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-maxdocs")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activation().activateWriter(plane, "alpha", 0);
            for (int i = 1; i <= 10; i++) {
                send(http, "PUT", "/alpha/_doc/d" + i + "?refresh=true", "{\"msg\":\"x\",\"n\":" + i + ",\"keep\":false}");
            }
            publish(node, plane, "alpha", 1);

            final Response result = send(http, "POST", "/alpha/_delete_by_query?max_docs=4&refresh=true", "{\"query\":{\"match_all\":{}}}");
            assertEquals(result.body(), 200, result.status());
            assertTrue(result.body(), result.body().contains("\"matched\":4"));

            final Response search = send(http, "POST", "/alpha/_search", "{\"size\":10,\"query\":{\"match_all\":{}}}");
            assertTrue("6 of the 10 must remain: " + search.body(), search.body().contains("\"value\":6"));
        }
    }

    /** No query at all is refused; there is no implicit match_all. */
    public void testAMissingQueryIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-noquery")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activation().activateWriter(plane, "alpha", 0);
            send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"x\",\"n\":1,\"keep\":true}");

            assertEquals(400, send(http, "POST", "/alpha/_delete_by_query", "{}").status());
            assertEquals(200, send(http, "GET", "/alpha/_doc/1", null).status());
        }
    }

    /** A sort in the body is refused, not silently overridden by the internal one. */
    public void testASortInTheBodyIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-sort")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activation().activateWriter(plane, "alpha", 0);

            final Response refused = send(
                http,
                "POST",
                "/alpha/_delete_by_query",
                "{\"query\":{\"match_all\":{}},\"sort\":[{\"n\":\"asc\"}]}"
            );
            assertEquals(refused.body(), 501, refused.status());
            assertTrue(refused.body(), refused.body().contains("sort"));
        }
    }

    /** No index at all is a plain miss, not an error. */
    public void testAMissingIndexIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-missing-index")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            final Response refused = send(http, "POST", "/ghost/_delete_by_query", "{\"query\":{\"match_all\":{}}}");
            assertEquals(404, refused.status());
        }
    }

    /** The operation opens no frozen view and records no point in time: nothing is left to release. */
    public void testNoFrozenViewIsLeftBehind() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "dbq-release")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activation().activateWriter(plane, "alpha", 0);
            send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"x\",\"n\":1,\"keep\":false}");
            publish(node, plane, "alpha", 1);

            send(http, "POST", "/alpha/_delete_by_query", "{\"query\":{\"match_all\":{}}}");

            assertTrue("no frozen views must be left open on this node", node.reconciler().frozenShards().isEmpty());
            assertTrue("and the record itself must be gone", plane.livePointsInTime(clock.get()).isEmpty());
        }
    }
}
