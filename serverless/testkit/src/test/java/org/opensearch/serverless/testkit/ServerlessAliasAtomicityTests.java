/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Alias and data-stream mutations under contention, and the shape of what they create.
 *
 * <p><b>A compare-and-swap token is only a token if it was read with the value it protects.</b> The alias
 * handlers used to read the record in one request and its generation in a second. A writer landing between
 * the two handed the second reader a fresh generation for a stale value: its swap succeeded, and the first
 * writer's acknowledged change was gone. The retry loop never saw it, because it retries a swap that
 * <em>fails</em>, and this one succeeded. The first test here arranges exactly that interleaving with a
 * blob store that makes two callers meet on the read and lets the second one proceed only once the first
 * has written -- deterministic rather than a race that hopes.
 *
 * <p>The rest pin the shape of what alias and template paths create: a data stream that survives being
 * touched through the alias API, a backing index that inherits the template that declared its stream, and
 * template settings honoured in each spelling core accepts.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessAliasAtomicityTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-alias-atomicity")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /**
     * A blob store that choreographs two callers of one alias register.
     *
     * <p>The first two threads to read the register wait for each other, so both hold the same value at
     * the same generation. After that, the first of them (the leader) is never held up, and every later
     * read by anyone else waits until a compare-and-swap on that register has landed. With a handler that
     * reads the generation separately from the value, the follower's generation read is therefore made to
     * happen after the leader's swap -- the interleaving that loses the leader's change. With a handler
     * that reads both in one read, the follower's swap simply fails and its re-read merges.
     */
    private static final class MeetingBlobStore implements BlobStore {

        private final BlobStore delegate;
        private final String blob;
        private final AtomicBoolean armed = new AtomicBoolean();
        private final AtomicInteger arrivals = new AtomicInteger();
        private final CyclicBarrier bothRead = new CyclicBarrier(2);
        private final CountDownLatch swapped = new CountDownLatch(1);
        private final AtomicReference<Thread> leader = new AtomicReference<>();

        MeetingBlobStore(BlobStore delegate, String aliasName) {
            this.delegate = delegate;
            this.blob = RegisterMap.descriptorBlob(aliasName);
        }

        void arm() {
            armed.set(true);
        }

        void disarm() {
            armed.set(false);
        }

        private boolean theAlias(BlobPath path, String blobName) {
            return armed.get() && blob.equals(blobName) && path.buildAsString().endsWith("indices/");
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            return new DelegatingBlobContainer(delegate.blobContainer(path)) {
                @Override
                public Optional<BlobRegister> readRegister(String blobName) throws IOException {
                    final Optional<BlobRegister> read = super.readRegister(blobName);
                    if (theAlias(path, blobName)) {
                        try {
                            final int arrival = arrivals.incrementAndGet();
                            if (arrival == 1) {
                                leader.set(Thread.currentThread());
                                bothRead.await(20, TimeUnit.SECONDS);
                            } else if (arrival == 2) {
                                bothRead.await(20, TimeUnit.SECONDS);
                            } else if (Thread.currentThread() != leader.get()) {
                                assertTrue("the leader must have swapped first", swapped.await(20, TimeUnit.SECONDS));
                            }
                        } catch (Exception e) {
                            throw new IOException("choreography broke", e);
                        }
                    }
                    return read;
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
                    throws IOException {
                    final BlobRegisterCasResult result = super.compareAndSwapRegister(blobName, expectedGeneration, newValue);
                    if (theAlias(path, blobName) && result.applied()) {
                        swapped.countDown();
                    }
                    return result;
                }
            };
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /**
     * Two callers adding different indices to one alias both land.
     *
     * <p>The interleaving is forced (see {@link MeetingBlobStore}); the assertion is on the record: after
     * both requests are acknowledged, the alias names everything that was added to it.
     */
    public void testTwoRacingAliasUpdatesBothLand() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MeetingBlobStore store = new MeetingBlobStore(new FsBlobStore(1024, createTempDir(), false), "current");
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        for (String index : new String[] { "base", "left", "right" }) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index + "-000000000000", 1, MAPPING, null));
        }

        try (ServerlessNode node = new ServerlessNode(nodeSettings("alias-race"))) {
            node.start();
            node.setMetadataPlane(plane);
            assertEquals(200, send(node, "PUT", "/base/_alias/current", null).status());

            store.arm();
            final ExecutorService callers = Executors.newFixedThreadPool(2);
            try {
                final Future<Response> left = callers.submit(() -> send(node, "PUT", "/left/_alias/current", null));
                final Future<Response> right = callers.submit(() -> send(node, "PUT", "/right/_alias/current", null));
                final Response first = left.get(60, TimeUnit.SECONDS);
                final Response second = right.get(60, TimeUnit.SECONDS);
                assertEquals(first.body(), 200, first.status());
                assertEquals(second.body(), 200, second.status());
            } finally {
                store.disarm();
                callers.shutdownNow();
            }

            final Response read = send(node, "GET", "/_alias/current", null);
            assertEquals(read.body(), 200, read.status());
            for (String index : new String[] { "base", "left", "right" }) {
                assertTrue(
                    "both acknowledged adds must be in the record, not one overwritten by the other: " + read.body(),
                    read.body().contains("\"" + index + "\":{\"aliases\":{\"current\":{}}}")
                );
            }
            // And the reverse hint on each index agrees.
            for (String index : new String[] { "left", "right" }) {
                assertTrue(send(node, "GET", "/" + index + "/_alias", null).body().contains("\"current\":{}"));
            }
        }
    }

    /** A data stream touched through the alias API stays a data stream, and the request is refused in core's words. */
    public void testAliasActionsOnADataStreamAreRefusedAndTheStreamSurvives() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("alias-on-stream"))) {
            node.start();
            node.setMetadataPlane(plane);
            assertEquals(
                200,
                send(
                    node,
                    "PUT",
                    "/_index_template/events",
                    "{\"index_patterns\":[\"events*\"],\"data_stream\":{},\"template\":{\"mappings\":" + MAPPING + "}}"
                ).status()
            );
            assertEquals(200, send(node, "PUT", "/_data_stream/events", null).status());
            assertEquals(200, send(node, "PUT", "/other", "{\"mappings\":" + MAPPING + "}").status());

            final Response added = send(node, "POST", "/_aliases", "{\"actions\":[{\"add\":{\"index\":\"other\",\"alias\":\"events\"}}]}");
            assertEquals("core refuses an alias action on a data stream: " + added.body(), 400, added.status());
            assertTrue(added.body(), added.body().contains("matches a data stream"));
            assertEquals(400, send(node, "PUT", "/other/_alias/events", null).status());
            assertEquals(400, send(node, "DELETE", "/_alias/events", null).status());
            assertEquals(400, send(node, "DELETE", "/.ds-events-000001/_alias/events", null).status());
            assertEquals("a data stream is not an alias", 404, send(node, "GET", "/_alias/events", null).status());
            assertEquals(404, send(node, "HEAD", "/_alias/events", null).status());

            // The stream is intact: still a stream, still generation 1, still writable through the name.
            final Response described = send(node, "GET", "/_data_stream/events", null);
            assertEquals("the stream must still exist: " + described.body(), 200, described.status());
            assertTrue(described.body(), described.body().contains("\"generation\":1"));
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want(".ds-events-000001", 0);
            loop.tick(clock.get());
            assertEquals(
                201,
                send(node, "POST", "/events/_create/1?refresh=true", "{\"@timestamp\":\"2026-09-04T00:00:00Z\",\"msg\":\"still a stream\"}")
                    .status()
            );
            final Response rolled = send(node, "POST", "/events/_rollover", null);
            assertEquals(rolled.body(), 200, rolled.status());
            assertTrue(rolled.body(), rolled.body().contains("\"new_index\":\".ds-events-000002\""));

            // _resolve/index renders it as what it is.
            final Response resolved = send(node, "GET", "/_resolve/index/events", null);
            assertTrue(
                "a data stream under data_streams: " + resolved.body(),
                resolved.body().contains("\"data_streams\":[{\"name\":\"events\"")
            );
            assertTrue(resolved.body(), resolved.body().contains("\"backing_indices\":[\".ds-events-000001\",\".ds-events-000002\"]"));
            assertTrue("and not under aliases: " + resolved.body(), resolved.body().contains("\"aliases\":[]"));

            // The write index of a stream cannot be deleted from under it; an older backing index can.
            final Response writeIndex = send(node, "DELETE", "/.ds-events-000002", null);
            assertEquals(writeIndex.body(), 400, writeIndex.status());
            assertTrue(writeIndex.body(), writeIndex.body().contains("is the write index for data stream [events]"));
        }
    }

    /** A backing index inherits the settings and mappings of the template that declared its stream. */
    public void testABackingIndexHasTheShapeOfTheStreamsTemplate() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("stream-shape"))) {
            node.start();
            node.setMetadataPlane(plane);
            assertEquals(
                200,
                send(
                    node,
                    "PUT",
                    "/_index_template/events",
                    "{\"index_patterns\":[\"events*\"],\"data_stream\":{},\"template\":{"
                        + "\"settings\":{\"number_of_shards\":2,\"refresh_interval\":\"9s\"},"
                        + "\"mappings\":{\"properties\":{\"msg\":{\"type\":\"keyword\"}}}}}"
                ).status()
            );
            assertEquals(200, send(node, "PUT", "/_data_stream/events", null).status());
            assertEquals(200, send(node, "POST", "/events/_rollover", null).status());

            for (String backing : new String[] { ".ds-events-000001", ".ds-events-000002" }) {
                final Response settings = send(node, "GET", "/" + backing + "/_settings", null);
                assertEquals(settings.body(), 200, settings.status());
                assertTrue(
                    "the template's shard count, not the default of one: " + settings.body(),
                    settings.body().contains("\"number_of_shards\":\"2\"")
                );
                assertTrue("and its settings: " + settings.body(), settings.body().contains("\"refresh_interval\":\"9s\""));
                final Response mapping = send(node, "GET", "/" + backing + "/_mapping", null);
                assertTrue("the template's field: " + mapping.body(), mapping.body().contains("\"msg\":{\"type\":\"keyword\"}"));
                assertTrue("and the timestamp: " + mapping.body(), mapping.body().contains("\"@timestamp\":{\"type\":\"date\"}"));
            }

            // The names a rollover computes are not for users to take.
            final Response taken = send(node, "PUT", "/.ds-events-000003", null);
            assertEquals(taken.body(), 400, taken.status());
            assertTrue(taken.body(), taken.body().contains("must not start with '.ds-'"));
            // And a stream's rollover takes neither a name nor a shape.
            assertEquals(400, send(node, "POST", "/events/_rollover/elsewhere", null).status());
            assertEquals(400, send(node, "POST", "/events/_rollover", "{\"settings\":{\"refresh_interval\":\"1s\"}}").status());
        }
    }

    /** Template settings in core's nested and dotted spellings are the same settings as the bare one. */
    public void testTemplateSettingsInEverySpellingAreHonoured() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("template-spellings"))) {
            node.start();
            node.setMetadataPlane(plane);
            assertEquals(
                200,
                send(
                    node,
                    "PUT",
                    "/_index_template/nested",
                    "{\"index_patterns\":[\"nested-*\"],\"template\":{\"settings\":{\"index\":{\"number_of_shards\":3,\"refresh_interval\":\"5s\"}}}}"
                ).status()
            );
            assertEquals(
                200,
                send(
                    node,
                    "PUT",
                    "/_index_template/dotted",
                    "{\"index_patterns\":[\"dotted-*\"],\"template\":{\"settings\":{\"index.number_of_shards\":2,\"index.refresh_interval\":\"7s\"}}}"
                ).status()
            );

            final Response nested = send(node, "PUT", "/nested-1", null);
            assertEquals(nested.body(), 200, nested.status());
            assertTrue("the nested spelling's shard count: " + nested.body(), nested.body().contains("\"shards\":3"));
            final Response nestedSettings = send(node, "GET", "/nested-1/_settings", null);
            assertTrue(nestedSettings.body(), nestedSettings.body().contains("\"number_of_shards\":\"3\""));
            assertTrue(nestedSettings.body(), nestedSettings.body().contains("\"refresh_interval\":\"5s\""));
            assertFalse("no mangled index.index key: " + nestedSettings.body(), nestedSettings.body().contains("\"index\":\"{"));

            final Response dotted = send(node, "PUT", "/dotted-1", null);
            assertEquals(dotted.body(), 200, dotted.status());
            assertTrue("the dotted spelling's shard count: " + dotted.body(), dotted.body().contains("\"shards\":2"));
            final Response dottedSettings = send(node, "GET", "/dotted-1/_settings", null);
            assertTrue(dottedSettings.body(), dottedSettings.body().contains("\"refresh_interval\":\"7s\""));
            assertFalse(dottedSettings.body(), dottedSettings.body().contains("index.refresh_interval"));

            // A value core cannot parse is refused when the template is stored, not by the first index.
            final Response bad = send(
                node,
                "PUT",
                "/_index_template/broken",
                "{\"index_patterns\":[\"broken-*\"],\"template\":{\"settings\":{\"refresh_interval\":\"banana\"}}}"
            );
            assertEquals(bad.body(), 400, bad.status());
            assertEquals(404, send(node, "GET", "/_index_template/broken", null).status());
        }
    }

    /** An unparseable setting is refused before the compare-and-swap, so nothing reads it back. */
    public void testAnUnparseableSettingIsRefusedNotCommitted() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("settings-parse"))) {
            node.start();
            node.setMetadataPlane(plane);
            assertEquals(200, send(node, "PUT", "/alpha", "{\"mappings\":" + MAPPING + "}").status());

            final Response refused = send(node, "PUT", "/alpha/_settings", "{\"refresh_interval\":\"banana\"}");
            assertEquals(refused.body(), 400, refused.status());
            final Response settings = send(node, "GET", "/alpha/_settings", null);
            assertFalse("the value must not have been committed: " + settings.body(), settings.body().contains("banana"));

            // The same at creation, in the request's own settings.
            final Response badCreate = send(node, "PUT", "/bad", "{\"settings\":{\"refresh_interval\":\"banana\"}}");
            assertEquals(badCreate.body(), 400, badCreate.status());
            assertEquals(404, send(node, "GET", "/bad", null).status());

            // The pipeline setting nothing here reads is still refused with the reason, not stored. The two
            // ingest ones used to be refused alongside it and are read by the write path now, so the
            // dividing line is exactly "does anything run it", which is what this pins.
            final Response searchPiped = send(node, "PUT", "/alpha/_settings", "{\"index.search.default_pipeline\":\"tag\"}");
            assertEquals(searchPiped.body(), 501, searchPiped.status());
            assertTrue(searchPiped.body(), searchPiped.body().contains("default_pipeline"));
            assertFalse(send(node, "GET", "/alpha/_settings", null).body().contains("default_pipeline"));

            final Response ingestPiped = send(node, "PUT", "/alpha/_settings", "{\"index.default_pipeline\":\"tag\"}");
            assertEquals("the write path reads it, so it is an ordinary setting: " + ingestPiped.body(), 200, ingestPiped.status());
            assertTrue(send(node, "GET", "/alpha/_settings", null).body().contains("default_pipeline"));
        }
    }

    /** A remove that removes nothing is core's 404, and a lost last-index delete is not a lost add. */
    public void testRemovingWhatIsNotThereIsNotFound() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("one", "uuid-one-00000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("two", "uuid-two-00000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("alias-remove"))) {
            node.start();
            node.setMetadataPlane(plane);
            assertEquals(200, send(node, "PUT", "/one/_alias/x", null).status());

            final Response notUnder = send(node, "DELETE", "/two/_alias/x", null);
            assertEquals("core answers 404 for an index the alias does not cover: " + notUnder.body(), 404, notUnder.status());
            final Response noOp = send(node, "POST", "/_aliases", "{\"actions\":[{\"remove\":{\"index\":\"two\",\"alias\":\"x\"}}]}");
            assertEquals("and for a remove that matched nothing: " + noOp.body(), 404, noOp.status());
            assertTrue(noOp.body(), noOp.body().contains("aliases_not_found_exception"));
            assertEquals(
                "must_exist=false asks for silence",
                200,
                send(node, "POST", "/_aliases", "{\"actions\":[{\"remove\":{\"index\":\"two\",\"alias\":\"x\",\"must_exist\":false}}]}")
                    .status()
            );
            assertEquals(501, send(node, "POST", "/_aliases", "{\"actions\":[{\"remove_index\":{\"index\":\"two\"}}]}").status());

            // Deleting an index drops it from the aliases that named it, so a later index of the same
            // name is not silently under them.
            assertEquals(200, send(node, "PUT", "/two/_alias/x", null).status());
            assertEquals(200, send(node, "DELETE", "/one", null).status());
            final Response after = send(node, "GET", "/_alias/x", null);
            assertFalse("the deleted index is gone from the alias: " + after.body(), after.body().contains("\"one\""));
            assertEquals(200, send(node, "PUT", "/one", "{\"mappings\":" + MAPPING + "}").status());
            assertFalse(send(node, "GET", "/_alias/x", null).body().contains("\"one\""));
            // The last index gone, the alias keeps naming it, so a search through it says which index is
            // missing rather than "no such index: x" -- the answer a named index gets.
            assertEquals(200, send(node, "DELETE", "/two", null).status());
            final Response stale = send(node, "POST", "/x/_search", "{\"query\":{\"match_all\":{}}}");
            assertEquals(stale.body(), 404, stale.status());
            assertTrue("it names the index that is gone: " + stale.body(), stale.body().contains("two"));
        }
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
