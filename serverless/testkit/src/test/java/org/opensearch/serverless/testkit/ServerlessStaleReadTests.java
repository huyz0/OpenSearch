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
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A reader never answers from a commit behind an acknowledged write while nobody is writing the shard.
 *
 * <p>Every write is in the log before it is acknowledged and in a published commit only later. A writer that lets go
 * of a shard publishes first; one that dies, or lapses and gives the shard back, does not, and until some node takes
 * the shard and replays the log the published commit is all a reader has. Each fixture here acknowledges a document,
 * leaves it unpublished, takes the writer away, and asks another node -- which must find the document or say it
 * cannot, never answer as though it did not exist.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessStaleReadTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-stale-read")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .build();
    }

    /**
     * A writer gives the shard back without publishing -- what a lapsed lease does -- and a get on another node must
     * not report the acknowledged document missing.
     */
    public void testAGetOfAShardGivenBackUnpublishedDoesNotMissAnAcknowledgedWrite() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode writer = new ServerlessNode(nodeSettings("stale-writer", "ingest"));
            ServerlessNode reader = new ServerlessNode(nodeSettings("stale-reader", "search"))
        ) {
            writer.start();
            reader.start();
            writer.setMetadataPlane(plane);
            reader.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            assertEquals(201, send(writer, "PUT", "/alpha/_doc/published", "{\"msg\":\"old\",\"n\":1}").status());
            loop.publishAll();
            // Acknowledged, and in the log only.
            assertEquals(201, send(writer, "PUT", "/alpha/_doc/unpublished", "{\"msg\":\"new\",\"n\":2}").status());

            // What a lapsed writer does when its lease comes back: gives the head back, unpublished.
            assertTrue(plane.heads().release("alpha", 0, writer.localNode().getId()));

            final Response published = send(reader, "GET", "/alpha/_doc/published", null);
            final Response got = send(reader, "GET", "/alpha/_doc/unpublished", null);
            if (got.status() == 200) {
                assertTrue("found, or not answered at all: " + got.body(), got.body().contains("\"found\":true"));
            } else {
                assertEquals("refused as behind the log: " + got.body(), 503, got.status());
                assertTrue(got.body(), got.body().contains("shard_behind_log"));
                assertEquals("and a document the commit does hold refuses the same way, it is the same commit", 503, published.status());
            }

            final Response multi = send(reader, "POST", "/alpha/_mget", "{\"ids\":[\"unpublished\"]}");
            assertFalse("a multi-get must not say it is missing either: " + multi.body(), multi.body().contains("\"found\":false"));
        }
    }

    /**
     * The writer's lease lapses with a document unpublished, its head still naming it -- a crash -- and a search on
     * another node must find the document or report the shard, never return no hits and no failure.
     */
    public void testASearchOfAShardWhoseWriterDiedDoesNotMissAnAcknowledgedWrite() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode writer = new ServerlessNode(nodeSettings("dead-writer", "ingest"));
            ServerlessNode reader = new ServerlessNode(nodeSettings("dead-reader", "search"))
        ) {
            writer.start();
            reader.start();
            writer.setMetadataPlane(plane);
            reader.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            assertEquals(201, send(writer, "PUT", "/alpha/_doc/published", "{\"msg\":\"needle\",\"n\":1}").status());
            loop.publishAll();

            // A reader opened while the writer is alive serves the commit, which the publish window allows.
            final Response before = send(reader, "POST", "/alpha/_search", "{\"query\":{\"match\":{\"msg\":\"needle\"}}}");
            assertEquals(before.body(), 200, before.status());
            assertEquals(before.body(), 1L, number(before.body(), "value"));

            assertEquals(201, send(writer, "PUT", "/alpha/_doc/unpublished", "{\"msg\":\"needle\",\"n\":2}").status());
            // The writer dies: its lease runs out and nobody has taken the shard, so the head still names it.
            clock.addAndGet(2 * TTL);
            reader.renewLease(plane);

            final Response after = send(reader, "POST", "/alpha/_search", "{\"query\":{\"match\":{\"msg\":\"needle\"}}}");
            final long hits = after.status() == 200 ? number(after.body(), "value") : -1L;
            final long failed = after.status() == 200 ? number(after.body(), "failed") : -1L;
            assertTrue(
                "both documents, or the shard reported as not answered -- never one hit and no failure: " + after.body(),
                hits == 2L || failed > 0L || after.status() >= 500
            );
        }
    }

    /**
     * A reader that validated a shard nobody owned, after which a writer took it, acknowledged a write and gave the head
     * back unpublished -- what a lease lapse under throttling does -- must not go on serving the old commit as current.
     *
     * <p>The reader remembered "nobody owns it" and, while no node had departed, answered from that memory without a
     * read: a fleet's stale-read check found two searches returning one of two acknowledged documents and no failure.
     */
    public void testAReaderNoticesAWriterThatCameAndWentUnpublished() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode writer = new ServerlessNode(nodeSettings("came-and-went-writer", "ingest"));
            ServerlessNode reader = new ServerlessNode(nodeSettings("came-and-went-reader", "search"))
        ) {
            writer.start();
            reader.start();
            writer.setMetadataPlane(plane);
            reader.setMetadataPlane(plane);
            writer.renewLease(plane);
            reader.renewLease(plane);
            // Re-checked on every search here, so the test needs no sleep; the production bound is a second.
            reader.setReaderCheckMillis(0);
            final BackgroundReconciler loop = new BackgroundReconciler(writer, plane).setDemandDrivenActivation(true);
            loop.activateOnDemand(java.util.List.of(java.util.Map.entry("alpha", 0)));
            assertEquals(201, send(writer, "PUT", "/alpha/_doc/first", "{\"msg\":\"needle\",\"n\":1}").status());
            loop.publishAll();
            loop.setIdleAfterMillis(1);
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
            assertNull("given back cleanly, published", plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());

            final Response before = send(reader, "POST", "/alpha/_search", "{\"query\":{\"match\":{\"msg\":\"needle\"}}}");
            assertEquals(before.body(), 1L, number(before.body(), "value"));

            // A writer comes, acknowledges a write, and goes without publishing it.
            loop.activateOnDemand(java.util.List.of(java.util.Map.entry("alpha", 0)));
            final var shardId = writer.reconciler().openShards().iterator().next();
            assertEquals(201, send(writer, "PUT", "/alpha/_doc/second", "{\"msg\":\"needle\",\"n\":2}").status());
            final long term = plane.heads().read("alpha", 0).orElseThrow().term();
            writer.reconciler().releaseShard(shardId, "test: lease lapsed, given back unpublished");
            assertTrue(plane.heads().release("alpha", 0, writer.localNode().getId(), term));

            final Response after = send(reader, "POST", "/alpha/_search", "{\"query\":{\"match\":{\"msg\":\"needle\"}}}");
            final long hits = after.status() == 200 ? number(after.body(), "value") : -1L;
            final long failed = after.status() == 200 ? number(after.body(), "failed") : -1L;
            assertTrue(
                "both documents, or the shard reported as not answered -- never one hit and no failure: " + after.body(),
                hits == 2L || failed > 0L || after.status() >= 500
            );
        }
    }

    /**
     * A commit published before commits recorded how far into the log they reach is served when the log holds
     * nothing it lacks. Treating such a commit as covering none of its term made every shard ever written look
     * behind -- a term's highest record stays in the log after a publish -- and a fleet reopened on old manifests
     * took over its whole population on the first searches.
     */
    public void testAnOldCommitThatCoversItsLogIsServed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final org.opensearch.common.blobstore.BlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (
            ServerlessNode writer = new ServerlessNode(nodeSettings("legacy-writer", "ingest"));
            ServerlessNode reader = new ServerlessNode(nodeSettings("legacy-reader", "search"))
        ) {
            writer.start();
            reader.start();
            writer.setMetadataPlane(plane);
            reader.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(writer, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            for (int i = 0; i < 3; i++) {
                assertEquals(201, send(writer, "PUT", "/alpha/_doc/d" + i, "{\"msg\":\"needle\",\"n\":" + i + "}").status());
            }
            loop.publishAll();
            // The manifest as it was written before it recorded its reach.
            final var publisher = plane.segmentPublisher("alpha", 0);
            final var current = publisher.readManifest().orElseThrow();
            final var legacy = new org.opensearch.serverless.store.CommitManifest(
                current.term(),
                current.files(),
                current.writer(),
                current.lengths(),
                current.digest()
            );
            final var container = store.blobContainer(
                org.opensearch.serverless.metadata.RegisterMap.shardData(BlobPath.cleanPath(), "alpha", "uuid-alpha-00000000", 0)
            );
            final var register = container.readRegister(org.opensearch.serverless.store.SegmentPublisher.MANIFEST).orElseThrow();
            assertTrue(
                container.compareAndSwapRegister(
                    org.opensearch.serverless.store.SegmentPublisher.MANIFEST,
                    register.generation(),
                    legacy.toBytes()
                ).applied()
            );
            assertTrue(plane.heads().release("alpha", 0, writer.localNode().getId()));

            final Response found = send(reader, "POST", "/alpha/_search", "{\"query\":{\"match\":{\"msg\":\"needle\"}}}");
            assertEquals(found.body(), 200, found.status());
            assertEquals("served, not refused as behind: " + found.body(), 0L, number(found.body(), "failed"));
            assertEquals(found.body(), 3L, number(found.body(), "value"));
        }
    }

    /** The first occurrence of a numeric field in a JSON body. */
    private static long number(String body, String field) {
        final Matcher matcher = Pattern.compile("\"" + field + "\":(-?\\d+)").matcher(body);
        assertTrue("expected " + field + " in " + body, matcher.find());
        return Long.parseLong(matcher.group(1));
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
