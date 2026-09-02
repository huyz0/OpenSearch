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
import org.opensearch.serverless.reconcile.GarbageCollector;
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
 * Snapshot and restore, backed by this deployment's own object store rather than a distinct repository
 * type (M45).
 *
 * <p><b>Taking one costs no data movement.</b> Every blob a snapshot names was already durable before the
 * record was written, so the tests here that only take a snapshot never assert on object-store request
 * counts the way {@code ServerlessCostTests} does for other paths — there is one write, regardless of how
 * many shards or how large they are. What has a real cost, and what these tests are mostly about, is
 * restoring: a shard's storage is keyed by its index's own uuid, so a restore into a new index (a fresh
 * uuid, always — reusing one is the exact bug M32 closed) has to copy the referenced blobs.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessSnapshotTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-snapshot")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** A repository is registered once, described, and refused a second time under the same name. */
    public void testARepositoryIsRegisteredOnceAndDescribed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-repo"))) {
            node.start();
            node.setMetadataPlane(plane);

            assertEquals(404, send(node, "GET", "/_snapshot/backups", null).status());

            final Response created = send(node, "PUT", "/_snapshot/backups", null);
            assertEquals(created.body(), 200, created.status());
            assertTrue(created.body().contains("\"acknowledged\":true"));

            final Response described = send(node, "GET", "/_snapshot/backups", null);
            assertEquals(200, described.status());
            assertTrue(described.body().contains("\"repository\":\"backups\""));

            final Response duplicate = send(node, "PUT", "/_snapshot/backups", null);
            assertEquals("registering the same name twice must be refused: " + duplicate.body(), 400, duplicate.status());
        }
    }

    /** A repository still holding a snapshot refuses deletion; empty, it deletes cleanly. */
    public void testARepositoryInUseRefusesDeletion() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("held", "uuid-held-000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-inuse"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "held", 1);
            assertEquals(201, send(node, "PUT", "/held/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());

            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups/first", "{\"indices\":\"held\"}").status());

            final Response refused = send(node, "DELETE", "/_snapshot/backups", null);
            assertEquals("a repository still holding a snapshot must refuse deletion: " + refused.body(), 409, refused.status());

            assertEquals(200, send(node, "DELETE", "/_snapshot/backups/first", null).status());
            assertEquals("empty now, deletion must succeed", 200, send(node, "DELETE", "/_snapshot/backups", null).status());
        }
    }

    /** Taking a snapshot needs an explicit 'indices' field; this surface does not enumerate what exists. */
    public void testASnapshotWithoutIndicesIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-noindices"))) {
            node.start();
            node.setMetadataPlane(plane);
            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());

            final Response refused = send(node, "PUT", "/_snapshot/backups/whatever", "{}");
            assertEquals("no 'indices' field must be refused, not treated as 'everything': " + refused.body(), 400, refused.status());
        }
    }

    /** A shard that has published nothing cannot be captured, the same refusal a point in time gives. */
    public void testASnapshotOfAnUnpublishedShardIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("fresh", "uuid-fresh-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-fresh"))) {
            node.start();
            node.setMetadataPlane(plane);
            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());

            final Response refused = send(node, "PUT", "/_snapshot/backups/tooSoon", "{\"indices\":\"fresh\"}");
            assertEquals("nothing published, nothing to capture: " + refused.body(), 409, refused.status());
            assertTrue(refused.body().contains("published nothing yet"));
        }
    }

    /**
     * A restored index is a genuinely new index with the original data, byte for byte -- the property the
     * whole feature exists for.
     */
    public void testARestoredIndexHasTheOriginalDocuments() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("original", "uuid-original-0000000", 2, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-restore"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "original", 2);
            for (int i = 1; i <= 6; i++) {
                assertEquals(201, send(node, "PUT", "/original/_doc/d" + i + "?refresh=true", body(i)).status());
            }
            loop.tick(clock.get());

            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());
            final Response taken = send(node, "PUT", "/_snapshot/backups/full", "{\"indices\":\"original\"}");
            assertEquals(taken.body(), 200, taken.status());

            // The index moves on after the snapshot -- the restored copy must not see this.
            assertEquals(201, send(node, "PUT", "/original/_doc/d7?refresh=true", body(7)).status());
            loop.tick(clock.get() + 1);

            final Response restored = send(node, "POST", "/_snapshot/backups/full/_restore", "{\"rename\":{\"original\":\"restored\"}}");
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(restored.body().contains("\"restored_as\":\"restored\""));

            long found = 0;
            for (int i = 1; i <= 6; i++) {
                final Response got = send(node, "GET", "/restored/_doc/d" + i, null);
                assertEquals("d" + i + " must be found in the restored index: " + got.body(), 200, got.status());
                assertTrue(got.body().contains("\"found\":true"));
                found++;
            }
            assertEquals(6, found);
            final Response notThere = send(node, "GET", "/restored/_doc/d7", null);
            assertFalse(
                "the write made after the snapshot must not appear in the restore: " + notThere.body(),
                notThere.body().contains("\"found\":true")
            );

            // And the two indices are genuinely independent: the original still has its own seventh
            // document, unaffected by anything the restore did.
            final Response originalStill = send(node, "GET", "/original/_doc/d7", null);
            assertTrue(originalStill.body().contains("\"found\":true"));
        }
    }

    /** Restoring into a name that already exists is refused per index, not the whole request. */
    public void testRestoringOverAnExistingNameIsRefusedPerIndex() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("taken", "uuid-taken-000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-taken"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "taken", 1);
            assertEquals(201, send(node, "PUT", "/taken/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());

            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups/one", "{\"indices\":\"taken\"}").status());

            // Restoring under the same name collides with the live index that is still there.
            final Response collided = send(node, "POST", "/_snapshot/backups/one/_restore", null);
            assertEquals(collided.body(), 200, collided.status());
            assertTrue("a name collision must be reported per index, not fail the whole request", collided.body().contains("\"error\""));
            assertFalse("nothing should claim to have been restored", collided.body().contains("restored_as"));
        }
    }

    /**
     * The sweep does not collect what a live snapshot is holding.
     *
     * <p>The assertion this whole feature stands on, the same as {@code testASweepDoesNotCollectWhatA
     * ViewIsHolding} is for a point in time: a snapshot durable enough to be called a backup that loses its
     * files to the garbage collector is not a backup.
     */
    public void testASweepDoesNotCollectWhatASnapshotIsHolding() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("swept", "uuid-swept-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-sweep"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "swept", 1);
            for (int i = 1; i <= 4; i++) {
                assertEquals(201, send(node, "PUT", "/swept/_doc/s" + i + "?refresh=true", body(i)).status());
            }
            loop.tick(clock.get());

            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups/pinned", "{\"indices\":\"swept\"}").status());

            // Move the shard to a new term and publish there, so the captured commit's files are in a dead
            // term container and unreferenced by the live manifest -- the exact condition the sweep deletes.
            assertTrue(plane.heads().release("swept", 0, node.localNode().getId()));
            node.activateWriter(plane, "swept", 0);
            for (int i = 5; i <= 12; i++) {
                assertEquals(201, send(node, "PUT", "/swept/_doc/s" + i + "?refresh=true", body(i)).status());
            }
            loop.tick(clock.get() + 1);

            final var deleted = new GarbageCollector(store, BlobPath.cleanPath()).collectShard(plane, "swept", 0);
            logger.info("snapshot: the sweep collected {} blobs with a snapshot held", deleted.size());

            // And the snapshot can still be restored -- the real proof, not just that the collector's own
            // bookkeeping thinks it left something alone.
            final Response restored = send(node, "POST", "/_snapshot/backups/pinned/_restore", "{\"rename\":{\"swept\":\"unswept\"}}");
            assertEquals(restored.body(), 200, restored.status());
            for (int i = 1; i <= 4; i++) {
                final Response got = send(node, "GET", "/unswept/_doc/s" + i, null);
                assertTrue("s" + i + " must survive the sweep via the snapshot: " + got.body(), got.body().contains("\"found\":true"));
            }
        }
    }

    /**
     * A snapshot pins by the index's uuid, not its name -- so a name reused by an unrelated later index is
     * never mistaken for the one the snapshot actually captured.
     */
    public void testASnapshotDoesNotPinAnUnrelatedIndexThatReusedTheName() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("reused", "uuid-reused-first-000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-reuse"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler firstLoop = hold(node, plane, clock, "reused", 1);
            assertEquals(201, send(node, "PUT", "/reused/_doc/1?refresh=true", body(1)).status());
            firstLoop.tick(clock.get());

            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups/gen1", "{\"indices\":\"reused\"}").status());

            // The first "reused" is deleted and a second, unrelated index takes the name -- a new uuid, per
            // M32. The old snapshot's referencedBlobs must not be mistaken for anything this new index owns.
            assertEquals(200, send(node, "DELETE", "/reused", null).status());
            plane.createIndex(new IndexDescriptor("reused", "uuid-reused-second-00", 1, MAPPING, null));
            final BackgroundReconciler secondLoop = hold(node, plane, clock, "reused", 1);
            assertEquals(201, send(node, "PUT", "/reused/_doc/2?refresh=true", body(2)).status());
            secondLoop.tick(clock.get() + 1);

            final var snapshot = plane.snapshot("backups", "gen1").orElseThrow();
            final String secondUuid = plane.describe("reused").orElseThrow().uuid();
            assertEquals(
                "a snapshot of the first index must not claim to reference the second index's own shard",
                java.util.List.of(),
                snapshot.referencedBlobs(secondUuid, 0)
            );
        }
    }

    private BackgroundReconciler hold(ServerlessNode node, MetadataPlane plane, AtomicLong clock, String index, int shards)
        throws Exception {
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < shards; shard++) {
            loop.want(index, shard);
        }
        loop.tick(clock.get());
        return loop;
    }

    private static String body(int n) {
        return "{\"msg\":\"doc\",\"n\":" + n + "}";
    }

    private static final Pattern FIELD = Pattern.compile("\"(\\w+)\":\"([^\"]*)\"");

    private static String field(String body, String name) {
        final Matcher matcher = Pattern.compile("\"" + name + "\":\"([^\"]+)\"").matcher(body);
        assertTrue("no " + name + " in " + body, matcher.find());
        return matcher.group(1);
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
