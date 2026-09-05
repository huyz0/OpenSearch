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

/**
 * Snapshot and restore, backed by this deployment's own object store rather than a distinct repository
 * type, with request and response shapes matching real OpenSearch's own {@code _snapshot} API (M45, M46).
 *
 * <p><b>Shallow or standard is a repository setting</b> ({@code remote_store_index_shallow_copy}), not a
 * per-request choice, matching real OpenSearch — every snapshot taken against one repository behaves the
 * same way. Standard is the default, also matching real OpenSearch's own default for that setting; tests
 * exercising shallow-specific behaviour (pinning, the uuid-not-name scoping, deletion protection) opt in
 * explicitly via {@link #shallowRepo}. A repository created with no settings at all is standard, and its
 * own tests below prove standard mode needs none of the shallow-only protections to keep its data safe.
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

    /** Registers a standard (the default) repository. */
    private Response standardRepo(ServerlessNode node, String name) throws Exception {
        return send(node, "PUT", "/_snapshot/" + name, null);
    }

    /** Registers a repository whose snapshots reference live blobs rather than copying them. */
    private Response shallowRepo(ServerlessNode node, String name) throws Exception {
        return send(node, "PUT", "/_snapshot/" + name, "{\"settings\":{\"remote_store_index_shallow_copy\":true}}");
    }

    /** A repository is idempotently registered: PUT twice updates rather than refuses. */
    public void testARepositoryIsIdempotentlyRegisteredAndDescribed() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-repo"))) {
            node.start();
            node.setMetadataPlane(plane);

            assertEquals(404, send(node, "GET", "/_snapshot/backups", null).status());

            final Response created = standardRepo(node, "backups");
            assertEquals(created.body(), 200, created.status());
            assertTrue(created.body().contains("\"acknowledged\":true"));

            final Response described = send(node, "GET", "/_snapshot/backups", null);
            assertEquals(200, described.status());
            assertTrue(
                "real OpenSearch keys the response by repository name: " + described.body(),
                described.body().contains("\"backups\":{")
            );
            assertFalse(
                "standard (the default) must not report shallow copy: " + described.body(),
                described.body().contains("\"remote_store_index_shallow_copy\":\"true\"")
            );

            // A second PUT with different settings updates it -- matching real OpenSearch's own repository
            // PUT, which is an upsert, not a create-once.
            final Response updated = shallowRepo(node, "backups");
            assertEquals("re-registering the same name must update, not refuse: " + updated.body(), 200, updated.status());
            final Response describedAgain = send(node, "GET", "/_snapshot/backups", null);
            assertTrue(
                "the update must actually have taken: " + describedAgain.body(),
                describedAgain.body().contains("\"remote_store_index_shallow_copy\":\"true\"")
            );
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

            assertEquals(200, standardRepo(node, "backups").status());
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
            assertEquals(200, standardRepo(node, "backups").status());

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
            assertEquals(200, standardRepo(node, "backups").status());

            final Response refused = send(node, "PUT", "/_snapshot/backups/tooSoon", "{\"indices\":\"fresh\"}");
            assertEquals("nothing published, nothing to capture: " + refused.body(), 409, refused.status());
            assertTrue(refused.body().contains("published nothing yet"));
        }
    }

    /**
     * A restored index is a genuinely new index with the original data, byte for byte -- the property the
     * whole feature exists for. Standard mode (the default): the restore's source is the snapshot's own
     * repository-scoped copy, not the live index.
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

            assertEquals(200, standardRepo(node, "backups").status());
            final Response taken = send(node, "PUT", "/_snapshot/backups/full", "{\"indices\":\"original\"}");
            assertEquals(taken.body(), 200, taken.status());

            // The index moves on after the snapshot -- the restored copy must not see this.
            assertEquals(201, send(node, "PUT", "/original/_doc/d7?refresh=true", body(7)).status());
            loop.tick(clock.get() + 1);

            final Response restored = send(
                node,
                "POST",
                "/_snapshot/backups/full/_restore?wait_for_completion=true",
                "{\"rename_pattern\":\"original\",\"rename_replacement\":\"restored\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(
                "real OpenSearch's own restore response shape: " + restored.body(),
                restored.body().contains("\"indices\":[\"restored\"]")
            );

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

    /**
     * A standard snapshot's capture copies blobs, and {@code wait_for_completion=false} (the default) does
     * not skip that work -- it only changes which response shape is sent. A caller that does not wait still
     * finds the snapshot fully there the moment it asks.
     */
    public void testWaitForCompletionFalseStillCompletesSynchronously() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("async", "uuid-async-000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-async"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "async", 1);
            assertEquals(201, send(node, "PUT", "/async/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());

            assertEquals(200, standardRepo(node, "backups").status());
            final Response accepted = send(node, "PUT", "/_snapshot/backups/quick", "{\"indices\":\"async\"}");
            assertEquals(accepted.body(), 200, accepted.status());
            assertEquals("no wait_for_completion means the 'accepted' shape, not the full one", "{\"accepted\":true}", accepted.body());

            final Response described = send(node, "GET", "/_snapshot/backups/quick", null);
            assertEquals(200, described.status());
            assertTrue(
                "the snapshot must already be fully captured by the time a caller asks: " + described.body(),
                described.body().contains("\"state\":\"SUCCESS\"")
            );
        }
    }

    /** Restoring over a name that already exists refuses the whole restore, matching real OpenSearch. */
    public void testRestoringOverAnExistingNameIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("taken", "uuid-taken-000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-taken"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "taken", 1);
            assertEquals(201, send(node, "PUT", "/taken/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());

            assertEquals(200, standardRepo(node, "backups").status());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups/one", "{\"indices\":\"taken\"}").status());

            // Restoring under the same name collides with the live index that is still there.
            final Response collided = send(node, "POST", "/_snapshot/backups/one/_restore", null);
            assertEquals("a name collision must refuse the whole restore: " + collided.body(), 400, collided.status());
            assertTrue(collided.body().contains("index_already_exists"));

            // And the live index answering unchanged is the whole proof nothing partial happened.
            assertEquals(200, send(node, "GET", "/taken/_doc/1", null).status());
        }
    }

    /** {@code rename_pattern}/{@code rename_replacement} is a Java regex applied per index, not a literal map. */
    public void testRenamePatternAppliesARegexNotALiteralMap() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("logs-2026", "uuid-logs-2026-00000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-rename"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "logs-2026", 1);
            assertEquals(201, send(node, "PUT", "/logs-2026/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());

            assertEquals(200, standardRepo(node, "backups").status());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups/one", "{\"indices\":\"logs-2026\"}").status());

            final Response restored = send(
                node,
                "POST",
                "/_snapshot/backups/one/_restore?wait_for_completion=true",
                "{\"rename_pattern\":\"logs-(.+)\",\"rename_replacement\":\"restored-logs-$1\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(
                "the capture group must have been substituted, not treated as a literal string: " + restored.body(),
                restored.body().contains("\"indices\":[\"restored-logs-2026\"]")
            );
            assertEquals(200, send(node, "GET", "/restored-logs-2026/_doc/1", null).status());
        }
    }

    /**
     * The sweep does not collect what a live shallow snapshot is holding.
     *
     * <p>The assertion the shallow mode stands on, the same as {@code testASweepDoesNotCollectWhatA
     * ViewIsHolding} is for a point in time: a snapshot durable enough to be called a backup that loses its
     * files to the garbage collector is not a backup.
     */
    public void testASweepDoesNotCollectWhatAShallowSnapshotIsHolding() throws Exception {
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

            assertEquals(200, shallowRepo(node, "backups").status());
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
            logger.info("snapshot: the sweep collected {} blobs with a shallow snapshot held", deleted.size());

            // And the snapshot can still be restored -- the real proof, not just that the collector's own
            // bookkeeping thinks it left something alone.
            final Response restored = send(
                node,
                "POST",
                "/_snapshot/backups/pinned/_restore",
                "{\"rename_pattern\":\"swept\",\"rename_replacement\":\"unswept\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            for (int i = 1; i <= 4; i++) {
                final Response got = send(node, "GET", "/unswept/_doc/s" + i, null);
                assertTrue("s" + i + " must survive the sweep via the snapshot: " + got.body(), got.body().contains("\"found\":true"));
            }
        }
    }

    /**
     * A shallow snapshot pins by the index's uuid, not its name -- so a name reused by an unrelated later
     * index is never mistaken for the one the snapshot actually captured.
     */
    public void testAShallowSnapshotDoesNotPinAnUnrelatedIndexThatReusedTheName() throws Exception {
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

            assertEquals(200, shallowRepo(node, "backups").status());
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

    /**
     * Deleting an index whose data a live shallow snapshot still names directly must not destroy that
     * data.
     *
     * <p>Found while scoping standard-mode snapshots: a shallow snapshot's whole argument for costing
     * nothing at capture time is that it references the live shard's own blobs rather than a copy. Index
     * deletion purged those blobs unconditionally, so taking a snapshot and then deleting the index it was
     * taken from -- the ordinary shape a backup is used for -- silently destroyed the backup along with the
     * index. The garbage collector already knew to protect a live snapshot's blobs from its own sweep;
     * index deletion did not know to ask the same question.
     */
    public void testDeletingAnIndexDoesNotDestroyWhatALiveShallowSnapshotIsHolding() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("ephemeral", "uuid-ephemeral-00000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-delete"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "ephemeral", 1);
            assertEquals(201, send(node, "PUT", "/ephemeral/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());

            assertEquals(200, shallowRepo(node, "backups").status());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups/before-delete", "{\"indices\":\"ephemeral\"}").status());

            // The ordinary shape a backup is used for: snapshot, then delete the thing it backed up.
            assertEquals(200, send(node, "DELETE", "/ephemeral", null).status());
            assertEquals("the index must actually be gone", 404, send(node, "GET", "/ephemeral", null).status());

            final Response restored = send(
                node,
                "POST",
                "/_snapshot/backups/before-delete/_restore?wait_for_completion=true",
                "{\"rename_pattern\":\"ephemeral\",\"rename_replacement\":\"recovered\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            assertTrue(restored.body().contains("\"indices\":[\"recovered\"]"));

            final Response got = send(node, "GET", "/recovered/_doc/1", null);
            assertTrue(
                "the document must survive the index's deletion via the snapshot: " + got.body(),
                got.body().contains("\"found\":true")
            );
        }
    }

    /**
     * A standard snapshot needs none of the shallow-only protections to survive the index it was taken
     * from being deleted -- its bytes were already copied into the repository's own storage at capture
     * time, entirely independent of the live index's own lifecycle. Proved by planting the same shape of
     * failure the shallow test above exists to catch and finding nothing to catch.
     */
    public void testAStandardSnapshotSurvivesIndexDeletionWithNoSpecialProtectionNeeded() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("ephemeral", "uuid-ephemeral-00001", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-standard-delete"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "ephemeral", 1);
            assertEquals(201, send(node, "PUT", "/ephemeral/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());

            // No shallow setting: standard, the default.
            assertEquals(200, standardRepo(node, "backups").status());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups/before-delete", "{\"indices\":\"ephemeral\"}").status());

            assertEquals(200, send(node, "DELETE", "/ephemeral", null).status());
            assertEquals(404, send(node, "GET", "/ephemeral", null).status());

            final Response restored = send(
                node,
                "POST",
                "/_snapshot/backups/before-delete/_restore?wait_for_completion=true",
                "{\"rename_pattern\":\"ephemeral\",\"rename_replacement\":\"recovered\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            final Response got = send(node, "GET", "/recovered/_doc/1", null);
            assertTrue(
                "standard mode's own copy must survive independent of the original index: " + got.body(),
                got.body().contains("\"found\":true")
            );
        }
    }

    /** Deleting a standard snapshot reclaims its own repository-scoped storage, not just its record. */
    public void testDeletingAStandardSnapshotReclaimsItsOwnStorage() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("cleanup", "uuid-cleanup-0000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("snap-cleanup"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "cleanup", 1);
            assertEquals(201, send(node, "PUT", "/cleanup/_doc/1?refresh=true", body(1)).status());
            loop.tick(clock.get());

            assertEquals(200, standardRepo(node, "backups").status());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups/gone-soon", "{\"indices\":\"cleanup\"}").status());

            final var container = plane.blobStore()
                .blobContainer(
                    org.opensearch.serverless.metadata.RegisterMap.snapshotShardData(plane.basePath(), "backups", "gone-soon", "cleanup", 0)
                );
            // "t=1" is a child container (segments live under it), not a blob of this one -- children()
            // is the listing that sees it, the same way GarbageCollector's own sweep enumerates term dirs.
            assertFalse("standard capture must actually have copied something", container.children().isEmpty());
            assertFalse(container.children().get("t=1").listBlobs().isEmpty());

            assertEquals(200, send(node, "DELETE", "/_snapshot/backups/gone-soon", null).status());
            // delete() removes the container's own directory, not just what was in it -- children() on a
            // directory that no longer exists at all throws rather than reporting empty, so the container
            // being genuinely gone is what a caught exception here means, not a failure of this check.
            try {
                assertTrue(
                    "deleting the snapshot must reclaim its own repository-scoped storage, not just its record",
                    container.children().isEmpty()
                );
            } catch (java.nio.file.NoSuchFileException expected) {
                // The directory itself is gone, which is the stronger version of "reclaimed" this asserts.
            }
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
