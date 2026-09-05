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
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * What a snapshot must not say, and what a restore must not hide.
 *
 * <p><b>A snapshot's record is written before its manifests are read.</b> The record is what index deletion
 * and the collector consult before removing shard data, so a delete that lands during a capture leaves the
 * captured blobs alone until the capture has finished with them. Written after, a {@code SUCCESS} snapshot
 * could name blobs a concurrent delete had already purged, and the first restore was the first anyone heard
 * of it. The delete-during-capture test here fires the delete from inside the first manifest read.
 *
 * <p><b>A restore reports the manifest swap's result.</b> A writer that activated a shard of the freshly
 * created index and published before the restore's swap used to be ignored: the shard stayed empty behind
 * {@code "failed": 0}. The conflict test plants that commit from inside the swap.
 *
 * <p>The rest: settings travel through a snapshot (the analyzers a mapping names live there), and a plugin's
 * own index can be neither captured nor restored onto.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessSnapshotConsistencyTests extends OpenSearchTestCase {

    private static final long TTL = 300_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-snapshot-consistency")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** A plugin that owns an index, the way the shell's own authentication does. */
    public static final class SecretKeeperPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(new SystemIndexDescriptor(".secret_*", "a plugin's own records"));
        }
    }

    /** A blob store that runs one hook, once, at one register operation on containers a test names. */
    private static final class HookedBlobStore implements BlobStore {

        private final BlobStore delegate;
        private volatile Predicate<BlobPath> where = path -> false;
        private volatile String blob;
        private volatile HookedAction beforeSwap;
        private volatile HookedAction afterRead;
        private final AtomicBoolean fired = new AtomicBoolean();

        interface HookedAction {
            void run(BlobContainer container) throws Exception;
        }

        HookedBlobStore(BlobStore delegate) {
            this.delegate = delegate;
        }

        void onFirstManifestRead(Predicate<BlobPath> where, HookedAction action) {
            this.where = where;
            this.blob = SegmentPublisher.MANIFEST;
            this.afterRead = action;
        }

        void beforeFirstManifestSwap(Predicate<BlobPath> where, HookedAction action) {
            this.where = where;
            this.blob = SegmentPublisher.MANIFEST;
            this.beforeSwap = action;
        }

        boolean fired() {
            return fired.get();
        }

        private void fire(HookedAction action, BlobContainer container) throws IOException {
            if (action != null && fired.compareAndSet(false, true)) {
                try {
                    action.run(container);
                } catch (Exception e) {
                    throw new IOException("the hook failed", e);
                }
            }
        }

        @Override
        public BlobContainer blobContainer(BlobPath path) {
            final BlobContainer real = delegate.blobContainer(path);
            if (where.test(path) == false) {
                return real;
            }
            return new DelegatingBlobContainer(real) {
                @Override
                public Optional<BlobRegister> readRegister(String blobName) throws IOException {
                    final Optional<BlobRegister> read = super.readRegister(blobName);
                    if (blobName.equals(blob)) {
                        fire(afterRead, real);
                    }
                    return read;
                }

                @Override
                public BlobRegisterCasResult compareAndSwapRegister(String blobName, long expectedGeneration, BytesReference newValue)
                    throws IOException {
                    if (blobName.equals(blob)) {
                        fire(beforeSwap, real);
                    }
                    return super.compareAndSwapRegister(blobName, expectedGeneration, newValue);
                }
            };
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    /** A restore whose manifest swap loses to a writer answers 409 and leaves nothing half-restored. */
    public void testARestoreWhoseManifestSwapLosesIsReportedNotCountedAsSuccess() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final HookedBlobStore store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("original", "uuid-original-0000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("restore-conflict"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "original", 1);
            assertEquals(201, send(node, "PUT", "/original/_doc/1?refresh=true", "{\"msg\":\"doc\",\"n\":1}").status());
            loop.tick(clock.get());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups/full", "{\"indices\":\"original\"}").status());

            // Something else publishes on the new index's shard just before the restore's swap: the shape
            // a writer activated by an early client write has.
            store.beforeFirstManifestSwap(
                path -> path.buildAsString().contains("segments/restored#"),
                container -> container.createRegisterIfAbsent(
                    SegmentPublisher.MANIFEST,
                    new CommitManifest(1L, Map.of(), "intruder").toBytes()
                )
            );
            final Response restored = send(
                node,
                "POST",
                "/_snapshot/backups/full/_restore?wait_for_completion=true",
                "{\"rename_pattern\":\"original\",\"rename_replacement\":\"restored\"}"
            );
            assertTrue("the hook must have run", store.fired());
            assertEquals("a lost swap is a failed restore, not failed: 0 -- " + restored.body(), 409, restored.status());
            assertTrue(restored.body(), restored.body().contains("shard 0 of [restored]"));
            assertEquals("and the half-made index is rolled back", 404, send(node, "GET", "/restored", null).status());
        }
    }

    /** Settings captured with an index come back with it, including the analyzers its mapping names. */
    public void testSettingsRoundTripThroughASnapshot() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("settings-roundtrip"))) {
            node.start();
            node.setMetadataPlane(plane);
            final Response created = send(
                node,
                "PUT",
                "/origin",
                "{\"settings\":{\"refresh_interval\":\"7s\",\"analysis\":{\"analyzer\":{\"lower\":{\"type\":\"custom\","
                    + "\"tokenizer\":\"standard\",\"filter\":[\"lowercase\"]}}}},"
                    + "\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\",\"analyzer\":\"lower\"}}}}"
            );
            assertEquals(created.body(), 200, created.status());
            final BackgroundReconciler loop = hold(node, plane, clock, "origin", 1);
            assertEquals(201, send(node, "PUT", "/origin/_doc/1?refresh=true", "{\"msg\":\"Hello World\"}").status());
            loop.tick(clock.get());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());
            final Response taken = send(node, "PUT", "/_snapshot/backups/full?wait_for_completion=true", "{\"indices\":\"origin\"}");
            assertEquals(taken.body(), 200, taken.status());
            assertTrue(taken.body(), taken.body().contains("\"state\":\"SUCCESS\""));

            final Response restored = send(
                node,
                "POST",
                "/_snapshot/backups/full/_restore?wait_for_completion=true",
                "{\"rename_pattern\":\"origin\",\"rename_replacement\":\"restored\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            final Response settings = send(node, "GET", "/restored/_settings", null);
            assertTrue("the dynamic setting travelled: " + settings.body(), settings.body().contains("\"refresh_interval\":\"7s\""));
            assertTrue("and the analysis chain: " + settings.body(), settings.body().contains("analysis.analyzer.lower"));
            // The restored index can be opened and searched through the analyzer its mapping names -- held
            // first, since this node serves only the shards it holds.
            hold(node, plane, clock, "restored", 1);
            final Response searched = send(node, "POST", "/restored/_search", "{\"query\":{\"match\":{\"msg\":\"hello\"}}}");
            assertEquals(searched.body(), 200, searched.status());
            assertTrue("the analyzer is there to lower-case the query: " + searched.body(), searched.body().contains("\"value\":1"));

            // The refusal of index_settings now states what is true.
            final Response overridden = send(
                node,
                "POST",
                "/_snapshot/backups/full/_restore",
                "{\"index_settings\":{\"refresh_interval\":\"1s\"},\"rename_pattern\":\"origin\",\"rename_replacement\":\"again\"}"
            );
            assertEquals(501, overridden.status());
            assertTrue(overridden.body(), overridden.body().contains("restored with it"));
        }
    }

    /** An index deleted while it is being captured stays restorable from the snapshot that captured it. */
    public void testAnIndexDeletedDuringCaptureIsStillRestorable() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final HookedBlobStore store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("delete-during-capture"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "alpha", 1);
            assertEquals(201, send(node, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"doc\",\"n\":1}").status());
            loop.tick(clock.get());
            // Shallow: the snapshot names the live blobs, which is what a concurrent delete would purge.
            assertEquals(
                200,
                send(node, "PUT", "/_snapshot/shallow", "{\"settings\":{\"remote_store_index_shallow_copy\":true}}").status()
            );

            // The delete lands after the capture has read the shard's manifest and before it writes
            // anything else -- inside the read, so there is no timing to be unlucky with.
            store.onFirstManifestRead(path -> path.buildAsString().contains("segments/alpha#"), container -> {
                if (plane.deleteIndex("alpha") == false) {
                    throw new IllegalStateException("the delete must find the index");
                }
            });
            final Response taken = send(node, "PUT", "/_snapshot/shallow/s?wait_for_completion=true", "{\"indices\":\"alpha\"}");
            assertTrue("the delete must have run inside the capture", store.fired());
            assertEquals(taken.body(), 200, taken.status());
            assertTrue(taken.body(), taken.body().contains("\"state\":\"SUCCESS\""));
            assertEquals("the index is gone", 404, send(node, "GET", "/alpha", null).status());

            // A SUCCESS snapshot restores. Before the record was written first, this was a 500 naming a
            // blob that no longer existed.
            final Response restored = send(
                node,
                "POST",
                "/_snapshot/shallow/s/_restore?wait_for_completion=true",
                "{\"rename_pattern\":\"alpha\",\"rename_replacement\":\"restored\"}"
            );
            assertEquals(restored.body(), 200, restored.status());
            final Response got = send(node, "GET", "/restored/_doc/1", null);
            assertTrue("the captured document is there: " + got.body(), got.body().contains("\"found\":true"));
        }
    }

    /** A plugin's own index is refused as a snapshot source and as a restore target. */
    public void testASystemIndexIsNeitherCapturedNorRestoredOnto() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor(".secret_state", "uuid-secret-0000000000", 1, MAPPING, null));
        plane.createIndex(new IndexDescriptor("plain", "uuid-plain-00000000000", 1, MAPPING, null));
        final Settings settings = nodeSettings("snapshot-system-index");

        try (ServerlessNode node = new ServerlessNode(settings, List.of(new SecretKeeperPlugin()))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = hold(node, plane, clock, "plain", 1);
            assertEquals(201, send(node, "PUT", "/plain/_doc/1?refresh=true", "{\"msg\":\"doc\",\"n\":1}").status());
            loop.tick(clock.get());
            assertEquals(200, send(node, "PUT", "/_snapshot/backups", null).status());

            final Response captured = send(node, "PUT", "/_snapshot/backups/leak", "{\"indices\":\".secret_state\"}");
            assertEquals("capturing a plugin's index is refused: " + captured.body(), 403, captured.status());
            assertTrue(captured.body(), captured.body().contains("belongs to a plugin"));
            assertEquals("and nothing was recorded", 404, send(node, "GET", "/_snapshot/backups/leak", null).status());
            assertEquals(
                "not even alongside an ordinary index",
                403,
                send(node, "PUT", "/_snapshot/backups/leak", "{\"indices\":\"plain,.secret_state\"}").status()
            );

            assertEquals(200, send(node, "PUT", "/_snapshot/backups/ok", "{\"indices\":\"plain\"}").status());
            final Response onto = send(
                node,
                "POST",
                "/_snapshot/backups/ok/_restore",
                "{\"rename_pattern\":\"plain\",\"rename_replacement\":\".secret_state\"}"
            );
            assertEquals("restoring onto a system-index name is refused: " + onto.body(), 403, onto.status());
            final Response ontoNew = send(
                node,
                "POST",
                "/_snapshot/backups/ok/_restore",
                "{\"rename_pattern\":\"plain\",\"rename_replacement\":\".secret_shadow\"}"
            );
            assertEquals("a name under the pattern, not only the existing one: " + ontoNew.body(), 403, ontoNew.status());
            assertFalse("nothing was created under it", plane.describe(".secret_shadow").isPresent());

            // Names that would make register keys ambiguous are refused at the door.
            assertEquals(400, send(node, "PUT", "/_snapshot/backups/a%23b", "{\"indices\":\"plain\"}").status());
            assertEquals(400, send(node, "PUT", "/_snapshot/backups/x,y", "{\"indices\":\"plain\"}").status());
            assertEquals(400, send(node, "PUT", "/_snapshot/a%23b", null).status());
            assertEquals(
                "a repository that is not registered is a 404, not an empty list",
                404,
                send(node, "GET", "/_snapshot/nope/_all", null).status()
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
