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
import org.opensearch.serverless.bucket.BucketAssigner;
import org.opensearch.serverless.bucket.BucketManifest;
import org.opensearch.serverless.bucket.BucketPolicy;
import org.opensearch.serverless.bucket.BucketStore;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A small index joins its group's open bucket when it is made, the manifest listing it before the descriptor names the
 * bucket; a full bucket moves its stripe on; the setting keeps an index out; a delete takes it back out.
 */
public class BucketAssignmentTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    public void testJoinsFillABucketThenMoveTheStripeOn() throws Exception {
        try (FsBlobStore blobs = new FsBlobStore(8192, createTempDir(), false)) {
            final BucketStore store = new BucketStore(blobs, BlobPath.cleanPath());
            final BucketAssigner assigner = new BucketAssigner(store, 3);
            final Set<String> buckets = new HashSet<>();
            final String first = assigner.join("tenant-1", "logs-0", "uuid-0");
            buckets.add(first);
            assertEquals("joining the open bucket again is a no-op", first, assigner.join("tenant-1", "logs-0", "uuid-0"));
            for (int i = 1; i < 7; i++) {
                buckets.add(assigner.join("tenant-1", "logs-" + i, "uuid-" + i));
            }
            assertEquals("seven members, three to a bucket, fill three buckets: " + buckets, 3, buckets.size());
            assertEquals(3, store.read(first).orElseThrow().manifest().size());
            assigner.leave(first, "uuid-0");
            final BucketManifest left = store.read(first).orElseThrow().manifest();
            assertTrue(left.member("uuid-0").isEmpty());
            assertEquals(2, left.size());
        }
    }

    public void testTheIndexSettingWinsOverTheClusterAndNodeDefaults() {
        final Settings off = Settings.builder().put(BucketPolicy.INDEX_SETTING, "off").build();
        final Settings auto = Settings.builder().put(BucketPolicy.INDEX_SETTING, "AUTO").build();
        assertFalse(BucketPolicy.wanted(off, "auto", true));
        assertTrue(BucketPolicy.wanted(auto, "off", false));
        assertFalse(BucketPolicy.wanted(Settings.EMPTY, "off", true));
        assertTrue(BucketPolicy.wanted(Settings.EMPTY, null, true));
        assertFalse(BucketPolicy.wanted(Settings.EMPTY, null, false));
        expectThrows(IllegalArgumentException.class, () -> BucketPolicy.check("sometimes"));
        assertEquals("tenant-9", BucketPolicy.group("logs", Settings.builder().put(BucketPolicy.GROUP_SETTING, "tenant-9").build()));
        assertEquals("logs", BucketPolicy.group("logs", Settings.EMPTY));
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-bucket-assignment")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.buckets.enabled", true)
            .build();
    }

    private static String call(ServerlessNode node, String method, String path, String body) throws Exception {
        final int port = node.boundHttpAddress().publishAddress().getPort();
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpResponse<String> response = http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                    .header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(60))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
            assertTrue(method + " " + path + " -> " + response.statusCode() + " " + response.body(), response.statusCode() < 300);
            return response.body();
        }
    }

    private static final String ONE_SHARD =
        "{\"settings\":{\"number_of_shards\":1},\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"}}}}";

    public void testASmallIndexIsBucketedWhenMadeAndServedThroughItsBucket() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        try (FsBlobStore blobs = new FsBlobStore(8192, createTempDir(), false)) {
            final MetadataPlane plane = new MetadataPlane(blobs, BlobPath.cleanPath(), clock::get, TTL);
            try (ServerlessNode node = new ServerlessNode(nodeSettings("bucket-assign"))) {
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
                plane.membership().refresh();

                call(node, "PUT", "/small", ONE_SHARD);
                call(node, "PUT", "/wide", "{\"settings\":{\"number_of_shards\":2}}");
                call(node, "PUT", "/alone", "{\"settings\":{\"number_of_shards\":1,\"index\":{\"serverless.bucket\":\"off\"}}}");

                final IndexDescriptor small = plane.describe("small").orElseThrow();
                assertNotNull("a one-shard index joins a bucket", small.bucket());
                assertEquals(
                    "the manifest lists it",
                    "small",
                    plane.buckets().read(small.bucket()).orElseThrow().manifest().member(small.uuid()).orElseThrow().indexName()
                );
                assertNull("a multi-shard index stays on its own", plane.describe("wide").orElseThrow().bucket());
                assertNull("the index setting keeps one out", plane.describe("alone").orElseThrow().bucket());

                final org.opensearch.serverless.reconcile.BackgroundReconciler loop =
                    new org.opensearch.serverless.reconcile.BackgroundReconciler(node, plane);
                loop.want("small", 0);
                loop.tick(clock.get());
                call(node, "PUT", "/small/_doc/1?refresh=true", "{\"msg\":\"hello bucket\"}");
                final String hits = call(node, "POST", "/small/_search", "{\"query\":{\"match\":{\"msg\":\"bucket\"}}}");
                assertTrue(hits, hits.contains("\"value\":1"));
                assertEquals(
                    "the member is owned through its bucket's head",
                    node.localNode().getId(),
                    plane.heads().read("small", 0).orElseThrow().ownerNodeId()
                );

                call(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"serverless.bucket\":\"off\"}}");
                call(node, "PUT", "/later", ONE_SHARD);
                assertNull("the cluster setting keeps new indices out", plane.describe("later").orElseThrow().bucket());

                call(node, "DELETE", "/small", "");
                assertTrue(
                    "a delete takes the member out of its bucket",
                    plane.buckets().read(small.bucket()).orElseThrow().manifest().member(small.uuid()).isEmpty()
                );
            }
        }
    }
}
