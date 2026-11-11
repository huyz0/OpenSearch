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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.rest.DocumentRouting;
import org.opensearch.serverless.routing.ShardMode;
import org.opensearch.serverless.routing.UnitLayout;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.split.UnitSplitBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Building a split's two halves from a unit's published commit, then routing to them: every document is in
 * exactly the half its id hashes to, and a node that reads the split layout finds, counts and searches all
 * of them, and writes new ones into the halves.
 *
 * <p>Runs against {@code FsBlobContainer} only.
 */
public class UnitSplitBuildTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int DOCS = 60;

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-split-build")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private record Response(int status, String body) {
    }

    private static ReconcileScheduler scheduler(ServerlessNode node, MetadataPlane plane, AtomicLong clock, BackgroundReconciler loop) {
        node.setMetadataPlane(plane);
        final ReconcileScheduler scheduler = new ReconcileScheduler(
            loop,
            node.threadPool(),
            clock::get,
            TimeValue.timeValueHours(1),
            TimeValue.timeValueHours(1),
            null
        );
        node.setSignals(scheduler);
        scheduler.start();
        return scheduler;
    }

    public void testASplitUnitsHalvesHoldEveryDocumentOnceAndServeIt() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (ServerlessNode first = new ServerlessNode(nodeSettings("split-first"))) {
            first.startup().start();
            final BackgroundReconciler firstLoop = new BackgroundReconciler(first, plane).setDemandDrivenActivation(true);
            try (ReconcileScheduler ignored = scheduler(first, plane, clock, firstLoop)) {
                final String create = "{\"settings\":{\"number_of_shards\":4,\""
                    + ShardMode.SETTING
                    + "\":\"adaptive\"},"
                    + "\"mappings\":{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}}";
                assertEquals(200, send(first, "PUT", "/grow", create).status());
                for (int doc = 0; doc < DOCS; doc++) {
                    final Response put = send(first, "PUT", "/grow/_doc/doc-" + doc, "{\"msg\":\"before split\",\"n\":" + doc + "}");
                    assertEquals(put.body(), 201, put.status());
                }
                // One overwrite and one delete, so the halves must leave superseded versions and tombstones behind.
                assertEquals(200, send(first, "PUT", "/grow/_doc/doc-1", "{\"msg\":\"before split again\",\"n\":1}").status());
                assertEquals(200, send(first, "DELETE", "/grow/_doc/doc-2", null).status());
                firstLoop.publishAll();
            }
        }

        final IndexDescriptor before = plane.describe("grow").orElseThrow();
        final UnitLayout.Split split = before.layout().planSplit(0);
        final UnitSplitBuilder.Built built = UnitSplitBuilder.build(plane, before, split, createTempDir());
        assertEquals("every live document lands in exactly one half", DOCS - 1, built.leftDocs() + built.rightDocs());
        assertTrue("both halves hold something: " + built, built.leftDocs() > 0 && built.rightDocs() > 0);

        final IndexDescriptor after = before.withLayout(before.layout().apply(split));
        assertTrue(plane.updateDescriptor(after, plane.descriptorGeneration("grow")).isPresent());
        int expectedLeft = 0;
        for (int doc = 0; doc < DOCS; doc++) {
            if (doc != 2 && DocumentRouting.shardFor(after, "doc-" + doc) == split.left().id()) {
                expectedLeft++;
            }
        }
        assertEquals(expectedLeft, built.leftDocs());

        // A node that has only ever seen the split layout serves the halves.
        try (ServerlessNode second = new ServerlessNode(nodeSettings("split-second"))) {
            second.startup().start();
            final BackgroundReconciler secondLoop = new BackgroundReconciler(second, plane).setDemandDrivenActivation(true);
            clock.addAndGet(TTL + 1_000L);
            try (ReconcileScheduler ignored = scheduler(second, plane, clock, secondLoop)) {
                for (int doc = 0; doc < DOCS; doc++) {
                    final Response got = send(second, "GET", "/grow/_doc/doc-" + doc, null);
                    if (doc == 2) {
                        assertEquals(got.body(), 404, got.status());
                    } else {
                        assertTrue(doc + ": " + got.body(), got.body().contains("\"found\":true"));
                    }
                }
                assertTrue(send(second, "GET", "/grow/_doc/doc-1", null).body().contains("before split again"));
                final Response put = send(second, "PUT", "/grow/_doc/after-split?refresh=true", "{\"msg\":\"after split\",\"n\":100}");
                assertEquals(put.body(), 201, put.status());
                final Response count = send(second, "GET", "/grow/_count", null);
                assertTrue(count.body(), count.body().contains("\"count\":" + DOCS));
                assertTrue("two units searched: " + count.body(), count.body().contains("\"total\":2"));
                final Response searched = send(second, "GET", "/grow/_search?q=msg:after", null);
                assertTrue(searched.body(), searched.body().contains("after-split"));
            }
        }
    }

    public void testSplittingAUnitThatPublishedNothingIsRefused() throws Exception {
        final MetadataPlane plane = new MetadataPlane(
            new FsBlobStore(1024, createTempDir(), false),
            BlobPath.cleanPath(),
            () -> 1_000L,
            TTL
        );
        final IndexDescriptor empty = new IndexDescriptor("empty", "uuid-empty-000000000", 1, null, null).withLayout(
            UnitLayout.initial(ShardMode.ADAPTIVE, 2)
        );
        plane.createIndex(empty);
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> UnitSplitBuilder.build(plane, empty, empty.layout().planSplit(0), createTempDir())
        );
        assertTrue(e.getMessage(), e.getMessage().contains("published nothing"));
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
