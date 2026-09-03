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
 * {@code POST /{index}/_update/{id}} — a partial-document merge.
 *
 * <p><b>Not a transaction, and every test here is written knowing that.</b> The read and the write are two
 * separate calls with nothing holding the document still in between — the honest version of the feature
 * without the version model classic OpenSearch's {@code _update} uses to make it one. See
 * {@code ShardOperations#update} for the reasoning in full.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessUpdateTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    // No dynamic mapping is offered, so a nested field used to prove the merge recurses has to be
    // declared up front like everything else.
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"},"
        + "\"meta\":{\"properties\":{\"kept\":{\"type\":\"keyword\"},\"changed\":{\"type\":\"keyword\"}}}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-update")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
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
                .timeout(Duration.ofSeconds(20))
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }

    private ServerlessNode started(MetadataPlane plane, String name) throws Exception {
        final ServerlessNode node = new ServerlessNode(nodeSettings(name));
        node.start();
        node.setMetadataPlane(plane);
        return node;
    }

    /**
     * A merge changes what {@code doc} named and leaves everything else, including a field nested one
     * level down.
     *
     * <p>Asserted on the nested field deliberately: a shallow merge would overwrite the whole {@code meta}
     * object and lose {@code kept}, which is exactly the bug {@code XContentHelper.update}'s recursion
     * exists to avoid. Using core's own merge rather than a hand-written one is the point, and this is the
     * test that would notice if a future edit swapped it for something shallower.
     */
    public void testUpdateMergesAPartialDocumentIncludingNestedFields() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "update-merge")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);

            assertEquals(
                201,
                send(
                    http,
                    "PUT",
                    "/alpha/_doc/1?refresh=true",
                    "{\"msg\":\"original\",\"n\":1,\"meta\":{\"kept\":\"yes\",\"changed\":\"old\"}}"
                ).status()
            );

            final Response updated = send(
                http,
                "POST",
                "/alpha/_update/1?refresh=true",
                "{\"doc\":{\"n\":2,\"meta\":{\"changed\":\"new\"}}}"
            );
            assertEquals(updated.body(), 200, updated.status());
            assertTrue("the result must say updated: " + updated.body(), updated.body().contains("\"result\":\"updated\""));

            final Response got = send(http, "GET", "/alpha/_doc/1", null);
            assertTrue("the merged field must change: " + got.body(), got.body().contains("\"n\":2"));
            assertTrue("the untouched field must survive: " + got.body(), got.body().contains("\"msg\":\"original\""));
            assertTrue("the changed nested field must change: " + got.body(), got.body().contains("\"changed\":\"new\""));
            assertTrue("and its sibling in the same object must survive: " + got.body(), got.body().contains("\"kept\":\"yes\""));
        }
    }

    /** An update on a document that does not exist, with nothing to fall back to, is a plain miss. */
    public void testUpdateOnAMissingDocumentIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "update-missing")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);

            final Response missing = send(http, "POST", "/alpha/_update/ghost", "{\"doc\":{\"n\":1}}");
            assertEquals(missing.body(), 404, missing.status());
            assertTrue("must name the reason: " + missing.body(), missing.body().contains("document_missing_exception"));
        }
    }

    /** {@code doc_as_upsert} writes {@code doc} itself when nothing exists to merge into. */
    public void testDocAsUpsertCreatesWhenMissing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "update-doc-upsert")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);

            final Response created = send(
                http,
                "POST",
                "/alpha/_update/new1?refresh=true",
                "{\"doc\":{\"msg\":\"born-here\",\"n\":9},\"doc_as_upsert\":true}"
            );
            assertEquals(created.body(), 200, created.status());
            assertTrue("the result must say created: " + created.body(), created.body().contains("\"result\":\"created\""));

            final Response got = send(http, "GET", "/alpha/_doc/new1", null);
            assertTrue(got.body(), got.body().contains("\"born-here\""));
        }
    }

    /** {@code upsert} writes a document of its own choosing, distinct from {@code doc}, when nothing exists. */
    public void testUpsertCreatesADifferentDocumentWhenMissing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "update-upsert")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);

            final Response created = send(
                http,
                "POST",
                "/alpha/_update/new2?refresh=true",
                "{\"doc\":{\"n\":1},\"upsert\":{\"msg\":\"from-upsert\",\"n\":0}}"
            );
            assertEquals(created.body(), 200, created.status());
            assertTrue("the result must say created: " + created.body(), created.body().contains("\"result\":\"created\""));

            final Response got = send(http, "GET", "/alpha/_doc/new2", null);
            // upsert wrote, not doc -- the document must be the upsert body, unmerged with doc.
            assertTrue("the upsert body must have been written: " + got.body(), got.body().contains("\"from-upsert\""));
            assertTrue("with its own n rather than doc's: " + got.body(), got.body().contains("\"n\":0"));
        }
    }

    /**
     * A merge that changes nothing is reported as a noop, and does not actually write.
     *
     * <p><b>Asserted on the shard's own sequence number, not only on the label.</b> A response that says
     * {@code "result":"noop"} proves nothing about whether a write happened — a version of this code that
     * labelled every write "noop" would pass a test that only read the field back. The engine's own max
     * sequence number moves on every real write and nowhere else, so it is the one signal here that is
     * not the thing being tested reporting on itself.
     */
    public void testAnIdenticalMergeIsANoop() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "update-noop")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);
            send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"same\",\"n\":1}");
            final var shardId = node.reconciler().openShards().iterator().next();
            final long before = node.reconciler().shard(shardId).seqNoStats().getMaxSeqNo();

            final Response noop = send(http, "POST", "/alpha/_update/1", "{\"doc\":{\"msg\":\"same\"}}");
            assertEquals(noop.body(), 200, noop.status());
            assertTrue("a merge that changed nothing must say noop: " + noop.body(), noop.body().contains("\"result\":\"noop\""));

            final long after = node.reconciler().shard(shardId).seqNoStats().getMaxSeqNo();
            assertEquals("and must not actually have written anything", before, after);
        }
    }

    /**
     * With {@code detect_noop} turned off, an identical merge is still written and reported as updated.
     *
     * <p>The pair to the test above: this asserts the sequence number <em>does</em> move, which is what
     * makes the "must not actually write" assertion there mean something rather than an artefact of a
     * shard that never writes at all.
     */
    public void testDetectNoopFalseAlwaysReportsUpdated() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "update-noop-off")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);
            send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"same\",\"n\":1}");
            final var shardId = node.reconciler().openShards().iterator().next();
            final long before = node.reconciler().shard(shardId).seqNoStats().getMaxSeqNo();

            final Response updated = send(http, "POST", "/alpha/_update/1", "{\"doc\":{\"msg\":\"same\"},\"detect_noop\":false}");
            assertEquals(updated.body(), 200, updated.status());
            assertTrue("detect_noop:false must force a write: " + updated.body(), updated.body().contains("\"result\":\"updated\""));

            final long after = node.reconciler().shard(shardId).seqNoStats().getMaxSeqNo();
            assertTrue("and must actually have written: before=" + before + " after=" + after, after > before);
        }
    }

    /** A scripted update is refused, the same way scripting is refused everywhere else. */
    public void testAScriptedUpdateRunsAndAStoredOneIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "update-script")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);
            send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"x\",\"n\":1}");

            // Since M57 a scripted update runs: an engine is registered, chosen by name the same way the
            // transport is. What is still refused is a script referenced by id, because a stored script
            // resolves through cluster state this design does not have.
            final Response ran = send(http, "POST", "/alpha/_update/1", "{\"script\":{\"source\":\"ctx._source.n += 1\"}}");
            assertEquals(ran.body(), 200, ran.status());
            assertTrue(ran.body(), ran.body().contains("\"result\":\"updated\""));

            final Response got = send(http, "GET", "/alpha/_doc/1", null);
            assertTrue("the script's edit must reach the document: " + got.body(), got.body().contains("\"n\":2"));

            final Response stored = send(http, "POST", "/alpha/_update/1", "{\"script\":{\"id\":\"somewhere\"}}");
            assertEquals(stored.body(), 400, stored.status());
            assertTrue("must say why: " + stored.body(), stored.body().contains("stored script cannot be used here"));
        }
    }

    /** A conditional update is refused, the same way a conditional plain write is. */
    public void testExternalVersioningIsRefusedWhileAConditionIsHonoured() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "update-conditional")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);
            send(http, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"x\",\"n\":1}");

            // External versioning is still refused -- it asks for a model this system does not keep.
            final Response refused = send(http, "POST", "/alpha/_update/1?version=3", "{\"doc\":{\"n\":2}}");
            assertEquals(refused.body(), 501, refused.status());
            assertTrue(refused.body(), refused.body().contains("external versioning"));

            // A condition, by contrast, is now honoured: a stale one conflicts rather than overwriting.
            final Response stale = send(http, "POST", "/alpha/_update/1?if_seq_no=500&if_primary_term=1", "{\"doc\":{\"n\":2}}");
            assertEquals("a stale condition must conflict: " + stale.body(), 409, stale.status());
        }
    }

    /** A body with no {@code doc} and no {@code upsert} is a bad request, not a silent no-op. */
    public void testAnUpdateWithNeitherDocNorUpsertIsRejected() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = started(plane, "update-empty")) {
            final TransportAddress http = node.boundHttpAddress().publishAddress();
            send(http, "PUT", "/alpha?shards=1", MAPPING);
            node.activateWriter(plane, "alpha", 0);

            final Response rejected = send(http, "POST", "/alpha/_update/1", "{}");
            assertEquals(rejected.body(), 400, rejected.status());
        }
    }

    /**
     * An update to a shard this node does not own is forwarded, both halves.
     *
     * <p>An update composes a read and a write, and each already forwards on its own — this is the test
     * that both halves actually do, end to end, on a node holding neither.
     */
    public void testUpdateForwardsToTheOwner() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (
            ServerlessNode owner = started(plane, "update-owner");
            ServerlessNode other = new ServerlessNode(nodeSettings("update-other"))
        ) {
            final TransportAddress ownerHttp = owner.boundHttpAddress().publishAddress();
            send(ownerHttp, "PUT", "/alpha?shards=1", MAPPING);
            owner.activateWriter(plane, "alpha", 0);
            send(ownerHttp, "PUT", "/alpha/_doc/1?refresh=true", "{\"msg\":\"original\",\"n\":1}");

            other.start();
            other.setMetadataPlane(plane);
            final TransportAddress otherHttp = other.boundHttpAddress().publishAddress();
            assertTrue("the other node must not own the shard", other.reconciler().openShards().isEmpty());

            final Response updated = send(otherHttp, "POST", "/alpha/_update/1?refresh=true", "{\"doc\":{\"n\":2}}");
            assertEquals(updated.body(), 200, updated.status());
            assertTrue("the result must say updated: " + updated.body(), updated.body().contains("\"result\":\"updated\""));

            final Response got = send(ownerHttp, "GET", "/alpha/_doc/1", null);
            assertTrue("the merge must have reached the owner's data: " + got.body(), got.body().contains("\"n\":2"));
        }
    }
}
