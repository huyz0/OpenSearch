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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The owner hint: what a coordinator remembers about who owns a shard, and when it must stop believing it.
 *
 * <p>A hint saves a register read per forwarded write, which was the largest fixed cost of a write the
 * coordinator does not serve itself. It is safe only because a wrong hint is corrected: the node it names
 * refuses, the coordinator reads the register once, and forwards again. These fixtures are the cases where
 * that correction did not happen -- the hinted node was dead rather than refusing, or the index behind the
 * name had been replaced -- and the coordinator either failed forever or, worse, was told its write landed.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessRoutingHintTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-routing-hint")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane plane(AtomicLong clock) throws Exception {
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        return plane;
    }

    /**
     * A hint naming an owner that died is forgotten the moment it fails, and the next write goes to whoever
     * took over -- without restarting the coordinator.
     *
     * <p>The hint used to be forgotten only when the hinted node answered "does not own". A dead node
     * answers nothing: its lease lapses, the coordinator reports the owner unreachable, keeps the hint,
     * and reports the same thing on every later write, get and update through it, indefinitely -- while
     * {@code _bulk} through the same node, which reads the register each time, worked. Three nodes,
     * because the successor must be a node the coordinator has never heard of.
     */
    public void testAStaleHintToADeadOwnerIsForgottenWhenAnotherNodeTakesOver() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);

        try (
            ServerlessNode coordinator = new ServerlessNode(nodeSettings("hint-coordinator"));
            ServerlessNode successor = new ServerlessNode(nodeSettings("hint-successor"))
        ) {
            coordinator.start();
            successor.start();
            coordinator.setMetadataPlane(plane);
            successor.setMetadataPlane(plane);

            final ServerlessNode owner = new ServerlessNode(nodeSettings("hint-owner"));
            owner.start();
            owner.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(owner, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());

            // One write through the coordinator, forwarded to the owner, so the coordinator remembers it.
            final Response first = send(coordinator, "PUT", "/alpha/_doc/1", "{\"msg\":\"before\",\"n\":1}");
            assertEquals(first.body(), 201, first.status());
            assertTrue(first.body(), first.body().contains(owner.localNode().getId()));
            assertEquals(owner.localNode().getId(), coordinator.ownerHint("alpha", 0).orElseThrow());
            final var headNamingOwner = plane.heads().read("alpha", 0).orElseThrow();

            // The owner dies without releasing anything, its lease lapses, and a node the coordinator has
            // never talked to takes the shard.
            owner.close();
            clock.set(clock.get() + TTL + 1_000);
            final ShardId onSuccessor = successor.activateWriter(plane, "alpha", 0).orElseThrow();
            assertEquals(successor.localNode().getId(), plane.heads().read("alpha", 0).orElseThrow().ownerNodeId());
            // A hint older than a lease is dropped by age, which is right and is not the case under test.
            // The case is a hint refreshed a moment before the owner died -- the coordinator's own hint
            // rebuild read the head while the owner was still alive -- so it is re-noted here, fresh,
            // still naming the dead node. The write below must recover from that without a restart.
            coordinator.noteHead("alpha", 0, headNamingOwner);

            // The coordinator still believes the dead node owns the shard, and must recover on its own.
            assertEquals(owner.localNode().getId(), coordinator.ownerHint("alpha", 0).orElseThrow());
            final Response afterTakeover = send(coordinator, "PUT", "/alpha/_doc/2?refresh=true", "{\"msg\":\"after\",\"n\":2}");
            assertEquals("a write through a stale hint must reach the new owner: " + afterTakeover.body(), 201, afterTakeover.status());
            assertTrue("and be applied there: " + afterTakeover.body(), afterTakeover.body().contains(successor.localNode().getId()));
            assertEquals(
                "the hint now names the successor",
                successor.localNode().getId(),
                coordinator.ownerHint("alpha", 0).orElseThrow()
            );
            assertEquals(1L, ShardOps.hits(successor.searchService(), onSuccessor, "msg", "after"));

            // The read path recovers the same way: a get through the coordinator is answered by the successor.
            final Response got = send(coordinator, "GET", "/alpha/_doc/2", null);
            assertEquals(got.body(), 200, got.status());
            assertTrue(got.body(), got.body().contains(successor.localNode().getId()));
            // And a write with no hint at all -- the next one -- costs no correction: same answer.
            assertEquals(201, send(coordinator, "PUT", "/alpha/_doc/3", "{\"msg\":\"after\",\"n\":3}").status());
        }
    }

    /**
     * An index deleted and recreated under the same name does not take writes into the old incarnation,
     * neither forwarded on a stale hint nor applied locally by the node that still holds the old shard.
     *
     * <p>The single-document local path compared uuids; the forwarded request carried no uuid and the
     * receiving side matched by name, and the batch path matched by name on both sides. So for one
     * heartbeat interval after a recreate, a write through any node with a hint -- and a batch through
     * the old holder itself -- was applied to the deleted index's shard, appended to a log nothing would
     * replay, and acknowledged with 201. The new index stayed empty.
     */
    public void testARecreatedIndexRefusesWritesAimedAtTheOldIncarnation() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);

        try (
            ServerlessNode coordinator = new ServerlessNode(nodeSettings("recreate-coordinator"));
            ServerlessNode holder = new ServerlessNode(nodeSettings("recreate-holder"))
        ) {
            coordinator.start();
            holder.start();
            coordinator.setMetadataPlane(plane);
            holder.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(holder, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId oldShard = holder.reconciler().openShards().iterator().next();

            assertEquals(201, send(coordinator, "PUT", "/alpha/_doc/1", "{\"msg\":\"old\",\"n\":1}").status());
            assertEquals(holder.localNode().getId(), coordinator.ownerHint("alpha", 0).orElseThrow());

            // Deleted and recreated, as any caller would do it, before the holder's next heartbeat: it
            // still has the first incarnation's shard open under the name.
            assertTrue(plane.deleteIndex("alpha"));
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-second-00", 1, MAPPING, null));
            assertTrue("the fixture needs the holder to still hold the old shard", holder.reconciler().openShards().contains(oldShard));
            assertTrue("and nobody to own the new one yet", plane.heads().read("alpha", 0).isEmpty());

            // Forwarded on the stale hint: the holder must refuse, and the coordinator must not say 201.
            final Response forwarded = send(coordinator, "PUT", "/alpha/_doc/2", "{\"msg\":\"new\",\"n\":2}");
            assertNotEquals("a write into a deleted index must never be acknowledged: " + forwarded.body(), 201, forwarded.status());
            assertEquals(
                "nobody owns the new incarnation, which is a 421 as it is everywhere: " + forwarded.body(),
                421,
                forwarded.status()
            );
            assertTrue(forwarded.body(), forwarded.body().contains("not_the_writer"));

            // A batch through the holder itself, which matches by name and shard and used to find the old one.
            final Response batch = send(holder, "POST", "/alpha/_bulk", "{\"index\":{\"_id\":\"3\"}}\n{\"msg\":\"new\",\"n\":3}\n");
            assertEquals(batch.body(), 200, batch.status());
            assertFalse(
                "a batch into a deleted index must never be acknowledged: " + batch.body(),
                batch.body().contains("\"status\":201")
            );
            assertTrue(batch.body(), batch.body().contains("\"status\":421"));

            // Nothing reached the old shard.
            holder.reconciler().shard(oldShard).refresh("test");
            assertEquals(
                "the old incarnation must hold only what was written before the delete",
                0L,
                ShardOps.hits(holder.searchService(), oldShard, "msg", "new")
            );
        }
    }

    private record Response(int status, String body) {
    }

    private static Response send(ServerlessNode node, String method, String path, String body) throws Exception {
        final var address = node.boundHttpAddress().publishAddress();
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            final HttpRequest.BodyPublisher payload = body == null
                ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(body);
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://" + address.getAddress() + ":" + address.getPort() + path))
                .timeout(Duration.ofSeconds(60))
                .header("Content-Type", "application/json")
                .method(method, payload)
                .build();
            final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
            return new Response(response.statusCode(), response.body());
        }
    }
}
