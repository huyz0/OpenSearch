/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The {@code Client} a plugin would use, over the shell's own operations.
 *
 * <p>Plugins that keep state — Security's config, an ISM policy store — do it by calling {@code Client}
 * against an index. The shell builds a {@code NodeClient} and never gives it an action registry, so every
 * such call failed; that single gap is what made plugin support impossible rather than merely incomplete.
 *
 * <p><b>The refusal test matters as much as the working ones.</b> {@code AbstractClient} funnels dozens of
 * methods through one {@code doExecute}, so an unimplemented action is one line away from returning a
 * default-constructed response — "no results" for a search, "done" for a delete. Refusing by name is the
 * same rule D2 applies to REST, one layer down.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessClientTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-client")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    private MetadataPlane plane(AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
    }

    /**
     * The whole round trip a plugin needs: make an index, write a document, read it back, delete it.
     *
     * <p>Written the way a plugin writes it — {@code client.index(...).actionGet()} — rather than through
     * the shell's own methods, because the interface is the thing under test.
     */
    public void testAPluginCanKeepStateInAnIndexThroughTheClient() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane metadata = plane(clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("client-roundtrip"))) {
            node.start();
            node.setMetadataPlane(metadata);
            final Client client = node.client();

            // A plugin bootstrapping its config index.
            client.admin().indices().create(new CreateIndexRequest(".plugin_config").mapping(MAPPING)).actionGet();
            assertTrue("the index should exist in the metadata plane", metadata.describe(".plugin_config").isPresent());

            // It has to be activated before it can be written, exactly as a user index does.
            final BackgroundReconciler loop = new BackgroundReconciler(node, metadata);
            loop.want(".plugin_config", 0);
            loop.tick(clock.get());

            client.index(new IndexRequest(".plugin_config").id("roles").source("{\"msg\":\"config\",\"n\":1}", XContentType.JSON))
                .actionGet();

            final var got = client.get(new GetRequest(".plugin_config", "roles")).actionGet();
            assertTrue("the plugin must find what it wrote: " + got, got.isExists());
            assertTrue("and get its body back: " + got.getSourceAsString(), got.getSourceAsString().contains("\"msg\":\"config\""));

            final var deleted = client.delete(new DeleteRequest(".plugin_config", "roles")).actionGet();
            assertEquals("a delete of an existing document must report it deleted", "DELETED", deleted.getResult().name());

            assertFalse("and it must be gone afterwards", client.get(new GetRequest(".plugin_config", "roles")).actionGet().isExists());
        }
    }

    /** A document that was never written is reported absent, not as an error and not as an empty success. */
    public void testAMissingDocumentIsReportedAbsent() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane metadata = plane(clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("client-missing"))) {
            node.start();
            node.setMetadataPlane(metadata);
            metadata.createIndex(new IndexDescriptor(".plugin_config", "uuid-cfg-000000000", 1, MAPPING, null));
            final BackgroundReconciler loop = new BackgroundReconciler(node, metadata);
            loop.want(".plugin_config", 0);
            loop.tick(clock.get());

            final var got = node.client().get(new GetRequest(".plugin_config", "nothing-here")).actionGet();
            assertFalse("absent must be absent, not an error: " + got, got.isExists());
            assertNull(got.getSourceAsString());
        }
    }

    /**
     * An action the shell does not implement is refused by name, not answered with a default.
     *
     * <p>This is the one that keeps the façade honest. Search goes through the same funnel as everything
     * else, and the failure mode of forgetting it is not a crash — it is a plugin being told, plausibly,
     * that its config index is empty.
     */
    public void testAnUnimplementedActionIsRefusedByName() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane metadata = plane(clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("client-refuse"))) {
            node.start();
            node.setMetadataPlane(metadata);
            metadata.createIndex(new IndexDescriptor(".plugin_config", "uuid-cfg-000000000", 1, MAPPING, null));

            final var failure = expectThrows(Exception.class, () -> node.client().search(new SearchRequest(".plugin_config")).actionGet());
            final String message = failure.getMessage() == null ? failure.toString() : failure.getMessage();
            assertTrue("the refusal must name the action it refused: " + message, message.contains("indices:data/read/search"));
            assertTrue("and say who refused it: " + message, message.contains("serverless shell"));
        }
    }

    /** Creating an index that already exists is refused rather than silently accepted. */
    public void testCreatingAnIndexTwiceIsRefused() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane metadata = plane(clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("client-twice"))) {
            node.start();
            node.setMetadataPlane(metadata);
            node.client().admin().indices().create(new CreateIndexRequest(".plugin_config").mapping(MAPPING)).actionGet();
            // Asserted on the type, not the message: ResourceAlreadyExistsException carries the index name
            // as its message and the meaning in its class, which is the same contract classic OpenSearch
            // has -- and a plugin catches the type.
            final var failure = expectThrows(
                org.opensearch.ResourceAlreadyExistsException.class,
                () -> node.client().admin().indices().create(new CreateIndexRequest(".plugin_config").mapping(MAPPING)).actionGet()
            );
            assertTrue("and it must name the index: " + failure.getMessage(), failure.getMessage().contains(".plugin_config"));
        }
    }

    /**
     * A write for a shard this node does not own is routed to the node that does.
     *
     * <p>A plugin's config index is one shard somewhere in the deployment, and every node needs to read
     * and write it. If the client only worked where the shard happened to live it would be useless for
     * exactly the thing it exists for.
     */
    public void testTheClientRoutesToTheNodeThatOwnsTheShard() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane metadata = plane(clock);
        metadata.createIndex(new IndexDescriptor(".plugin_config", "uuid-cfg-000000000", 1, MAPPING, null));

        try (
            ServerlessNode owner = new ServerlessNode(nodeSettings("client-owner"));
            ServerlessNode other = new ServerlessNode(nodeSettings("client-other"))
        ) {
            owner.start();
            other.start();
            owner.setMetadataPlane(metadata);
            other.setMetadataPlane(metadata);
            final BackgroundReconciler loop = new BackgroundReconciler(owner, metadata);
            loop.want(".plugin_config", 0);
            loop.tick(clock.get());
            assertTrue("the other node must hold nothing", other.reconciler().openShards().isEmpty());

            // Written through the node that does NOT own the shard.
            other.client()
                .index(new IndexRequest(".plugin_config").id("k").source("{\"msg\":\"routed\",\"n\":1}", XContentType.JSON))
                .actionGet();

            final var fromOther = other.client().get(new GetRequest(".plugin_config", "k")).actionGet();
            assertTrue("a read through the non-owner must be forwarded, not answered empty: " + fromOther, fromOther.isExists());
            assertTrue(fromOther.getSourceAsString().contains("routed"));

            final var fromOwner = owner.client().get(new GetRequest(".plugin_config", "k")).actionGet();
            assertTrue("and the owner must have it too", fromOwner.isExists());
        }
    }
}
