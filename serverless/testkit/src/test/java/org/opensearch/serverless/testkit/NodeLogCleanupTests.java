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
import org.opensearch.serverless.store.NodeLogEpoch;
import org.opensearch.serverless.store.NodeLogStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** A node deletes its own log behind the trim point its lease records, by name, once every shard has published. */
public class NodeLogCleanupTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 2;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    public void testPublishedRecordsAreDeletedAfterTheNextRenewal() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final FsBlobStore store = new FsBlobStore(8192, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        final Settings settings = Settings.builder()
            .put("node.name", "node-log-cleanup")
            .put("cluster.name", "serverless-node-log-cleanup")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 0)
            .build();
        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (int shard = 0; shard < SHARDS; shard++) {
                loop.want("alpha", shard);
            }
            loop.tick(clock.get());
            final List<ShardId> held = new ArrayList<>(node.reconciler().openShards());
            for (int i = 0; i < 10; i++) {
                node.index(held.get(i % SHARDS), "doc-" + i, "{\"msg\":\"m\"}");
            }
            final NodeLogStore log = new NodeLogStore(plane.blobStore(), plane.basePath(), node.localNode().getId());
            final NodeLogEpoch before = node.nodeLogWriter().epochs().get(0);
            assertTrue("ten writes, ten blobs with a zero window", before.landed() >= 10);
            assertTrue(log.readEntry(before.epoch(), 0L).isPresent());

            for (ShardId shardId : held) {
                node.reconciler().publish(shardId, node.reconciler().shard(shardId).getOperationPrimaryTerm());
            }
            final NodeLogEpoch published = node.nodeLogWriter().epochs().get(0);
            assertEquals("nothing unpublished: the trim point is the next slot", published.landed(), published.trimPoint());
            assertTrue("not deleted before a lease records the trim point", log.readEntry(before.epoch(), 0L).isPresent());

            node.renewLease(plane);
            for (long ordinal = 0; ordinal < published.trimPoint(); ordinal++) {
                assertTrue(
                    "slot " + ordinal + " is deleted once the lease records the trim point",
                    log.readEntry(before.epoch(), ordinal).isEmpty()
                );
            }
            assertEquals(
                "the lease records it",
                published.trimPoint(),
                plane.membership().read(node.localNode().getId()).orElseThrow().nodeLog().get(0).trimPoint()
            );

            // Writing again goes on in the same epoch, above what was deleted.
            node.index(held.get(0), "doc-after", "{\"msg\":\"m\"}");
            assertTrue(log.readEntry(before.epoch(), published.landed()).isPresent());
        }
    }
}
