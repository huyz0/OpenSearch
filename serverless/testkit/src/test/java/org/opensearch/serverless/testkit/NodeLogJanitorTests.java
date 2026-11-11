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

/** A dead node's write log is deleted by a survivor once every shard it names has published past it, and not before. */
public class NodeLogJanitorTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 3;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-node-log-janitor")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 20)
            .build();
    }

    private static BackgroundReconciler takeAll(ServerlessNode node, MetadataPlane plane, AtomicLong clock) throws Exception {
        node.leases().renewLease(plane);
        plane.membership().refresh();
        final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
        for (int shard = 0; shard < SHARDS; shard++) {
            loop.want("alpha", shard);
        }
        loop.tick(clock.get());
        assertEquals(SHARDS, node.reconciler().openShards().size());
        return loop;
    }

    public void testADeadWritersLogStaysUntilItsShardsArePublishedThenGoes() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        final ServerlessNode first = new ServerlessNode(nodeSettings("node-log-janitor-first"));
        try {
            first.startup().start();
            first.setMetadataPlane(plane);
            takeAll(first, plane, clock);
            for (ShardId shardId : new ArrayList<>(first.reconciler().openShards())) {
                first.writes().index(shardId, "unpublished-" + shardId.id(), "{\"msg\":\"u\",\"n\":" + shardId.id() + "}");
            }
            final String dead = first.localNode().getId();
            assertEquals("a live node's log is its own", MetadataPlane.NodeLogCollected.NOTHING, plane.collectNodeLog(dead));

            clock.addAndGet(2 * TTL);
            final List<NodeLogEpoch> epochs = plane.membership().read(dead).orElseThrow().nodeLog();
            assertFalse(epochs.isEmpty());
            assertEquals(
                "nobody has taken its shards, so what it holds is not published anywhere",
                MetadataPlane.NodeLogCollected.STILL_NEEDED,
                plane.collectNodeLog(dead)
            );
            assertEquals("and the lease still names it", epochs, plane.membership().read(dead).orElseThrow().nodeLog());

            try (ServerlessNode second = new ServerlessNode(nodeSettings("node-log-janitor-second"))) {
                second.startup().start();
                second.setMetadataPlane(plane);
                final BackgroundReconciler loop = takeAll(second, plane, clock);
                for (ShardId shardId : second.reconciler().openShards()) {
                    assertTrue(second.reads().get(shardId, "unpublished-" + shardId.id()).found());
                }

                // Long enough after the departure that takeovers are done; the survivor's janitor deletes the log.
                for (int step = 0; step < 6; step++) {
                    clock.addAndGet(TTL / 2);
                    second.leases().renewLease(plane);
                }
                plane.membership().refresh();
                loop.sweepAbandoned(BackgroundReconciler.DEFAULT_JANITOR_BUDGET, 0);
                assertEquals(1L, loop.nodeLogsCollected());
                assertEquals(
                    "the lease names no epochs, so it can be pruned",
                    List.of(),
                    plane.membership().read(dead).orElseThrow().nodeLog()
                );
                final NodeLogStore log = new NodeLogStore(plane.blobStore(), plane.basePath(), dead);
                for (NodeLogEpoch epoch : epochs) {
                    for (long ordinal = epoch.trimPoint(); ordinal <= epoch.landed(); ordinal++) {
                        assertTrue(
                            "blob " + ordinal + " of epoch " + epoch.epoch() + " is gone",
                            log.readEntry(epoch.epoch(), ordinal).isEmpty()
                        );
                    }
                }
                assertEquals("a second look finds nothing", MetadataPlane.NodeLogCollected.NOTHING, plane.collectNodeLog(dead));
            }
        } finally {
            try {
                first.close();
            } catch (Exception e) {
                // It lost every shard it held.
            }
        }
    }
}
