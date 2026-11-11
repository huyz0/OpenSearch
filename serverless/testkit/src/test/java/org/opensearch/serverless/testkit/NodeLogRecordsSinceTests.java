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
import org.opensearch.serverless.shell.NodeLogWriter;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.shell.WriteOutcome;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** A shard's holder reads back what its node log keeps of the shard past a commit, without changing anything. */
public class NodeLogRecordsSinceTests extends OpenSearchTestCase {

    private static final long TTL = 600_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-node-log-since")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 20)
            .build();
    }

    private static List<String> idsSince(NodeLogWriter log, ShardId shardId, long term, long afterSeqNo) throws Exception {
        final List<String> ids = new ArrayList<>();
        final long streamed = log.forEachRecordSince(shardId, term, afterSeqNo, (WalRecord record) -> ids.add(record.id()));
        assertEquals(ids.size(), streamed);
        return ids;
    }

    public void testRecordsPastACommitAreStreamedAndTheLogIsLeftAsItWas() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("node-log-since"))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            final NodeLogWriter log = node.nodeLogWriter();
            assertNotNull(log);

            WriteOutcome last = null;
            for (int i = 0; i < 3; i++) {
                last = node.writes().index(shardId, "published-" + i, "{\"msg\":\"m\",\"n\":" + i + "}");
            }
            final long term = last.primaryTerm();
            assertEquals(
                "unpublished, the log keeps every write",
                List.of("published-0", "published-1", "published-2"),
                idsSince(log, shardId, term, -1L)
            );
            loop.publishAll();
            final long checkpoint = last.seqNo();
            assertEquals("a published commit's records are let go of", List.of(), idsSince(log, shardId, term, -1L));

            final List<String> after = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                final String id = "after-" + i;
                node.writes().index(shardId, id, "{\"msg\":\"m\",\"n\":" + i + "}");
                after.add(id);
            }
            assertEquals("past the commit's checkpoint, every acknowledged write", after, idsSince(log, shardId, term, checkpoint));
            final long secondOfThem = checkpoint + 2;
            assertEquals(
                "past a later sequence number, only those after it",
                after.subList(2, 4),
                idsSince(log, shardId, term, secondOfThem)
            );
            assertEquals("reading changes nothing: the same records again", after, idsSince(log, shardId, term, checkpoint));
            for (String id : after) {
                assertTrue(id, node.reads().get(shardId, id).found());
            }
            node.writes().index(shardId, "still-writable", "{\"msg\":\"m\",\"n\":9}");
            assertTrue(idsSince(log, shardId, term, checkpoint).contains("still-writable"));
        }
    }
}
