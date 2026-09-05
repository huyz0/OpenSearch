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
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Phase 3 end to end: create an index in the object store, and a node serves it.
 *
 * <p>Nothing tells the node what to do. It reads descriptors and shard-heads and works it out — which
 * is the whole of what cluster-state publication was for.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only. No durability claim for S3 or GCS until R11 runs.
 */
public class ServerlessEndToEndTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-e2e")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .build();
    }

    private MetadataPlane planeOver(Path dir, AtomicLong clock) throws Exception {
        return new MetadataPlane(new FsBlobStore(1024, dir, false), BlobPath.cleanPath(), clock::get, TTL);
    }

    public void testCreateIndexThenServeThenDelete() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("e2e-a"))) {
            node.start();
            final String nodeId = node.localNode().getId();

            // Nothing exists yet, and a sync must say so rather than inventing something.
            assertTrue("a sync against an empty object store must open nothing", node.syncFrom(plane).isEmpty());

            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
            // A descriptor alone is not an assignment: the index exists, but no node owns its shard.
            assertTrue("an unactivated index must not open anywhere", node.syncFrom(plane).isEmpty());

            assertTrue(plane.activate("alpha", 0, nodeId, node.localNode().getEphemeralId()).acquired());

            final var opened = node.syncFrom(plane);
            assertEquals("the node did not open the shard it now owns", 1, opened.size());

            final ShardId shardId = opened.iterator().next();
            final IndexShard shard = node.reconciler().shard(shardId);
            assertEquals(IndexShardState.STARTED, shard.state());

            ShardOps.indexDoc(shard, "1", "{\"msg\":\"object store is the truth\",\"n\":1}");
            ShardOps.indexDoc(shard, "2", "{\"msg\":\"object store is the truth\",\"n\":2}");
            shard.refresh("e2e");
            assertEquals("the shard served nothing", 2L, ShardOps.hits(node.searchService(), shardId, "msg", "truth"));

            // Delete in the metadata plane, then sync: the node lets go because TRUTH changed.
            assertTrue(plane.deleteIndex("alpha"));
            node.syncFrom(plane);
            assertTrue("the node kept a shard whose index was deleted", node.reconciler().openShards().isEmpty());
            assertEquals(IndexShardState.CLOSED, shard.state());
        }
    }

    public void testASecondNodeOwningNothingOpensNothing() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode a = new ServerlessNode(nodeSettings("e2e-b1")); ServerlessNode b = new ServerlessNode(nodeSettings("e2e-b2"))) {
            a.start();
            b.start();
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
            plane.activate("alpha", 0, a.localNode().getId(), a.localNode().getEphemeralId());

            assertEquals(1, a.syncFrom(plane).size());
            assertTrue("node B owns nothing and must open nothing", b.syncFrom(plane).isEmpty());
            // B does not even learn the index exists: residency tracks the working set, not the world.
            assertFalse(b.clusterService().state().metadata().hasIndex("alpha"));
        }
    }

    /**
     * Failover: A's lease lapses, B wins the head, and each node's sync does the right thing.
     *
     * <p><b>This is ownership failover, not data failover.</b> Each node here has its own
     * {@code path.home} and recovers from its own empty store, so B takes over the shard's
     * <em>identity</em> and serves an empty shard. Moving the <em>data</em> requires the object-store
     * engine, which is phases 4 and 5. Asserting a document count across the handover would be testing
     * something that does not exist yet, so this asserts what actually holds: the term, the ownership,
     * and that the loser lets go.
     */
    public void testFailoverMovesTheShardAndTheLoserLetsGo() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = planeOver(createTempDir(), clock);

        try (ServerlessNode a = new ServerlessNode(nodeSettings("e2e-c1")); ServerlessNode b = new ServerlessNode(nodeSettings("e2e-c2"))) {
            a.start();
            b.start();
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));
            plane.activate("alpha", 0, a.localNode().getId(), a.localNode().getEphemeralId());
            assertEquals(1, a.syncFrom(plane).size());

            // While A holds a live lease, B cannot take the shard and must not open it.
            assertFalse(plane.activate("alpha", 0, b.localNode().getId(), b.localNode().getEphemeralId()).acquired());
            assertTrue("B opened a shard it does not own", b.syncFrom(plane).isEmpty());

            // A's lease lapses. B wins the head at a bumped term.
            clock.set(1_000L + TTL);
            final var won = plane.activate("alpha", 0, b.localNode().getId(), b.localNode().getEphemeralId());
            assertTrue(won.acquired());
            assertEquals("failover must bump the term", 2L, won.head().term());

            assertEquals("B did not take over the shard", 1, b.syncFrom(plane).size());
            final ShardId shardId = b.reconciler().openShards().iterator().next();
            assertEquals(2L, b.reconciler().shard(shardId).getOperationPrimaryTerm());

            // A syncs and discovers it no longer owns the shard. It lets go because truth said so.
            a.syncFrom(plane);
            assertTrue("A kept a shard it had lost", a.reconciler().openShards().isEmpty());
        }
    }
}
