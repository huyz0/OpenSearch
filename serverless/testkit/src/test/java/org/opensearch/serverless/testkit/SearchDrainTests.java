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
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.SearchDrain;
import org.opensearch.serverless.shell.SearchReadiness;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A search node that drains drops out of placement, hands its warm shards to the peer that now ranks first for them,
 * and that peer serves them without reading the store.
 */
public class SearchDrainTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 6;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-drain")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.block_cache.block_size", 4096)
            .put(SearchDrain.SETTLE, "0ms")
            .build();
    }

    public void testADrainingNodeHandsItsWarmShardsToItsSuccessor() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("drained", "uuid-drained-000000", SHARDS, MAPPING, null));
        try (ServerlessNode writer = new ServerlessNode(nodeSettings("drain-w", "ingest"))) {
            writer.startup().start();
            for (int s = 0; s < SHARDS; s++) {
                final ShardId shardId = writer.activation().activateWriter(plane, "drained", s).orElseThrow();
                for (int i = 0; i < 1_000; i++) {
                    ShardOps.indexDoc(writer.reconciler().shard(shardId), s + "-" + i, "{\"msg\":\"leaving " + i + "\",\"n\":" + i + "}");
                }
                writer.reconciler().shard(shardId).refresh("drain");
                writer.shards().publishShard(shardId, plane.heads().read("drained", s).orElseThrow().term());
            }
        }
        try (
            ServerlessNode leaving = new ServerlessNode(nodeSettings("drain-leaving", "search"));
            ServerlessNode staying = new ServerlessNode(nodeSettings("drain-staying", "search"))
        ) {
            for (ServerlessNode node : java.util.List.of(leaving, staying)) {
                node.startup().start();
                node.setMetadataPlane(plane);
                node.leases().renewLease(plane);
            }
            for (int s = 0; s < SHARDS; s++) {
                final ShardId onLeaving = leaving.readers().serveAsReader(plane, "drained", s);
                assertEquals(1_000L, ShardOps.hits(leaving.searchService(), onLeaving, "msg", "leaving"));
            }

            final SearchDrain.Outcome outcome = SearchDrain.drain(leaving, plane);
            logger.info("DRAIN|{}", outcome);
            assertEquals(SearchReadiness.State.DRAINING, SearchReadiness.of(leaving).state());
            assertEquals(
                "out of placement",
                Set.of(),
                plane.membership().read(leaving.localNode().getId()).map(NodeLease::roles).orElseThrow()
            );
            assertEquals(1, outcome.peersAsked());
            assertEquals(0, outcome.peersFailed());
            assertEquals("the only other reader takes every shard", SHARDS, outcome.shardsTakenOver());
            assertTrue(outcome.blocksHandedOver() > 0);
            assertEquals(SHARDS, staying.reconciler().readerShards().size());

            for (ShardId onStaying : staying.reconciler().readerShards()) {
                assertEquals(1_000L, ShardOps.hits(staying.searchService(), onStaying, "msg", "leaving"));
            }
            assertEquals("the successor served them from what it was handed", 0L, staying.blockCache().bytesFetched());

            assertEquals("a second drain does nothing", 0, SearchDrain.drain(leaving, plane).peersAsked());
        }
    }
}
