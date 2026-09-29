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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ShardHead;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The per-node caches hold what is recent, not everything ever seen.
 *
 * <p>Both the routing cache and the head sightings used to drop an entry only when a lookup happened to
 * find it stale, so a name or shard touched once stayed for the life of the process -- at a million
 * distinct indices, gigabytes on a coordinator. Past a threshold, entries no reader would believe are
 * dropped; entries a reader would still believe are kept, so no answer changes.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessCacheBoundTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    /** Names resolved long ago are dropped once the cache is past its threshold; a fresh one is kept. */
    public void testTheRoutingCacheKeepsOnlyWhatIsRecent() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL)
            .setRoutingCachePruneAbove(5);
        final int names = 20;
        for (int i = 0; i < names; i++) {
            plane.createIndex(new IndexDescriptor("idx-" + i, "uuid-idx-" + i, 1, MAPPING, null));
            plane.resolveForRouting("idx-" + i);
        }
        assertTrue("every name resolved inside one window is held: " + plane.routingCacheSize(), plane.routingCacheSize() >= names - 1);

        clock.addAndGet(MetadataPlane.DEFAULT_ROUTING_CACHE_MILLIS + 1);
        plane.resolveForRouting("idx-0");
        assertTrue(
            "past the window, names nothing would answer from must be dropped: " + plane.routingCacheSize(),
            plane.routingCacheSize() <= 1
        );
        assertEquals("and the name just resolved is still served", "uuid-idx-0", plane.describeForRouting("idx-0").orElseThrow().uuid());
    }

    /** Head sightings older than a lease are dropped once past the threshold; a fresh one is kept. */
    public void testHeadSightingsKeepOnlyWhatIsRecent() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        try (ServerlessNode node = new ServerlessNode(nodeSettings())) {
            node.setMetadataPlane(plane);
            node.setRecentHeadsPruneAbove(5);
            final int shards = 20;
            for (int i = 0; i < shards; i++) {
                node.noteHead("idx", i, new ShardHead("idx", i, 1L, "owner", "ephemeral", clock.get() + TTL));
            }
            assertTrue("every sighting inside one lease is held: " + node.recentHeadCount(), node.recentHeadCount() >= shards - 1);

            clock.addAndGet(TTL + 1);
            node.noteHead("idx", 0, new ShardHead("idx", 0, 1L, "owner", "ephemeral", clock.get() + TTL));
            assertTrue(
                "past a lease, sightings nothing would believe must be dropped: " + node.recentHeadCount(),
                node.recentHeadCount() <= 1
            );
            assertEquals("and the fresh one is still a hint", "owner", node.ownerHint("idx", 0).orElseThrow());
        }
    }

    private Settings nodeSettings() {
        return Settings.builder()
            .put("node.name", "cache-bounds")
            .put("cluster.name", "serverless-cache-bounds")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }
}
