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

    public void testFreshRoutingEntriesHaveAHardBudget() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, TTL).setRoutingCacheMaximumWeight(64L * 1024);
            for (int i = 0; i < 100; i++) {
                final String name = "bounded-" + i;
                plane.createIndex(new IndexDescriptor(name, "uuid-" + i, 1, MAPPING, null));
                assertEquals(name, plane.resolveForRouting(name).index().name());
            }
            assertTrue("fresh resolutions must also be bounded: " + plane.routingCacheSize(), plane.routingCacheSize() <= 16);
            assertEquals("the most recent entry stays warm", "bounded-99", plane.resolveForRouting("bounded-99").index().name());
        }
    }

    public void testAnOversizedMappingIsResolvedWithoutRetention() throws Exception {
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, TTL).setRoutingCacheMaximumWeight(64L * 1024);
            final String mapping = "{\"_meta\":{\"description\":\"" + "x".repeat(10_000) + "\"}}";
            plane.createIndex(new IndexDescriptor("large", "uuid-large", 1, mapping, null));
            assertEquals(mapping, plane.resolveForRouting("large").index().mapping());
            assertEquals("an oversized resolution must not remain resident", 0, plane.routingCacheSize());
            assertTrue(plane.readConfirmingIncarnation("large", "uuid-large").isEmpty());
        }
    }

    public void testAnOldLookupCannotRestoreAnInvalidatedResolution() throws Exception {
        try (var store = new HookedBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1_000L, TTL);
            plane.createIndex(new IndexDescriptor("changing", "uuid-changing", 1, MAPPING, null));
            store.onNextRegisterRead("indices", "changing", () -> plane.forgetRouting("changing"));
            assertEquals("changing", plane.resolveForRouting("changing").index().name());
            assertTrue("invalidate while the old read is in flight", store.fired());
            assertEquals("the old result must not undo invalidation", 0, plane.routingCacheSize());
            assertEquals("changing", plane.resolveForRouting("changing").index().name());
            assertEquals("a later read can be cached", 1, plane.routingCacheSize());
        }
    }

    public void testAnOversizedLookupDoesNotEvictWarmSmallEntries() throws Exception {
        try (var counter = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false))) {
            final var plane = new MetadataPlane(counter, BlobPath.cleanPath(), () -> 1_000L, TTL).setRoutingCacheMaximumWeight(64L * 1024);
            plane.createIndex(new IndexDescriptor("small", "uuid-small", 1, MAPPING, null));
            plane.createIndex(
                new IndexDescriptor("large", "uuid-large", 1, "{\"_meta\":{\"description\":\"" + "x".repeat(10_000) + "\"}}", null)
            );
            plane.resolveForRouting("small");
            plane.resolveForRouting("large");
            counter.reset();
            assertEquals("small", plane.resolveForRouting("small").index().name());
            assertEquals(counter.breakdown(), 0L, counter.registerReads());
        }
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
