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
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lazy block-range reads: a reader fetches the bytes a query touches, not the segments it lives in.
 *
 * <p>This is what stops cache placement from carrying more weight than it should. Downloading a whole
 * segment to answer one query makes a cache miss cost seconds and makes affinity load-bearing; fetching
 * the blocks a query actually reads makes a miss cost tens of milliseconds and makes affinity an
 * optimisation. The assertions are all on <b>bytes fetched versus bytes published</b>, because a lazy
 * reader that quietly fetches everything is indistinguishable from an eager one unless someone counts.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessBlockReadTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-blocks")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            // A small block so a modest test index spans many of them. The mechanism is what is under
            // test; the production default trades differently.
            .put("serverless.block_cache.block_size", 4096)
            .build();
    }

    /** Total bytes the shard has published, so "fetched" can be compared against something real. */
    private long publishedBytes(MetadataPlane plane, java.nio.file.Path dir, String index, int shard) throws Exception {
        final var store = new FsBlobStore(1024, dir, false);
        final var manifest = plane.segmentPublisher(index, shard).readManifest().orElseThrow();
        long total = 0;
        for (Map.Entry<String, String> file : manifest.files().entrySet()) {
            final var meta = store.blobContainer(plane.shardData(index, shard).add(file.getValue())).listBlobs().get(file.getKey());
            if (meta != null) {
                total += meta.length();
            }
        }
        return total;
    }

    public void testAReaderFetchesFarLessThanTheSegmentsItServes() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path objectStore = createTempDir();
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, objectStore, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("bulky", "uuid-bulky-00000000", 1, MAPPING, null));

        try (ServerlessNode writer = new ServerlessNode(nodeSettings("blocks-w", "ingest"))) {
            writer.start();
            final ShardId shardId = writer.activateWriter(plane, "bulky", 0).orElseThrow();
            // Enough documents that the segments are meaningfully larger than one block.
            for (int i = 0; i < 20_000; i++) {
                // One document carries a term no other has. matchQuery tokenizes and ORs, so a
                // hyphenated "needle-unique" would have matched every "needle" -- the term has to be
                // genuinely unique, not merely look it.
                // Straight to the shard: this test is about read laziness, and routing 20k writes
                // through the WAL would be 20k object-store PUTs for no benefit here.
                ShardOps.indexDoc(
                    writer.reconciler().shard(shardId),
                    "d" + i,
                    "{\"msg\":\"haystack" + (i == 1234 ? " solitaire" : "") + "\",\"n\":" + i + "}"
                );
            }
            writer.reconciler().shard(shardId).refresh("blocks");
            writer.publishShard(shardId, plane.heads().read("bulky", 0).orElseThrow().term());
        }

        final long published = publishedBytes(plane, objectStore, "bulky", 0);
        assertTrue("the test needs segments spanning many blocks: " + published, published > 256 * 1024);

        try (ServerlessNode reader = new ServerlessNode(nodeSettings("blocks-r", "search"))) {
            reader.start();
            final ShardId onReader = reader.serveAsReader(plane, "bulky", 0);
            final long afterOpen = reader.blockCache().bytesFetched();

            assertEquals(
                "the reader must actually serve the data",
                1L,
                ShardOps.hits(reader.searchService(), onReader, "msg", "solitaire")
            );
            final long afterQuery = reader.blockCache().bytesFetched();

            logger.info(
                "lazy reads: published={} bytes, fetched after open={}, after query={} ({}% of the segments)",
                published,
                afterOpen,
                afterQuery,
                (afterQuery * 100) / published
            );

            // Two claims, and the second is the sharper one.
            assertTrue(
                "a reader fetched " + afterQuery + " of " + published + " published bytes -- that is not lazy",
                afterQuery < published / 2
            );
            assertTrue(
                "a selective query pulled " + (afterQuery - afterOpen) + " bytes of a " + published + " byte shard",
                (afterQuery - afterOpen) < published / 10
            );
            assertTrue("something must have been fetched, or this proves nothing", afterQuery > 0);

            // Open costs a roughly fixed amount -- segment metadata, and OpenSearch's Store hashing
            // small files whole for recovery diffing -- while a query costs only the blocks it touches.
            // So the ratio above improves as shards grow, and an earlier run of this test with a 53 KB
            // index showed 84%, which was that fixed cost dominating rather than eager reading.
            logger.info("lazy reads: open cost {} bytes, query cost {} bytes", afterOpen, afterQuery - afterOpen);
        }
    }

    /** A second identical query should be served from cache, not refetched. */
    public void testRepeatedQueriesAreServedFromCache() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final java.nio.file.Path objectStore = createTempDir();
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, objectStore, false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("warm", "uuid-warm-000000000", 1, MAPPING, null));

        try (ServerlessNode writer = new ServerlessNode(nodeSettings("blocks-w2", "ingest"))) {
            writer.start();
            final ShardId shardId = writer.activateWriter(plane, "warm", 0).orElseThrow();
            for (int i = 0; i < 500; i++) {
                writer.index(shardId, "d" + i, "{\"msg\":\"repeated\",\"n\":" + i + "}");
            }
            writer.reconciler().shard(shardId).refresh("blocks");
            writer.publishShard(shardId, plane.heads().read("warm", 0).orElseThrow().term());
        }

        try (ServerlessNode reader = new ServerlessNode(nodeSettings("blocks-r2", "search"))) {
            reader.start();
            final ShardId onReader = reader.serveAsReader(plane, "warm", 0);
            assertEquals(500L, ShardOps.hits(reader.searchService(), onReader, "msg", "repeated"));

            reader.blockCache().resetCounters();
            assertEquals(500L, ShardOps.hits(reader.searchService(), onReader, "msg", "repeated"));

            logger.info(
                "warm reads: {} bytes fetched, {} cache hits, {} misses on a repeat query",
                reader.blockCache().bytesFetched(),
                reader.blockCache().hits(),
                reader.blockCache().misses()
            );
            assertEquals("a repeated query must not refetch anything", 0L, reader.blockCache().bytesFetched());
            assertTrue("and it must actually have consulted the cache", reader.blockCache().hits() > 0);
        }
    }
}
