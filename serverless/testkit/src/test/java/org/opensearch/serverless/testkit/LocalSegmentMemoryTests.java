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
import org.opensearch.serverless.store.LocalSegmentTier;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A writer holding its freshly flushed segments in memory, with a budget small enough that files spill and are evicted to
 * disk: what it acknowledged is read back, published from memory, and read by a successor that takes its shards over with
 * none of that memory; and the writer gives its memory back when it closes.
 */
public class LocalSegmentMemoryTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-local-segments")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(LocalSegmentTier.ENABLED_SETTING, true)
            .put(LocalSegmentTier.MAX_BYTES_SETTING, "96kb")
            .put(LocalSegmentTier.MAX_FILE_BYTES_SETTING, "8kb")
            .build();
    }

    public void testWritesPublishedFromMemorySurviveATakeover() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final List<String> indices = List.of("alpha", "bravo");
        for (String index : indices) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index + "-000000000000".substring(index.length()), 1, MAPPING, null));
        }
        final int docs = 300;
        try (
            ServerlessNode first = new ServerlessNode(nodeSettings("memory-first"));
            ServerlessNode second = new ServerlessNode(nodeSettings("memory-second"))
        ) {
            first.startup().start();
            second.startup().start();
            first.setMetadataPlane(plane);
            second.setMetadataPlane(plane);
            final BackgroundReconciler writer = new BackgroundReconciler(first, plane).setDemandDrivenActivation(true);
            writer.activateOnDemand(indices.stream().map(i -> Map.entry(i, 0)).toList());
            assertEquals(indices.size(), first.reconciler().openShards().size());
            for (int round = 0; round < 3; round++) {
                for (ShardId shardId : first.reconciler().openShards()) {
                    // In bulks: one write waits out the log's window, and three hundred of them one at a time take minutes.
                    for (int from = round * docs / 3; from < (round + 1) * docs / 3; from += 20) {
                        final List<org.opensearch.serverless.store.WalRecord> records = new java.util.ArrayList<>();
                        for (int i = from; i < from + 20; i++) {
                            records.add(
                                new org.opensearch.serverless.store.WalRecord(
                                    "doc-" + i,
                                    "{\"msg\":\"round " + round + " of a document long enough to fill\",\"n\":" + i + "}"
                                )
                            );
                        }
                        first.writes().bulk(shardId, records);
                    }
                }
                // Each round published: a commit uploaded from what memory and disk hold between them.
                writer.publishAll();
            }
            for (ShardId shardId : first.reconciler().openShards()) {
                first.writes().index(shardId, "late", "{\"msg\":\"only in the log\",\"n\":-1}");
                assertTrue(first.reads().get(shardId, "doc-0").found());
            }
            final LocalSegmentTier.Stats stats = first.localSegments().stats();
            assertTrue("flushed files went to memory: " + stats, stats.memoryFlush() > 0);
            assertTrue("the small budget made files move to disk: " + stats, stats.evicted() + stats.spilledWhileWriting() > 0);
            assertTrue("the budget held: " + stats, stats.peakBytes() <= 96 * 1024);
            assertTrue("files were read from memory: " + stats, stats.memoryOpens() > 0);

            // The first node stops; the second takes its shards from what was published and the node log.
            clock.addAndGet(2 * TTL);
            second.leases().renewLease(plane);
            plane.membership().refresh();
            final BackgroundReconciler loop = new BackgroundReconciler(second, plane).setDemandDrivenActivation(true);
            loop.takeOverFrom(first.localNode().getId());
            assertBusy(
                () -> assertEquals(indices.size(), second.reconciler().openShards().size()),
                30,
                java.util.concurrent.TimeUnit.SECONDS
            );
            for (ShardId shardId : second.reconciler().openShards()) {
                for (int i = 0; i < docs; i++) {
                    assertTrue("doc-" + i + " in " + shardId, second.reads().get(shardId, "doc-" + i).found());
                }
                assertTrue(second.reads().get(shardId, "late").found());
            }
        }
    }
}
