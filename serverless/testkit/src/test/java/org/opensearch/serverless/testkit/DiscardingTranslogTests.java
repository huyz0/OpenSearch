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
import org.opensearch.serverless.engine.ServerlessWriterEngine;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A writer that keeps no local translog: writes are acknowledged, read back realtime, survive a takeover through the node log,
 * and leave no translog file open.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*") // reads /proc/self/fd, which the mock file system hides
public class DiscardingTranslogTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-no-translog")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put(ServerlessWriterEngine.LOCAL_TRANSLOG_SETTING, false)
            .build();
    }

    private static long openTranslogFiles() throws Exception {
        final Path fds = org.opensearch.common.io.PathUtils.get("/proc/self/fd");
        assumeTrue("needs /proc", Files.isDirectory(fds));
        long count = 0;
        try (var listing = Files.list(fds)) {
            for (Path fd : (Iterable<Path>) listing::iterator) {
                try {
                    final String target = Files.readSymbolicLink(fd).toString();
                    if (target.endsWith(".tlog") || target.endsWith(".ckp")) {
                        count++;
                    }
                } catch (java.io.IOException gone) {
                    // Closed while listed.
                }
            }
        }
        return count;
    }

    public void testWritesAreReadBackAndSurviveATakeoverWithNoLocalTranslog() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final List<String> indices = List.of("alpha", "bravo");
        for (String index : indices) {
            plane.createIndex(new IndexDescriptor(index, "uuid-" + index + "-000000000000".substring(index.length()), 1, MAPPING, null));
        }
        final long translogFilesBefore = openTranslogFiles();
        try (
            ServerlessNode first = new ServerlessNode(nodeSettings("no-translog-first"));
            ServerlessNode second = new ServerlessNode(nodeSettings("no-translog-second"))
        ) {
            first.startup().start();
            second.startup().start();
            first.setMetadataPlane(plane);
            second.setMetadataPlane(plane);
            new BackgroundReconciler(first, plane).setDemandDrivenActivation(true)
                .activateOnDemand(indices.stream().map(i -> Map.entry(i, 0)).toList());
            assertEquals(indices.size(), first.reconciler().openShards().size());
            for (ShardId shardId : first.reconciler().openShards()) {
                for (int i = 0; i < 20; i++) {
                    first.writes().index(shardId, "doc-" + i, "{\"msg\":\"first\",\"n\":" + i + "}");
                }
                // Written again: the realtime read must see the newest, which the engine now gets by refreshing.
                first.writes().index(shardId, "doc-3", "{\"msg\":\"second\",\"n\":33}");
                first.writes().delete(shardId, "doc-4");
                assertEquals(
                    "the writer kept none of its operations in a translog",
                    0,
                    first.reconciler().shard(shardId).translogStats().estimatedNumberOfOperations()
                );
                assertTrue(first.reads().get(shardId, "doc-0").found());
                assertTrue(first.reads().get(shardId, "doc-3").found());
                assertFalse("a deleted document is gone", first.reads().get(shardId, "doc-4").found());
            }
            assertEquals("no translog file is held open", translogFilesBefore, openTranslogFiles());

            // The first node stops; the second takes its shards from the node log.
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
                for (int i = 0; i < 20; i++) {
                    assertEquals("doc-" + i + " in " + shardId, i != 4, second.reads().get(shardId, "doc-" + i).found());
                }
            }
        }
    }
}
