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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A writer keeps only the commit it last published.
 *
 * <p>The engine's deletion policy keeps every commit from the newest one at or below the global checkpoint, and reads
 * that commit back on every new one. The shell never moved the global checkpoint, so the commit a writer opened at
 * stayed "safe" for as long as it held the shard: every later commit was kept, and so was the opening commit's file
 * list -- files inherited from the previous term, read lazily from the store. Once the newest manifest stopped naming
 * them the collector deleted them, as it should, and the writer's next commit read a deleted file and failed its
 * engine. A fleet run lost the use of a shard that way, its acknowledged writes stranded in the log.
 *
 * <p><b>D5:</b> {@code FsBlobContainer} only.
 */
public class ServerlessCommitRetentionTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-commit-retention")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .build();
    }

    /**
     * The fleet's failure, made to happen: a writer that took over a shard, merged away the files it inherited and
     * published, has those files collected -- and must go on committing.
     */
    public void testAWriterSurvivesTheCollectionOfFilesItInherited() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final org.opensearch.common.blobstore.BlobStore store = new FsBlobStore(1024, createTempDir(), false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        // The first writer publishes at term 1, and lets go.
        try (ServerlessNode first = new ServerlessNode(nodeSettings("inherit-first", "ingest"))) {
            first.start();
            first.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(first, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId onFirst = first.reconciler().openShards().iterator().next();
            for (int i = 0; i < 10; i++) {
                first.index(onFirst, "a" + i, "{\"msg\":\"first\",\"n\":" + i + "}");
            }
            loop.publishAll();
            loop.stopWanting("alpha", 0);
            loop.setIdleAfterMillis(1);
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
        }

        // One block of cache, so a file read twice is fetched twice: the fleet's node had evicted the inherited files'
        // blocks long before its deletion policy read them back.
        final Settings uncached = Settings.builder()
            .put(nodeSettings("inherit-second", "ingest"))
            .put("serverless.block_cache.max_blocks", 1)
            .build();
        try (ServerlessNode second = new ServerlessNode(uncached)) {
            second.start();
            second.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(second, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = second.reconciler().openShards().iterator().next();
            final IndexShard shard = second.reconciler().shard(shardId);
            assertTrue("the fixture needs a takeover", shard.getOperationPrimaryTerm() > 1);

            // Writes, then one segment, so the commit it publishes names none of the files it inherited.
            for (int i = 0; i < 5; i++) {
                second.index(shardId, "b" + i, "{\"msg\":\"second\",\"n\":" + i + "}");
            }
            shard.forceMerge(new org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest().maxNumSegments(1).flush(true));
            loop.publishAll();
            final var manifest = plane.segmentPublisher("alpha", 0).readManifest().orElseThrow();
            assertFalse(
                "the fixture needs a commit that no longer names the first term's files: " + manifest.files(),
                manifest.files().containsValue("t=1")
            );

            // The collector does what it should: the first term's files are named by nothing published.
            final var collected = new org.opensearch.serverless.reconcile.GarbageCollector(store, BlobPath.cleanPath())
                .setMinimumUnreferencedMillis(0L)
                .collectShard(plane, "alpha", 0);
            assertFalse("the fixture needs the first term's files collected", collected.isEmpty());
            // The writer keeps only the commit it published. The one its recovery made at the takeover -- which names
            // the files it inherited, the ones just collected -- is gone, so its deletion policy cannot read it back.
            final java.util.List<String> localCommits = new java.util.ArrayList<>();
            try (var files = java.nio.file.Files.list(shard.shardPath().resolveIndex())) {
                files.map(f -> f.getFileName().toString()).filter(n -> n.startsWith("segments_")).forEach(localCommits::add);
            }
            assertEquals("one commit kept on disk, the published one: " + localCommits, 1, localCommits.size());

            // And the writer goes on: it writes, commits and publishes.
            for (int i = 5; i < 10; i++) {
                second.index(shardId, "b" + i, "{\"msg\":\"second\",\"n\":" + i + "}");
            }
            loop.publishAll();
            assertTrue(
                "its engine must not have failed",
                second.reconciler().shard(shardId).state() == org.opensearch.index.shard.IndexShardState.STARTED
            );
            assertTrue(second.get(shardId, "b9").found());
            assertTrue(second.get(shardId, "a3").found());
        }
    }

    /** After several publishes a writer holds one commit, and its safe commit is the one it published last. */
    public void testAWriterKeepsOnlyItsLastPublishedCommit() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", 1, MAPPING, null));

        try (ServerlessNode node = new ServerlessNode(nodeSettings("retention", "ingest"))) {
            node.start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("alpha", 0);
            loop.tick(clock.get());
            final ShardId shardId = node.reconciler().openShards().iterator().next();
            for (int round = 0; round < 4; round++) {
                for (int i = 0; i < 5; i++) {
                    node.index(shardId, "d" + round + "-" + i, "{\"msg\":\"m\",\"n\":" + i + "}");
                }
                loop.publishAll();
            }
            final IndexShard shard = node.reconciler().shard(shardId);
            assertEquals(
                "the global checkpoint of a copy with no replicas is its local checkpoint",
                shard.getLocalCheckpoint(),
                shard.getLastKnownGlobalCheckpoint()
            );
            assertEquals(
                "one commit kept, the published one -- not every commit since the shard opened",
                1,
                org.apache.lucene.index.DirectoryReader.listCommits(shard.store().directory()).size()
            );
        }
    }
}
