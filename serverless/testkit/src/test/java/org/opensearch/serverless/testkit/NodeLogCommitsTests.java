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
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Commits recorded with the node's write log: a routine publish keeps its commit on the writer instead of writing the
 * shard's manifest, readers ask the writer for it, a checkpoint writes it, and a crash loses none of it.
 */
public class NodeLogCommitsTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 3;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, String roles, boolean packs) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-node-log-commits")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 20)
            .put("serverless.wal.node_log.commits", true)
            .put("serverless.wal.node_log.commits.checkpoint_millis", 2_000L)
            .put("serverless.publish.min_interval_millis", 0L)
            .put("serverless.segments.node_pack.enabled", packs)
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

    private static void writeRound(ServerlessNode node, String tag, int perShard) throws Exception {
        for (ShardId shardId : node.reconciler().openShards()) {
            for (int i = 0; i < perShard; i++) {
                node.writes().index(shardId, tag + "-" + shardId.id() + "-" + i, "{\"msg\":\"staged\",\"n\":" + i + "}");
            }
        }
    }

    private static CommitManifest register(MetadataPlane plane, ShardId shardId) throws Exception {
        return plane.segmentPublisher(shardId.getIndexName(), shardId.getIndex().getUUID(), shardId.id()).readManifest().orElseThrow();
    }

    private void readersSeeStagedCommits(boolean packs) throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
            try (
                ServerlessNode writer = new ServerlessNode(nodeSettings("commits-writer", "ingest", packs));
                ServerlessNode reader = new ServerlessNode(nodeSettings("commits-reader", "search", packs))
            ) {
                writer.startup().start();
                writer.setMetadataPlane(plane);
                reader.startup().start();
                reader.setMetadataPlane(plane);
                reader.leases().renewLease(plane);
                final BackgroundReconciler loop = takeAll(writer, plane, clock);
                writeRound(writer, "first", 5);
                assertEquals(SHARDS, loop.publishAll().size());
                final List<ShardId> held = new ArrayList<>(writer.reconciler().openShards());
                for (ShardId shardId : held) {
                    assertTrue(
                        "the first publish of a term writes the manifest, saying staged commits follow",
                        register(plane, shardId).stageSequence() >= 0L
                    );
                }

                store.reset();
                for (int round = 0; round < 3; round++) {
                    writeRound(writer, "round" + round, 5);
                    assertEquals(SHARDS, loop.publishAll().size());
                }
                assertEquals("routine publishes stage their commits rather than write the manifest", 0L, store.registerWrites());
                for (ShardId shardId : held) {
                    assertTrue(writer.reconciler().newestCommit(shardId).stagedAfter(register(plane, shardId)));
                }

                // A reader asks the writer, so it serves the newest commit, not the register's.
                for (int shard = 0; shard < SHARDS; shard++) {
                    final ShardId onReader = reader.readers().serveAsReader(plane, "alpha", shard);
                    assertEquals(20L, ShardOps.hits(reader.searchService(), onReader, "msg", "staged"));
                }

                // Once the oldest staged commit has waited its time, the renewal pass writes it.
                assertBusy(() -> assertEquals(SHARDS, loop.checkpointDue().size()));
                for (ShardId shardId : held) {
                    final CommitManifest written = register(plane, shardId);
                    assertEquals(writer.reconciler().newestCommit(shardId).files(), written.files());
                    assertFalse(writer.reconciler().newestCommit(shardId).stagedAfter(written));
                }
                assertEquals("nothing left to write", 0, loop.checkpointDue().size());
            }
        }
    }

    public void testReadersSeeStagedCommitsWithSegmentBlobs() throws Exception {
        readersSeeStagedCommits(false);
    }

    public void testReadersSeeStagedCommitsWithNodePacks() throws Exception {
        readersSeeStagedCommits(true);
    }

    public void testACrashedWritersStagedCommitsAreReplayedFromItsLog() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        final ServerlessNode first = new ServerlessNode(nodeSettings("commits-first", "ingest", true));
        try {
            first.startup().start();
            first.setMetadataPlane(plane);
            final BackgroundReconciler loop = takeAll(first, plane, clock);
            writeRound(first, "checkpointed", 4);
            loop.publishAll();
            writeRound(first, "staged", 4);
            loop.publishAll();
            for (ShardId shardId : first.reconciler().openShards()) {
                assertTrue("staged, not in the manifest", first.reconciler().newestCommit(shardId).stagedAfter(register(plane, shardId)));
            }

            // The writer never lets go; its lease runs out. Its successor restores the manifest and replays its log.
            clock.addAndGet(2 * TTL);
            try (ServerlessNode second = new ServerlessNode(nodeSettings("commits-second", "ingest", true))) {
                second.startup().start();
                second.setMetadataPlane(plane);
                takeAll(second, plane, clock);
                for (ShardId shardId : second.reconciler().openShards()) {
                    for (int i = 0; i < 4; i++) {
                        assertTrue(second.reads().get(shardId, "checkpointed-" + shardId.id() + "-" + i).found());
                        assertTrue(
                            "a staged commit's writes survive",
                            second.reads().get(shardId, "staged-" + shardId.id() + "-" + i).found()
                        );
                    }
                }
            }
        } finally {
            try {
                first.close();
            } catch (Exception e) {
                // It lost every shard it held.
            }
        }
    }

    public void testATakeoverOpensFromTheNewestLoggedCommit() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        final ServerlessNode first = new ServerlessNode(nodeSettings("logged-first", "ingest", true));
        try {
            first.startup().start();
            first.setMetadataPlane(plane);
            final BackgroundReconciler loop = takeAll(first, plane, clock);
            writeRound(first, "checkpointed", 3);
            loop.publishAll();
            writeRound(first, "staged", 3);
            loop.publishAll();
            // Writes after the staged commit carry it to the log, in the blob that holds them.
            writeRound(first, "after", 2);
            final java.util.Map<Integer, CommitManifest> staged = new java.util.HashMap<>();
            for (ShardId shardId : first.reconciler().openShards()) {
                final CommitManifest newest = first.reconciler().newestCommit(shardId);
                assertTrue(newest.stagedAfter(register(plane, shardId)));
                staged.put(shardId.id(), newest);
            }

            clock.addAndGet(2 * TTL);
            try (ServerlessNode second = new ServerlessNode(nodeSettings("logged-second", "ingest", true))) {
                second.startup().start();
                second.setMetadataPlane(plane);
                takeAll(second, plane, clock);
                assertEquals("every shard opened from its logged commit", SHARDS, second.reconciler().loggedCommitOpens());
                for (ShardId shardId : second.reconciler().openShards()) {
                    for (String tag : new String[] { "checkpointed", "staged" }) {
                        for (int i = 0; i < 3; i++) {
                            assertTrue(second.reads().get(shardId, tag + "-" + shardId.id() + "-" + i).found());
                        }
                    }
                    for (int i = 0; i < 2; i++) {
                        assertTrue(
                            "replayed past the logged commit",
                            second.reads().get(shardId, "after-" + shardId.id() + "-" + i).found()
                        );
                    }
                    final CommitManifest now = register(plane, shardId);
                    assertTrue(
                        "the register moved past the checkpoint before the predecessor's log was let go",
                        now.term() > staged.get(shardId.id()).term() || now.files().equals(staged.get(shardId.id()).files())
                    );
                }
            }
        } finally {
            try {
                first.close();
            } catch (Exception e) {
                // It lost every shard it held.
            }
        }
    }

    public void testAReleaseWritesTheStagedCommit() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("commits-release", "ingest", false))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            final BackgroundReconciler loop = takeAll(node, plane, clock);
            writeRound(node, "a", 3);
            loop.publishAll();
            writeRound(node, "b", 3);
            loop.publishAll();
            final List<ShardId> held = new ArrayList<>(node.reconciler().openShards());
            final List<CommitManifest> newest = new ArrayList<>();
            for (ShardId shardId : held) {
                newest.add(node.reconciler().newestCommit(shardId));
            }
            loop.setIdleAfterMillis(1);
            for (int shard = 0; shard < SHARDS; shard++) {
                loop.stopWanting("alpha", shard);
            }
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
            assertTrue(node.reconciler().openShards().isEmpty());
            for (int i = 0; i < held.size(); i++) {
                assertEquals("released with its staged commit written", newest.get(i).files(), register(plane, held.get(i)).files());
            }
        }
    }
}
