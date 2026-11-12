/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.bucket.BucketManifest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.SegmentPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bucket's members publish, stage and checkpoint as shards of their own do, with every commit in the bucket's one
 * manifest: their due commits land in one swap, nothing is written to the members' own registers, and a reader, and the
 * writer after a release, open each member at the commit its entry holds.
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
// On a live endpoint (MeterStores) the S3 client keeps its threads past the test; they are the client's, not leaks.
@com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters(filters = {
    org.opensearch.test.BouncyCastleThreadFilter.class,
    S3ClientThreads.class })
public class BucketCheckpointTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long IDLE_AFTER = 300_000L;
    private static final String BUCKET = "s07-1";
    private static final int MEMBERS = 3;

    private Settings nodeSettings(String name, String roles) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-bucket-checkpoint")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.commits", true)
            .put("serverless.segments.node_pack.enabled", true)
            .put("serverless.buckets.enabled", true)
            .put(org.opensearch.serverless.shell.ServerlessBootstrap.NODE_LOG_PUBLISH_MAX_AGE, 0L)
            .put(org.opensearch.serverless.shell.ServerlessBootstrap.PUBLISH_MIN_INTERVAL, 0L)
            .put(
                org.opensearch.serverless.shell.LogCommitCheckpoint.SETTING,
                org.opensearch.serverless.store.CommitStaging.MIN_STAGED_MILLIS
            )
            .build();
    }

    private static String name(int i) {
        return "member-" + i;
    }

    public void testMembersCheckpointInOneSwapAndOpenFromTheirEntries() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        try (CountingBlobStore store = new CountingBlobStore(MeterStores.create(createTempDir(), createTempDir()))) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            BucketManifest bucket = BucketManifest.empty(BUCKET);
            for (int i = 0; i < MEMBERS; i++) {
                bucket = bucket.withMember("uuid-" + i, new BucketManifest.Member(name(i), null));
            }
            plane.buckets().swap(bucket, BlobRegister.ABSENT_GENERATION).orElseThrow();
            for (int i = 0; i < MEMBERS; i++) {
                plane.createIndex(
                    new IndexDescriptor(name(i), "uuid-" + i, 1, "{\"properties\":{\"msg\":{\"type\":\"text\"}}}", null).withBucket(BUCKET)
                );
            }
            try (
                ServerlessNode writer = new ServerlessNode(nodeSettings("bucket-writer", "ingest"));
                ServerlessNode reader = new ServerlessNode(nodeSettings("bucket-reader", "search"))
            ) {
                writer.startup().start();
                writer.setMetadataPlane(plane);
                writer.leases().renewLease(plane);
                plane.membership().refresh();
                final BackgroundReconciler loop = new BackgroundReconciler(writer, plane).setIdleAfterMillis(IDLE_AFTER);
                for (int i = 0; i < MEMBERS; i++) {
                    loop.want(name(i), 0);
                }
                for (int pass = 0; pass < 20 && writer.reconciler().openShards().size() < MEMBERS; pass++) {
                    loop.tick(clock.get());
                }
                final List<ShardId> held = new ArrayList<>(writer.reconciler().openShards());
                assertEquals(MEMBERS, held.size());

                // The first publish has no commit to stage over, so each member writes its entry once.
                for (ShardId shardId : held) {
                    writer.writes().index(shardId, "first", "{\"msg\":\"first\"}");
                }
                loop.publishAll();
                // Then routine publishes stage, and the checkpoint writes every staged member at once.
                for (ShardId shardId : held) {
                    writer.writes().index(shardId, "second", "{\"msg\":\"second\"}");
                }
                loop.publishAll();
                final long stagedAt = System.nanoTime();
                assertBusy(() -> assertTrue(System.nanoTime() - stagedAt > 1_100_000_000L));
                store.reset();
                store.traceCallers(4);
                assertEquals(MEMBERS, loop.checkpointDue().size());
                assertEquals("every member's checkpoint in one swap: " + store.byCaller(), 1L, store.registerWritesMatching("manifest"));
                assertEquals(store.byCaller(), 1L, store.registerWrites());

                final BucketManifest written = plane.buckets().read(BUCKET).orElseThrow().manifest();
                for (int i = 0; i < MEMBERS; i++) {
                    final BucketManifest.Member member = written.member("uuid-" + i).orElseThrow();
                    assertEquals("the first publish and the checkpoint", 2L, member.version());
                    final var own = org.opensearch.common.blobstore.ConditionalBlobContainer.of(
                        store.blobContainer(RegisterMap.shardData(plane.basePath(), name(i), "uuid-" + i, 0))
                    ).readRegister(SegmentPublisher.MANIFEST);
                    assertTrue("a member's own register is never written", own.isEmpty());
                    assertEquals(
                        member.commit().files(),
                        plane.segmentPublisher(name(i), "uuid-" + i, 0).readManifest().orElseThrow().files()
                    );
                }

                reader.startup().start();
                reader.setMetadataPlane(plane);
                reader.leases().renewLease(plane);
                for (int i = 0; i < MEMBERS; i++) {
                    final ShardId onReader = reader.readers().serveAsReader(plane, name(i), 0);
                    assertEquals(
                        "a reader opens the member's entry",
                        2L,
                        ShardOps.hits(reader.searchService(), onReader, "msg", "first second")
                    );
                }

                for (int i = 0; i < MEMBERS; i++) {
                    loop.stopWanting(name(i), 0);
                }
                clock.addAndGet(IDLE_AFTER + 1);
                loop.tick(clock.get());
                assertEquals("released when idle", 0, writer.reconciler().openShards().size());
                for (int i = 0; i < MEMBERS; i++) {
                    loop.want(name(i), 0);
                }
                for (int pass = 0; pass < 20 && writer.reconciler().openShards().size() < MEMBERS; pass++) {
                    loop.tick(clock.get());
                }
                for (ShardId shardId : writer.reconciler().openShards()) {
                    assertEquals(
                        "the writer reopens at its entry",
                        2L,
                        ShardOps.hits(writer.searchService(), shardId, "msg", "first second")
                    );
                }
            }
        }
    }
}
