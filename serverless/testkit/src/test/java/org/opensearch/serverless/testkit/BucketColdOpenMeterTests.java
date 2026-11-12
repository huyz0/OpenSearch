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
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.bucket.BucketManifest;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * How long a reader takes to open a small index cold and answer its first search: on its own, as a member of a full
 * bucket with the compact manifest, and with the JSON one. Each request to the store takes a fixed extra time, as a
 * real store's do; a bucket member's open reads the bucket's manifest where an index of its own reads its own.
 *
 * <p>Off unless {@code -Dtests.meter.bucket_open=true}; {@code -Dtests.meter.bucket_open.members} sets the bucket's size
 * (1024) and {@code .delay} the per-request delay in milliseconds (20).
 */
@org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems("*")
public class BucketColdOpenMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SAMPLES = 12;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name, String roles, String codec) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-bucket-open")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", roles)
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.commits", true)
            .put("serverless.segments.node_pack.enabled", true)
            .put("serverless.buckets.enabled", true)
            .put(org.opensearch.serverless.bucket.BucketSettings.MANIFEST_CODEC, codec)
            .build();
    }

    /** Milliseconds per open, and GETs per open, for indices written and published by a writer then opened by a reader. */
    private String measure(String mode, int bucketMembers, String codec, long delayMillis) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        try (
            CountingBlobStore counting = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false));
            LatencyInjectingBlobStore store = new LatencyInjectingBlobStore(counting, 0)
        ) {
            final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
            final String bucketId = "s09-1";
            final boolean bucketed = bucketMembers > 0;
            if (bucketed) {
                // The bucket's other members, shaped as real ones are, so the manifest is as large as a full bucket's.
                BucketManifest padded = BucketManifestCodecMeterTests.bucket(bucketMembers - SAMPLES, 8, 7);
                for (int i = 0; i < SAMPLES; i++) {
                    padded = padded.withMember("uuid-" + i, new BucketManifest.Member("open-" + i, null));
                }
                padded = new BucketManifest(bucketId, 0, padded.members());
                plane.buckets().configure(nodeSettings("x", "ingest", codec));
                plane.buckets().swap(padded, BlobRegister.ABSENT_GENERATION).orElseThrow();
            }
            for (int i = 0; i < SAMPLES; i++) {
                final IndexDescriptor descriptor = new IndexDescriptor("open-" + i, "uuid-" + i, 1, MAPPING, null);
                plane.createIndex(bucketed ? descriptor.withBucket(bucketId) : descriptor);
            }
            try (
                ServerlessNode writer = new ServerlessNode(nodeSettings("open-writer", "ingest", codec));
                ServerlessNode reader = new ServerlessNode(nodeSettings("open-reader", "search", codec))
            ) {
                writer.startup().start();
                writer.setMetadataPlane(plane);
                writer.leases().renewLease(plane);
                plane.membership().refresh();
                final BackgroundReconciler loop = new BackgroundReconciler(writer, plane).setMaxShardsHeld(SAMPLES * 2);
                for (int i = 0; i < SAMPLES; i++) {
                    loop.want("open-" + i, 0);
                }
                for (int pass = 0; pass < 20 && writer.reconciler().openShards().size() < SAMPLES; pass++) {
                    loop.tick(clock.get());
                }
                for (ShardId shardId : new ArrayList<>(writer.reconciler().openShards())) {
                    for (int d = 0; d < 5; d++) {
                        writer.writes().index(shardId, "d" + d, "{\"msg\":\"small open " + d + "\"}");
                    }
                }
                loop.publishAll();

                reader.startup().start();
                reader.setMetadataPlane(plane);
                reader.leases().renewLease(plane);
                store.setDelayMillis(delayMillis);
                final List<Double> millis = new ArrayList<>();
                counting.reset();
                for (int i = 0; i < SAMPLES; i++) {
                    final long start = System.nanoTime();
                    final ShardId onReader = reader.readers().serveAsReader(plane, "open-" + i, 0);
                    assertEquals(5L, ShardOps.hits(reader.searchService(), onReader, "msg", "small"));
                    millis.add((System.nanoTime() - start) / 1e6);
                }
                final double gets = (counting.blobReads() + counting.registerReads()) / (double) SAMPLES;
                store.setDelayMillis(0);
                millis.sort(Double::compare);
                final long manifestBytes = bucketed
                    ? org.opensearch.common.blobstore.ConditionalBlobContainer.of(
                        counting.blobContainer(plane.buckets().bucketPath(bucketId))
                    ).readRegister(org.opensearch.serverless.bucket.BucketStore.MANIFEST).orElseThrow().value().length()
                    : 0;
                return String.format(
                    Locale.ROOT,
                    "|%s: p50 %.0f ms, max %.0f ms, %.1f GET/open, manifest %d KB",
                    mode,
                    millis.get(millis.size() / 2),
                    millis.get(millis.size() - 1),
                    gets,
                    manifestBytes / 1024
                );
            }
        }
    }

    public void testColdOpenOfABucketMember() throws Exception {
        assumeTrue(
            "a meter runs only when -Dtests.meter.bucket_open=true",
            org.opensearch.common.Booleans.parseBoolean(System.getProperty("tests.meter.bucket_open", "false"))
        );
        final int members = Integer.parseInt(System.getProperty("tests.meter.bucket_open.members", "1024"));
        final long delay = Long.parseLong(System.getProperty("tests.meter.bucket_open.delay", "20"));
        final StringBuilder out = new StringBuilder();
        out.append(measure("own index", 0, "auto", delay));
        out.append(measure(members + " members, compact", members, "auto", delay));
        out.append(measure(members + " members, json", members, "json", delay));
        logger.info("BUCKET-OPEN|{} ms per request{}", delay, out);
    }
}
