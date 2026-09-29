/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessBootstrap;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Background metadata cost, measured against a real S3 API at deployment sizes an order of magnitude apart.
 *
 * <p>The claim this measures: a node's background work -- lease renewal, head verification, descriptor
 * refresh, the tombstone sweep, membership -- costs the same whatever the number of indices in the
 * deployment. One node holds a handful of shards; around them sit {@code P} idle indices it never touches and
 * a batch of indices deleted long enough ago that their tombstones are due. The node then runs 120 backstop
 * passes with the production intervals, which at the 30-second backstop is an hour of background and
 * includes one tombstone sweep, and every request is counted. The same run at each population must cost the
 * same. Before the sweep was rebuilt it read every descriptor, so its share alone grew by exactly {@code P}.
 *
 * <p>Populations come from {@code tests.serverless.scale.populations} (default {@code 1000,10000}); the
 * figures in {@code STATUS.md} were taken at {@code 10000,100000}. Skipped without an endpoint.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ServerlessScaleMeasurementTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";
    private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:9000";
    private static final int HOT_INDICES = 4;
    private static final int DELETED_INDICES = 50;
    private static final int PASSES = 120;

    private final List<String> created = new ArrayList<>();

    private String endpoint() {
        return System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT, DEFAULT_ENDPOINT);
    }

    /** Background cost at each population, and the assertion that it does not grow with it. */
    public void testBackgroundCostIsFlatInIndexCount() throws Exception {
        assumeTrue("no S3-compatible endpoint at " + endpoint(), reachable(endpoint()));
        final String[] populations = System.getProperty("tests.serverless.scale.populations", "1000,10000").split(",");
        final List<long[]> results = new ArrayList<>();
        for (String population : populations) {
            final int p = Integer.parseInt(population.trim());
            final long[] measured = measure(p);
            results.add(measured);
            logger.info(
                "scale: population {}: {} requests over {} passes ({} register reads, {} register writes, {} listings, {} blob reads,"
                    + " {} blob writes, {} deletes); the sweep removed {} tombstones; creating the population took {}s",
                p,
                measured[0],
                PASSES,
                measured[1],
                measured[2],
                measured[3],
                measured[4],
                measured[5],
                measured[6],
                measured[7],
                measured[8]
            );
        }
        final long smallest = results.get(0)[0];
        for (int i = 1; i < results.size(); i++) {
            final long larger = results.get(i)[0];
            assertTrue(
                "background requests grew with the population: "
                    + smallest
                    + " at "
                    + populations[0]
                    + ", "
                    + larger
                    + " at "
                    + populations[i],
                larger <= smallest + smallest / 20 + 10
            );
        }
    }

    /** One node's background requests over {@link #PASSES} passes, with {@code population} idle indices beside it. */
    private long[] measure(int population) throws Exception {
        final String bucket = "scale-" + population + "-" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        final BlobStore s3 = org.opensearch.repositories.s3.MinioBlobStores.create(
            endpoint(),
            System.getProperty("tests.serverless.s3.access_key", "minioadmin"),
            System.getProperty("tests.serverless.s3.secret_key", "minioadmin"),
            bucket,
            createTempDir()
        );
        created.add(bucket);
        final CountingBlobStore store = new CountingBlobStore(s3);
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);

        final long startedAt = System.nanoTime();
        final ExecutorService creators = Executors.newFixedThreadPool(32);
        try {
            final List<Future<?>> pending = new ArrayList<>();
            final int perTask = 500;
            for (int from = 0; from < population; from += perTask) {
                final int start = from;
                pending.add(creators.submit(() -> {
                    for (int i = start; i < Math.min(population, start + perTask); i++) {
                        plane.createIndex(
                            new IndexDescriptor(String.format(Locale.ROOT, "idle-%07d", i), "uuid-idle-" + i, 1, MAPPING, null)
                        );
                    }
                    return null;
                }));
            }
            for (Future<?> f : pending) {
                f.get();
            }
        } finally {
            creators.shutdown();
            assertTrue(creators.awaitTermination(1, TimeUnit.MINUTES));
        }
        final long creationSeconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startedAt);
        for (int i = 0; i < DELETED_INDICES; i++) {
            plane.createIndex(new IndexDescriptor("gone-" + i, "uuid-gone-" + i, 1, MAPPING, null));
            assertTrue(plane.deleteIndex("gone-" + i));
        }
        for (int i = 0; i < HOT_INDICES; i++) {
            plane.createIndex(new IndexDescriptor("hot-" + i, "uuid-hot-" + i, 2, MAPPING, null));
        }
        // The deletions are now old enough for the sweep to take their tombstones.
        clock.addAndGet(DescriptorStore.DEFAULT_TOMBSTONE_QUARANTINE_MILLIS + DescriptorStore.TOMBSTONE_BUCKET_MILLIS);

        try (ServerlessNode node = new ServerlessNode(nodeSettings("scale-" + population))) {
            node.start();
            node.setMetadataPlane(plane);
            node.setHeadVerifyIntervalMillis(ServerlessBootstrap.DEFAULT_HEAD_VERIFY_INTERVAL_MILLIS);
            node.setDescriptorRefreshIntervalMillis(ServerlessBootstrap.DEFAULT_DESCRIPTOR_REFRESH_INTERVAL_MILLIS);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (int i = 0; i < HOT_INDICES; i++) {
                loop.want("hot-" + i, 0);
                loop.want("hot-" + i, 1);
            }
            // Activation, which is not background cost.
            loop.tick(clock.get());
            assertEquals(HOT_INDICES * 2, node.reconciler().openShards().size());

            store.reset();
            for (int pass = 0; pass < PASSES; pass++) {
                // The clock jumps ten seconds a pass here, which a sweep of fifty names never takes in real
                // time; letting it jump mid-sweep would age its claims past their deadline and it would
                // rightly decline to delete. So a pass waits for a sweep in flight, as real time would.
                loop.tombstoneSweep().get(5, TimeUnit.MINUTES);
                // A renewal interval apart, so the lease never lapses: this is a healthy node's hour.
                clock.addAndGet(TTL / 3);
                loop.tick(clock.get());
            }
            // The sweep runs beside the passes; its requests are background cost too, so it is waited for.
            loop.tombstoneSweep().get(5, TimeUnit.MINUTES);
            // Counters first: the check of what the sweep removed is itself a listing.
            final long[] measured = new long[] {
                store.total(),
                store.registerReads(),
                store.registerWrites(),
                store.listings(),
                store.blobReads(),
                store.blobWrites(),
                store.deletes(),
                0L,
                creationSeconds };
            measured[7] = DELETED_INDICES - countDescriptorsUnder(store, "gone-");
            assertEquals("the one sweep in the window must have removed every expired tombstone", DELETED_INDICES, measured[7]);
            return measured;
        }
    }

    /** How many descriptor blobs, tombstoned or not, remain under a prefix -- a swept tombstone is gone. */
    private static long countDescriptorsUnder(CountingBlobStore store, String prefix) throws Exception {
        return store.blobContainer(org.opensearch.serverless.metadata.RegisterMap.indices(BlobPath.cleanPath()))
            .listBlobsByPrefix(prefix)
            .size();
    }

    private Settings nodeSettings(String name) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-scale")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
    }

    /** Removes the buckets this test made; an unreachable endpoint is not a failure. */
    @org.junit.After
    public void removeBucketsThisTestMade() throws Exception {
        for (String bucket : created) {
            org.opensearch.repositories.s3.MinioBlobStores.deleteBucket(
                endpoint(),
                System.getProperty("tests.serverless.s3.access_key", "minioadmin"),
                System.getProperty("tests.serverless.s3.secret_key", "minioadmin"),
                bucket,
                createTempDir()
            );
        }
        created.clear();
    }

    private static boolean reachable(String endpoint) {
        try (HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()) {
            final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint + "/minio/health/live"))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            return client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() < 500;
        } catch (Exception e) {
            return false;
        }
    }
}
