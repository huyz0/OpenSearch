/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.DescriptorStore;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ReclaimQueue;
import org.opensearch.serverless.metadata.RegisterMap;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Real S3 requests, counted by the S3 client's own metrics, for a compare-and-swap and for deleting an index.
 *
 * <p>A compare-and-swap against a version the container has just read or written is one conditional PUT; it
 * was a GET and a PUT, because the generation lived in the body and had to be read to be compared. And a
 * delete no longer pays for a tombstone, a marker, a quarantine and a claim: where the store honours a
 * conditional delete, the descriptor is removed outright and a reclaim intent finishes the rest.
 *
 * <p>Against MinIO, which does <em>not</em> honour {@code If-Match} on a delete: the conditional path is forced
 * here only to count its requests, which are the same whether or not the store checks the condition. The probe
 * is what keeps a real node on MinIO on tombstones.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class ServerlessDeleteCostTests extends OpenSearchTestCase {

    private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:9000";
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";
    private final List<String> created = new ArrayList<>();

    private String endpoint() {
        return System.getProperty(MinioBlobContainerConformanceTests.ENDPOINT, DEFAULT_ENDPOINT);
    }

    private BlobStore bucket() throws Exception {
        assumeTrue("no S3-compatible endpoint at " + endpoint(), reachable(endpoint()));
        final String name = "delcost-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        created.add(name);
        return org.opensearch.repositories.s3.MinioBlobStores.create(endpoint(), "minioadmin", "minioadmin", name, createTempDir());
    }

    /** A swap on a version this container knows is one PUT; from a container that does not, a GET and a PUT. */
    public void testASwapOnAKnownVersionIsOneRequest() throws Exception {
        final BlobStore store = bucket();
        final BlobPath path = BlobPath.cleanPath().add("cas");
        final BlobContainer container = store.blobContainer(path);
        container.createRegisterIfAbsent("head", value("v0"));
        final long generation = container.readRegister("head").orElseThrow().generation();

        Map<String, Long> before = org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store);
        final long next = container.compareAndSwapRegister("head", generation, value("v1")).currentGeneration();
        final Map<String, Long> warm = delta(before, org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store));

        before = org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store);
        assertTrue(store.blobContainer(path).compareAndSwapRegister("head", next, value("v2")).applied());
        final Map<String, Long> cold = delta(before, org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store));

        logger.info("delete cost: a swap on a known version cost {}; from a fresh container {}", warm, cold);
        assertEquals("a swap on a version the container knows must be one PUT and no GET", Map.of("PutObject", 1L), warm);
        assertEquals("from a container that does not know it, a GET and a PUT", Map.of("GetObject", 1L, "PutObject", 1L), cold);
    }

    /**
     * What deleting an index costs at the moment of the delete and in its deferred work, on each path.
     *
     * <p>Asserted on where the writes go, which is what the design changes. The tombstone path writes a marker
     * and a tombstone at the delete and a claim later; the conditional path writes one reclaim intent and
     * nothing else -- no quarantine, no claim. Its deferred work lists and reads more than the tombstone
     * sweep does, because it re-purges the deleted uuid's bytes and re-checks its heads: the tombstone path
     * never did that, and leaked what a writer that missed the delete published.
     */
    public void testTheRequestsADeleteCostsOnEachPath() throws Exception {
        final Phases tombstoned = deleteCost(false);
        final Phases conditional = deleteCost(true);
        logger.info(
            "delete cost: tombstone path {} at the delete + {} deferred (total {}); conditional path {} at the delete + {} deferred (total {})",
            tombstoned.atDelete(),
            tombstoned.deferred(),
            total(tombstoned.atDelete()) + total(tombstoned.deferred()),
            conditional.atDelete(),
            conditional.deferred(),
            total(conditional.atDelete()) + total(conditional.deferred())
        );
        assertEquals(
            "the tombstone path writes a marker and a tombstone at the delete",
            2L,
            (long) tombstoned.atDelete().getOrDefault("PutObject", 0L)
        );
        assertEquals("and a claim later", 1L, (long) tombstoned.deferred().getOrDefault("PutObject", 0L));
        assertEquals(
            "the conditional path writes one intent at the delete",
            1L,
            (long) conditional.atDelete().getOrDefault("PutObject", 0L)
        );
        assertEquals("and nothing later: no quarantine, no claim", 0L, (long) conditional.deferred().getOrDefault("PutObject", 0L));
    }

    private Phases deleteCost(boolean conditional) throws Exception {
        final BlobStore store = bucket();
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, 30_000L);
        plane.descriptors().setConditionalDelete(conditional);
        plane.createIndex(new IndexDescriptor("doomed", "uuid-doomed", 1, MAPPING, null));
        assertTrue(plane.heads().acquire("doomed", 0, "owner", "owner-eph", "uuid-doomed").acquired());

        Map<String, Long> before = org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store);
        assertTrue(plane.deleteIndex("doomed"));
        final Map<String, Long> atDelete = delta(before, org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store));
        before = org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store);
        if (conditional) {
            clock.addAndGet(ReclaimQueue.DEFAULT_DELAY_MILLIS + 61_000L);
            assertEquals(1, plane.reclaimQueue().drain(clock.get(), bucket -> true, plane::reclaim));
        } else {
            clock.addAndGet(DescriptorStore.DEFAULT_TOMBSTONE_QUARANTINE_MILLIS + DescriptorStore.TOMBSTONE_BUCKET_MILLIS);
            assertEquals(
                List.of("doomed"),
                plane.descriptors().sweepTombstones(clock.get(), DescriptorStore.DEFAULT_TOMBSTONE_QUARANTINE_MILLIS)
            );
        }
        final Map<String, Long> deferred = delta(before, org.opensearch.repositories.s3.MinioBlobStores.requestCounts(store));
        assertTrue(
            "nothing left under the name",
            store.blobContainer(RegisterMap.indices(BlobPath.cleanPath())).readRegister("doomed").isEmpty()
        );
        return new Phases(atDelete, deferred);
    }

    /** Requests at the delete, and in the work it defers. */
    private record Phases(Map<String, Long> atDelete, Map<String, Long> deferred) {
    }

    private static Map<String, Long> delta(Map<String, Long> before, Map<String, Long> after) {
        final Map<String, Long> delta = new HashMap<>();
        for (Map.Entry<String, Long> entry : after.entrySet()) {
            final long d = entry.getValue() - before.getOrDefault(entry.getKey(), 0L);
            if (d != 0L) {
                delta.put(entry.getKey(), d);
            }
        }
        return delta;
    }

    private static long total(Map<String, Long> counts) {
        return counts.values().stream().mapToLong(Long::longValue).sum();
    }

    private static BytesArray value(String text) {
        return new BytesArray(text.getBytes(StandardCharsets.UTF_8));
    }

    @org.junit.After
    public void removeBucketsThisTestMade() throws Exception {
        for (String bucket : created) {
            org.opensearch.repositories.s3.MinioBlobStores.deleteBucket(endpoint(), "minioadmin", "minioadmin", bucket, createTempDir());
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
