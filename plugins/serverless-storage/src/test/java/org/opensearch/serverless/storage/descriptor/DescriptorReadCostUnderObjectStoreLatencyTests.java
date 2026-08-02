/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.benchmark.LatencyInjectingBlobContainer;
import org.opensearch.serverless.storage.benchmark.LatencyProfile;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * What resolving a gated index costs when the object store behaves like an object store.
 *
 * <h2>The gap this closes</h2>
 *
 * Every descriptor measurement on this branch was taken against {@code FsBlobContainer}, where a read is
 * microseconds. The design's whole argument is that a cache hides object-store latency, and the hit rate had
 * been measured while the latency it is hiding never had been. So "the cache makes this affordable" was an
 * assumption with a number attached to the wrong half.
 *
 * <p>{@link LatencyProfile#TYPICAL} is the branch's own stated approximation of S3: GET 20-40 ms, PUT 40-80,
 * LIST 50-100. Simulated rather than real, and that is the honest limit of this test. It gets the order of
 * magnitude and the shape right, which is what the design question needs, and it costs no credentials.
 *
 * <h2>What it found</h2>
 *
 * {@link BlobDescriptorBackend} had no cache. {@code DescriptorCache} was extracted by T7 so "the blob
 * backend reuses it rather than growing a second copy", and the blob backend never took it, so every point
 * read went to the store. Switching the descriptor backend to the object store, which is the entire point of
 * the design, turned each of the eleven synchronous resolution sites into a network round trip.
 *
 * <p>Invisible on a filesystem. At 20-40 ms per GET it is the difference between the design working and not.
 */
public class DescriptorReadCostUnderObjectStoreLatencyTests extends OpenSearchTestCase {

    /** Reads per tenant, enough that the difference is latency rather than noise. */
    private static final int READS = 12;

    private BlobDescriptorBackend backendUnderLatency() throws Exception {
        BlobContainer real = new FsBlobStore(1024, createTempDir(), false).blobContainer(BlobPath.cleanPath());
        return new BlobDescriptorBackend(new LatencyInjectingBlobContainer(real, LatencyProfile.TYPICAL));
    }

    private static IndexDescriptor descriptor(String name) {
        return IndexDescriptor.from(
            org.opensearch.cluster.metadata.IndexMetadata.builder(name)
                .settings(
                    org.opensearch.common.settings.Settings.builder()
                        .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                        .build()
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build()
        );
    }

    /**
     * A hot tenant resolved repeatedly must cost one round trip, not one per resolution.
     *
     * <p>Asserted as a ratio rather than a millisecond figure, because a wall-clock threshold on a shared
     * build machine is a flake generator. The ratio is the property: whatever the machine, twelve cached
     * reads must not cost twelve uncached ones.
     */
    public void testAHotTenantCostsOneRoundTripNotTwelve() throws Exception {
        BlobDescriptorBackend backend = backendUnderLatency();
        backend.create(descriptor("tenant-hot"));

        long firstNanos = System.nanoTime();
        assertNotNull(backend.get("tenant-hot"));
        firstNanos = System.nanoTime() - firstNanos;

        long repeatNanos = System.nanoTime();
        for (int i = 0; i < READS; i++) {
            assertNotNull(backend.get("tenant-hot"));
        }
        repeatNanos = System.nanoTime() - repeatNanos;

        long firstMillis = TimeUnit.NANOSECONDS.toMillis(firstNanos);
        long repeatMillis = TimeUnit.NANOSECONDS.toMillis(repeatNanos);
        logger.info(
            "G6 descriptor read under {}: first (cold) {} ms, {} repeats {} ms total",
            "LatencyProfile.TYPICAL",
            firstMillis,
            READS,
            repeatMillis
        );

        assertTrue(
            "the first read must actually pay object-store latency, or the profile is not being applied "
                + "and this test proves nothing; measured " + firstMillis + " ms",
            firstMillis >= 15
        );
        assertTrue(
            READS + " cached reads cost " + repeatMillis + " ms against " + firstMillis + " ms for one "
                + "uncached read, so the cache is not being consulted",
            repeatMillis < firstMillis
        );
    }

    /**
     * A miss costs two round trips, because absence is only absence once the tombstone prefix agrees.
     *
     * <p>Recorded rather than optimised. It is the correct behaviour: "deleted" and "never existed" have
     * opposite safe responses, and the second read is what tells them apart. Worth a number because it is
     * the most expensive descriptor operation and nothing had costed it.
     */
    public void testAMissCostsTwoRoundTrips() throws Exception {
        BlobDescriptorBackend backend = backendUnderLatency();

        long start = System.nanoTime();
        assertNull(backend.get("tenant-that-never-existed"));
        long missMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        logger.info("G6 descriptor miss under LatencyProfile.TYPICAL: {} ms", missMillis);
        assertTrue(
            "a miss reads the live prefix and then the tombstone prefix, so it should cost about two GETs; "
                + "measured " + missMillis + " ms",
            missMillis >= 30
        );
    }

    /**
     * A miss is never cached, so a tenant created moments later resolves at once.
     *
     * <p>This is the property that lets the cache exist at all: caching absence would make a just-created
     * index unresolvable for a freshness window, which H18 refused.
     */
    public void testAMissIsNotCachedSoACreationIsVisibleImmediately() throws Exception {
        BlobDescriptorBackend backend = backendUnderLatency();
        assertNull(backend.get("tenant-later"));

        backend.create(descriptor("tenant-later"));

        assertNotNull("a name created after a miss must resolve without waiting out a cache window", backend.get("tenant-later"));
    }

    /** A deleted tenant must stop resolving at once, not after the freshness window. */
    public void testDeletionInvalidatesTheCachedDescriptor() throws Exception {
        BlobDescriptorBackend backend = backendUnderLatency();
        backend.create(descriptor("tenant-doomed"));
        assertNotNull(backend.get("tenant-doomed"));

        backend.putTombstoneAsync(descriptor("tenant-doomed").tombstoned());

        IndexDescriptor after = backend.get("tenant-doomed");
        assertTrue(
            "a deleted index that still resolves as live would accept a write against a shard the cluster "
                + "no longer believes in",
            after == null || after.exists() == false
        );
    }

    /** Distinct tenants each pay their own read, so the cache is not accidentally answering for everyone. */
    public void testColdTenantsEachPayTheirOwnReadCost() throws Exception {
        BlobDescriptorBackend backend = backendUnderLatency();
        List<String> names = List.of("tenant-a", "tenant-b", "tenant-c", "tenant-d");
        for (String name : names) {
            backend.create(descriptor(name));
        }

        long start = System.nanoTime();
        for (String name : names) {
            assertNotNull(backend.get(name));
        }
        long coldMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        long warmStart = System.nanoTime();
        for (String name : names) {
            assertNotNull(backend.get(name));
        }
        long warmMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - warmStart);

        logger.info("G6 four distinct tenants under LatencyProfile.TYPICAL: cold {} ms, warm {} ms", coldMillis, warmMillis);
        assertTrue("four cold tenants must each pay a read; measured " + coldMillis + " ms", coldMillis >= 60);
        assertTrue("and warm ones must not; measured " + warmMillis + " ms", warmMillis < coldMillis);
    }
}
