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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.storage.security.ObjectStoreRequestCounter;
import org.opensearch.serverless.storage.security.RequestCountingBlobContainer;
import org.opensearch.test.OpenSearchTestCase;

/**
 * How many object-store round trips a descriptor read costs.
 *
 * <h2>Counted rather than timed, and that is the point</h2>
 *
 * The first version of this measured milliseconds under a simulated S3 latency profile. It found a real
 * defect, that {@link BlobDescriptorBackend} had no cache at all, and it was still the wrong shape of test:
 * a wall-clock assertion on shared hardware measures the machine as much as the code. Every flaky failure
 * this branch spent time triaging came from tests of that shape.
 *
 * <p>Round trips are the property that actually matters and the one the code controls. A read that goes to
 * the store costs a round trip whether the store answers in ten microseconds or forty milliseconds, and the
 * count is identical on a loaded build agent and an idle laptop. The latency figures those earlier runs
 * produced are recorded in the design document; they did their job and do not need re-deriving on every
 * build.
 */
public class DescriptorReadRoundTripTests extends OpenSearchTestCase {

    private ObjectStoreRequestCounter counter;

    private BlobDescriptorBackend countingBackend() throws Exception {
        counter = new ObjectStoreRequestCounter();
        return new BlobDescriptorBackend(
            new RequestCountingBlobContainer(new FsBlobStore(1024, createTempDir(), false).blobContainer(BlobPath.cleanPath()), counter)
        );
    }

    private static IndexDescriptor descriptor(String name) {
        return IndexDescriptor.from(
            IndexMetadata.builder(name)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                        .build()
                )
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build()
        );
    }

    /**
     * A hot tenant costs one read however many times it is resolved.
     *
     * <p>The eleven synchronous resolution sites mean a single request can resolve one name several times.
     * Without a cache each of those was a round trip, which is invisible on a filesystem and is the whole
     * cost on an object store.
     */
    public void testRepeatedReadsOfOneTenantCostOneRoundTrip() throws Exception {
        BlobDescriptorBackend backend = countingBackend();
        backend.create(descriptor("tenant-hot"));
        long afterCreate = counter.getCount();

        for (int i = 0; i < 12; i++) {
            assertNotNull(backend.get("tenant-hot"));
        }

        assertEquals(
            "twelve resolutions of one name must cost one read, not twelve",
            1,
            counter.getCount() - afterCreate
        );
    }

    /**
     * A miss costs two reads, because absence is only absence once the tombstone prefix agrees.
     *
     * <p>Recorded rather than optimised: "deleted" and "never existed" have opposite safe responses, and the
     * second read is what tells them apart. It is the most expensive descriptor operation.
     */
    public void testAMissCostsTwoRoundTrips() throws Exception {
        BlobDescriptorBackend backend = countingBackend();
        long before = counter.getCount();

        assertNull(backend.get("tenant-that-never-existed"));

        assertEquals("the live prefix, then the tombstone prefix", 2, counter.getCount() - before);
    }

    /** A miss is never cached, so an index created just after a failed lookup resolves at once. */
    public void testAMissIsNotCached() throws Exception {
        BlobDescriptorBackend backend = countingBackend();
        assertNull(backend.get("tenant-later"));

        backend.create(descriptor("tenant-later"));

        assertNotNull("caching absence would hide a just-created index for a freshness window", backend.get("tenant-later"));
    }

    /** Distinct tenants each pay their own read, so the cache is not answering for names it never saw. */
    public void testEachTenantPaysItsOwnRead() throws Exception {
        BlobDescriptorBackend backend = countingBackend();
        for (String name : new String[] { "tenant-a", "tenant-b", "tenant-c" }) {
            backend.create(descriptor(name));
        }
        long afterCreates = counter.getCount();

        for (String name : new String[] { "tenant-a", "tenant-b", "tenant-c" }) {
            assertNotNull(backend.get(name));
        }
        assertEquals("three cold tenants, three reads", 3, counter.getCount() - afterCreates);

        long afterCold = counter.getCount();
        for (String name : new String[] { "tenant-a", "tenant-b", "tenant-c" }) {
            assertNotNull(backend.get(name));
        }
        assertEquals("and none the second time", 0, counter.getCount() - afterCold);
    }

    /** A deleted tenant must stop resolving at once rather than after the freshness window. */
    public void testDeletionInvalidatesTheCachedDescriptor() throws Exception {
        BlobDescriptorBackend backend = countingBackend();
        backend.create(descriptor("tenant-doomed"));
        assertNotNull(backend.get("tenant-doomed"));

        backend.putTombstoneAsync(descriptor("tenant-doomed").tombstoned());

        IndexDescriptor after = backend.get("tenant-doomed");
        assertTrue(
            "a deleted index still resolving as live would accept a write against a shard the cluster no "
                + "longer believes in",
            after == null || after.exists() == false
        );
    }
}
