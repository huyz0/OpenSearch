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
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * P8. How many times resolving a name goes to the descriptor index.
 *
 * <p>{@code AbsentIndexDescriptorSuppliers} states that a supplier is expected to answer from a cache. This
 * one had none: every call issued a real get. Resolution consults it per unresolved name per request, and
 * P7 made it per routing resolution for gated indices, because synthesising placement metadata reads the
 * descriptor every time.
 *
 * <p><b>The same mistake three times in one session, which is the finding as much as the latency.</b> P3
 * found the mapping guard sitting after the store read it existed to avoid. The first P5 attempt keyed a
 * memo by identity against a caller that allocated, so it always missed and cost more than no memo at all.
 * P7 then checked its synthesis cache after the descriptor read that populates it. Each time the cache was
 * real and each time it was consulted too late to matter.
 *
 * <p>So these tests count reads rather than asserting a cache exists, because the shape of this mistake is
 * a cache that is present and useless.
 */
public class DescriptorReadCountIT extends OpenSearchIntegTestCase {

    private final AtomicLong now = new AtomicLong(1_000_000_000L);

    private DescriptorStore store() {
        return new DescriptorStore(client(), 1, now::get);
    }

    /** Repeated resolution of the same name must not repeatedly go to the index. */
    public void testRepeatedResolutionCostsOneRead() {
        DescriptorStore store = store();
        store.create(descriptor("cached-idx"));
        long readsAfterCreate = store.readCount();

        for (int i = 0; i < 50; i++) {
            assertNotNull(store.get("cached-idx"));
        }

        assertEquals(
            "fifty resolutions must cost one read. Before P8 this was fifty, and after P7 it was one per "
                + "routing resolution for every gated index",
            readsAfterCreate + 1,
            store.readCount()
        );
    }

    /** Past the window it reads again, so a descriptor changed elsewhere is eventually seen. */
    public void testTheWindowExpires() {
        DescriptorStore store = store();
        store.create(descriptor("expiring-idx"));
        store.get("expiring-idx");
        long afterFirst = store.readCount();

        store.get("expiring-idx");
        assertEquals("inside the window, no further read", afterFirst, store.readCount());

        now.addAndGet(DescriptorStore.CACHE_TTL_NANOS + 1);
        store.get("expiring-idx");

        assertEquals("past the window it must look again", afterFirst + 1, store.readCount());
    }

    /**
     * A miss is not cached, so an index created a moment ago is nameable immediately. H18 made that the
     * contract for exact names, and caching absence would have quietly broken it.
     */
    public void testAMissIsNotCachedSoANewIndexIsVisibleImmediately() {
        DescriptorStore store = store();

        assertNull("not there yet", store.get("appearing-idx"));
        store.create(descriptor("appearing-idx"));

        assertNotNull("an index must be nameable the moment it is created, so absence must never be cached", store.get("appearing-idx"));
    }

    /** A write invalidates, so the store never serves a value it has just replaced. */
    public void testAWriteInvalidatesItsOwnCachedValue() {
        DescriptorStore store = store();
        store.create(descriptor("evolving-idx"));
        assertEquals(0L, store.get("evolving-idx").mappingGeneration());

        store.put(store.get("evolving-idx").withMappingGeneration(7L));

        assertEquals("a write must not leave its own stale value readable", 7L, store.get("evolving-idx").mappingGeneration());
    }

    private static IndexDescriptor descriptor(String name) {
        return new IndexDescriptor(
            name,
            name + "-uuid",
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
            Version.CURRENT.id,
            false,
            false,
            false,
            false,
            0L,
            1_700_000_000_000L
        );
    }
}
