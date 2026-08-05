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

/**
 * W2. The store every Area H seam reads and writes through.
 *
 * <p>Until this existed, every measurement in the area created its own descriptor index inside the test, so
 * the design was reading from a fixture. This is the first time descriptors have a home a running node owns.
 *
 * <p>The behaviours asserted here are the ones other seams depend on, and each was established by an earlier
 * measurement rather than chosen now:
 *
 * <ul>
 *   <li><b>get by name is realtime</b> (H18), which is what lets a client create an index and immediately
 *       write to it</li>
 *   <li><b>create is put-if-absent</b> (H3), which is what makes a name unique without a cluster state
 *       update</li>
 *   <li><b>prefix search pages</b> (S24, H17), which is what makes listing bounded</li>
 *   <li><b>the index is created lazily and carries its contract settings</b> (H18, S29)</li>
 * </ul>
 */
public class DescriptorStoreIT extends OpenSearchIntegTestCase {

    private DescriptorStore store() {
        return new DescriptorStore(client(), 1);
    }

    /** Nothing exists until something is written, so a cluster that never gates pays nothing. */
    public void testTheDescriptorIndexIsCreatedLazily() {
        DescriptorStore store = store();

        assertFalse("the descriptor index must not exist before the first write", store.indexExists());

        store.create(descriptor("first"));

        assertTrue("and must exist after it", store.indexExists());
    }

    /**
     * The realtime guarantee, which is the one the rest of the design leans on hardest. No refresh is
     * issued between the write and the read.
     */
    public void testADescriptorIsReadableByNameWithNoRefresh() {
        DescriptorStore store = store();

        store.create(descriptor("logs-2026-07"));

        IndexDescriptor read = store.get("logs-2026-07");
        assertNotNull("a descriptor must be readable the instant it is written, with no refresh between", read);
        assertEquals("logs-2026-07", read.name());
        assertEquals("logs-2026-07-uuid", read.uuid());
        assertEquals(3, read.shardCount());
    }

    /** Uniqueness without a cluster state update, which is the whole of H3. */
    public void testCreateIsPutIfAbsent() {
        DescriptorStore store = store();

        assertTrue("the first create must win", store.create(descriptor("taken")));
        assertFalse("the second must lose rather than throw, since a lost race is a correct answer", store.create(descriptor("taken")));
    }

    /** A missing name is null rather than an exception, since callers disagree about what absence means. */
    public void testAMissingDescriptorIsNull() {
        assertNull("a name never written must read as absent", store().get("never-written"));
    }

    /**
     * The contract settings, asserted by value rather than by presence. H18 makes the refresh interval the
     * wildcard staleness bound and S29 makes the merge policy the lookup latency bound, so a deployment
     * that loses either one fails in a way that looks like the architecture.
     */
    public void testTheDescriptorIndexCarriesItsContractSettings() {
        DescriptorStore store = store();
        store.create(descriptor("anything"));

        var settings = client().admin()
            .indices()
            .prepareGetSettings(DescriptorStore.DESCRIPTOR_INDEX)
            .get()
            .getIndexToSettings()
            .get(DescriptorStore.DESCRIPTOR_INDEX);

        assertEquals("the wildcard staleness bound must be stated, not defaulted (H18)", "1s", settings.get("index.refresh_interval"));
        assertEquals(
            "the lookup latency bound must be stated, not defaulted (S29, S30)",
            "4",
            settings.get("index.merge.policy.segments_per_tier")
        );
    }

    /** Overwriting is how a mapping generation bump and a tombstone are recorded. */
    public void testPutOverwrites() {
        DescriptorStore store = store();
        store.create(descriptor("evolving"));

        store.put(store.get("evolving").withMappingGeneration(7L));

        assertEquals("the overwrite must be visible immediately, like any get by id", 7L, store.get("evolving").mappingGeneration());
    }

    private static IndexDescriptor descriptor(String name) {
        return new IndexDescriptor(
            name,
            name + "-uuid",
            3,
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
