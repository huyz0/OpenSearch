/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * H2e. The seam that lets resolution answer for an index cluster state does not hold.
 *
 * <p>Shaped after {@code AbsentIndexRoutingSuppliers} from Area C rather than designed fresh, because
 * that seam was applied across nine call sites and the ways it can go wrong are known. The two that
 * matter most are asserted here.
 *
 * <p><b>An ordinary cluster must not pay.</b> A name present in metadata must never reach the supplier,
 * or every request on every cluster acquires a remote lookup it did not have. That is asserted by
 * counting supplier invocations rather than by checking the answer, since the right answer would come
 * back either way.
 *
 * <p><b>A tombstone must answer false.</b> Deletion in Area H records rather than removes, so a resolver
 * that treated any returned descriptor as existence would resurrect deleted indices, which is what
 * {@code IndexGraveyard} exists to prevent.
 */
public class AbsentIndexDescriptorSuppliersTests extends OpenSearchTestCase {

    @After
    public void clearSupplier() {
        AbsentIndexDescriptorSuppliers.register(null);
    }

    /** With nothing installed, behaviour is exactly what it was: metadata decides, and nothing else. */
    public void testWithoutASupplierOnlyMetadataAnswers() {
        Metadata metadata = metadataWith("present");

        assertTrue(AbsentIndexDescriptorSuppliers.exists(metadata, "present"));
        assertFalse("an absent name must stay absent when nothing is installed", AbsentIndexDescriptorSuppliers.exists(metadata, "absent"));
    }

    /** The point of the seam: a name cluster state does not hold can still resolve. */
    public void testASuppliedDescriptorResolves() {
        Metadata metadata = metadataWith("present");
        AbsentIndexDescriptorSuppliers.register(name -> descriptor(name, IndexDescriptor.State.OPEN));

        assertTrue("a supplied descriptor must make an absent name resolve", AbsentIndexDescriptorSuppliers.exists(metadata, "computed"));
    }

    /**
     * The control that keeps this from being a latency regression. A name in metadata must never reach
     * the supplier, counted rather than inferred, since the answer would be identical either way.
     */
    public void testAPresentNameNeverReachesTheSupplier() {
        Metadata metadata = metadataWith("present");
        AtomicInteger supplierCalls = new AtomicInteger();
        AbsentIndexDescriptorSuppliers.register(name -> {
            supplierCalls.incrementAndGet();
            return descriptor(name, IndexDescriptor.State.OPEN);
        });

        assertTrue(AbsentIndexDescriptorSuppliers.exists(metadata, "present"));

        assertEquals("a name present in metadata must not cost a descriptor lookup", 0, supplierCalls.get());
    }

    /** A tombstoned descriptor is a durable no, which is what replaces the graveyard. */
    public void testATombstonedDescriptorDoesNotExist() {
        Metadata metadata = metadataWith("present");
        AbsentIndexDescriptorSuppliers.register(name -> descriptor(name, IndexDescriptor.State.DELETED));

        assertFalse(
            "a deleted index must not resolve, or a partitioned node could resurrect its data",
            AbsentIndexDescriptorSuppliers.exists(metadata, "deleted-index")
        );
    }

    /**
     * A supplier that throws must degrade rather than fail the request, which is the rule Area C settled
     * after a plugin bug would otherwise have turned an absence into an outage.
     */
    public void testASupplierThatThrowsIsTreatedAsNoAnswer() {
        Metadata metadata = metadataWith("present");
        AbsentIndexDescriptorSuppliers.register(name -> { throw new IllegalStateException("supplier is broken"); });

        assertFalse(
            "a broken supplier must degrade to absence, not propagate",
            AbsentIndexDescriptorSuppliers.exists(metadata, "anything")
        );
    }

    /** The plural form exists so a ten-index request is one round trip rather than ten. */
    public void testSupplyAllResolvesEveryName() {
        AbsentIndexDescriptorSuppliers.register(name -> name.startsWith("known") ? descriptor(name, IndexDescriptor.State.OPEN) : null);

        List<IndexDescriptor> resolved = AbsentIndexDescriptorSuppliers.supplyAll(List.of("known-1", "unknown", "known-2"));

        assertEquals("only the names the supplier knows may come back", 2, resolved.size());
    }

    /** With nothing installed the plural form must not allocate or call anything. */
    public void testSupplyAllIsEmptyWithoutASupplier() {
        assertTrue(AbsentIndexDescriptorSuppliers.supplyAll(List.of("a", "b")).isEmpty());
    }

    // ---------------------------------------------------------------- helpers

    private static IndexDescriptor descriptor(String name, IndexDescriptor.State state) {
        return new IndexDescriptor(name, name + "-uuid", 1, 0, true, state, List.of(), Version.CURRENT.id, false, false, false, false);
    }

    private static Metadata metadataWith(String name) {
        IndexMetadata indexMetadata = IndexMetadata.builder(name)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, name + "-uuid")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        return Metadata.builder().put(indexMetadata, false).build();
    }
}
