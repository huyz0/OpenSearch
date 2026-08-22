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

/**
 * Phase C5 of core-pluggability-refactor-plan.md. Since {@link AbsentIndexDescriptorSuppliers} is a static,
 * process-wide registry, every test here registers and unregisters within itself rather than relying on
 * {@code @Before}/{@code @After} alone -- a registration left behind leaks into whichever unrelated test
 * runs next in the same JVM, the exact class of mistake
 * {@code ResolverAttachingClusterStateApplierTests}' own singleton-mutation finding was about.
 */
public class SupplierBackedIndexMetadataResolverTests extends OpenSearchTestCase {

    private final SupplierBackedIndexMetadataResolver resolver = new SupplierBackedIndexMetadataResolver();

    @After
    public void clearRegistry() {
        AbsentIndexDescriptorSuppliers.register(null);
        AbsentIndexDescriptorSuppliers.registerCached(null);
        AbsentIndexDescriptorSuppliers.clearSynthesised();
    }

    public void testReturnsNullWhenNothingIsRegistered() {
        assertNull(resolver.resolve(Metadata.builder().build(), "any-index"));
    }

    public void testDelegatesToWhateverIsRegisteredWithTheStaticRegistry() {
        IndexDescriptor descriptor = descriptor("gated-index");
        AbsentIndexDescriptorSuppliers.register(name -> "gated-index".equals(name) ? descriptor : null);

        IndexMetadata resolved = resolver.resolve(Metadata.builder().build(), "gated-index");

        assertNotNull(resolved);
        assertEquals("gated-index", resolved.getIndex().getName());
    }

    public void testReturnsNullForATombstonedDescriptor() {
        IndexDescriptor tombstone = descriptor("deleted-index").tombstoned();
        AbsentIndexDescriptorSuppliers.register(name -> "deleted-index".equals(name) ? tombstone : null);

        assertNull(
            "a tombstoned descriptor must resolve like a genuinely missing index, matching "
                + "AbsentIndexDescriptorSuppliers#synthesisedMetadata's own contract",
            resolver.resolve(Metadata.builder().build(), "deleted-index")
        );
    }

    public void testMatchesWhatMetadataOrDescriptorAlreadyReturns() {
        // The whole point of delegating to synthesisedMetadata rather than reimplementing lookup logic:
        // this class's answer must be identical to the pre-existing call sites' answer, not just similar.
        IndexDescriptor descriptor = descriptor("gated-index");
        AbsentIndexDescriptorSuppliers.register(name -> "gated-index".equals(name) ? descriptor : null);
        Metadata metadata = Metadata.builder().build();

        assertSame(AbsentIndexDescriptorSuppliers.metadataOrDescriptor(metadata, "gated-index"), resolver.resolve(metadata, "gated-index"));
    }

    private static IndexDescriptor descriptor(String name) {
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
        return IndexDescriptor.from(indexMetadata);
    }
}
