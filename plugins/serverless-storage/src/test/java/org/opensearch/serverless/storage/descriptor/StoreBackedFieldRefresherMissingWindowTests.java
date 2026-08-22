/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.AbsentIndexDescriptorSuppliers;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * T59. Whether {@link StoreBackedFieldRefresher} consults the descriptor's mapping generation rather than
 * treating a store answering nothing as an index with no fields.
 *
 * <p>This is the read half of what T50 closed on the write side: a descriptor this node resolved says the
 * index declared fields, and the store nonetheless answers absent, which is the window between T47's
 * deletion-time prune and the moment this node's descriptor cache catches up.
 */
public class StoreBackedFieldRefresherMissingWindowTests extends OpenSearchTestCase {

    private static final String INDEX_NAME = "gated-in-the-window";
    private static final String INDEX_UUID = "uuid-the-descriptor-still-confirms";

    @After
    public void clearRegistrations() {
        MappingGenerationStore.register(null);
        AbsentIndexDescriptorSuppliers.register(null);
    }

    private static MapperService mapperServiceFor(String uuid) {
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.index()).thenReturn(new Index(INDEX_NAME, uuid));
        return mapperService;
    }

    private static IndexDescriptor liveDescriptor(String name, String uuid, long mappingGeneration) {
        return new IndexDescriptor(
            name,
            uuid,
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            java.util.List.of(),
            org.opensearch.Version.CURRENT.id,
            false,
            false,
            false,
            false,
            mappingGeneration,
            0L
        );
    }

    /**
     * Criterion 1, at the wiring that reaches production. A store with nothing for this uuid, against a
     * descriptor that resolves the same uuid at generation 1, must fail the refresh rather than report the
     * field absent -- which would send the caller to infer it fresh, silently.
     */
    public void testARefreshFailsWhenTheDescriptorClaimsAGenerationTheStoreDoesNotHave() {
        MappingGenerationStore.register(new MappingGenerationStore.Store() {
            @Override
            public MappingGenerationStore.MappingGeneration read(String indexUuid) {
                return null;
            }

            @Override
            public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
                throw new AssertionError("not exercised by this test");
            }

            @Override
            public void delete(String indexUuid) {
                throw new AssertionError("not exercised by this test");
            }
        });
        AbsentIndexDescriptorSuppliers.register(
            candidate -> INDEX_NAME.equals(candidate) ? liveDescriptor(INDEX_NAME, INDEX_UUID, 1L) : null
        );

        MappingGenerationStore.MissingMappingException thrown = expectThrows(
            MappingGenerationStore.MissingMappingException.class,
            () -> new StoreBackedFieldRefresher().refresh(mapperServiceFor(INDEX_UUID), INDEX_UUID, "region")
        );
        assertTrue(thrown.getMessage().contains(INDEX_UUID));
    }

    /** The control: a genuinely empty mapping (generation 0) still reports the field absent, not a failure. */
    public void testARefreshOfAGenuinelyEmptyMappingReportsTheFieldAbsent() {
        MappingGenerationStore.register(new MappingGenerationStore.Store() {
            @Override
            public MappingGenerationStore.MappingGeneration read(String indexUuid) {
                return null;
            }

            @Override
            public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
                throw new AssertionError("not exercised by this test");
            }

            @Override
            public void delete(String indexUuid) {
                throw new AssertionError("not exercised by this test");
            }
        });
        AbsentIndexDescriptorSuppliers.register(
            candidate -> INDEX_NAME.equals(candidate) ? liveDescriptor(INDEX_NAME, INDEX_UUID, 0L) : null
        );

        assertFalse(new StoreBackedFieldRefresher().refresh(mapperServiceFor(INDEX_UUID), INDEX_UUID, "region"));
    }

    /** A present mapping at generation 1 merges normally; the check does not stand in its way. */
    public void testARefreshStillMergesAFieldTheStoreActuallyHas() {
        MappingGenerationStore.register(new MappingGenerationStore.Store() {
            @Override
            public MappingGenerationStore.MappingGeneration read(String indexUuid) {
                return new MappingGenerationStore.MappingGeneration(1L, Map.of("region", "keyword"));
            }

            @Override
            public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
                throw new AssertionError("not exercised by this test");
            }

            @Override
            public void delete(String indexUuid) {
                throw new AssertionError("not exercised by this test");
            }
        });
        AbsentIndexDescriptorSuppliers.register(
            candidate -> INDEX_NAME.equals(candidate) ? liveDescriptor(INDEX_NAME, INDEX_UUID, 1L) : null
        );

        // The merge itself needs a real MapperService, not a mock, so this only asserts the call does not
        // throw and reaches the merge attempt -- FieldRefresherReadCountTests and StoreBackedFieldRefresherIT
        // already cover the merge succeeding end to end with a real one.
        try {
            new StoreBackedFieldRefresher().refresh(mapperServiceFor(INDEX_UUID), INDEX_UUID, "region");
        } catch (MappingGenerationStore.MissingMappingException e) {
            fail("a mapping the store actually has must not be reported missing: " + e.getMessage());
        } catch (NullPointerException expectedFromTheMockNotMerging) {
            // mapperService.merge(...) on a bare mock has no real behaviour; reaching it is the assertion.
        }
    }

    /**
     * When descriptor resolution is not registered at all, the refresher behaves exactly as it did before
     * T59 -- the fallback {@code MetadataMappingService#refuseIfDescriptorShowsTheIndexIsGone} documents the
     * same contract for the write side.
     */
    public void testWithNoDescriptorResolutionRegisteredAMissingMappingIsReportedAbsentNotFailed() {
        MappingGenerationStore.register(new MappingGenerationStore.Store() {
            @Override
            public MappingGenerationStore.MappingGeneration read(String indexUuid) {
                return null;
            }

            @Override
            public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
                throw new AssertionError("not exercised by this test");
            }

            @Override
            public void delete(String indexUuid) {
                throw new AssertionError("not exercised by this test");
            }
        });
        assertFalse(AbsentIndexDescriptorSuppliers.isRegistered());

        assertFalse(new StoreBackedFieldRefresher().refresh(mapperServiceFor(INDEX_UUID), INDEX_UUID, "region"));
    }
}
