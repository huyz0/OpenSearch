/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.cache.CacheBuilder;

import java.util.function.Supplier;

/**
 * Mappings for gated indices stored directly inside {@link IndexDescriptor} objects in Object Storage.
 *
 * <p>Eliminates system index (.opensearch-index-mappings) round-trips entirely. Dynamic field updates
 * perform compare-and-swap register writes directly on the index's {@link IndexDescriptor} in Object Storage.
 */
public final class DescriptorBackedMappingStore implements MappingGenerationStore.Store {

    private static final Logger logger = LogManager.getLogger(DescriptorBackedMappingStore.class);

    private static final Cache<String, String> UUID_TO_NAME = CacheBuilder.<String, String>builder().setMaximumWeight(50_000).build();

    private final Supplier<DescriptorBackend> backendSupplier;
    private final DescriptorCache descriptorCache;

    public DescriptorBackedMappingStore(Supplier<DescriptorBackend> backendSupplier, DescriptorCache descriptorCache) {
        this.backendSupplier = backendSupplier;
        this.descriptorCache = descriptorCache;
    }

    public static void registerUuidToName(String uuid, String name) {
        if (uuid != null && name != null) {
            UUID_TO_NAME.put(uuid, name);
        }
    }

    public static void registerDescriptor(IndexDescriptor descriptor) {
        if (descriptor != null) {
            registerUuidToName(descriptor.uuid(), descriptor.name());
        }
    }

    @Override
    public MappingGenerationStore.MappingGeneration read(String indexUuid) {
        String name = UUID_TO_NAME.get(indexUuid);
        if (name == null) {
            return null;
        }
        IndexDescriptor descriptor = getDescriptor(name);
        if (descriptor == null || descriptor.exists() == false) {
            return null;
        }
        return new MappingGenerationStore.MappingGeneration(descriptor.mappingGeneration(), descriptor.initialMapping());
    }

    @Override
    public boolean compareAndSwap(String indexUuid, long expectedGeneration, MappingGenerationStore.MappingGeneration updated) {
        String name = UUID_TO_NAME.get(indexUuid);
        if (name == null) {
            logger.warn("cannot update mapping for uuid [{}]: no index name mapped for uuid", indexUuid);
            return false;
        }
        IndexDescriptor descriptor = getDescriptor(name);
        if (descriptor == null || descriptor.exists() == false) {
            logger.warn("cannot update mapping for index [{}]: descriptor absent or deleted", name);
            return false;
        }
        if (descriptor.mappingGeneration() != expectedGeneration) {
            logger.debug(
                "mapping swap for [{}] conflict: expected gen [{}], current gen [{}]",
                name,
                expectedGeneration,
                descriptor.mappingGeneration()
            );
            return false;
        }

        IndexDescriptor updatedDescriptor = descriptor.withMapping(updated.generation(), updated.fields());
        DescriptorBackend backend = backendSupplier.get();
        if (backend == null) {
            logger.error("cannot update mapping for [{}]: no descriptor backend installed", name);
            return false;
        }

        try {
            backend.put(updatedDescriptor);
            if (descriptorCache != null) {
                descriptorCache.invalidate(name);
            }
            registerDescriptor(updatedDescriptor);
            return true;
        } catch (Exception e) {
            logger.warn("mapping swap for [{}] failed at backend put: {}", name, e);
            return false;
        }
    }

    @Override
    public void delete(String indexUuid) {
        if (indexUuid != null) {
            UUID_TO_NAME.remove(indexUuid);
        }
    }

    private IndexDescriptor getDescriptor(String name) {
        DescriptorBackend backend = backendSupplier.get();
        IndexDescriptor descriptor = null;
        if (descriptorCache != null) {
            descriptor = descriptorCache.get(name, k -> backend != null ? backend.get(k) : null);
        } else if (backend != null) {
            descriptor = backend.get(name);
        }
        if (descriptor != null) {
            registerDescriptor(descriptor);
        }
        return descriptor;
    }
}
