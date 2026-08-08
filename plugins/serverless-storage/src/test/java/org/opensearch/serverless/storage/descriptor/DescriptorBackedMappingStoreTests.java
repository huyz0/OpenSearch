/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.cluster.metadata.MappingGenerationStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DescriptorBackedMappingStoreTests extends OpenSearchTestCase {

    private static class InMemoryBackend implements DescriptorBackend {
        private final Map<String, IndexDescriptor> descriptors = new ConcurrentHashMap<>();

        @Override
        public IndexDescriptor get(String name) {
            return descriptors.get(name);
        }

        @Override
        public boolean create(IndexDescriptor descriptor) {
            return descriptors.putIfAbsent(descriptor.name(), descriptor) == null;
        }

        @Override
        public java.util.concurrent.CompletableFuture<Boolean> createAsync(IndexDescriptor descriptor) {
            return java.util.concurrent.CompletableFuture.completedFuture(create(descriptor));
        }

        @Override
        public void put(IndexDescriptor descriptor) {
            descriptors.put(descriptor.name(), descriptor);
        }

        @Override
        public void putAsync(IndexDescriptor descriptor) {
            put(descriptor);
        }

        @Override
        public void putTombstoneAsync(IndexDescriptor tombstone) {
            descriptors.put(tombstone.name(), tombstone);
        }

        @Override
        public void putTombstoneAsync(IndexDescriptor tombstone, org.opensearch.core.action.ActionListener<Void> listener) {
            putTombstoneAsync(tombstone);
            listener.onResponse(null);
        }

        @Override
        public boolean available() {
            return true;
        }

        @Override
        public void warmAsync(java.util.Collection<String> names, org.opensearch.core.action.ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        public void invalidate(String name) {}
    }

    public void testReadAndCompareAndSwapDirectlyInObjectStorage() {
        InMemoryBackend backend = new InMemoryBackend();
        DescriptorBackedMappingStore store = new DescriptorBackedMappingStore(() -> backend, null);

        Map<String, Object> initialMapping = Map.of("field1", Map.of("type", "keyword"));
        IndexDescriptor desc = new IndexDescriptor(
            "test-index",
            "uuid-123",
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            java.util.List.of(),
            1L,
            false,
            false,
            false,
            false,
            1L,
            1000L,
            0,
            0,
            0L,
            initialMapping
        );

        backend.create(desc);
        DescriptorBackedMappingStore.registerDescriptor(desc);

        // Read initial mapping
        MappingGenerationStore.MappingGeneration readGen = store.read("uuid-123");
        assertNotNull(readGen);
        assertEquals(1L, readGen.generation());
        assertEquals("keyword", MappingGenerationStore.typeOf(readGen.fields().get("field1")));

        // CAS update dynamic field
        Map<String, Object> updatedFields = Map.of("field1", Map.of("type", "keyword"), "field2", Map.of("type", "long"));
        MappingGenerationStore.MappingGeneration nextGen = new MappingGenerationStore.MappingGeneration(2L, updatedFields);
        boolean swapped = store.compareAndSwap("uuid-123", 1L, nextGen);
        assertTrue(swapped);

        // Verify updated descriptor in backend
        IndexDescriptor updatedDesc = backend.get("test-index");
        assertNotNull(updatedDesc);
        assertEquals(2L, updatedDesc.mappingGeneration());
        assertEquals("long", MappingGenerationStore.typeOf(updatedDesc.initialMapping().get("field2")));

        // CAS with stale generation fails
        boolean staleSwapped = store.compareAndSwap("uuid-123", 1L, new MappingGenerationStore.MappingGeneration(3L, updatedFields));
        assertFalse(staleSwapped);
    }
}
