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

    private static IndexDescriptor descriptor(String name, String uuid, Map<String, Object> mapping, long generation) {
        return new IndexDescriptor(
            name,
            uuid,
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
            generation,
            1000L,
            0,
            0,
            0L,
            mapping
        );
    }

    /**
     * Two writers observing one generation: exactly one swap takes, and the loser is told so.
     *
     * <p>This is the data loss the swap exists to prevent, and it was not being prevented. The check ran
     * against a descriptor read through a cache with a sixty second window, and the write underneath it was
     * unconditional -- worse, a lost write was retried at the winner's generation. Two shards inferring
     * different dynamic fields from the same bulk both observed generation N, both were told their swap
     * took, and one of the two fields was erased from the mapping while the documents that introduced it
     * were being indexed. Nothing failed and nothing logged; the field was simply not queryable.
     *
     * <p>Run against a real blob-backed store rather than a map, because the property under test is the
     * conditional write's, and a map with {@code put} would demonstrate nothing.
     */
    public void testTwoSwapsAtOneGenerationCannotBothSucceed() throws Exception {
        org.opensearch.common.blobstore.fs.FsBlobStore blobStore = new org.opensearch.common.blobstore.fs.FsBlobStore(
            1024,
            createTempDir(),
            false
        );
        BlobDescriptorBackend backend = new BlobDescriptorBackend(
            blobStore.blobContainer(org.opensearch.common.blobstore.BlobPath.cleanPath())
        );
        DescriptorBackedMappingStore store = new DescriptorBackedMappingStore(() -> backend, null);

        IndexDescriptor initial = descriptor("serverless_tenant-a", "uuid-a", Map.of(), 0L);
        assertTrue(backend.create(initial));
        DescriptorBackedMappingStore.registerDescriptor(initial);

        boolean first = store.compareAndSwap("uuid-a", 0L, new MappingGenerationStore.MappingGeneration(1L, Map.of("age", "long")));
        boolean second = store.compareAndSwap("uuid-a", 0L, new MappingGenerationStore.MappingGeneration(1L, Map.of("city", "keyword")));

        assertTrue("the first writer wins", first);
        assertFalse("and the second must be told to re-read and merge, not silently overwrite", second);
        Object survivingField = store.read("uuid-a").fields().get("age");
        assertEquals("the winner's field must still be there", "long", MappingGenerationStore.typeOf(survivingField));
    }

    /**
     * And the retry loop above this one then keeps both fields.
     *
     * <p>The loser returning false is only half the fix. The re-read that follows it goes back through
     * {@link DescriptorBackedMappingStore#read}, which is cached, so without dropping the cached copy on a
     * failed swap the loop reads the same stale generation until it gives up and reports sustained
     * contention for a value it was never going to see change.
     */
    public void testALostSwapIsFollowedByAMergeThatKeepsBothFields() throws Exception {
        org.opensearch.common.blobstore.fs.FsBlobStore blobStore = new org.opensearch.common.blobstore.fs.FsBlobStore(
            1024,
            createTempDir(),
            false
        );
        BlobDescriptorBackend backend = new BlobDescriptorBackend(
            blobStore.blobContainer(org.opensearch.common.blobstore.BlobPath.cleanPath())
        );
        DescriptorBackedMappingStore store = new DescriptorBackedMappingStore(() -> backend, null);

        IndexDescriptor initial = descriptor("serverless_tenant-b", "uuid-b", Map.of(), 0L);
        assertTrue(backend.create(initial));
        DescriptorBackedMappingStore.registerDescriptor(initial);

        MappingGenerationStore.register(store);
        try {
            assertEquals(1L, MappingGenerationStore.updateMapping("uuid-b", Map.of("age", Map.of("type", "long"))));
            assertEquals(2L, MappingGenerationStore.updateMapping("uuid-b", Map.of("city", Map.of("type", "keyword"))));

            MappingGenerationStore.MappingGeneration current = store.read("uuid-b");
            assertEquals(2L, current.generation());
            assertEquals("long", MappingGenerationStore.typeOf(current.fields().get("age")));
            assertEquals("keyword", MappingGenerationStore.typeOf(current.fields().get("city")));
        } finally {
            MappingGenerationStore.register(null);
        }
    }
}
