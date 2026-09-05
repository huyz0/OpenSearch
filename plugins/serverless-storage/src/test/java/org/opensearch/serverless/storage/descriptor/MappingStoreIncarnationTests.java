/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.cluster.metadata.IndexDescriptor;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A mapping write carries a uuid; the store resolves it to a name. What happens when the name has since
 * come to mean a different index.
 *
 * <h2>Why the map goes stale, and why nothing corrects it</h2>
 *
 * {@code DescriptorBackedMappingStore} keeps a per-JVM uuid-to-name map, because descriptors are keyed by
 * name and a dynamic-mapping update arrives with a uuid. Delete "logs" (uuid A) and recreate it (uuid B)
 * from another node, and this node's entry {@code A -> "logs"} survives: {@code removeStoredMappings}
 * calls {@code delete(A)} on the deleting node only, and the change tailer that would eventually
 * invalidate here runs on a five second interval and is documented as able to step over a bucket it cannot
 * read. Meanwhile this node may still have a writer shard of uuid A open.
 *
 * <p>The store then resolved A to "logs", read uuid <em>B's</em> descriptor, and compared only the mapping
 * generation -- which matched, because generations are per index and both start near 1. So a field
 * inferred from a document written into the old index was merged into the new one's mapping, at the new
 * one's store version, and the swap applied. With a type collision it raises a conflict against a document
 * that has nothing to do with it; without one it corrupts in silence.
 */
public class MappingStoreIncarnationTests extends OpenSearchTestCase {

    private static final class InMemoryBackend implements DescriptorBackend {
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
        public CompletableFuture<Boolean> createAsync(IndexDescriptor descriptor) {
            return CompletableFuture.completedFuture(create(descriptor));
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
        public void putTombstoneAsync(IndexDescriptor tombstone, ActionListener<Void> listener) {
            putTombstoneAsync(tombstone);
            listener.onResponse(null);
        }

        @Override
        public boolean available() {
            return true;
        }

        @Override
        public void warmAsync(Collection<String> names, ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        public void invalidate(String name) {}
    }

    private static IndexDescriptor descriptor(String name, String uuid, long mappingGeneration, Map<String, Object> mapping) {
        return new IndexDescriptor(
            name,
            uuid,
            1,
            0,
            true,
            IndexDescriptor.State.OPEN,
            List.of(),
            1L,
            false,
            false,
            false,
            false,
            mappingGeneration,
            1000L,
            0,
            0,
            0L,
            mapping
        );
    }

    /**
     * The write half: a swap presented with a uuid the name no longer belongs to must not land.
     *
     * <p>Raised as {@link IndexNotFoundException} rather than returned as {@code false}, because false
     * means "someone got there first, re-read and merge" and would send {@code updateMapping} round its
     * retry loop fifteen more times to report "sustained contention" -- an actively misleading answer
     * during an incident for a write against an index that no longer exists.
     */
    public void testASwapAgainstAPreviousIncarnationsUuidIsRefused() {
        InMemoryBackend backend = new InMemoryBackend();
        DescriptorBackedMappingStore store = new DescriptorBackedMappingStore(() -> backend, null);

        IndexDescriptor old = descriptor("logs", "uuid-A", 1L, Map.of("host", Map.of("type", "keyword")));
        backend.create(old);
        DescriptorBackedMappingStore.registerDescriptor(old);

        // Elsewhere: "logs" is deleted and recreated. This node's uuid-to-name entry for A survives.
        IndexDescriptor recreated = descriptor("logs", "uuid-B", 1L, Map.of("city", Map.of("type", "long")));
        backend.put(recreated);

        expectThrows(
            IndexNotFoundException.class,
            () -> store.compareAndSwap(
                "uuid-A",
                1L,
                new MappingGenerationStore.MappingGeneration(2L, Map.of("city", Map.of("type", "keyword")))
            )
        );

        IndexDescriptor live = backend.get("logs");
        assertEquals("uuid-B", live.uuid());
        assertEquals("the new incarnation's mapping must be untouched", 1L, live.mappingGeneration());
        assertEquals(
            "and must not have acquired a field inferred from a document written into the old index",
            "long",
            MappingGenerationStore.typeOf(live.initialMapping().get("city"))
        );
    }

    /**
     * The read half: the old incarnation's uuid must not be told the new incarnation's generation.
     *
     * <p>Answered as absence, which is the same answer a deleted index gets, because for the uuid being
     * asked about there genuinely is no mapping -- there is no index. Reporting the other incarnation's
     * generation is what let a stale shard's field refresher believe it was up to date against a mapping it
     * had never seen.
     */
    public void testAReadForAPreviousIncarnationsUuidDoesNotAnswerWithTheNewOnes() {
        InMemoryBackend backend = new InMemoryBackend();
        DescriptorBackedMappingStore store = new DescriptorBackedMappingStore(() -> backend, null);

        IndexDescriptor old = descriptor("logs", "uuid-A", 7L, Map.of("host", Map.of("type", "keyword")));
        backend.create(old);
        DescriptorBackedMappingStore.registerDescriptor(old);
        assertEquals(7L, store.read("uuid-A").generation());

        backend.put(descriptor("logs", "uuid-B", 1L, Map.of("city", Map.of("type", "long"))));

        assertNull("a uuid whose name now means another index has no mapping of its own", store.read("uuid-A"));
        // And the stale entry is dropped rather than left to resolve wrongly again on the next read.
        assertNull(store.read("uuid-A"));
    }

    /** The ordinary path is untouched: same uuid, same name, the swap applies. */
    public void testASwapAgainstTheCurrentIncarnationStillApplies() {
        InMemoryBackend backend = new InMemoryBackend();
        DescriptorBackedMappingStore store = new DescriptorBackedMappingStore(() -> backend, null);

        IndexDescriptor live = descriptor("logs", "uuid-A", 1L, Map.of("host", Map.of("type", "keyword")));
        backend.create(live);
        DescriptorBackedMappingStore.registerDescriptor(live);

        assertTrue(
            store.compareAndSwap(
                "uuid-A",
                1L,
                new MappingGenerationStore.MappingGeneration(
                    2L,
                    Map.of("host", Map.of("type", "keyword"), "city", Map.of("type", "keyword"))
                )
            )
        );
        assertEquals(2L, backend.get("logs").mappingGeneration());
    }
}
