/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.serverless.cluster.IndexDescriptor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Index descriptors as object-store registers.
 *
 * <p>Creation is a single put-if-absent, which is what makes name uniqueness free: there is no global
 * lock, no elected serializer and no sweep over the existing population. That last point is the whole
 * reason this design exists — {@code plan-area-h-metadata-off-cluster-state.md} measured index creation
 * at 7.4 ms with 200 indices present and 98.8 ms at 6,000, because
 * {@code Metadata.Builder.build()} rebuilds name lookups across every index on every create. A CAS on
 * one blob costs the same at zero indices and at a hundred million.
 *
 * <p>{@code createRegisterIfAbsent} exists as a distinct call rather than a CAS against
 * {@code ABSENT_GENERATION} because the general form has to learn the current generation first, so it
 * reads before it writes. Creation does not need to, and a factor of two matters on the one operation
 * this design expects to run at 10^8.
 *
 * <p>Per D5, exercised against {@code FsBlobContainer} only. See this package's documentation.
 */
public final class DescriptorStore {

    private final BlobContainer container;

    /**
     * Creates a store over a container.
     *
     * @param container the container holding the {@code indices/} prefix
     */
    public DescriptorStore(BlobContainer container) {
        this.container = container;
    }

    /**
     * Creates an index descriptor, failing if the name is taken.
     *
     * @param descriptor the descriptor to write
     * @return the generation the register now holds
     * @throws IndexAlreadyExistsException if a descriptor for that name already exists
     * @throws IOException if the write fails
     */
    public long create(IndexDescriptor descriptor) throws IOException {
        final BlobRegisterCasResult result = container.createRegisterIfAbsent(
            RegisterMap.descriptorBlob(descriptor.name()),
            descriptor.toBytes()
        );
        if (result.applied() == false) {
            // Name uniqueness, arbitrated by the object store rather than by a cluster-manager.
            throw new IndexAlreadyExistsException(descriptor.name());
        }
        return result.currentGeneration();
    }

    /**
     * Reads a descriptor.
     *
     * @param indexName the index
     * @return the descriptor, or empty if no such index exists
     * @throws IOException if the register exists but cannot be read or parsed
     */
    public Optional<IndexDescriptor> get(String indexName) throws IOException {
        final Optional<BlobRegister> register = container.readRegister(RegisterMap.descriptorBlob(indexName));
        if (register.isEmpty()) {
            return Optional.empty();
        }
        // Absent and unreadable are different answers, and conflating them is how a deleted index and a
        // corrupt one become indistinguishable. A parse failure propagates.
        try (InputStream in = register.get().value().streamInput()) {
            return Optional.of(IndexDescriptor.fromStream(in));
        }
    }

    /**
     * Returns the generation a descriptor register currently holds, for a compare-and-swap.
     *
     * @param indexName the index
     * @return the generation, or {@link BlobRegister#ABSENT_GENERATION} if absent
     * @throws IOException if the register cannot be read
     */
    public long generationOf(String indexName) throws IOException {
        return container.readRegister(RegisterMap.descriptorBlob(indexName))
            .map(BlobRegister::generation)
            .orElse(BlobRegister.ABSENT_GENERATION);
    }

    /**
     * Replaces a descriptor, succeeding only if the register still holds the expected generation.
     *
     * @param descriptor the new descriptor
     * @param expectedGeneration the generation the caller read
     * @return the new generation, or empty if another writer got there first
     * @throws IOException if the write fails
     */
    public Optional<Long> update(IndexDescriptor descriptor, long expectedGeneration) throws IOException {
        final BlobRegisterCasResult result = container.compareAndSwapRegister(
            RegisterMap.descriptorBlob(descriptor.name()),
            expectedGeneration,
            descriptor.toBytes()
        );
        return result.applied() ? Optional.of(result.currentGeneration()) : Optional.empty();
    }

    /**
     * Deletes a descriptor. Idempotent.
     *
     * @param indexName the index to delete
     * @throws IOException if the delete fails
     */
    public void delete(String indexName) throws IOException {
        container.deleteBlobsIgnoringIfNotExists(List.of(RegisterMap.descriptorBlob(indexName)));
    }

    /**
     * Lists every descriptor.
     *
     * <p>A bounded {@code ListObjectsV2}, not a search. At scale a caller wants a prefix or a name index
     * rather than this; it exists because a small deployment and a test both need to enumerate, and
     * pretending otherwise would mean neither could.
     *
     * @return descriptors by index name
     * @throws IOException if listing or reading fails
     */
    public Map<String, IndexDescriptor> listAll() throws IOException {
        final Map<String, IndexDescriptor> descriptors = new LinkedHashMap<>();
        final List<String> names = new ArrayList<>(container.listBlobs().keySet());
        for (String blobName : names) {
            final Optional<BlobRegister> register = container.readRegister(blobName);
            if (register.isEmpty()) {
                continue;
            }
            try (InputStream in = register.get().value().streamInput()) {
                final IndexDescriptor descriptor = IndexDescriptor.fromStream(in);
                descriptors.put(descriptor.name(), descriptor);
            }
        }
        return descriptors;
    }
}
