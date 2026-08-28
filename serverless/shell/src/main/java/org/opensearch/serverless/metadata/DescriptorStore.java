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

    /** Marks a descriptor as deleted between the swap that orders the delete and the blob's removal. */
    private static final byte[] TOMBSTONE = "{\"tombstone\":true}".getBytes(java.nio.charset.StandardCharsets.UTF_8);

    private final BlobContainer container;

    private static boolean isTombstone(org.opensearch.core.common.bytes.BytesReference value) {
        return value.length() == TOMBSTONE.length
            && java.util.Arrays.equals(org.opensearch.core.common.bytes.BytesReference.toBytes(value), TOMBSTONE);
    }

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
        if (isTombstone(register.get().value())) {
            // Deleted, and the blob has not been removed yet. Absent is the truthful answer.
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
     * Deletes an index only if its descriptor still holds the generation the caller read.
     *
     * <p>Every other index-lifecycle operation is a compare-and-swap — creation is put-if-absent, a
     * mapping change is {@link #update} — and delete was the one that was not. An unconditional delete
     * races: a concurrent mapping update and a delete could both report success, with the update's
     * write landing on an object the delete then removed, or the delete removing a descriptor the
     * caller had never seen. Lifecycle is the one place in this design where operations on a single
     * index are ordered, and it is only ordered if <em>all</em> of them go through the register.
     *
     * <p>Implemented as a swap to a tombstone followed by removal, because the object store has no
     * conditional delete. The swap is the linearization point: after it, {@link #get} reports the index
     * as absent whether or not the blob has actually gone yet.
     *
     * @param indexName the index to delete
     * @param expectedGeneration the generation the caller read
     * @return true if this caller deleted it; false if the descriptor changed first
     * @throws IOException if the write fails
     */
    public boolean deleteIfUnchanged(String indexName, long expectedGeneration) throws IOException {
        final String blobName = RegisterMap.descriptorBlob(indexName);
        final BlobRegisterCasResult swapped = container.compareAndSwapRegister(
            blobName,
            expectedGeneration,
            new org.opensearch.core.common.bytes.BytesArray(TOMBSTONE)
        );
        if (swapped.applied() == false) {
            return false;
        }
        container.deleteBlobsIgnoringIfNotExists(List.of(blobName));
        return true;
    }

    /**
     * One bounded page of descriptors, in name order.
     *
     * <p><b>This is the only enumeration a deployment at target scale may use.</b> "List every index" is
     * not a slow operation at 100 million indices; it is not an operation at all — the answer does not
     * fit in a response, cannot be consumed by a caller, and is stale before it finishes. So the API is
     * a cursor walk, and callers that want everything pay for everything, one page at a time, visibly.
     *
     * <p>Two costs, and they are bounded differently. The descriptor <b>reads</b> are bounded here by
     * {@code limit}, on any backend. The <b>listing</b> is bounded natively by S3's
     * {@code ListObjectsV2} {@code start-after} plus {@code max-keys} and GCS's equivalent;
     * {@code FsBlobContainer} has no such primitive and enumerates the directory, so on a filesystem
     * this is bounded in reads but not in the listing itself. That half cannot be demonstrated here and
     * belongs with R11.
     *
     * @param after return names strictly greater than this, or null to start at the beginning
     * @param limit maximum descriptors to return
     * @return the page
     * @throws IOException if listing or reading fails
     */
    public Page listPage(String after, int limit) throws IOException {
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be positive, got " + limit);
        }
        final List<String> names = new ArrayList<>(container.listBlobs().keySet());
        names.sort(String::compareTo);

        final Map<String, IndexDescriptor> page = new LinkedHashMap<>();
        String last = null;
        for (String blobName : names) {
            if (after != null && blobName.compareTo(after) <= 0) {
                continue;
            }
            if (page.size() >= limit) {
                // There is more. Report a cursor rather than a total: knowing how many there are in
                // total is itself a full scan, and is the question this API refuses to answer.
                return new Page(page, last);
            }
            final Optional<BlobRegister> register = container.readRegister(blobName);
            if (register.isEmpty()) {
                continue;
            }
            if (isTombstone(register.get().value())) {
                last = blobName;
                continue;
            }
            try (InputStream in = register.get().value().streamInput()) {
                final IndexDescriptor descriptor = IndexDescriptor.fromStream(in);
                page.put(descriptor.name(), descriptor);
                last = blobName;
            }
        }
        return new Page(page, null);
    }

    /** A bounded page of descriptors, and where to resume. */
    public static final class Page {

        private final Map<String, IndexDescriptor> descriptors;
        private final String nextAfter;

        Page(Map<String, IndexDescriptor> descriptors, String nextAfter) {
            this.descriptors = Map.copyOf(descriptors);
            this.nextAfter = nextAfter;
        }

        /**
         * Returns the descriptors in this page.
         *
         * @return descriptors by index name
         */
        public Map<String, IndexDescriptor> descriptors() {
            return descriptors;
        }

        /**
         * Returns the cursor to resume from, or null when the walk is complete.
         *
         * @return the next cursor, or null
         */
        public String nextAfter() {
            return nextAfter;
        }

        /**
         * Reports whether another page follows.
         *
         * @return true when there is more
         */
        public boolean hasMore() {
            return nextAfter != null;
        }
    }

    /**
     * Lists every descriptor.
     *
     * <p><b>Do not put this on a request path.</b> It is O(population) by construction and phase 9
     * measured what that costs: 2N+1 operations when it backed {@code truthFor}. It survives for offline
     * work — a sweep that already intends to visit everything — and {@link #listPage} is what an API
     * uses. A caller that reaches for this to answer a user is answering the wrong question.
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
            if (isTombstone(register.get().value())) {
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
