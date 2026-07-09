/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * {@link DurablePinRegistry} backed by any {@link BlobContainer} that implements
 * {@link BlobContainer#compareAndSwapRegister}, mirroring
 * {@link org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore}: the whole
 * pin set for one shard lives in a single register blob, and additions/removals are
 * read-modify-CAS-retry loops rather than lock-based mutation, so this works unchanged against
 * any backend that implements the primitive.
 */
public final class BlobContainerDurablePinRegistry implements DurablePinRegistry {

    private static final int MAX_CAS_ATTEMPTS = 50;

    private final BlobContainer blobContainer;

    /**
     * Creates a registry backed by the given blob container.
     *
     * @param blobContainer the container holding the per-shard pin register blobs; must support
     *                      {@link BlobContainer#compareAndSwapRegister}.
     */
    public BlobContainerDurablePinRegistry(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    @Override
    public Set<PinRecord> getPins(String indexUuid, int shardId) throws IOException {
        return blobContainer.readRegister(registerName(indexUuid, shardId)).map(this::deserialize).orElseGet(HashSet::new);
    }

    @Override
    public void addPin(String indexUuid, int shardId, PinRecord pin) throws IOException {
        mutate(indexUuid, shardId, current -> {
            if (current.contains(pin)) {
                return current; // idempotent: already pinned
            }
            Set<PinRecord> next = new HashSet<>(current);
            next.add(pin);
            return next;
        });
    }

    @Override
    public void removePin(String indexUuid, int shardId, String pinId) throws IOException {
        mutate(indexUuid, shardId, current -> {
            Set<PinRecord> next = new HashSet<>();
            boolean changed = false;
            for (PinRecord existing : current) {
                if (existing.pinId().equals(pinId)) {
                    changed = true;
                } else {
                    next.add(existing);
                }
            }
            return changed ? next : current;
        });
    }

    @Override
    public void removePin(String indexUuid, int shardId, PinRecord pin) throws IOException {
        mutate(indexUuid, shardId, current -> {
            if (current.contains(pin) == false) {
                return current; // no-op: that exact pin was never present
            }
            Set<PinRecord> next = new HashSet<>(current);
            next.remove(pin);
            return next;
        });
    }

    private void mutate(String indexUuid, int shardId, UnaryOperator<Set<PinRecord>> mutation) throws IOException {
        String registerName = registerName(indexUuid, shardId);
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            long currentGeneration;
            Set<PinRecord> current;
            var existing = blobContainer.readRegister(registerName);
            if (existing.isPresent()) {
                currentGeneration = existing.get().generation();
                current = deserialize(existing.get());
            } else {
                currentGeneration = BlobRegister.ABSENT_GENERATION;
                current = new HashSet<>();
            }

            Set<PinRecord> next = mutation.apply(current);
            if (next.equals(current)) {
                return; // no-op mutation (idempotent add of an existing pin, or remove of an absent one)
            }

            BlobRegisterCasResult result = blobContainer.compareAndSwapRegister(registerName, currentGeneration, serialize(next));
            if (result.applied()) {
                return;
            }
            // conflict -- another pin/unpin raced us; re-read and retry the mutation against the new state.
        }
        throw new IOException(
            "failed to update pin registry for " + indexUuid + "/" + shardId + " after " + MAX_CAS_ATTEMPTS + " CAS attempts"
        );
    }

    private Set<PinRecord> deserialize(BlobRegister register) {
        try {
            StreamInput in = register.value().streamInput();
            return new HashSet<>(in.readList(PinRecord::new));
        } catch (IOException e) {
            throw new IllegalStateException("failed to deserialize pin registry", e);
        }
    }

    private static BytesReference serialize(Set<PinRecord> pins) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeCollection(pins, (o, pin) -> pin.writeTo(o));
        return out.bytes();
    }

    private static String registerName(String indexUuid, int shardId) {
        return "pins-" + indexUuid + "-" + shardId;
    }
}
