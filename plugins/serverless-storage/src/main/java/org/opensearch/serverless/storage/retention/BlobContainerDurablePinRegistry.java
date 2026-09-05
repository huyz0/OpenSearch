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
    public void confirmPin(String indexUuid, int shardId, String pinId, long expiresAtMillis) throws IOException {
        mutate(indexUuid, shardId, current -> {
            Set<PinRecord> next = new HashSet<>();
            boolean changed = false;
            for (PinRecord existing : current) {
                if (existing.pinId().equals(pinId) && existing.expiresAtMillis() != expiresAtMillis) {
                    next.add(existing.withExpiry(expiresAtMillis));
                    changed = true;
                } else {
                    next.add(existing);
                }
            }
            return changed ? next : current;
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

    @Override
    public void replacePin(String indexUuid, int shardId, PinRecord newPin) throws IOException {
        mutate(indexUuid, shardId, current -> {
            Set<PinRecord> next = new HashSet<>();
            next.add(newPin);
            for (PinRecord existing : current) {
                if (existing.pinId().equals(newPin.pinId()) == false) {
                    next.add(existing);
                }
            }
            return next;
        });
    }

    @Override
    public void applyPinDiff(String indexUuid, int shardId, java.util.Collection<PinRecord> toAdd, java.util.Collection<PinRecord> toRemove)
        throws IOException {
        if (toAdd.isEmpty() && toRemove.isEmpty()) {
            return;
        }
        mutate(indexUuid, shardId, current -> {
            Set<PinRecord> next = new HashSet<>(current);
            boolean changed = false;
            // Add-then-remove within one computed set, so a pin named by both collections ends up removed --
            // the same result the one-call-at-a-time default produces. `changed` is tracked from each set
            // operation's own answer rather than by comparing the two sets afterwards: PinRecord equality
            // excludes expiry, so a set comparison is exactly the check mutate's own javadoc explains is
            // unsafe here.
            for (PinRecord pin : toAdd) {
                changed |= next.add(pin);
            }
            for (PinRecord pin : toRemove) {
                changed |= next.remove(pin);
            }
            return changed ? next : current;
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
            if (next == current) {
                // Reference, not equality, and the difference is load-bearing. PinRecord's equality is its
                // identity -- pin id, term, generation -- and deliberately excludes the owner and expiry,
                // so that adding the same pin twice is the no-op it is documented to be. That makes a set
                // whose expiries have been re-stamped equal to the set before, so an equality check here
                // silently discards exactly the mutation confirmPin exists to perform. Every mutation
                // above returns `current` itself when it means no-op, so identity is both sufficient and
                // exact.
                return;
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

    /**
     * Marks a register written after pins gained an owner and an expiry.
     *
     * <p>The old format begins with the collection's size as a vInt, so any value a pin count could never
     * take identifies the new one. Needed because these registers are durable: a deployment upgrading into
     * this change has pins on disk in the old shape, and reading five fields out of a three-field record
     * would not fail cleanly -- it would read into the next record and produce nonsense, which for a
     * registry that decides what garbage collection may delete is the worst available outcome.
     */
    private static final int VERSIONED_MARKER = 0x7FFF_FFF0;

    /** The only version written today. Present so the next change has somewhere to go. */
    private static final int VERSION_WITH_OWNER_AND_EXPIRY = 1;

    private Set<PinRecord> deserialize(BlobRegister register) {
        try {
            StreamInput in = register.value().streamInput();
            int first = in.readVInt();
            if (first != VERSIONED_MARKER) {
                // The old shape, and `first` was its collection size. Read exactly that many, as records
                // that never expire -- see PinRecord#readLegacy for why that is the only safe reading.
                Set<PinRecord> legacy = new HashSet<>(first);
                for (int i = 0; i < first; i++) {
                    legacy.add(PinRecord.readLegacy(in));
                }
                return legacy;
            }
            in.readVInt(); // version, only one so far
            return new HashSet<>(in.readList(PinRecord::new));
        } catch (IOException e) {
            throw new IllegalStateException("failed to deserialize pin registry", e);
        }
    }

    private static BytesReference serialize(Set<PinRecord> pins) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeVInt(VERSIONED_MARKER);
        out.writeVInt(VERSION_WITH_OWNER_AND_EXPIRY);
        out.writeCollection(pins, (o, pin) -> pin.writeTo(o));
        return out.bytes();
    }

    private static String registerName(String indexUuid, int shardId) {
        return "pins-" + indexUuid + "-" + shardId;
    }
}
