/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Atomic PIT state, separate from the active discovery inventory. Released IDs keep compact
 * tombstones outside that inventory: a delayed request can neither revive an ID nor remove a
 * later lifetime. Discovery markers remain readable by older collectors and conservatively pin
 * the index; authoritative state determines the actual files and deadline.
 */
final class PointInTimeStore {
    private static final Logger LOGGER = LogManager.getLogger(PointInTimeStore.class);
    private static final BytesReference RELEASED = new BytesArray("{\"_released\":true}");
    private final BlobContainer inventory;
    private final BlobContainer states;
    private final LongSupplier clock;

    PointInTimeStore(BlobContainer inventory, BlobContainer states, LongSupplier clock) {
        this.inventory = inventory;
        this.states = states;
        this.clock = clock;
    }

    private record State(BlobRegister register, PointInTime pin) {
    }

    private State decode(String id, BlobRegister register) throws IOException {
        if (RELEASED.equals(register.value())) {
            return new State(register, null);
        }
        final PointInTime pin = PointInTime.fromStream(register.value().streamInput());
        if (id.equals(pin.id()) == false) {
            throw new IOException("point in time identity does not match its key");
        }
        return new State(register, pin);
    }

    private Optional<State> state(String id) throws IOException {
        final var current = states.readRegister(id);
        if (current.isPresent()) {
            return Optional.of(decode(id, current.get()));
        }
        // Adopt records written before atomic PIT state was introduced. State presence is monotonic.
        final PointInTime legacy;
        try (var input = inventory.readBlob(id)) {
            legacy = PointInTime.fromStream(input);
        } catch (FileNotFoundException | NoSuchFileException missing) {
            return Optional.empty();
        }
        if (id.equals(legacy.id()) == false) {
            throw new IOException("point in time identity does not match its key");
        }
        states.createRegisterIfAbsent(id, legacy.toBytes());
        final var adopted = states.readRegister(id);
        if (adopted.isEmpty()) {
            throw new IOException("point in time state disappeared");
        }
        return Optional.of(decode(id, adopted.get()));
    }

    void create(PointInTime pin) throws IOException {
        if (inventory.blobExists(pin.id())) {
            throw new FileAlreadyExistsException(pin.id());
        }
        final var created = states.createRegisterIfAbsent(pin.id(), pin.toBytes());
        if (created.applied() == false) {
            throw new FileAlreadyExistsException(pin.id());
        }
        try {
            writeMarker(pin, true);
        } catch (IOException | RuntimeException failure) {
            try {
                states.compareAndSwapRegister(pin.id(), created.currentGeneration(), RELEASED);
                inventory.deleteBlobsIgnoringIfNotExists(List.of(pin.id()));
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private void writeMarker(PointInTime pin, boolean create) throws IOException {
        final var bytes = new PointInTime(pin.id(), pin.index(), pin.indexUuid(), Long.MAX_VALUE, Map.of(), true).toBytes();
        try (var input = bytes.streamInput()) {
            inventory.writeBlobAtomic(pin.id(), input, bytes.length(), create);
        }
    }

    private void protectLegacyMarker(PointInTime pin) throws IOException {
        try (var input = inventory.readBlob(pin.id())) {
            final var marker = PointInTime.fromStream(input);
            if (marker.isCapturing() && marker.expiresAtMillis() == Long.MAX_VALUE) {
                return;
            }
        } catch (FileNotFoundException | NoSuchFileException missing) {
            // A concurrent release can remove it. The state CAS below still prevents resurrection.
        }
        writeMarker(pin, false);
    }

    void finish(PointInTime pin) throws IOException {
        final var current = state(pin.id());
        if (current.isEmpty() || current.get().pin() == null || current.get().pin().isCapturing() == false || pin.isCapturing()) {
            throw new IOException("point in time capture is no longer active");
        }
        final var stored = current.get().pin();
        if (stored.index().equals(pin.index()) == false || Objects.equals(stored.indexUuid(), pin.indexUuid()) == false) {
            throw new IOException("point in time capture identity changed");
        }
        if (states.compareAndSwapRegister(
            pin.id(),
            current.get().register().generation(),
            pin.withExpiry(stored.expiresAtMillis()).toBytes()
        ).applied() == false) {
            throw new IOException("point in time capture changed while finishing");
        }
    }

    boolean extend(PointInTime pin, Long expectedExpiry) throws IOException {
        for (int attempt = 0; attempt < 16; attempt++) {
            final var current = state(pin.id());
            if (current.isEmpty() || current.get().pin() == null) {
                return false;
            }
            final var stored = current.get().pin();
            if (stored.isCapturing()
                || stored.expiredAt(clock.getAsLong())
                || (expectedExpiry != null && stored.expiresAtMillis() != expectedExpiry)) {
                return false;
            }
            final long deadline = Math.max(stored.expiresAtMillis(), pin.expiresAtMillis());
            if (deadline == stored.expiresAtMillis()) {
                return true;
            }
            protectLegacyMarker(stored);
            if (states.compareAndSwapRegister(pin.id(), current.get().register().generation(), stored.withExpiry(deadline).toBytes())
                .applied()) {
                return true;
            }
            if (expectedExpiry != null) {
                return false;
            }
        }
        throw new IOException("point in time extension conflicted repeatedly");
    }

    Optional<PointInTime> read(String id) throws IOException {
        return state(id).map(State::pin).filter(pin -> pin.isCapturing() == false);
    }

    boolean release(String id) throws IOException {
        for (int attempt = 0; attempt < 16; attempt++) {
            final var current = state(id);
            if (current.isEmpty() || current.get().pin() == null) {
                return false;
            }
            if (states.compareAndSwapRegister(id, current.get().register().generation(), RELEASED).applied()) {
                inventory.deleteBlobsIgnoringIfNotExists(List.of(id));
                return true;
            }
        }
        throw new IOException("point in time release conflicted repeatedly");
    }

    private record Expiry(PointInTime live, boolean removed) {
    }

    private Expiry expire(String id, long now) throws IOException {
        for (int attempt = 0; attempt < 16; attempt++) {
            final var current = state(id);
            if (current.isEmpty()) {
                return new Expiry(null, false);
            }
            final var pin = current.get().pin();
            if (pin == null) {
                inventory.deleteBlobsIgnoringIfNotExists(List.of(id));
                return new Expiry(null, false);
            }
            if (pin.expiredAt(now) == false) {
                return new Expiry(pin, false);
            }
            // Claim expiry before dropping the pin. A successful concurrent keep-alive changes
            // the generation and forces a reread, including in a collector's inventory pass.
            if (states.compareAndSwapRegister(id, current.get().register().generation(), RELEASED).applied()) {
                inventory.deleteBlobsIgnoringIfNotExists(List.of(id));
                return new Expiry(null, true);
            }
        }
        throw new IOException("point in time expiry conflicted repeatedly");
    }

    List<PointInTime> live(long now) throws IOException {
        final List<PointInTime> live = new ArrayList<>();
        for (String id : inventory.listBlobs().keySet()) {
            if (PointInTime.looksLikeAnId(id) == false) {
                continue;
            }
            try {
                final var expiry = expire(id, now);
                if (expiry.live() != null) {
                    live.add(expiry.live());
                }
            } catch (Exception failure) {
                LOGGER.warn("could not read point in time {}; treating it as live", id, failure);
                live.add(new PointInTime(id, "", Long.MAX_VALUE, Map.of()));
            }
        }
        return live;
    }

    int reap(long now) throws IOException {
        int removed = 0;
        for (String id : inventory.listBlobs().keySet()) {
            if (PointInTime.looksLikeAnId(id) == false) {
                continue;
            }
            try {
                if (expire(id, now).removed()) {
                    removed++;
                }
            } catch (Exception failure) {
                LOGGER.warn("could not expire point in time {}; leaving it alone", id, failure);
            }
        }
        return removed;
    }
}
