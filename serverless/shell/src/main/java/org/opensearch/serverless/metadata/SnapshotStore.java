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
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.NoSuchFileException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/** CAS state for reusable snapshot names; discovery and copied files have unique lifetime identities. */
final class SnapshotStore {
    private static final Logger LOGGER = LogManager.getLogger(SnapshotStore.class);
    private static final BytesReference RELEASED = new BytesArray("{\"_released\":true}");
    private final BlobContainer inventory;
    private final BlobContainer states;

    SnapshotStore(BlobContainer inventory, BlobContainer states) {
        this.inventory = inventory;
        this.states = states;
    }

    private record State(BlobRegister register, String marker, SnapshotRecord snapshot, boolean finished) {
    }

    record Removed(SnapshotRecord snapshot, String marker) {
    }

    private static String hash(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String prefix(String repo) {
        return "v2-" + hash(repo) + "#";
    }

    private static BytesReference encode(String marker, SnapshotRecord snapshot, boolean finished) throws IOException {
        try (var output = new BytesStreamOutput()) {
            output.writeString(marker);
            output.writeBytesReference(snapshot.toBytes());
            output.writeBoolean(finished);
            return output.bytes();
        }
    }

    private static State decode(String key, BlobRegister register) throws IOException {
        if (RELEASED.equals(register.value())) {
            return new State(register, null, null, false);
        }
        try (var input = register.value().streamInput()) {
            final String marker = input.readString();
            final SnapshotRecord snapshot = SnapshotRecord.fromStream(input.readBytesReference().streamInput());
            final boolean finished = input.readBoolean();
            if ((key != null && key.equals(snapshot.key()) == false) || input.available() != 0) {
                throw new IOException("snapshot state identity or encoding is invalid");
            }
            return new State(register, marker, snapshot, finished);
        }
    }

    private Optional<State> state(String repo, String name) throws IOException {
        final String key = RegisterMap.snapshotKey(repo, name);
        final var current = states.readRegister(hash(key));
        if (current.isPresent()) {
            return Optional.of(decode(key, current.get()));
        }
        // A long legacy key is valid on cloud stores but cannot be resolved as one filesystem
        // component. Prefix discovery checks existence without constructing that component.
        if (key.getBytes(StandardCharsets.UTF_8).length > 255 && inventory.listBlobsByPrefix(key).containsKey(key) == false) {
            return Optional.empty();
        }
        final SnapshotRecord legacy;
        try (var input = inventory.readBlob(key)) {
            legacy = SnapshotRecord.fromStream(input);
        } catch (FileNotFoundException | NoSuchFileException missing) {
            return Optional.empty();
        }
        if (key.equals(legacy.key()) == false) {
            throw new IOException("snapshot identity does not match its key");
        }
        states.createRegisterIfAbsent(hash(key), encode(key, legacy, MetadataPlane.inProgress(legacy) == false));
        final var adopted = states.readRegister(hash(key));
        if (adopted.isEmpty()) {
            throw new IOException("snapshot state disappeared");
        }
        return Optional.of(decode(key, adopted.get()));
    }

    SnapshotRecord create(SnapshotRecord requested) throws IOException {
        final SnapshotRecord reserved = requested.withStorageGeneration(UUIDs.randomBase64UUID());
        final String stateKey = hash(reserved.key());
        final String marker = prefix(reserved.repo()) + stateKey + "#" + reserved.storageGeneration();
        final var bytes = encode(marker, reserved, false);
        long generation = 0;
        boolean won = false;
        for (int attempt = 0; attempt < 16; attempt++) {
            final var current = state(reserved.repo(), reserved.name());
            if (current.isPresent() && current.get().snapshot() != null) {
                throw new SnapshotAlreadyExistsException(reserved.repo(), reserved.name());
            }
            final var result = current.isEmpty()
                ? states.createRegisterIfAbsent(stateKey, bytes)
                : states.compareAndSwapRegister(stateKey, current.get().register().generation(), bytes);
            if (result.applied()) {
                generation = result.currentGeneration();
                won = true;
                break;
            }
        }
        if (won == false) {
            throw new IOException("snapshot reservation conflicted repeatedly");
        }
        // Publish a protective marker before the producer reads manifests. Older collectors can
        // read its ordinary JSON; current collectors resolve the exact state through the name head.
        final Map<String, SnapshotRecord.SnapshottedIndex> indices = new LinkedHashMap<>();
        for (var entry : reserved.indices().entrySet()) {
            final var index = entry.getValue();
            indices.put(
                entry.getKey(),
                new SnapshotRecord.SnapshottedIndex(index.uuid(), index.numberOfShards(), null, Map.of(), Map.of())
            );
        }
        final var protective = new SnapshotRecord(
            reserved.repo(),
            reserved.name(),
            reserved.uuid(),
            reserved.shallow(),
            reserved.startTimeMillis(),
            reserved.startTimeMillis(),
            indices
        ).withStorageGeneration(reserved.storageGeneration()).toBytes();
        try (var input = protective.streamInput()) {
            inventory.writeBlobAtomic(marker, input, protective.length(), true);
            final var published = states.readRegister(stateKey);
            if (published.isEmpty() || marker.equals(decode(reserved.key(), published.get()).marker()) == false) {
                throw new IOException("snapshot was deleted while publishing its capture marker");
            }
        } catch (IOException | RuntimeException failure) {
            try {
                boolean mayDelete = states.compareAndSwapRegister(stateKey, generation, RELEASED).applied();
                if (mayDelete == false) {
                    final var current = states.readRegister(stateKey);
                    mayDelete = current.isEmpty() || marker.equals(decode(reserved.key(), current.get()).marker()) == false;
                }
                if (mayDelete) {
                    inventory.deleteBlobsIgnoringIfNotExists(List.of(marker));
                }
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        return reserved;
    }

    void finish(SnapshotRecord finished) throws IOException {
        final var current = state(finished.repo(), finished.name());
        if (current.isEmpty()
            || current.get().snapshot() == null
            || current.get().finished()
            || sameLifetime(current.get().snapshot(), finished) == false) {
            throw new IOException("snapshot capture is no longer active");
        }
        if (current.get().snapshot().shallow() != finished.shallow()) {
            throw new IOException("snapshot storage mode changed while capturing");
        }
        if (states.compareAndSwapRegister(
            hash(finished.key()),
            current.get().register().generation(),
            encode(current.get().marker(), finished, true)
        ).applied() == false) {
            throw new IOException("snapshot changed while finishing capture");
        }
    }

    private static boolean sameLifetime(SnapshotRecord left, SnapshotRecord right) {
        return Objects.equals(left.uuid(), right.uuid()) && Objects.equals(left.storageGeneration(), right.storageGeneration());
    }

    Optional<SnapshotRecord> read(String repo, String name) throws IOException {
        return state(repo, name).map(State::snapshot);
    }

    Optional<Removed> remove(String repo, String name, SnapshotRecord expected) throws IOException {
        SnapshotRecord lifetime = expected;
        for (int attempt = 0; attempt < 16; attempt++) {
            final var current = state(repo, name);
            if (current.isEmpty() || current.get().snapshot() == null) {
                return Optional.empty();
            }
            final var record = current.get().snapshot();
            if (lifetime == null) {
                lifetime = record;
            } else if (sameLifetime(lifetime, record) == false) {
                return Optional.empty();
            }
            if (states.compareAndSwapRegister(hash(record.key()), current.get().register().generation(), RELEASED).applied()) {
                return Optional.of(new Removed(record, current.get().marker()));
            }
        }
        throw new IOException("snapshot deletion conflicted repeatedly");
    }

    void removeMarker(Removed removed) throws IOException {
        // Every new lifetime uses a different marker. Legacy keys are never reused for new markers.
        inventory.deleteBlobsIgnoringIfNotExists(List.of(removed.marker()));
    }

    private static boolean isHash(String value) {
        if (value.length() != 64) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            final char c = value.charAt(i);
            if ((c < '0' || c > '9') && (c < 'a' || c > 'f')) {
                return false;
            }
        }
        return true;
    }

    private Optional<SnapshotRecord> fromMarker(String marker) throws IOException {
        final String[] parts = marker.split("#", -1);
        final boolean currentLayout = parts.length == 3
            && parts[0].startsWith("v2-")
            && parts[0].length() == 67
            && isHash(parts[0].substring(3))
            && isHash(parts[1])
            && PointInTime.looksLikeAnId(parts[2]);
        final String keyHash = currentLayout ? parts[1] : hash(marker);
        final var known = states.readRegister(keyHash);
        if (known.isPresent()) {
            final var decoded = decode(null, known.get());
            if (decoded.snapshot() != null && keyHash.equals(hash(decoded.snapshot().key())) == false) {
                throw new IOException("snapshot state does not match the discovered key");
            }
            return marker.equals(decoded.marker()) ? Optional.ofNullable(decoded.snapshot()) : Optional.empty();
        }
        if (currentLayout) {
            throw new IOException("snapshot discovery marker has no authoritative state");
        }
        final SnapshotRecord discovered;
        try (var input = inventory.readBlob(marker)) {
            discovered = SnapshotRecord.fromStream(input);
        } catch (FileNotFoundException | NoSuchFileException missing) {
            return Optional.empty();
        }
        if (marker.equals(discovered.key()) == false) {
            throw new IOException("legacy snapshot identity does not match its discovery key");
        }
        final var current = state(discovered.repo(), discovered.name());
        if (current.isEmpty()) {
            return Optional.empty();
        }
        return current.filter(value -> marker.equals(value.marker())).map(State::snapshot);
    }

    List<SnapshotRecord> list(String repo) throws IOException {
        final List<SnapshotRecord> found = new ArrayList<>();
        final var names = new java.util.LinkedHashSet<>(inventory.listBlobsByPrefix(repo + "#").keySet());
        names.addAll(inventory.listBlobsByPrefix(prefix(repo)).keySet());
        for (String marker : names) {
            try {
                final var record = fromMarker(marker);
                if (record.isPresent() && repo.equals(record.get().repo())) {
                    found.add(record.get());
                }
            } catch (Exception failure) {
                LOGGER.warn("could not read snapshot {}; omitting it from the listing", marker, failure);
            }
        }
        return found;
    }

    List<SnapshotRecord> live() throws IOException {
        final List<SnapshotRecord> found = new ArrayList<>();
        for (String marker : inventory.listBlobs().keySet()) {
            final int separator = marker.indexOf('#');
            if (separator <= 0 || separator == marker.length() - 1) {
                continue;
            }
            try {
                fromMarker(marker).ifPresent(found::add);
            } catch (Exception failure) {
                throw new IOException("cannot establish snapshot protection: could not read snapshot [" + marker + "]", failure);
            }
        }
        return found;
    }
}
