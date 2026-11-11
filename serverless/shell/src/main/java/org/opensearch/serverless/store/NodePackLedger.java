/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobRegister;
import org.opensearch.common.blobstore.BlobRegisterCasResult;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Which of a node's packs may still exist, so collection can name every one without listing.
 *
 * <p>One register per node, at {@code packs/<node>/ledger}, holding each epoch the node has written packs in, how far
 * collection has got through it, and the highest ordinal it has recorded. Every ordinal of an epoch below
 * {@code collectedBelow} is gone; every one at or above it may exist, up to {@code high} as last recorded, and a little
 * past it if the writer died between recording and writing. A writer records its epoch here before it writes its first
 * pack, so no pack is ever written in an epoch collection does not know about.
 *
 * <p>The register is written when a writer starts and when a collection pass moves something on, never per pack.
 */
public final class NodePackLedger {

    /** The register's name inside the node's pack directory. */
    public static final String NAME = "ledger";
    private static final byte VERSION = 1;
    private static final int MAX_ATTEMPTS = 16;

    /**
     * One epoch's progress.
     *
     * @param epoch the epoch
     * @param collectedBelow every ordinal below this is gone
     * @param high one past the highest ordinal recorded as used; packs may exist a little past it
     */
    public record Epoch(long epoch, long collectedBelow, long high) {

        /**
         * Validates an epoch.
         *
         * @param epoch the epoch
         * @param collectedBelow the collected prefix
         * @param high the recorded end
         */
        public Epoch {
            if (epoch < 0L || collectedBelow < 0L || high < 0L) {
                throw new IllegalArgumentException("invalid ledger epoch [" + epoch + "][" + collectedBelow + "][" + high + "]");
            }
        }
    }

    /**
     * The ledger as read.
     *
     * @param generation the register's generation, to swap against
     * @param epochs every epoch, oldest first
     */
    public record State(long generation, List<Epoch> epochs) {

        /**
         * Finds one epoch.
         *
         * @param epoch the epoch
         * @return its progress, if the ledger has it
         */
        public Optional<Epoch> epoch(long epoch) {
            return epochs.stream().filter(e -> e.epoch() == epoch).findFirst();
        }
    }

    private final BlobContainer container;
    private final String nodeId;

    /**
     * Opens one node's ledger.
     *
     * @param blobStore the object store
     * @param base the cluster's base path
     * @param nodeId the node whose packs it records
     */
    public NodePackLedger(BlobStore blobStore, BlobPath base, String nodeId) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.container = blobStore.blobContainer(base.add(NodePackStore.ROOT).add(nodeId));
    }

    /**
     * Returns the node whose ledger this is.
     *
     * @return the node id
     */
    public String nodeId() {
        return nodeId;
    }

    /**
     * Reads the ledger.
     *
     * @return its state; no epochs if it was never written
     * @throws IOException if the read fails or the register is not a ledger
     */
    public State read() throws IOException {
        final Optional<BlobRegister> register = container.readRegister(NAME);
        if (register.isEmpty()) {
            return new State(BlobRegister.ABSENT_GENERATION, List.of());
        }
        return new State(register.get().generation(), decode(register.get().value()));
    }

    /**
     * Records an epoch a writer is about to write packs in. Must return before the writer writes its first pack.
     *
     * @param epoch the new epoch
     * @return the ledger with it
     * @throws IOException if the ledger cannot be written
     */
    public State addEpoch(long epoch) throws IOException {
        return update(read(), epochs -> {
            if (epochs.stream().anyMatch(e -> e.epoch() == epoch)) {
                return epochs;
            }
            final List<Epoch> next = new ArrayList<>(epochs);
            next.add(new Epoch(epoch, 0L, 0L));
            return next;
        });
    }

    /**
     * Applies a change to the ledger with compare-and-swap, re-reading and re-applying on a conflict. Writes nothing
     * when the change leaves the epochs as they are.
     *
     * @param from the state the caller last saw
     * @param change the change, applied to the epochs as stored
     * @return the ledger as written, or as it stood if nothing changed
     * @throws IOException if the ledger cannot be read or written
     */
    public State update(State from, UnaryOperator<List<Epoch>> change) throws IOException {
        State current = from;
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            final List<Epoch> next = List.copyOf(change.apply(current.epochs()));
            if (next.equals(current.epochs())) {
                return current;
            }
            final BytesReference bytes = encode(next);
            final BlobRegisterCasResult result = current.generation() == BlobRegister.ABSENT_GENERATION
                ? container.createRegisterIfAbsent(NAME, bytes)
                : container.compareAndSwapRegister(NAME, current.generation(), bytes);
            if (result.applied()) {
                return new State(result.currentGeneration(), next);
            }
            current = read();
        }
        throw new IOException("the node pack ledger of [" + nodeId + "] kept moving under " + MAX_ATTEMPTS + " attempts to update it");
    }

    static BytesReference encode(List<Epoch> epochs) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (DataOutputStream data = new DataOutputStream(out)) {
            data.writeByte(VERSION);
            data.writeInt(epochs.size());
            for (Epoch epoch : epochs) {
                data.writeLong(epoch.epoch());
                data.writeLong(epoch.collectedBelow());
                data.writeLong(epoch.high());
            }
        }
        return new BytesArray(out.toByteArray());
    }

    static List<Epoch> decode(BytesReference bytes) throws IOException {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(BytesReference.toBytes(bytes)))) {
            final byte version = in.readByte();
            if (version != VERSION) {
                throw new IOException("node pack ledger of version " + version + "; this node reads " + VERSION);
            }
            final int count = in.readInt();
            if (count < 0) {
                throw new IOException("node pack ledger names " + count + " epochs");
            }
            final List<Epoch> epochs = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                try {
                    epochs.add(new Epoch(in.readLong(), in.readLong(), in.readLong()));
                } catch (IllegalArgumentException e) {
                    throw new IOException("malformed node pack ledger", e);
                }
            }
            return List.copyOf(epochs);
        }
    }
}
