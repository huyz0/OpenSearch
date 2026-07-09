/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

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
 * A durable, CAS-backed registry of every shard known to have mirrored at least one operation
 * into a shared WAL container (rfc-serverless-opensearch.md &sect;6.4) -- the piece a future
 * retention sweep needs to know *whose* latest published {@code WalPosition} bounds what is still
 * safe to delete, since the WAL container itself carries no per-shard structure of its own (chunk
 * blobs are keyed purely by a globally-continuous sequence number, not by shard -- see {@link
 * WalChunkService}'s own class javadoc).
 *
 * <p><b>Deliberately grow-only</b>: this registry never removes an entry on its own. A sweep that
 * treats "known to use this WAL" as monotonically growing is safe by construction -- the same
 * shape {@code formal/CloneGc.tla}'s {@code Fixed} variant already proves sound for a different
 * mechanism (pin before read, never remove early) -- whereas *removing* an entry safely requires
 * knowing a shard will genuinely never publish through this container again, which is real,
 * separate design work (see this class's own removal method for what's actually safe to remove
 * today: an explicitly deleted index, not staleness or inactivity).
 */
public final class WalShardRegistry {

    private static final int MAX_CAS_ATTEMPTS = 50;
    private static final String REGISTER_NAME = "registered-shards";

    private final BlobContainer blobContainer;

    public WalShardRegistry(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    /** Every shard ever registered -- a full listing, expected to be read rarely (a retention sweep), not on any indexing hot path. */
    public Set<RegisteredShard> registeredShards() throws IOException {
        return blobContainer.readRegister(REGISTER_NAME).map(this::deserialize).orElseGet(HashSet::new);
    }

    /**
     * Idempotently records that {@code indexUuid}/{@code shardId} has mirrored into this
     * container. Safe, and expected, to call more than once for the same shard (e.g. once per
     * writer engine activation, not once ever) -- a no-op once already registered.
     */
    public void register(String indexUuid, int shardId) throws IOException {
        RegisteredShard shard = new RegisteredShard(indexUuid, shardId);
        mutate(current -> {
            if (current.contains(shard)) {
                return current; // already registered
            }
            Set<RegisteredShard> next = new HashSet<>(current);
            next.add(shard);
            return next;
        });
    }

    /**
     * Removes exactly one shard from the registry -- safe to call only when the caller has
     * independent proof this shard will never publish through this container again (today, that
     * proof is "its index was just deleted": see {@code ServerlessStoragePlugin}'s clone-pin
     * deletion listener for the established pattern this follows). A no-op if the shard was never
     * registered, or already removed.
     */
    public void deregister(String indexUuid, int shardId) throws IOException {
        RegisteredShard shard = new RegisteredShard(indexUuid, shardId);
        mutate(current -> {
            if (current.contains(shard) == false) {
                return current;
            }
            Set<RegisteredShard> next = new HashSet<>(current);
            next.remove(shard);
            return next;
        });
    }

    private void mutate(UnaryOperator<Set<RegisteredShard>> mutation) throws IOException {
        for (int attempt = 0; attempt < MAX_CAS_ATTEMPTS; attempt++) {
            long currentGeneration;
            Set<RegisteredShard> current;
            var existing = blobContainer.readRegister(REGISTER_NAME);
            if (existing.isPresent()) {
                currentGeneration = existing.get().generation();
                current = deserialize(existing.get());
            } else {
                currentGeneration = BlobRegister.ABSENT_GENERATION;
                current = new HashSet<>();
            }

            Set<RegisteredShard> next = mutation.apply(current);
            if (next.equals(current)) {
                return; // no-op mutation
            }

            BlobRegisterCasResult result = blobContainer.compareAndSwapRegister(REGISTER_NAME, currentGeneration, serialize(next));
            if (result.applied()) {
                return;
            }
            // conflict -- another registration raced us; re-read and retry.
        }
        throw new IOException("failed to update WAL shard registry after " + MAX_CAS_ATTEMPTS + " CAS attempts");
    }

    private Set<RegisteredShard> deserialize(BlobRegister register) {
        try {
            StreamInput in = register.value().streamInput();
            return new HashSet<>(in.readList(RegisteredShard::new));
        } catch (IOException e) {
            throw new IllegalStateException("failed to deserialize WAL shard registry", e);
        }
    }

    private static BytesReference serialize(Set<RegisteredShard> shards) throws IOException {
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeCollection(shards, (o, shard) -> shard.writeTo(o));
        return out.bytes();
    }
}
