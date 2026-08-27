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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.function.LongSupplier;

/**
 * Shard ownership, arbitrated by compare-and-swap on one register per shard.
 *
 * <p>This is section 6 of {@code rfc-serverless-metadata-plane.md} in code. There is no allocator and
 * no elected node: a candidate acquires a shard by winning a CAS on its head, and concurrent attempts
 * are made safe by the register rather than by agreement. Placement <em>quality</em> is somebody else's
 * problem — load-aware candidate selection, gossip hints. Placement <em>safety</em> is entirely here.
 *
 * <p><b>Rule: a live lease is not stolen.</b> If the head is held and unexpired by another node, this
 * refuses and returns the winner's head so the caller can route to it. The alternative — retrying
 * elsewhere — produces exactly the ping-pong the RFC warns about, where two nodes take turns owning a
 * shard neither can keep.
 *
 * <p>Per D5, exercised against {@code FsBlobContainer} only. R11 is what would let any of this be
 * claimed of S3 or GCS.
 */
public final class ShardHeadStore {

    private final BlobContainer container;
    private final LongSupplier clock;
    private final long leaseTtlMillis;
    private final LivenessOracle oracle;

    /**
     * Creates a store.
     *
     * @param container the container holding the {@code shards/} prefix
     * @param clock source of wall-clock millis, injected so tests need not sleep
     * @param leaseTtlMillis how far ahead an acquisition or renewal stamps its lease
     */
    public ShardHeadStore(BlobContainer container, LongSupplier clock, long leaseTtlMillis) {
        this(container, clock, leaseTtlMillis, null);
    }

    /**
     * Creates a store whose liveness comes from node leases rather than from per-head expiry.
     *
     * <p>This is §7's batching. Without an oracle each head carries its own expiry and every one must be
     * rewritten to stay alive; with one, a head is held for as long as its owner is, and a node renews
     * once however many shards it holds. Phase 8's measurement is what says the difference is worth
     * having: the per-shard write was the whole of the steady-state write cost.
     *
     * @param container the container holding the {@code shards/} prefix
     * @param clock source of wall-clock millis
     * @param leaseTtlMillis how far ahead an acquisition stamps its lease, still recorded for diagnosis
     * @param oracle answers whether an owner is alive, or null for per-head expiry
     */
    public ShardHeadStore(BlobContainer container, LongSupplier clock, long leaseTtlMillis, LivenessOracle oracle) {
        this.container = container;
        this.clock = clock;
        this.leaseTtlMillis = leaseTtlMillis;
        this.oracle = oracle;
    }

    /**
     * Reports whether a head is currently held, by whichever liveness rule this store was built with.
     *
     * @param head the head to judge
     * @param nowMillis the observer's clock
     * @return true when the head has a live owner
     */
    public boolean isHeld(ShardHead head, long nowMillis) {
        if (oracle == null) {
            return head.isHeldAt(nowMillis);
        }
        return head.ownerNodeId() != null && oracle.isLive(head.ownerNodeId(), head.ownerEphemeralId());
    }

    /**
     * Reads a shard-head.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the head, or empty if the shard was never activated
     * @throws IOException if the register cannot be read or parsed
     */
    public Optional<ShardHead> read(String indexName, int shardId) throws IOException {
        final Optional<BlobRegister> register = container.readRegister(RegisterMap.shardHeadBlob(indexName, shardId));
        if (register.isEmpty()) {
            return Optional.empty();
        }
        try (InputStream in = register.get().value().streamInput()) {
            return Optional.of(ShardHead.fromStream(in));
        }
    }

    /**
     * Attempts to acquire a shard, bumping its term.
     *
     * <p>Absent head: created at term 1 with put-if-absent, so a never-activated shard costs nothing
     * until someone wants it. Expired or unowned head: term is bumped and ownership taken. Live head
     * owned by someone else: refused, with the winner's head returned.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @param nodeId the acquiring node
     * @param ephemeralId the acquiring node's ephemeral id
     * @return whether the caller acquired it, and the authoritative head either way
     * @throws IOException if the register cannot be read or written
     */
    public Acquisition acquire(String indexName, int shardId, String nodeId, String ephemeralId) throws IOException {
        final String name = RegisterMap.shardHeadBlob(indexName, shardId);
        final long now = clock.getAsLong();
        final Optional<BlobRegister> existing = container.readRegister(name);

        if (existing.isEmpty()) {
            final ShardHead head = new ShardHead(indexName, shardId, 1L, nodeId, ephemeralId, now + leaseTtlMillis);
            final BlobRegisterCasResult created = container.createRegisterIfAbsent(name, head.toBytes());
            if (created.applied()) {
                return Acquisition.won(head);
            }
            // Lost a creation race. Re-read rather than assume: the winner is authoritative.
            return read(indexName, shardId).map(Acquisition::heldByAnother)
                .orElseThrow(() -> new IOException("shard head for " + indexName + "[" + shardId + "] vanished after a lost create race"));
        }

        final ShardHead current;
        try (InputStream in = existing.get().value().streamInput()) {
            current = ShardHead.fromStream(in);
        }
        if (isHeld(current, now) && nodeId.equals(current.ownerNodeId()) == false) {
            return Acquisition.heldByAnother(current);
        }

        final ShardHead next = new ShardHead(indexName, shardId, current.term() + 1, nodeId, ephemeralId, now + leaseTtlMillis);
        final BlobRegisterCasResult result = container.compareAndSwapRegister(name, existing.get().generation(), next.toBytes());
        if (result.applied()) {
            return Acquisition.won(next);
        }
        // Someone swapped it between our read and our write. They won; route to them.
        return read(indexName, shardId).map(Acquisition::heldByAnother)
            .orElseThrow(() -> new IOException("shard head for " + indexName + "[" + shardId + "] vanished after a lost CAS"));
    }

    /**
     * Extends the current owner's lease without bumping the term.
     *
     * <p>The term stays put on purpose. A term bump means ownership changed, and anything downstream
     * that treats it as a fence would be wrong to see one where nothing moved.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @param nodeId the node claiming to own it
     * @return the renewed head, or empty if this node no longer owns the shard
     * @throws IOException if the register cannot be read or written
     */
    public Optional<ShardHead> renew(String indexName, int shardId, String nodeId) throws IOException {
        final String name = RegisterMap.shardHeadBlob(indexName, shardId);
        final Optional<BlobRegister> existing = container.readRegister(name);
        if (existing.isEmpty()) {
            return Optional.empty();
        }
        final ShardHead current;
        try (InputStream in = existing.get().value().streamInput()) {
            current = ShardHead.fromStream(in);
        }
        if (nodeId.equals(current.ownerNodeId()) == false) {
            return Optional.empty();
        }
        final ShardHead renewed = new ShardHead(
            indexName,
            shardId,
            current.term(),
            nodeId,
            current.ownerEphemeralId(),
            clock.getAsLong() + leaseTtlMillis
        );
        final BlobRegisterCasResult result = container.compareAndSwapRegister(name, existing.get().generation(), renewed.toBytes());
        return result.applied() ? Optional.of(renewed) : Optional.empty();
    }

    /**
     * Gives up ownership without bumping the term, so the next acquirer starts from the current one.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @param nodeId the node giving up ownership
     * @return true if this node owned it and released it
     * @throws IOException if the register cannot be read or written
     */
    public boolean release(String indexName, int shardId, String nodeId) throws IOException {
        final String name = RegisterMap.shardHeadBlob(indexName, shardId);
        final Optional<BlobRegister> existing = container.readRegister(name);
        if (existing.isEmpty()) {
            return false;
        }
        final ShardHead current;
        try (InputStream in = existing.get().value().streamInput()) {
            current = ShardHead.fromStream(in);
        }
        if (nodeId.equals(current.ownerNodeId()) == false) {
            return false;
        }
        final ShardHead released = new ShardHead(indexName, shardId, current.term(), null, null, 0L);
        return container.compareAndSwapRegister(name, existing.get().generation(), released.toBytes()).applied();
    }

    /**
     * Removes every head belonging to an index, for delete-index.
     *
     * @param indexName the index
     * @param numberOfShards how many shards it had
     * @throws IOException if the delete fails
     */
    public void deleteAllFor(String indexName, int numberOfShards) throws IOException {
        for (int shard = 0; shard < numberOfShards; shard++) {
            container.deleteBlobsIgnoringIfNotExists(List.of(RegisterMap.shardHeadBlob(indexName, shard)));
        }
    }
}
