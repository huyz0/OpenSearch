/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.store;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Gathers the segment blobs that a node's shards publish at about the same time into one node pack, so the node pays one
 * PUT for all of them.
 *
 * <p>A publish hands its blobs to {@link #add} and waits for the pack to land. The first blob into an empty batch makes
 * its caller the batch's leader: it waits up to the linger time, or until the batch holds {@code maxPackBytes}, then
 * takes the batch, writes it as one pack and tells every waiter where its blob landed. Blobs that arrive while a pack
 * is being written start the next batch. No thread of its own is needed: the publishing threads do the work, and a node
 * whose publishes never overlap writes one pack per publish, the same PUT count as without packs.
 *
 * <p>Publishes are lined up with the node log's window, so the shards a node publishes in one tick usually land in one
 * pack.
 *
 * <p>Each writer has its own epoch, taken from the clock when it is created, so a restarted node never reuses a name.
 * Ordinals count up from zero and are spent even by a pack whose write failed; a gap costs nothing, since nothing
 * reads packs by range.
 */
public final class NodePackWriter {

    /** Where one blob landed: the pack and the block. */
    public record Landed(String nodeId, long epoch, long ordinal, NodePackBlob.Block block) {

        /**
         * The location of a range of this block, for a manifest.
         *
         * @param offsetInBlock where the file starts within the block
         * @param length the file's length
         * @return its location in the pack
         */
        public FileLocation fileAt(long offsetInBlock, long length) {
            if (offsetInBlock < 0L || offsetInBlock + length > block.length()) {
                throw new IllegalArgumentException(
                    "range [" + offsetInBlock + ":" + length + "] is outside a block of " + block.length() + " bytes"
                );
            }
            return FileLocation.nodePacked(nodeId, epoch, ordinal, block.offset() + offsetInBlock, length);
        }
    }

    private static final class Pending {
        final NodePackStore.Part part;
        final CompletableFuture<Landed> landed = new CompletableFuture<>();

        Pending(NodePackStore.Part part) {
            this.part = part;
        }
    }

    private final NodePackStore store;
    private final long epoch;
    private final long lingerMillis;
    private final long maxPackBytes;
    private final AtomicLong nextOrdinal = new AtomicLong();

    private final Object lock = new Object();
    private List<Pending> batch = new ArrayList<>();
    private long batchBytes;

    /**
     * Creates a writer for one node's packs.
     *
     * @param store the node's packs
     * @param epoch this writer's epoch, which no earlier writer of the node used
     * @param lingerMillis how long the first blob of a batch waits for others
     * @param maxPackBytes the batch size that writes a pack without waiting out the linger
     */
    public NodePackWriter(NodePackStore store, long epoch, long lingerMillis, long maxPackBytes) {
        this.store = Objects.requireNonNull(store, "store");
        if (epoch < 0L || lingerMillis < 0L || maxPackBytes <= 0L) {
            throw new IllegalArgumentException("invalid node pack writer [" + epoch + "][" + lingerMillis + "][" + maxPackBytes + "]");
        }
        this.epoch = epoch;
        this.lingerMillis = lingerMillis;
        this.maxPackBytes = maxPackBytes;
    }

    /**
     * Returns the node whose packs this writes.
     *
     * @return the node id
     */
    public String nodeId() {
        return store.nodeId();
    }

    /**
     * Returns this writer's epoch.
     *
     * @return the epoch
     */
    public long epoch() {
        return epoch;
    }

    /**
     * Adds one shard's segment blobs to the next pack, all to the same one, and waits for the pack to land.
     *
     * @param parts the blobs of one publish
     * @return where each landed, in the same order
     * @throws IOException if the pack could not be written; the blobs are in no pack anyone can see
     */
    public List<Landed> add(List<NodePackStore.Part> parts) throws IOException {
        if (parts.isEmpty()) {
            return List.of();
        }
        final List<Pending> mine = new ArrayList<>(parts.size());
        for (NodePackStore.Part part : parts) {
            mine.add(new Pending(part));
        }
        final boolean leader;
        synchronized (lock) {
            leader = batch.isEmpty();
            batch.addAll(mine);
            for (NodePackStore.Part part : parts) {
                batchBytes += part.length();
            }
            if (batchBytes >= maxPackBytes) {
                lock.notifyAll();
            }
        }
        if (leader) {
            lead();
        }
        final List<Landed> landed = new ArrayList<>(mine.size());
        for (Pending pending : mine) {
            try {
                landed.add(pending.landed.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("interrupted waiting for a node pack");
            } catch (ExecutionException e) {
                throw new IOException("node pack write failed", e.getCause());
            }
        }
        return landed;
    }

    private void lead() {
        final List<Pending> taken;
        synchronized (lock) {
            final long deadline = System.nanoTime() + lingerMillis * 1_000_000L;
            long remaining;
            while (batchBytes < maxPackBytes && (remaining = deadline - System.nanoTime()) > 0L) {
                try {
                    lock.wait(Math.max(1L, remaining / 1_000_000L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            taken = batch;
            batch = new ArrayList<>();
            batchBytes = 0L;
        }
        final long ordinal = nextOrdinal.getAndIncrement();
        final List<NodePackStore.Part> parts = new ArrayList<>(taken.size());
        for (Pending pending : taken) {
            parts.add(pending.part);
        }
        try {
            final NodePackBlob.Header header = store.write(epoch, ordinal, parts);
            for (int i = 0; i < taken.size(); i++) {
                taken.get(i).landed.complete(new Landed(store.nodeId(), epoch, ordinal, header.blocks().get(i)));
            }
        } catch (Throwable t) {
            for (Pending pending : taken) {
                pending.landed.completeExceptionally(t);
            }
        }
    }
}
