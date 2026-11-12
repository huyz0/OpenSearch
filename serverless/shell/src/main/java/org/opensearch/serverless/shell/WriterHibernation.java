/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.serverless.shell;

import org.opensearch.core.index.shard.ShardId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Writer shards this node has closed for idleness while keeping their heads: hibernated.
 *
 * <p><b>Why a state between open and released.</b> An open writer costs heap (its engine, and the index's mapper
 * when it is the index's last shard) and the periodic work every open shard is part of. A released one costs
 * nothing, but the next write to it pays a whole takeover: a head swap, the predecessor's lease and logs, a
 * rollup mark, a cold open. A hibernated writer costs what a released one does -- nothing is open, so nothing
 * periodic visits it -- and wakes with what an open costs once the head is known to be ours: one head read
 * instead of a takeover, and no new term.
 *
 * <p><b>Why waking at the same term is safe.</b> A writer hibernates only once everything it acknowledged is in a
 * published commit, under the shard's write fence, so the closed engine held nothing the commit lacks. The head
 * still names this node at the term it was taken at, and nobody can take it while this node's lease is live; if
 * the lease lapsed and somebody did, the head read on waking says so and the shard is taken the ordinary way. The
 * open at the same term fences the log at that term, so the closed instance could write nothing after it even if
 * it tried.
 *
 * <p>Held only in memory. A node that restarts has a new ephemeral id, so a head it left naming its old process
 * is one no open here would accept, and is taken the ordinary way.
 */
public final class WriterHibernation {

    /**
     * One hibernated writer.
     *
     * @param shardId the shard, of the incarnation it was open under
     * @param term the term it was open at, which the head still named when it hibernated
     * @param lastUsedMillis when a request last touched it, by the plane's clock
     * @param hibernatedAtMillis when it hibernated, by the plane's clock
     */
    public record Hibernated(ShardId shardId, long term, long lastUsedMillis, long hibernatedAtMillis) {
    }

    private final ConcurrentHashMap<String, Hibernated> byShard = new ConcurrentHashMap<>();
    private final AtomicLong hibernations = new AtomicLong();
    private final AtomicLong wakes = new AtomicLong();
    private final AtomicLong missedWakes = new AtomicLong();
    private final AtomicLong releases = new AtomicLong();

    WriterHibernation() {}

    /**
     * Records a writer as hibernated, before its engine closes, so a request arriving during the close wakes it.
     *
     * @param hibernated the writer
     */
    public void put(Hibernated hibernated) {
        byShard.put(ServerlessNode.shardKey(hibernated.shardId().getIndexName(), hibernated.shardId().id()), hibernated);
        hibernations.incrementAndGet();
    }

    /**
     * Forgets a writer recorded by {@link #put} whose engine could not be closed after all.
     *
     * @param hibernated the record put
     */
    public void undo(Hibernated hibernated) {
        if (byShard.remove(ServerlessNode.shardKey(hibernated.shardId().getIndexName(), hibernated.shardId().id()), hibernated)) {
            hibernations.decrementAndGet();
        }
    }

    /**
     * Takes the record of a hibernated writer, so exactly one caller wakes it.
     *
     * @param indexName the index
     * @param shard the shard
     * @return the record, or null if it is not hibernated here
     */
    Hibernated take(String indexName, int shard) {
        return byShard.remove(ServerlessNode.shardKey(indexName, shard));
    }

    /**
     * Forgets a hibernated writer whose head was given back, or was found to be somebody else's.
     *
     * @param hibernated the record
     * @return true if it was still recorded
     */
    public boolean released(Hibernated hibernated) {
        final boolean removed = byShard.remove(
            ServerlessNode.shardKey(hibernated.shardId().getIndexName(), hibernated.shardId().id()),
            hibernated
        );
        if (removed) {
            releases.incrementAndGet();
        }
        return removed;
    }

    /**
     * Whether a writer of this shard is hibernated here.
     *
     * @param indexName the index
     * @param shard the shard
     * @return true if it is
     */
    public boolean isHibernated(String indexName, int shard) {
        return byShard.containsKey(ServerlessNode.shardKey(indexName, shard));
    }

    /** @return every hibernated writer, a copy */
    public List<Hibernated> all() {
        return new ArrayList<>(byShard.values());
    }

    /** @return how many writers are hibernated here */
    public int size() {
        return byShard.size();
    }

    void noteWoken() {
        wakes.incrementAndGet();
    }

    void noteMissedWake() {
        missedWakes.incrementAndGet();
    }

    /** @return writers hibernated since the node started */
    public long hibernations() {
        return hibernations.get();
    }

    /** @return writers woken at the term they hibernated at */
    public long wakes() {
        return wakes.get();
    }

    /** @return hibernated writers whose head had moved by the time they were asked for, and were taken the ordinary way */
    public long missedWakes() {
        return missedWakes.get();
    }

    /** @return hibernated writers whose head was given back after a longer idle */
    public long releases() {
        return releases.get();
    }
}
