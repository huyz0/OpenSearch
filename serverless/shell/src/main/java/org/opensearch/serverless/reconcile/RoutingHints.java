/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.reconcile;

import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ShardHead;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Which node is probably serving which shard.
 *
 * <p>Soft state, and the word matters: this is a cache of shard-heads, not a source of truth, and a
 * wrong entry costs a retry rather than correctness. Nothing here is agreed with anyone, no node is
 * obliged to keep it fresh, and two nodes holding different hints forever is the normal case.
 *
 * <p>{@code rfc-serverless-shell.md} §10.3 dissolved the dedicated directory tier this would have lived
 * in: gossip, if it is ever built, is how these spread. Until then a node refreshes its own, which is
 * the cheapest thing that works and is honest about what it is.
 *
 * <p>The one rule: <b>never write based on a hint.</b> Ownership is answered by the shard-head, which
 * is a read away. A hint may say a node owns a shard it lost a second ago, and acting on that without
 * checking is how two writers end up believing the same thing.
 */
public final class RoutingHints {

    /** Creates an empty set of hints; nothing is known until the first refresh. */
    public RoutingHints() {}

    private volatile Map<String, String> hints = Map.of();
    private volatile long refreshedAtMillis;

    /**
     * Rebuilds the hints from the shard-heads of the given indices.
     *
     * @param plane the metadata plane
     * @param shardCounts index name to shard count
     * @param nowMillis the observer's clock, recorded so staleness is visible
     * @return how many shards were found to have an owner
     * @throws IOException if the heads cannot be read
     */
    public int refresh(MetadataPlane plane, Map<String, Integer> shardCounts, long nowMillis) throws IOException {
        return refresh((index, shard) -> plane.heads().read(index, shard), shardCounts, nowMillis);
    }

    /** Where a head comes from: the register, or a copy read moments ago by the same pass. */
    @FunctionalInterface
    public interface HeadLookup {
        /**
         * Reads one shard's head.
         *
         * @param index the index
         * @param shard the shard number
         * @return the head, or empty if the shard was never activated
         * @throws IOException if the register cannot be read
         */
        Optional<ShardHead> read(String index, int shard) throws IOException;
    }

    /**
     * Rebuilds the hints from the given source of heads.
     *
     * @param heads where to read each head
     * @param shardCounts index name to shard count
     * @param nowMillis the clock
     * @return how many hints were rebuilt
     * @throws IOException if a head cannot be read
     */
    public int refresh(HeadLookup heads, Map<String, Integer> shardCounts, long nowMillis) throws IOException {
        final Map<String, String> rebuilt = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> index : shardCounts.entrySet()) {
            for (int shard = 0; shard < index.getValue(); shard++) {
                final Optional<ShardHead> head = heads.read(index.getKey(), shard);
                if (head.isPresent() && head.get().ownerNodeId() != null) {
                    rebuilt.put(key(index.getKey(), shard), head.get().ownerNodeId());
                }
            }
        }
        hints = Map.copyOf(rebuilt);
        refreshedAtMillis = nowMillis;
        return hints.size();
    }

    /**
     * Returns the node believed to serve a shard.
     *
     * @param indexName the index
     * @param shardId the shard number
     * @return the node id, or empty if unknown
     */
    public Optional<String> nodeFor(String indexName, int shardId) {
        return Optional.ofNullable(hints.get(key(indexName, shardId)));
    }

    /**
     * Returns when these hints were last rebuilt, on the observer's clock.
     *
     * @return the refresh time in millis
     */
    public long refreshedAtMillis() {
        return refreshedAtMillis;
    }

    /**
     * Returns how many shards currently have a hint.
     *
     * @return the hint count
     */
    public int size() {
        return hints.size();
    }

    private static String key(String indexName, int shardId) {
        return indexName + "#" + shardId;
    }
}
