/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.warm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.node.NodeContext;
import org.opensearch.serverless.node.SearchReadiness;
import org.opensearch.serverless.node.SearchSettings;
import org.opensearch.serverless.transport.CacheWarmCopy;
import org.opensearch.serverless.transport.HandOffResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Takes a search node out of search traffic before it stops, without leaving the shards it served cold.
 *
 * <ol>
 *   <li>It turns {@link SearchReadiness.State#DRAINING}: the readiness endpoint answers 503 at once, so the ingress
 *       stops sending it requests, and its lease is renewed without the search role, so coordinators stop ranking it.</li>
 *   <li>It asks each search peer to take over its share ({@link SearchWarmup#takeOverFrom}): each copies the warm
 *       blocks of the shards it now ranks first for, from this node, and opens them.</li>
 *   <li>It waits {@code serverless.search.drain.settle}, about as long as a coordinator takes to see the role gone, still answering any
 *       search that reaches it meanwhile.</li>
 * </ol>
 *
 * <p>All of it within {@code serverless.search.drain.max_wait}: a peer that is slow to take over is not waited on past it, and its shards
 * warm from searches as they would have without a drain. The lease is released afterwards, by shutdown, as always.
 */
public final class SearchDrain {

    private static final Logger LOG = LogManager.getLogger(SearchDrain.class);

    /**
     * What a drain did.
     *
     * @param peersAsked search peers asked to take over
     * @param peersFailed peers that could not be reached or did not answer in time
     * @param shardsTakenOver shards the peers opened
     * @param blocksHandedOver blocks the peers copied from this node
     * @param tookMillis how long it took, settling included
     */
    public record Outcome(int peersAsked, int peersFailed, int shardsTakenOver, long blocksHandedOver, long tookMillis) {
    }

    private SearchDrain() {}

    /**
     * Whether a node drains before it shuts down.
     *
     * @param node the node
     * @return true if it serves searches and is configured to
     */
    public static boolean onShutdown(NodeContext node) {
        return SearchReadiness.of(node).servesSearches() && SearchSettings.DRAIN_ON_SHUTDOWN.get(node.settings());
    }

    /**
     * Drains a node. A second call, or a call on a node that does not serve searches, does nothing.
     *
     * @param node the node
     * @param plane its metadata plane, or null before one is attached
     * @return what was done
     */
    public static Outcome drain(NodeContext node, MetadataPlane plane) {
        final long started = System.nanoTime();
        final SearchReadiness readiness = SearchReadiness.of(node);
        if (readiness.servesSearches() == false || readiness.startDraining() == false || plane == null) {
            return new Outcome(0, 0, 0, 0, 0);
        }
        try {
            node.leases().renewLease(plane);
        } catch (Exception e) {
            LOG.warn("could not drop the search role from this node's lease; coordinators see it go when the lease does", e);
        }
        final String self = node.localNode().getId();
        final List<String> peers = new ArrayList<>();
        final long now = plane.clock().getAsLong();
        try {
            for (NodeLease lease : plane.membership().refresh()) {
                if (lease.nodeId().equals(self) == false
                    && lease.roles().contains(NodeContext.ROLE_SEARCH)
                    && lease.isExpiredAt(now) == false) {
                    peers.add(lease.nodeId());
                }
            }
        } catch (Exception e) {
            LOG.warn("could not read the members to hand this node's shards to; they warm from searches instead", e);
        }
        peers.sort(null);
        final long deadline = System.nanoTime() + SearchSettings.DRAIN_MAX_WAIT.get(node.settings()).nanos();
        int failed = 0;
        int shards = 0;
        long blocks = 0;
        for (String peer : peers) {
            final long left = deadline - System.nanoTime();
            if (left <= 0) {
                failed++;
                continue;
            }
            try {
                final HandOffResponse answer = CacheWarmCopy.of(node).handOffTo(peer, TimeValue.timeValueNanos(left));
                if (answer == null) {
                    failed++;
                } else {
                    shards += answer.opened();
                    blocks += answer.blocksCopied();
                }
            } catch (Exception e) {
                failed++;
                LOG.warn("search peer [" + peer + "] did not take over this node's shards", e);
            }
        }
        if (peers.isEmpty() == false) {
            final TimeValue settle = node.settings()
                .getAsTime(SearchSettings.DRAIN_SETTLE.getKey(), TimeValue.timeValueMillis(plane.leaseTtlMillis() * 2 / 3));
            try {
                Thread.sleep(settle.millis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        final Outcome outcome = new Outcome(
            peers.size(),
            failed,
            shards,
            blocks,
            TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
        );
        LOG.info("search drain done: {}", outcome);
        return outcome;
    }
}
