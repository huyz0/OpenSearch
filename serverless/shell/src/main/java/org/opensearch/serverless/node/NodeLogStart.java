/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.node;

/**
 * Starts this node's write log the first time anything writes, and renews the lease that names its epochs.
 */
public final class NodeLogStart {

    private final NodeContext node;

    /** This node's write log, once started. */
    volatile NodeLogWriter nodeLogWriter;

    /** Whether the reconciler has been given {@link #nodeLogWriter}, which may start before the reconciler exists. */
    volatile boolean nodeLogHooked;

    /**
     * Until when, by the plane's clock, this node's own lease is known valid: the start of its last successful
     * renewal plus the TTL. Its own, not the membership's, which several nodes in one test share.
     */
    volatile long ownLeaseValidUntilMillis = 0L;

    NodeLogStart(NodeContext node) {
        this.node = node;
    }

    /**
     * Returns this node's write log, starting it on first use: it reads the epochs an earlier process of this node left in
     * the lease, before anything renews over them.
     */
    public NodeLogWriter nodeLog(org.opensearch.serverless.metadata.MetadataPlane plane) throws java.io.IOException {
        final NodeLogWriter started = nodeLogWriter;
        if (started != null && nodeLogHooked) {
            return started;
        }
        synchronized (node) {
            if (nodeLogWriter == null) {
                final NodeLogWriter writer = NodeLogWriter.start(plane, node.localNode.getId(), (epochs, opensEpoch) -> {
                    final long validUntil = ownLeaseValidUntilMillis;
                    // Only for a new epoch: a renewal is how a lapsed lease comes back, and refusing it left the node
                    // unable ever to renew again.
                    if (opensEpoch && validUntil != 0L && plane.clock().getAsLong() >= validUntil) {
                        // A lapsed lease is renewed only after this node lets go of what it held; recording an epoch is
                        // no way around that.
                        throw new java.io.IOException("this node's lease has lapsed; no new epoch of its log is recorded");
                    }
                    writeOwnLease(plane, epochs);
                },
                    (shardId, term) -> node.leases.ownsAtTerm(plane, shardId, term),
                    org.opensearch.serverless.store.NodeLogSettings.WINDOW_MILLIS.get(node.settings),
                    org.opensearch.serverless.store.NodeLogSettings.MAX_BYTES.get(node.settings),
                    org.opensearch.serverless.store.NodeLogSettings.MAX_RECORDS.get(node.settings)
                );
                final long flushAfter = 2 * org.opensearch.serverless.store.NodeLogSettings.WINDOW_MILLIS.get(node.settings);
                writer.flushLoggedCommitsWith(
                    task -> node.threadPool()
                        .schedule(
                            task,
                            org.opensearch.common.unit.TimeValue.timeValueMillis(flushAfter),
                            org.opensearch.threadpool.ThreadPool.Names.GENERIC
                        )
                );
                writer.runAfterOpensOn(node.threadPool().generic());
                writer.committer()
                    .hedgeAppends(
                        org.opensearch.serverless.store.NodeLogSettings.HEDGE_AFTER_MILLIS.get(node.settings),
                        node.threadPool().generic()
                    );
                writer.committer()
                    .admitConcurrently(
                        org.opensearch.serverless.store.NodeLogSettings.ADMISSION_CONCURRENCY.get(node.settings),
                        node.threadPool().generic()
                    );
                writer.committer().flushWhenIdle(org.opensearch.serverless.store.NodeLogSettings.IDLE_FLUSH.get(node.settings));
                nodeLogWriter = writer;
            }
            if (node.reconciler != null && nodeLogHooked == false) {
                node.reconciler.setNodeLog(nodeLogWriter);
                nodeLogHooked = true;
            }
            return nodeLogWriter;
        }
    }

    void writeOwnLease(
        org.opensearch.serverless.metadata.MetadataPlane plane,
        java.util.List<org.opensearch.serverless.store.NodeLogEpoch> nodeLogEpochs
    ) throws java.io.IOException {
        // Stamped from when the renewal began: the lease is valid at least that long after it.
        final long startedAt = plane.clock().getAsLong();
        ownLeaseValidUntilMillis = 0L;
        plane.membership()
            .renew(
                new org.opensearch.serverless.membership.NodeLease(
                    node.localNode.getId(),
                    node.localNode.getEphemeralId(),
                    node.localNode.getAddress().toString(),
                    // Less the search role while the node warms or drains, so no coordinator ranks it as a reader.
                    SearchReadiness.of(node).advertised(node.roles),
                    0L,
                    node.localNode.getName(),
                    org.opensearch.Version.CURRENT.toString()
                ).withLoad(
                    node.reconciler == null ? 0 : node.reconciler.heldShards().size(),
                    node.nodeCapacity().admission.maxShardsHeld(),
                    node.nodeCapacity().inUseLastMinute(),
                    node.nodeCapacity().refusedLastMinute()
                ).withNodeLog(nodeLogEpochs).withZone(org.opensearch.serverless.cluster.Zones.localZone(node.settings()))
            );
        ownLeaseValidUntilMillis = startedAt + plane.leaseTtlMillis();
    }
}
