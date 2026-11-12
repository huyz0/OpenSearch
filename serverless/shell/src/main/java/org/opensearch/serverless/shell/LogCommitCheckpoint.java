/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.shell;

import org.opensearch.common.settings.Settings;
import org.opensearch.serverless.reconcile.ReconcileScheduler;
import org.opensearch.serverless.store.CommitStaging;

/**
 * How old a commit recorded with the node log gets before its shard's manifest is written: the checkpoint age.
 *
 * <p>Set on the node ({@value #SETTING}), and changeable at runtime for the whole cluster with the same key in the
 * cluster's config register, which each node reads with the segment placement, in the same GET. The cluster's value
 * wins. Either must be at least {@link CommitStaging#MIN_COMMIT_PERIODS} commit periods, the least time between two
 * publishes of one shard, and at most half the pack deadline ({@link CommitStaging#check}). A node refuses to start on
 * an invalid value of its own; the cluster setting is refused with a 400 when written, and a node whose own commit
 * period makes a written value invalid keeps its own, with a warning.
 */
public final class LogCommitCheckpoint {

    /** The setting, on the node and in the cluster's config register. */
    public static final String SETTING = "serverless.wal.node_log.commits.checkpoint_millis";

    private LogCommitCheckpoint() {}

    /**
     * The least time between two publishes of one shard on a node: the longer of how long a write may wait to be
     * published and the least spacing between two publishes.
     *
     * @param settings the node's settings
     * @return the period
     */
    public static long commitPeriodMillis(Settings settings) {
        final long maxAge = settings.getAsLong(
            ServerlessBootstrap.NODE_LOG_PUBLISH_MAX_AGE,
            ServerlessBootstrap.DEFAULT_NODE_LOG_PUBLISH_MAX_AGE_MILLIS
        );
        final long spacing = settings.getAsLong(
            ServerlessBootstrap.PUBLISH_MIN_INTERVAL,
            ReconcileScheduler.DEFAULT_PUBLISH_MIN_INTERVAL.millis()
        );
        return Math.max(0L, Math.max(maxAge, spacing));
    }

    /**
     * The node's own checkpoint age.
     *
     * @param settings the node's settings
     * @return the age
     * @throws IllegalArgumentException if it is out of bounds for the node's commit period
     */
    static long nodeValue(Settings settings) {
        final long millis = settings.getAsLong(SETTING, CommitStaging.DEFAULT_MAX_STAGED_MILLIS);
        final String invalid = CommitStaging.check(millis, commitPeriodMillis(settings));
        if (invalid != null) {
            throw new IllegalArgumentException("[" + SETTING + "]: " + invalid);
        }
        return millis;
    }

    /**
     * Checks a value given for the cluster setting against this node's commit period.
     *
     * @param node the node handling the update, or null
     * @param value the value
     * @throws IllegalArgumentException if it is not a number, or out of bounds
     */
    public static void check(ServerlessNode node, Object value) {
        final long millis;
        try {
            millis = Long.parseLong(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("[" + SETTING + "] must be a number of milliseconds, got [" + value + "]");
        }
        final long period = node == null ? commitPeriodMillis(Settings.EMPTY) : commitPeriodMillis(node.settings);
        final String invalid = CommitStaging.check(millis, period);
        if (invalid != null) {
            throw new IllegalArgumentException("[" + SETTING + "]: " + invalid);
        }
    }

    /**
     * Applies the cluster's value as read or just written: this node's staged commits are checked against it from
     * their next check on. An unset, unreadable or, for this node, invalid value leaves the node's own.
     *
     * @param node the node, or null
     * @param cluster the register's settings
     */
    public static void applyCluster(ServerlessNode node, Settings cluster) {
        if (node == null) {
            return;
        }
        final String value = cluster.get(SETTING);
        long millis = node.nodeLogCommitCheckpointMillis;
        if (value != null && value.isEmpty() == false) {
            try {
                check(node, value);
                millis = Long.parseLong(value.trim());
            } catch (IllegalArgumentException e) {
                ServerlessNode.logger.warn("ignoring the cluster's checkpoint age: " + e.getMessage());
            }
        }
        node.logCommitCheckpointMillis = millis;
    }
}
