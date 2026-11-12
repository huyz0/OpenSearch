/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.common.settings.Settings;

import java.util.Locale;

/**
 * How far a write with {@code refresh=true} or {@code refresh=wait_for} is visible when it is answered.
 *
 * <p>Searches go to search nodes, not to the shard's writer, so refreshing the writer's own copy is not enough for a
 * search that follows the answer to find the write. The modes:
 * <ul>
 *   <li>{@code owner}: the writer refreshes its own copy and answers. A search node sees the write once its writer's
 *       push or the next commit reaches it.</li>
 *   <li>{@code serving}: the writer also waits, bounded, until every search node serving the shard has taken the
 *       write into its tail overlay, and a search node opening the shard waits for the writer's answer to its fetch
 *       before it searches. {@code forced_refresh} is true only when the wait completed; a node that was not reached
 *       is told nothing more and serves what it has.</li>
 *   <li>{@code everywhere}: {@code serving}, and a search node answers for a shard only while it knows it holds every
 *       write its writer has answered as refreshed. It knows that for a short grant after it last asked the writer, and
 *       the writer, before it answers a refreshing write, waits out the grant of any node it stopped pushing to. A node
 *       without a grant asks the writer first, one round trip; one that cannot know refuses, and the search asks the next
 *       copy, down to the writer, whose own copy is current. A search starting after a refreshing write's answer then
 *       finds the write or reports the shard failed, on any node.</li>
 * </ul>
 *
 * <p>The node's setting is the default; the cluster setting, in the {@code cluster/config} register, wins over it.
 * Both spellings of a refreshing write, {@code true} and {@code wait_for}, are treated the same way.
 */
public final class RefreshVisibility {

    /** The modes, weakest first. */
    public enum Mode {
        /** The writer's own copy only. */
        OWNER,
        /** The search nodes serving the shard when it is answered, and those opening it. */
        SERVING,
        /** Any search node, or the search does not answer for the shard. */
        EVERYWHERE;

        static Mode parse(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "owner" -> OWNER;
                case "serving" -> SERVING;
                case "everywhere" -> EVERYWHERE;
                default -> throw new IllegalArgumentException(
                    "refresh visibility must be one of [owner, serving, everywhere], got [" + value + "]"
                );
            };
        }

        /** @return the mode's name as the settings spell it */
        public String settingValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** The node's default. */
    public static final String NODE_SETTING = "serverless.search.refresh_visibility";

    /** The cluster setting, which wins over the node's. */
    public static final String CLUSTER_SETTING = "serverless.refresh_visibility";

    /** The default mode, when neither the cluster nor the node names one. */
    public static final Mode DEFAULT = Mode.EVERYWHERE;

    /** How long a refreshing write waits for the search nodes serving its shard to take it, at most. */
    public static final long WAIT_MILLIS = 500L;

    /**
     * How long a search node may answer for a shard after it last asked the shard's writer, in {@code everywhere} mode.
     * The node counts it from when it sent the request; the writer from when it handled it, plus
     * {@link #GRANT_MARGIN_MILLIS}, so the writer's end is always the later one.
     */
    public static final long GRANT_MILLIS = 500L;

    /** What the writer adds to a grant, for clocks that run at slightly different rates. */
    public static final long GRANT_MARGIN_MILLIS = 100L;

    private final Mode nodeDefault;
    private volatile Mode cluster;

    RefreshVisibility(Settings nodeSettings) {
        final String value = nodeSettings.get(NODE_SETTING);
        this.nodeDefault = value == null || value.isEmpty() ? DEFAULT : Mode.parse(value);
    }

    /**
     * Checks a value given for the cluster setting.
     *
     * @param value the value
     * @throws IllegalArgumentException if it names no mode
     */
    public static void check(String value) {
        Mode.parse(value);
    }

    /**
     * Takes the cluster's settings as last read or written.
     *
     * @param settings the cluster's settings
     */
    public void applyCluster(Settings settings) {
        final String value = settings.get(CLUSTER_SETTING);
        Mode parsed = null;
        if (value != null && value.isEmpty() == false) {
            try {
                parsed = Mode.parse(value);
            } catch (IllegalArgumentException e) {
                // A value the register holds that names no mode: the node's default decides.
            }
        }
        this.cluster = parsed;
    }

    /** @return the mode in force: the cluster's, else the node's */
    public Mode mode() {
        final Mode fromCluster = cluster;
        return fromCluster == null ? nodeDefault : fromCluster;
    }

    /** @return whether refreshing writes wait for the search nodes serving their shard */
    public boolean waitsForSearchNodes() {
        return mode().compareTo(Mode.SERVING) >= 0;
    }

    /** @return whether search nodes answer for a shard only under a grant from its writer, and writers wait grants out */
    public boolean grants() {
        return mode() == Mode.EVERYWHERE;
    }
}
