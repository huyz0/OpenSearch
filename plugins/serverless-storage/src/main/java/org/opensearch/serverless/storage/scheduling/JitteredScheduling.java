/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scheduling;

import org.opensearch.common.Randomness;
import org.opensearch.common.unit.TimeValue;

/**
 * Closes rfc-serverless-opensearch.md &sect;13's own "recovery stampede after restoration" chaos
 * requirement: "WAL backlogs, deferred publications, and queued activations must drain with
 * jittered backoff, not synchronized thundering herd." Applied here to this plugin's own
 * scheduled reconcilers (GC sweep, WAL GC sweep, compaction) -- exactly the "GC / compaction /
 * reconcilers" row &sect;13's own degraded-mode table calls out ("back off (they are never
 * urgent)").
 *
 * <p><b>Per-instance, one-time jitter, not per-tick</b>: core's own {@code
 * ThreadPool#scheduleWithFixedDelay} takes one fixed {@link TimeValue} for the life of a
 * scheduled task, not a per-tick recomputed one, so the jitter this class applies is computed
 * once, at task construction, and reused for every subsequent tick of that task instance. That is
 * still the real fix for the failure mode &sect;13 describes: a coordinated cluster-wide object-store
 * outage (or a coordinated node restart) tends to bring many shards' scheduler tasks up at
 * <em>the same wall-clock moment</em> -- without per-instance jitter, every one of those tasks
 * would tick in lockstep forever afterward (a fixed delay from a shared start time is still a
 * shared cadence). A one-time randomized offset per task instance permanently de-synchronizes
 * them from each other, which a per-tick jitter is not actually needed to achieve.
 *
 * <p>Deliberately conservative: the jittered interval is drawn uniformly from {@code [interval,
 * interval * (1 + jitterFraction)]} -- never <em>shorter</em> than the configured interval (a
 * shard's own configured reconciliation cadence is a real operator-set budget, not a target to
 * undercut), only ever pushed later, spreading the herd out over a bounded window instead of
 * tightening it.
 */
public final class JitteredScheduling {

    /**
     * The fraction of {@code interval} the jittered result may additionally extend by, applied
     * uniformly across every reconciler this class jitters -- large enough to meaningfully spread
     * a synchronized herd across real wall-clock time (20% of a multi-minute interval is itself
     * measured in seconds to tens of seconds), small enough that no shard's own reconciliation
     * cadence drifts far from what an operator actually configured.
     */
    public static final double DEFAULT_JITTER_FRACTION = 0.2;

    private JitteredScheduling() {}

    /**
     * Jitters {@code interval} using {@link #DEFAULT_JITTER_FRACTION}.
     *
     * @param interval the configured, un-jittered interval.
     * @return {@code interval}, extended by a uniformly-random amount in {@code [0, interval *
     *         DEFAULT_JITTER_FRACTION]} -- never shorter than {@code interval} itself.
     */
    public static TimeValue jitter(TimeValue interval) {
        return jitter(interval, DEFAULT_JITTER_FRACTION);
    }

    /**
     * Jitters {@code interval} by a caller-supplied fraction.
     *
     * @param interval the configured, un-jittered interval.
     * @param jitterFraction the maximum fraction of {@code interval} the result may additionally
     *                       extend by; must be {@code >= 0}.
     * @return {@code interval}, extended by a uniformly-random amount in {@code [0, interval *
     *         jitterFraction]} -- never shorter than {@code interval} itself.
     */
    public static TimeValue jitter(TimeValue interval, double jitterFraction) {
        if (jitterFraction < 0) {
            throw new IllegalArgumentException("jitterFraction must be >= 0, got " + jitterFraction);
        }
        if (interval.millis() <= 0 || jitterFraction == 0) {
            return interval;
        }
        long maxExtraMillis = Math.round(interval.millis() * jitterFraction);
        long extraMillis = maxExtraMillis <= 0 ? 0 : Math.floorMod(Randomness.get().nextLong(), maxExtraMillis + 1);
        return TimeValue.timeValueMillis(interval.millis() + extraMillis);
    }
}
