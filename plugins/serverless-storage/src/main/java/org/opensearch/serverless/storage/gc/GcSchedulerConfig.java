/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;

import java.util.Objects;

/**
 * Everything a reader shard's engine needs to schedule its own {@link GcSchedulerTask}, bundled
 * into one value -- same nullable-bundle shape as {@code PitrRetentionConfig}/{@code
 * CompactionSchedulerConfig}. {@code null} where this type is accepted means "background GC is not
 * configured for this shard," matching every other optional feature in this plugin.
 *
 * <p><b>Why {@code shardStateStore} is required rather than optional</b>: a sweep that cannot read the
 * shard head cannot tell a published manifest from one whose writer died between writing it and CASing the
 * head, and deleting on that mistake destroys the live head and its bundles (see {@link
 * ManifestRetentionPolicy}'s own javadoc for the interleaving). Making it a required component means a
 * caller that forgets it fails to compile, rather than silently getting a sweep that can delete live data;
 * that is worth the one extra argument at every construction site.
 *
 * @param retentionWindowMillis the sole time-based safety margin {@link ManifestRetentionPolicy}
 *        applies before a superseded, unpinned manifest becomes deletable -- see {@link
 *        GcSchedulerTask}'s own javadoc for why this, not a lease-pin signal, is this sweep's real
 *        safety net.
 */
public record GcSchedulerConfig(TimeValue interval, long retentionWindowMillis, BlobContainerManifestStore manifestStore,
    BlobContainerBundleStore bundleStore, DurablePinRegistry pinRegistry, ShardStateStore shardStateStore,
    GcSweepStateStore sweepStateStore) {

    /**
     * An upper bound with generous headroom over any legitimate retention window, chosen only to
     * keep {@code clock.getAsLong() - retentionWindowMillis} (in {@link GcSchedulerTask#sweep})
     * far away from {@code long} underflow -- a window anywhere near {@code Long.MAX_VALUE} would
     * wrap that subtraction around to a large *positive* cutoff, making the "generous safety
     * margin" invert into "no retention window at all" and every manifest immediately deletable.
     */
    public static final long MAX_RETENTION_WINDOW_MILLIS = TimeValue.timeValueDays(365).millis();

    /**
     * Validates the configured retention window.
     *
     * @param interval how often {@link GcSchedulerTask} runs its sweep.
     * @param retentionWindowMillis the time-based safety margin before a superseded, unpinned manifest becomes deletable.
     * @param manifestStore the shard's manifest store, to list and delete superseded manifests.
     * @param bundleStore the shard's bundle store, to list and delete unreferenced bundles.
     * @param pinRegistry the shard's durable pin registry, to exclude pinned manifests from deletion.
     * @param shardStateStore the shard's head, which is what "latest published manifest" actually means.
     * @param sweepStateStore where the sweep's cross-tick bookkeeping (orphan first-observation times, the
     *        last swept head) is persisted, so a node restart or shard relocation does not reset it.
     */
    public GcSchedulerConfig {
        Objects.requireNonNull(shardStateStore, "shardStateStore is required: a sweep that cannot read the head must not sweep");
        Objects.requireNonNull(sweepStateStore, "sweepStateStore is required");
        if (retentionWindowMillis <= 0) {
            throw new IllegalArgumentException("retentionWindowMillis must be > 0, got " + retentionWindowMillis);
        }
        if (retentionWindowMillis > MAX_RETENTION_WINDOW_MILLIS) {
            throw new IllegalArgumentException(
                "retentionWindowMillis must be <= " + MAX_RETENTION_WINDOW_MILLIS + ", got " + retentionWindowMillis
            );
        }
    }

    /** How often {@link GcSchedulerTask} runs its sweep. */
    @Override
    public TimeValue interval() {
        return interval;
    }

    /** The time-based safety margin before a superseded, unpinned manifest becomes deletable. */
    @Override
    public long retentionWindowMillis() {
        return retentionWindowMillis;
    }

    /** The shard's manifest store, to list and delete superseded manifests. */
    @Override
    public BlobContainerManifestStore manifestStore() {
        return manifestStore;
    }

    /** The shard's bundle store, to list and delete unreferenced bundles. */
    @Override
    public BlobContainerBundleStore bundleStore() {
        return bundleStore;
    }

    /** The shard's durable pin registry, to exclude pinned manifests from deletion. */
    @Override
    public DurablePinRegistry pinRegistry() {
        return pinRegistry;
    }

    /** The shard's head, which is what "latest published manifest" actually means. */
    @Override
    public ShardStateStore shardStateStore() {
        return shardStateStore;
    }

    /** Where the sweep's cross-tick bookkeeping is persisted. */
    @Override
    public GcSweepStateStore sweepStateStore() {
        return sweepStateStore;
    }
}
