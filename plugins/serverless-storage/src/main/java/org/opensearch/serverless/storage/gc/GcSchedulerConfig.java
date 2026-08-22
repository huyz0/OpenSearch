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

/**
 * Everything a reader shard's engine needs to schedule its own {@link GcSchedulerTask}, bundled
 * into one value -- same nullable-bundle shape as {@code PitrRetentionConfig}/{@code
 * CompactionSchedulerConfig}. {@code null} where this type is accepted means "background GC is not
 * configured for this shard," matching every other optional feature in this plugin.
 *
 * @param retentionWindowMillis the sole time-based safety margin {@link ManifestRetentionPolicy}
 *        applies before a superseded, unpinned manifest becomes deletable -- see {@link
 *        GcSchedulerTask}'s own javadoc for why this, not a lease-pin signal, is this sweep's real
 *        safety net.
 */
public record GcSchedulerConfig(TimeValue interval, long retentionWindowMillis, BlobContainerManifestStore manifestStore,
    BlobContainerBundleStore bundleStore, DurablePinRegistry pinRegistry) {

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
     */
    public GcSchedulerConfig {
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
}
