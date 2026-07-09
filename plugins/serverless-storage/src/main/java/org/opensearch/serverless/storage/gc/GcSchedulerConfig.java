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
    public GcSchedulerConfig {
        if (retentionWindowMillis <= 0) {
            throw new IllegalArgumentException("retentionWindowMillis must be > 0, got " + retentionWindowMillis);
        }
    }
}
