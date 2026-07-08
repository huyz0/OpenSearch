/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention;

import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;

/**
 * Everything an {@code ObjectStoreWriterEngine} needs to schedule its own
 * {@link PitrRetentionSchedulerTask}, bundled into one value so PITR support can be threaded
 * through as a single optional (nullable) constructor parameter instead of three. {@code null}
 * where this type is accepted means "PITR retention is not configured for this shard" -- matches
 * how {@code encryptionKeyProvider} being {@code null} means "encryption is off" elsewhere in this
 * plugin: an explicit absence, not a missing feature.
 */
public record PitrRetentionConfig(BlobContainerManifestStore manifestStore, DurablePinRegistry pinRegistry, long windowMillis) {

    public PitrRetentionConfig {
        if (windowMillis <= 0) {
            throw new IllegalArgumentException("windowMillis must be > 0, got " + windowMillis);
        }
    }
}
