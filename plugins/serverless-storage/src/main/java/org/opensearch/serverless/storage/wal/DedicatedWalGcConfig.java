/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.unit.TimeValue;

/**
 * Everything a writer shard's engine needs to schedule its own dedicated WAL stream's {@link
 * WalGcSchedulerTask} (rfc-serverless-opensearch.md &sect;12's "dedicated WAL streams" regulatory
 * co-residency bullet), bundled into one value -- same nullable-bundle shape as {@code
 * PitrRetentionConfig}/{@code CompactionSchedulerConfig}/{@code GcSchedulerConfig}. {@code null}
 * where this type is accepted means either WAL mirroring is off entirely, or this shard shares the
 * ordinary node-level WAL container (whose own retention is already swept by a separate,
 * node-level {@link WalGcSchedulerTask} unrelated to this one).
 *
 * <p>Deliberately does not itself construct the {@link WalGcSchedulerTask} -- that needs a {@code
 * ThreadPool}, which only the engine being constructed has (via {@code EngineConfig}), so this
 * record only carries the container/interval it needs once the engine's own constructor is ready
 * to build one, tying the task's lifecycle to the engine's own {@code close()} the same way {@code
 * PitrRetentionSchedulerTask} already is.
 *
 * @param walContainer this shard's own dedicated, single-shard-scoped WAL container -- what {@link
 *        WalGcSchedulerTask} sweeps chunks from.
 * @param shardBlobContainer this shard's own regular (non-WAL) blob container, holding its shard
 *        head/manifests -- what {@link WalGcSchedulerTask}'s shard-container resolver reads to
 *        find the {@code WalPosition} bound safe to delete up to. Since {@code walContainer} is
 *        scoped to exactly this one shard, that resolver never needs to resolve any other
 *        (indexUuid, shardId) than this shard's own.
 * @param gcInterval how often the sweep runs; must be positive (this record does not itself
 *        represent "GC disabled" -- omit the whole config, i.e. pass {@code null} where this type
 *        is accepted, for that).
 */
public record DedicatedWalGcConfig(BlobContainer walContainer, BlobContainer shardBlobContainer, TimeValue gcInterval) {

    /**
     * Validates the configured interval.
     *
     * @param walContainer this shard's own dedicated WAL container.
     * @param shardBlobContainer this shard's own regular blob container.
     * @param gcInterval how often the sweep runs.
     */
    public DedicatedWalGcConfig {
        if (gcInterval.millis() <= 0) {
            throw new IllegalArgumentException("gcInterval must be > 0, got " + gcInterval);
        }
    }

    /** This shard's own dedicated, single-shard-scoped WAL container -- what {@link WalGcSchedulerTask} sweeps chunks from. */
    @Override
    public BlobContainer walContainer() {
        return walContainer;
    }

    /**
     * This shard's own regular (non-WAL) blob container, holding its shard head/manifests -- what
     * {@link WalGcSchedulerTask}'s shard-container resolver reads to find the safe-to-delete-up-to bound.
     */
    @Override
    public BlobContainer shardBlobContainer() {
        return shardBlobContainer;
    }

    /** How often the sweep runs. */
    @Override
    public TimeValue gcInterval() {
        return gcInterval;
    }
}
