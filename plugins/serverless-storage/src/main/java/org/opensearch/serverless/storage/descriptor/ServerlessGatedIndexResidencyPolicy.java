/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.indices.cluster.IndexResidencyPolicy;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;

/**
 * Phase E2 of {@code core-pluggability-refactor-plan.md}: this plugin's {@link IndexResidencyPolicy},
 * tuning {@code IndicesClusterStateService}'s on-demand-opened-index sweep/eviction for gated indices --
 * see that interface's own javadoc for exactly what moved here and what stayed in core.
 *
 * <p>Backs the three settings declared on {@link ServerlessStoragePlugin} itself (this plugin's own
 * convention for every {@code Setting} it owns, enforced by that class's own {@code
 * testEveryDeclaredSettingFieldIsRegisteredInGetSettings} regression guard) under the exact same keys they
 * had when they were hardcoded {@code Setting} fields on {@code IndicesClusterStateService}: {@code
 * indices.gated.deleted_shard_sweep_interval}, {@code indices.gated.idle_eviction_after}, {@code
 * indices.gated.max_open}. An operator's existing configuration for these settings keeps working
 * unchanged; only which module defines and registers the {@link org.opensearch.common.settings.Setting}
 * objects moved.
 *
 * <p><b>The measured-heap-cost constant moved here too, and that is the real reason this is a plugin
 * concern and not a generic one.</b> {@code MEASURED_BYTES_PER_OPEN_GATED_INDEX} (150,888 bytes, from
 * {@code GatedResidencySoakIT}) is a property of this plugin's own {@code IndexShard}/directory
 * implementation, not of "on-demand-opened indices" in general -- a future, different plugin using the same
 * core mechanism would have its own number, and core should not be guessing this plugin's footprint for it.
 */
public final class ServerlessGatedIndexResidencyPolicy implements IndexResidencyPolicy {

    /**
     * Heap cost of one open gated index, measured rather than estimated. {@code GatedResidencySoakIT}:
     * 150,888 B per index, holding flat across two orders of magnitude of population. Used only to derive a
     * default cap, so being wrong here puts the ceiling in the wrong place rather than breaking anything,
     * and an operator who has measured their own workload should set {@link
     * ServerlessStoragePlugin#GATED_MAX_OPEN_SETTING} instead.
     */
    static final long MEASURED_BYTES_PER_OPEN_GATED_INDEX = 150_888L;

    private final Settings settings;

    public ServerlessGatedIndexResidencyPolicy(Settings settings) {
        this.settings = settings;
    }

    @Override
    public TimeValue sweepInterval() {
        return ServerlessStoragePlugin.GATED_SHARD_SWEEP_INTERVAL_SETTING.get(settings);
    }

    @Override
    public TimeValue idleEvictionAfter() {
        return ServerlessStoragePlugin.GATED_SHARD_IDLE_EVICTION_SETTING.get(settings);
    }

    @Override
    public int maxOpen() {
        // Zero when the operator has not set it, which asks core to derive a ceiling from the heap and
        // bytesPerOpenIndex() below -- the same formula (and the same clamp) this method used to inline;
        // it lives in one place now, GatedIndexResidency#resolveGatedMaxOpen, with this plugin supplying
        // only the measured number.
        return ServerlessStoragePlugin.GATED_MAX_OPEN_SETTING.get(settings);
    }

    @Override
    public long bytesPerOpenIndex() {
        return MEASURED_BYTES_PER_OPEN_GATED_INDEX;
    }
}
