/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.cluster;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of where searches of a shard are served: hot-shard spread and zone preference.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class ReaderPlacementSettings implements SettingsProvider {

    /** Searches a second, from one coordinator, that one reader of a shard is expected to absorb. */
    public static final Setting<Double> SPREAD_TARGET_PER_SECOND = Setting.doubleSetting(
        "serverless.search.reader_spread.target_per_second",
        HotShardSpread.DEFAULT_TARGET_PER_SECOND,
        -Double.MAX_VALUE,
        Property.NodeScope
    );

    /** The most readers one shard's searches are spread over. */
    public static final Setting<Integer> SPREAD_MAX_READERS = Setting.intSetting(
        "serverless.search.reader_spread.max_readers",
        HotShardSpread.DEFAULT_MAX_READERS,
        Property.NodeScope
    );

    /** Searches a second, from one coordinator, above which a shard gets a reader in the coordinator's zone. */
    public static final Setting<Double> ZONE_LOCAL_MIN_PER_SECOND = Setting.doubleSetting(
        "serverless.search.zone_local.min_per_second",
        ZonePreference.DEFAULT_MIN_PER_SECOND,
        -Double.MAX_VALUE,
        Property.NodeScope
    );

    /** How many more searches in flight a zone's reader may have than the fleet-wide choice before it gives way. */
    public static final Setting<Integer> ZONE_LOCAL_SPILL_IN_FLIGHT = Setting.intSetting(
        "serverless.search.zone_local.spill_in_flight",
        ZonePreference.DEFAULT_SPILL_IN_FLIGHT,
        Property.NodeScope
    );

    /** By when a joining reader is fully ramped, whatever it says about its cache; zero turns the slow start off. */
    public static final Setting<org.opensearch.common.unit.TimeValue> RAMP_MAX_DURATION = Setting.timeSetting(
        "serverless.search.reader_ramp.max_duration",
        org.opensearch.common.unit.TimeValue.ZERO,
        Property.NodeScope
    );

    /** The soonest a joining reader may be fully ramped, however warm it says it is. */
    public static final Setting<org.opensearch.common.unit.TimeValue> RAMP_MIN_DURATION = Setting.timeSetting(
        "serverless.search.reader_ramp.min_duration",
        org.opensearch.common.unit.TimeValue.timeValueSeconds(30),
        Property.NodeScope
    );

    /**
     * Reads the slow start's bounds from a node's settings.
     *
     * @param settings the node's settings
     * @return the bounds; off unless the maximum is set
     */
    public static ReaderRamp.Config rampConfig(org.opensearch.common.settings.Settings settings) {
        return new ReaderRamp.Config(RAMP_MIN_DURATION.get(settings).millis(), RAMP_MAX_DURATION.get(settings).millis());
    }

    /** Used by {@link java.util.ServiceLoader}. */
    public ReaderPlacementSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(
            SPREAD_TARGET_PER_SECOND,
            SPREAD_MAX_READERS,
            ZONE_LOCAL_MIN_PER_SECOND,
            ZONE_LOCAL_SPILL_IN_FLIGHT,
            RAMP_MAX_DURATION,
            RAMP_MIN_DURATION
        );
    }
}
