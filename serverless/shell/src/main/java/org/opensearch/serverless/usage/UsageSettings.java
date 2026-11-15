/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.usage;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;
import java.util.Locale;

/** The per-tenant cost guardrails' settings. */
public final class UsageSettings implements SettingsProvider {

    /** What the guardrails do: nothing, compute levels and report them, or act on them. */
    public enum Mode {
        /** Levels are not computed. */
        OFF,
        /** Levels are computed and reported; nothing is slowed. */
        OBSERVE,
        /** Levels are computed and acted on. */
        ENFORCE;

        /**
         * Parses a mode, case-insensitively.
         *
         * @param value the value
         * @return the mode
         * @throws IllegalArgumentException if it is none of them
         */
        public static Mode parse(String value) {
            try {
                return Mode.valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("expected off, observe or enforce, got [" + value + "]");
            }
        }
    }

    /** A tenant's budget class. */
    public enum Tier {
        /** Most tenants: a few indices, written in bursts. */
        SMALL,
        /** Busy tenants. */
        STANDARD,
        /** The largest tenants. */
        LARGE;

        /**
         * Parses a tier, case-insensitively.
         *
         * @param value the value
         * @return the tier
         * @throws IllegalArgumentException if it is none of them
         */
        public static Tier parse(String value) {
            try {
                return Tier.valueOf(value.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("expected small, standard or large, got [" + value + "]");
            }
        }
    }

    /** The node's guardrail mode. Observe by default: levels are reported, nothing is slowed. */
    public static final Setting<String> MODE = new Setting<>("serverless.cost.guardrails", "observe", value -> {
        Mode.parse(value);
        return value;
    }, Property.NodeScope);

    /** An index's own guardrail mode, which wins over the node's; empty follows the node. */
    public static final Setting<String> INDEX_MODE = new Setting<>("index.serverless.cost.guardrails", "", value -> {
        if (value.isEmpty() == false) {
            Mode.parse(value);
        }
        return value;
    }, Property.IndexScope, Property.Dynamic);

    /** The tier an index's tenant is on when the index names none. Large while the guardrails only observe. */
    public static final Setting<String> DEFAULT_TIER = new Setting<>("serverless.cost.default_tier", "large", value -> {
        Tier.parse(value);
        return value;
    }, Property.NodeScope);

    /** The tier of the index's tenant; empty takes the node's default. */
    public static final Setting<String> INDEX_TIER = new Setting<>("index.serverless.cost.tier", "", value -> {
        if (value.isEmpty() == false) {
            Tier.parse(value);
        }
        return value;
    }, Property.IndexScope, Property.Dynamic);

    /** A budget for the index's tenant in cost units a minute, overriding its tier's; zero or less takes the tier's. */
    public static final Setting<Long> INDEX_BUDGET = Setting.longSetting(
        "index.serverless.cost.budget",
        0L,
        Property.IndexScope,
        Property.Dynamic
    );

    /** The small tier's budget, in cost units (one PUT each) a minute. */
    public static final Setting<Long> SMALL_BUDGET = Setting.longSetting("serverless.cost.tier.small", 20L, 1L, Property.NodeScope);

    /** The standard tier's budget, in cost units a minute. */
    public static final Setting<Long> STANDARD_BUDGET = Setting.longSetting("serverless.cost.tier.standard", 200L, 1L, Property.NodeScope);

    /** The large tier's budget, in cost units a minute. */
    public static final Setting<Long> LARGE_BUDGET = Setting.longSetting("serverless.cost.tier.large", 2_000L, 1L, Property.NodeScope);

    /** Used by {@link java.util.ServiceLoader}. */
    public UsageSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(MODE, INDEX_MODE, DEFAULT_TIER, INDEX_TIER, INDEX_BUDGET, SMALL_BUDGET, STANDARD_BUDGET, LARGE_BUDGET);
    }
}
