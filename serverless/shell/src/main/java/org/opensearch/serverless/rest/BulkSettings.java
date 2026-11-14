/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.rest;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.serverless.settings.SettingsProvider;

import java.util.List;

/**
 * Settings of the bulk endpoint.
 *
 * <p>Each setting is defined here once, with its type and default, and found through {@link SettingsProvider}.
 */
public final class BulkSettings implements SettingsProvider {

    /**
     * Whether a batch's memory is charged to indexing pressure before its body is copied and parsed, at
     * {@link #MEMORY_FACTOR} times its length; off, it is charged its length once, after parsing, as before.
     */
    public static final Setting<Boolean> CHARGE_BEFORE_PARSE = Setting.boolSetting(
        "serverless.write.bulk.charge_before_parse",
        BulkHandler.DEFAULT_CHARGE_BEFORE_PARSE,
        Property.NodeScope
    );

    /**
     * How many times its length a batch is charged when {@link #CHARGE_BEFORE_PARSE} is on: what a node holds of
     * it at once, the body, each source as a string, the bytes the engine parses and the node-log blob carrying it.
     */
    public static final Setting<Double> MEMORY_FACTOR = Setting.doubleSetting(
        "serverless.write.bulk.memory_factor",
        BulkHandler.DEFAULT_MEMORY_FACTOR,
        -Double.MAX_VALUE,
        Property.NodeScope
    );

    /**
     * Whether {@code _bulk} bodies are streamed: read as they arrive, a slice at a time, with memory per request bounded
     * whatever the body's size ({@link BulkStream}). Needs the streaming HTTP transport, which the same setting selects.
     */
    public static final Setting<Boolean> STREAMING = Setting.boolSetting(
        "serverless.http.streaming",
        BulkHandler.DEFAULT_STREAMING,
        Property.NodeScope
    );

    /** How much of a streamed body is parsed and applied at once. */
    public static final Setting<ByteSizeValue> SLICE_BYTES = Setting.byteSizeSetting(
        "serverless.write.bulk.slice_bytes",
        new ByteSizeValue(BulkHandler.DEFAULT_SLICE_BYTES),
        Property.NodeScope
    );

    /** The heap all streamed batches on a node may hold at once while being applied. */
    public static final Setting<String> MEMORY_BUDGET = Setting.simpleString(
        "serverless.write.bulk.memory_budget",
        "10%",
        Property.NodeScope
    );

    /** How long a streamed slice waits for its share of the budget before its request is refused with a 429. */
    public static final Setting<TimeValue> MAX_WAIT = Setting.timeSetting(
        "serverless.write.bulk.max_wait",
        BulkHandler.DEFAULT_WAIT,
        Property.NodeScope
    );

    /** Bulk slices a node applies at once; by default one per allocated processor. */
    public static final Setting<Integer> MAX_APPLYING_SLICES = new Setting<>(
        "serverless.write.bulk.max_applying_slices",
        s -> Integer.toString(OpenSearchExecutors.allocatedProcessors(s)),
        s -> Setting.parseInt(s, Integer.MIN_VALUE, "serverless.write.bulk.max_applying_slices"),
        Property.NodeScope
    );

    /** Used by {@link java.util.ServiceLoader}. */
    public BulkSettings() {}

    @Override
    public List<Setting<?>> settings() {
        return List.of(CHARGE_BEFORE_PARSE, MEMORY_FACTOR, STREAMING, SLICE_BYTES, MEMORY_BUDGET, MAX_WAIT, MAX_APPLYING_SLICES);
    }
}
