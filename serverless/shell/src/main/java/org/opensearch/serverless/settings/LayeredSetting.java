/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.settings;

import java.util.function.Function;

/**
 * Resolves a setting that can be given per index, per cluster and per node: the most specific value wins.
 *
 * <p>The index's value comes first, then the cluster's (from the {@code cluster/config} register), then the node's.
 * An empty or absent value is unset. A value that does not parse is skipped, so a bad cluster value cannot stop an
 * index or a node from deciding: the next, less specific, layer decides instead.
 */
public final class LayeredSetting {

    private LayeredSetting() {}

    /**
     * The value of the most specific layer that is set and parses.
     *
     * @param <T> the value's type
     * @param index the index's raw value, or null
     * @param cluster the cluster's raw value, or null
     * @param parse turns a raw value into a value, throwing {@link IllegalArgumentException} if it cannot
     * @param node the node's value, used when neither the index nor the cluster decides
     * @return the value
     */
    public static <T> T resolve(String index, String cluster, Function<String, T> parse, T node) {
        for (String value : new String[] { index, cluster }) {
            if (value == null || value.isEmpty()) {
                continue;
            }
            try {
                return parse.apply(value);
            } catch (IllegalArgumentException e) {
                // Not a value this setting takes: the next, less specific, layer decides.
            }
        }
        return node;
    }
}
