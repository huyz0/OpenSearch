/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.nodelog;

import org.opensearch.serverless.nodelog.NodeLogBlob.Range;
import org.opensearch.serverless.nodelog.NodeLogBlob.ShardKey;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** What this node knows of one epoch of its log. */
final class NodeLogEpochState {
    /** For each shard, the runs of slots holding its records that it has not published. */
    final Map<ShardKey, List<Range>> unpublished = new TreeMap<>();
    /** For each pinned shard, the runs it published since the pin, kept readable until the pin goes. */
    final Map<ShardKey, List<Range>> retained = new HashMap<>();
    /** The term each shard was confirmed at for this epoch. */
    final Map<ShardKey, Long> admitted = new HashMap<>();
    /** The slot after the last that landed. */
    long next;
    boolean writing = true;

    long trimPoint() {
        long trim = next;
        for (List<Range> runs : unpublished.values()) {
            trim = Math.min(trim, runs.get(0).from());
        }
        for (List<Range> runs : retained.values()) {
            trim = Math.min(trim, runs.get(0).from());
        }
        return trim;
    }

    boolean complete() {
        return writing == false && unpublished.isEmpty() && retained.isEmpty();
    }
}
