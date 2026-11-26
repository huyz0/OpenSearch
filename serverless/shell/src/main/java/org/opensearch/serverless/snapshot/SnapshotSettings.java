/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.snapshot;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;

/** Node settings of the snapshot take. */
public final class SnapshotSettings {

    private SnapshotSettings() {}

    /**
     * Whether a take records the aliases that name each index it captures, so a restore can bring them back. Off, a take
     * writes the record it always wrote and reads no alias register. It stays off until every node that restores the
     * repository's snapshots reads the alias fields: a node that does not ignores them and loses the aliases silently.
     */
    public static final Setting<Boolean> CAPTURE_ALIASES = Setting.boolSetting(
        "serverless.snapshots.take.capture_aliases",
        false,
        Property.NodeScope
    );

    /**
     * Whether a take with {@code partial=true} keeps an index whose shard published no commit, recording the shards without
     * one, instead of leaving the index out. Off, {@code partial=true} keeps its old meaning. A record with failed shards
     * cannot be read safely by a node without the matching reader, so this is turned on only when every node reading the
     * repository has it.
     */
    public static final Setting<Boolean> PARTIAL_SHARDS = Setting.boolSetting(
        "serverless.snapshots.take.partial_shards",
        false,
        Property.NodeScope
    );
}
