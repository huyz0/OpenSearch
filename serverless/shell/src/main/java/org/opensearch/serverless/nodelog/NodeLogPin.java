/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.nodelog;

import org.opensearch.serverless.nodelog.NodeLogBlob.ShardKey;

/** A shard's records held readable past its publishes; see {@link NodeLogCommitter#pin}. */
public final class NodeLogPin implements AutoCloseable {
    private final NodeLogPins pins;
    final ShardKey shard;
    final long deadlineNanos;

    NodeLogPin(NodeLogPins pins, ShardKey shard, long deadlineNanos) {
        this.pins = pins;
        this.shard = shard;
        this.deadlineNanos = deadlineNanos;
    }

    /** Lets the log trim what the pin kept; a later {@link NodeLogCommitter#heldRuns} of the shard no longer reports a lapse. */
    @Override
    public void close() {
        pins.close(this);
    }
}
