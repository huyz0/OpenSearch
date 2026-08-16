/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.deepsnapshot.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/** The result of an {@link IndexDeepSnapshotAction} request: every shard copied and the snapshot finalized. */
public class IndexDeepSnapshotResponse extends ActionResponse implements ToXContentObject {

    private final String snapshotName;
    private final String snapshotUuid;
    private final int shardCount;

    /**
     * Creates a response.
     *
     * @param snapshotName the snapshot name every shard was copied under.
     * @param snapshotUuid the uuid generated for this snapshot, needed to address it precisely
     *                     ({@link org.opensearch.snapshots.SnapshotId} equality is by both fields).
     * @param shardCount how many shards were copied and finalized.
     */
    public IndexDeepSnapshotResponse(String snapshotName, String snapshotUuid, int shardCount) {
        this.snapshotName = snapshotName;
        this.snapshotUuid = snapshotUuid;
        this.shardCount = shardCount;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link IndexDeepSnapshotResponse}.
     */
    public IndexDeepSnapshotResponse(StreamInput in) throws IOException {
        super(in);
        this.snapshotName = in.readString();
        this.snapshotUuid = in.readString();
        this.shardCount = in.readVInt();
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(snapshotName);
        out.writeString(snapshotUuid);
        out.writeVInt(shardCount);
    }

    /** The snapshot name every shard was copied under. */
    public String snapshotName() {
        return snapshotName;
    }

    /** The uuid generated for this snapshot. */
    public String snapshotUuid() {
        return snapshotUuid;
    }

    /** How many shards were copied and finalized. */
    public int shardCount() {
        return shardCount;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("acknowledged", true)
            .field("snapshot", snapshotName)
            .field("uuid", snapshotUuid)
            .field("shard_count", shardCount)
            .endObject();
    }
}
