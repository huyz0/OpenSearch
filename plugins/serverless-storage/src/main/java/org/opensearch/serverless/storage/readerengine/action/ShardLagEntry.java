/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.action;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/** One reader shard's manifest-generation lag, as reported by {@link NodeManifestLagAction} for a single node. */
public final class ShardLagEntry implements Writeable, ToXContentObject {

    private final String indexUuid;
    private final int shardId;
    private final long manifestGenerationLag;

    /**
     * Creates an entry.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param manifestGenerationLag how many manifest generations behind the latest one this
     *                              shard's reader engine has observed published, on the node that
     *                              reported this entry.
     */
    public ShardLagEntry(String indexUuid, int shardId, long manifestGenerationLag) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.manifestGenerationLag = manifestGenerationLag;
    }

    /**
     * Deserializes an entry.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardLagEntry}.
     */
    public ShardLagEntry(StreamInput in) throws IOException {
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.manifestGenerationLag = in.readVLong();
    }

    /** @param out stream to write this entry's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeVLong(manifestGenerationLag);
    }

    /** UUID of the index the shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** How many manifest generations behind the latest one this shard's reader engine has observed published. */
    public long manifestGenerationLag() {
        return manifestGenerationLag;
    }

    /**
     * @param builder the builder to append this entry's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("index_uuid", indexUuid)
            .field("shard_id", shardId)
            .field("manifest_generation_lag", manifestGenerationLag)
            .endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShardLagEntry that = (ShardLagEntry) o;
        return shardId == that.shardId && manifestGenerationLag == that.manifestGenerationLag && Objects.equals(indexUuid, that.indexUuid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUuid, shardId, manifestGenerationLag);
    }
}
