/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * Represents the hash range assigned to a shard.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record ShardRange(int shardId, int start, int end) implements Comparable<ShardRange>, ToXContentFragment, Writeable {

    /**
     * Constructs a new shard range from a stream.
     * @param in the stream to read from
     * @throws IOException if an error occurs while reading from the stream
     * @see #writeTo(StreamOutput)
     */
    public ShardRange(StreamInput in) throws IOException {
        this(in.readVInt(), in.readInt(), in.readInt());
        validate();
    }

    /**
     * The field-level invariants every {@code ShardRange} must satisfy, re-checked on both
     * <em>read</em> paths ({@link #ShardRange(StreamInput)} and {@link #parse(XContentParser)})
     * because neither of them goes through the {@link SplitShardsMetadata.Builder} that enforces
     * the higher-level range invariants. This is the raw material those higher-level checks are
     * built on: {@code SplitShardsMetadata#validateShardRanges} reasons about contiguity in terms
     * of {@code start}/{@code end}, and {@link SplitShardsMetadata#getShardIdOfHash} binary-searches
     * on the assumption that a range actually spans forwards -- an inverted range silently makes
     * that search unable to find any owner for a hash.
     *
     * <p>{@code start} and {@code end} are hash bounds spanning the whole signed-int space (a
     * never-split root shard's range is literally {@code [Integer.MIN_VALUE, Integer.MAX_VALUE]}),
     * so a negative bound is entirely legitimate: it is the <em>relation</em> between them that is
     * checked here, not their sign. {@code shardId} is an ordinary shard number and must be
     * non-negative.
     *
     * @throws IllegalArgumentException if the shard id is negative or the range runs backwards.
     */
    private void validate() {
        if (shardId < 0) {
            throw new IllegalArgumentException("Shard range has a negative shard id: " + shardId);
        }
        if (start > end) {
            throw new IllegalArgumentException(
                "Shard range of shard " + shardId + " starts at " + start + " but ends before it, at " + end
            );
        }
    }

    public boolean contains(int hash) {
        return hash >= start && hash <= end;
    }

    @Override
    public int compareTo(ShardRange o) {
        return Integer.compare(start, o.start);
    }

    @Override
    public String toString() {
        return "ShardRange{" + "shardId=" + shardId + ", start=" + start + ", end=" + end + '}';
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeInt(start);
        out.writeInt(end);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("shard_id", shardId).field("start", start).field("end", end);
        builder.endObject();
        return builder;
    }

    /**
     * Reads one shard range from a {@code {"shard_id":..,"start":..,"end":..}} object.
     *
     * <p>All three fields are required, and the result is put through {@code validate()} exactly
     * as the stream constructor is. This blob is fed from remote-store manifests, so "field simply
     * absent" is not a shape a legitimate writer ever produces ({@link #toXContent} always writes
     * all three) but is very much a shape a corrupt or tampered blob can: silently defaulting a
     * missing bound used to admit a range nothing owns into the routing tables.
     *
     * @throws IllegalArgumentException if the object is truncated, is missing any of the three
     *                                  fields, or carries a range that fails {@code validate()}.
     */
    public static ShardRange parse(XContentParser parser) throws IOException {
        int shardId = -1, start = -1, end = -1;
        boolean sawShardId = false, sawStart = false, sawEnd = false;
        XContentParser.Token token;
        String fieldName = null;
        // `token != null` as well as the END_OBJECT check: at end of input nextToken() returns null
        // forever, so a blob truncated mid-object would otherwise spin this loop indefinitely rather
        // than failing.
        while ((token = parser.nextToken()) != null && token != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if ("shard_id".equals(fieldName)) {
                    shardId = parser.intValue();
                    sawShardId = true;
                } else if ("start".equals(fieldName)) {
                    start = parser.intValue();
                    sawStart = true;
                } else if ("end".equals(fieldName)) {
                    end = parser.intValue();
                    sawEnd = true;
                }
            }
        }
        if (token == null) {
            throw new IllegalArgumentException("Shard range object is truncated");
        }
        if (sawShardId == false || sawStart == false || sawEnd == false) {
            throw new IllegalArgumentException(
                "Shard range is missing required field(s):"
                    + (sawShardId ? "" : " shard_id")
                    + (sawStart ? "" : " start")
                    + (sawEnd ? "" : " end")
            );
        }

        ShardRange shardRange = new ShardRange(shardId, start, end);
        shardRange.validate();
        return shardRange;
    }
}
