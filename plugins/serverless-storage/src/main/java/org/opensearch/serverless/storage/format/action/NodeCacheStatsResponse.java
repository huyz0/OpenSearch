/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * The result of a {@link NodeCacheStatsAction} request: the node-shared in-memory bundle cache's
 * hit/miss counts plus every reader shard's local disk cache stats this node has tracked.
 */
public class NodeCacheStatsResponse extends ActionResponse implements ToXContentObject {

    private final long inMemoryCacheHitCount;
    private final long inMemoryCacheMissCount;
    private final List<ShardCacheStatsEntry> diskCacheEntries;

    /**
     * Creates a response.
     *
     * @param inMemoryCacheHitCount reads served from the node-shared in-memory bundle cache so far.
     * @param inMemoryCacheMissCount reads that missed the node-shared in-memory bundle cache so far.
     * @param diskCacheEntries every reader shard's local disk cache stats currently tracked on the
     *                         receiving node -- see {@link org.opensearch.serverless.storage.format.CacheStatsRegistry#snapshotAll}
     *                         for exactly which shards that is (and isn't).
     */
    public NodeCacheStatsResponse(long inMemoryCacheHitCount, long inMemoryCacheMissCount, List<ShardCacheStatsEntry> diskCacheEntries) {
        this.inMemoryCacheHitCount = inMemoryCacheHitCount;
        this.inMemoryCacheMissCount = inMemoryCacheMissCount;
        this.diskCacheEntries = diskCacheEntries;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeCacheStatsResponse}.
     */
    public NodeCacheStatsResponse(StreamInput in) throws IOException {
        super(in);
        this.inMemoryCacheHitCount = in.readVLong();
        this.inMemoryCacheMissCount = in.readVLong();
        this.diskCacheEntries = in.readList(ShardCacheStatsEntry::new);
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(inMemoryCacheHitCount);
        out.writeVLong(inMemoryCacheMissCount);
        out.writeList(diskCacheEntries);
    }

    /** Reads served from the node-shared in-memory bundle cache so far. */
    public long inMemoryCacheHitCount() {
        return inMemoryCacheHitCount;
    }

    /** Reads that missed the node-shared in-memory bundle cache so far. */
    public long inMemoryCacheMissCount() {
        return inMemoryCacheMissCount;
    }

    /** Every reader shard's local disk cache stats currently tracked on the node that answered this request. */
    public List<ShardCacheStatsEntry> diskCacheEntries() {
        return diskCacheEntries;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .startObject("in_memory_cache")
            .field("hit_count", inMemoryCacheHitCount)
            .field("miss_count", inMemoryCacheMissCount)
            .endObject()
            .startArray("disk_caches");
        for (ShardCacheStatsEntry entry : diskCacheEntries) {
            entry.toXContent(builder, params);
        }
        return builder.endArray().endObject();
    }
}
