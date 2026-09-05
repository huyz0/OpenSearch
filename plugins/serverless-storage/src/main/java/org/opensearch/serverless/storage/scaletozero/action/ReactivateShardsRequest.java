/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Reactivates every suspended shard of one index, either the writer or the reader (search-only)
 * copies (rfc-serverless-opensearch.md &sect;7.3) -- see {@code
 * org.opensearch.serverless.storage.scaletozero.ShardReactivationActionFilter}, this request's only
 * caller. A {@link ClusterManagerNodeRequest} specifically because {@code
 * ShardReactivationActionFilter} runs on whichever node happens to receive the triggering write or
 * search request, which is frequently not the elected cluster-manager node -- {@link
 * org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction} is what
 * transparently forwards this request to whichever node actually is, rather than every caller
 * needing to know or care.
 */
public final class ReactivateShardsRequest extends ClusterManagerNodeRequest<ReactivateShardsRequest> {

    private final String indexName;
    private final boolean reader;
    private final Set<Integer> shardIds;

    /**
     * Creates a request that reactivates every suspended shard of the index.
     *
     * @param indexName the index whose suspended shards (if any) should be reactivated.
     * @param reader {@code true} to reactivate reader (search-only) copies, {@code false} for writer copies.
     */
    public ReactivateShardsRequest(String indexName, boolean reader) {
        this(indexName, reader, Collections.emptySet());
    }

    /**
     * Creates a request that reactivates only the named shards.
     *
     * <p><b>Finding L-3: why this narrowing exists.</b> Reactivation used to be unavoidably
     * index-granular, and on a multi-shard index with any steady traffic at all that was a permanent
     * churn loop. Take a 100-shard index where shard 0 is continuously written and shards 1-99 are
     * cold. Shards 1-99 go idle, are suspended and evicted. The very next write to shard 0 makes
     * "some shard of this index is suspended" true, fires a whole-index reactivation, and all 99
     * cold shards recover -- real recoveries, real object-store reads -- sit idle for the idle
     * threshold, and are suspended and evicted again. Forever, roughly every
     * {@code idle_threshold + cooldown}. The index can never scale to zero and pays continuous
     * recover/evict churn, which is the exact opposite of what scale-to-zero is for. The filter's
     * own defence of index-granularity -- "reactivating an unrelated already-active shard is a
     * no-op" -- is true for an active shard and false for a suspended one, which is the case that
     * matters.
     *
     * @param indexName the index whose suspended shards should be reactivated.
     * @param reader {@code true} to reactivate reader (search-only) copies, {@code false} for writer copies.
     * @param shardIds the shard numbers to reactivate. An <b>empty</b> set means "every suspended
     *                 shard of the index" -- the original behaviour, still correct for a search,
     *                 which genuinely needs every shard, and the safe fallback whenever a caller
     *                 cannot work out which shards a request will touch.
     */
    public ReactivateShardsRequest(String indexName, boolean reader, Set<Integer> shardIds) {
        this.indexName = indexName;
        this.reader = reader;
        this.shardIds = shardIds == null ? Collections.emptySet() : Set.copyOf(shardIds);
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ReactivateShardsRequest}.
     */
    public ReactivateShardsRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.reader = in.readBoolean();
        int shardCount = in.readVInt();
        Set<Integer> ids = new LinkedHashSet<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            ids.add(in.readVInt());
        }
        this.shardIds = Collections.unmodifiableSet(ids);
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeBoolean(reader);
        out.writeVInt(shardIds.size());
        for (Integer shardId : shardIds) {
            out.writeVInt(shardId);
        }
    }

    /** The index whose suspended shards (if any) should be reactivated. */
    public String indexName() {
        return indexName;
    }

    /** {@code true} to reactivate reader (search-only) copies, {@code false} for writer copies. */
    public boolean reader() {
        return reader;
    }

    /**
     * The shard numbers to reactivate, or an empty set meaning "every suspended shard of the index"
     * -- see the three-argument constructor for why the distinction matters.
     */
    public Set<Integer> shardIds() {
        return shardIds;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexName == null || indexName.isEmpty()) {
            validationException = addValidationError("indexName must not be empty", null);
        }
        return validationException;
    }
}
