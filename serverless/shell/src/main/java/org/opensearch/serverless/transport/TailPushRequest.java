/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A writer handing a search node the writes it has just acknowledged, by shard; see {@link TailPush}. */
public final class TailPushRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/tail/push";

    private final Map<ShardId, List<WalRecord>> records;

    /**
     * Creates a request.
     *
     * @param records the acknowledged records, by shard
     */
    public TailPushRequest(Map<ShardId, List<WalRecord>> records) {
        this.records = records;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public TailPushRequest(StreamInput in) throws IOException {
        super(in);
        final int shards = in.readVInt();
        this.records = new LinkedHashMap<>();
        for (int i = 0; i < shards; i++) {
            final ShardId shardId = new ShardId(new Index(in.readString(), in.readString()), in.readVInt());
            records.put(shardId, TailRecords.read(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(records.size());
        for (Map.Entry<ShardId, List<WalRecord>> entry : records.entrySet()) {
            out.writeString(entry.getKey().getIndexName());
            out.writeString(entry.getKey().getIndex().getUUID());
            out.writeVInt(entry.getKey().id());
            TailRecords.write(out, entry.getValue());
        }
    }

    /** @return the records, by shard */
    public Map<ShardId, List<WalRecord>> records() {
        return records;
    }
}
