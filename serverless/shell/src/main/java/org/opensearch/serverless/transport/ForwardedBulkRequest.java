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
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A batch of writes handed to the node that owns one shard.
 *
 * <p><b>One request per (shard, owner), not one per document</b>, and that is the whole point. Bulk
 * exists to stop pricing the write path per document; forwarding item by item would preserve the cost it
 * was built to remove, so a batch that crosses the network stays a batch on the far side and becomes one
 * log append there.
 *
 * <p>Carries {@link WalRecord}s rather than a parallel operation type. A record already distinguishes a
 * write from a deletion, is already what the log stores, and is already what replay applies — inventing
 * a second representation for the wire would mean three encodings of one idea and two chances to let
 * them disagree.
 */
public final class ForwardedBulkRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/document/bulk";

    private final String index;
    private final int shard;
    private final List<WalRecord> operations;
    private final boolean refresh;

    /**
     * Creates a forwarded batch.
     *
     * @param index the index
     * @param shard the shard every operation routes to
     * @param operations the writes and deletions, in request order
     * @param refresh whether to make the batch visible before answering
     */
    public ForwardedBulkRequest(String index, int shard, List<WalRecord> operations, boolean refresh) {
        this.index = index;
        this.shard = shard;
        this.operations = List.copyOf(operations);
        this.refresh = refresh;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedBulkRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.shard = in.readVInt();
        final int count = in.readVInt();
        final List<WalRecord> read = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final String id = in.readString();
            final String source = in.readString();
            read.add(in.readBoolean() ? WalRecord.deletion(id) : new WalRecord(id, source));
        }
        this.operations = List.copyOf(read);
        this.refresh = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVInt(shard);
        out.writeVInt(operations.size());
        for (WalRecord operation : operations) {
            out.writeString(operation.id());
            out.writeString(operation.source());
            out.writeBoolean(operation.isDeletion());
        }
        out.writeBoolean(refresh);
    }

    /**
     * Returns the index.
     *
     * @return the index name
     */
    public String index() {
        return index;
    }

    /**
     * Returns the shard.
     *
     * @return the shard number
     */
    public int shard() {
        return shard;
    }

    /**
     * Returns the operations, in the order they must be applied.
     *
     * @return the batch
     */
    public List<WalRecord> operations() {
        return operations;
    }

    /**
     * Returns whether the batch should be refreshed before answering.
     *
     * @return true to refresh
     */
    public boolean refresh() {
        return refresh;
    }
}
