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
    private final String indexUuid;
    private final int shard;
    private final List<WalRecord> operations;
    private final List<org.opensearch.serverless.shell.ServerlessNode.BulkOperation> batch;
    private final boolean refresh;
    private final long bytes;

    /**
     * Creates a forwarded batch.
     *
     * @param index the index
     * @param shard the shard every operation routes to
     * @param operations the writes and deletions, in request order
     * @param refresh whether to make the batch visible before answering
     */
    public ForwardedBulkRequest(String index, int shard, List<WalRecord> operations, boolean refresh) {
        this(index, null, shard, unconditional(operations), refresh);
    }

    /**
     * Creates a forwarded batch whose items may carry conditions.
     *
     * <p>A factory rather than a constructor because a list of operations and a list of records erase to
     * the same signature.
     *
     * @param index the index
     * @param shard the shard every operation routes to
     * @param batch the operations, in request order
     * @param refresh whether to make the batch visible before answering
     * @return the request
     */
    public static ForwardedBulkRequest of(
        String index,
        int shard,
        List<org.opensearch.serverless.shell.ServerlessNode.BulkOperation> batch,
        boolean refresh
    ) {
        return new ForwardedBulkRequest(index, null, shard, batch, refresh);
    }

    /**
     * Creates a forwarded batch that names the incarnation of the index it is for.
     *
     * <p>The uuid travels for the reason {@link ForwardedIndexRequest} gives: a recreated index keeps its
     * name, and a batch matched by name alone was applied to the previous incarnation's shard and
     * acknowledged into a log nothing will replay.
     *
     * @param index the index
     * @param indexUuid the uuid of the index the sender resolved, or null to match by name only
     * @param shard the shard every operation routes to
     * @param batch the operations, in request order
     * @param refresh whether to make the batch visible before answering
     * @return the request
     */
    public static ForwardedBulkRequest of(
        String index,
        String indexUuid,
        int shard,
        List<org.opensearch.serverless.shell.ServerlessNode.BulkOperation> batch,
        boolean refresh
    ) {
        return new ForwardedBulkRequest(index, indexUuid, shard, batch, refresh);
    }

    private ForwardedBulkRequest(
        String index,
        String indexUuid,
        int shard,
        List<org.opensearch.serverless.shell.ServerlessNode.BulkOperation> batch,
        boolean refresh
    ) {
        this.index = index;
        this.indexUuid = indexUuid;
        this.shard = shard;
        this.batch = List.copyOf(batch);
        final List<WalRecord> records = new ArrayList<>(batch.size());
        long counted = 0L;
        for (org.opensearch.serverless.shell.ServerlessNode.BulkOperation item : batch) {
            records.add(item.record());
            counted += item.record().source() == null ? 0L : item.record().source().length();
        }
        this.operations = List.copyOf(records);
        this.refresh = refresh;
        this.bytes = counted;
    }

    private static List<org.opensearch.serverless.shell.ServerlessNode.BulkOperation> unconditional(List<WalRecord> operations) {
        final List<org.opensearch.serverless.shell.ServerlessNode.BulkOperation> batch = new ArrayList<>(operations.size());
        for (WalRecord record : operations) {
            batch.add(org.opensearch.serverless.shell.ServerlessNode.BulkOperation.of(record));
        }
        return batch;
    }

    /**
     * Returns the operations with their conditions, in the order they must be applied.
     *
     * @return the batch
     */
    public List<org.opensearch.serverless.shell.ServerlessNode.BulkOperation> batch() {
        return batch;
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
        final List<org.opensearch.serverless.shell.ServerlessNode.BulkOperation> readBatch = new ArrayList<>(count);
        long counted = 0L;
        for (int i = 0; i < count; i++) {
            final String id = in.readString();
            final String source = in.readString();
            final WalRecord record = in.readBoolean() ? WalRecord.deletion(id) : new WalRecord(id, source);
            final long ifSeqNo = in.readZLong();
            final long ifPrimaryTerm = in.readVLong();
            final boolean requireAbsent = in.readBoolean();
            read.add(record);
            readBatch.add(new org.opensearch.serverless.shell.ServerlessNode.BulkOperation(record, ifSeqNo, ifPrimaryTerm, requireAbsent));
            counted += source == null ? 0L : source.length();
        }
        this.operations = List.copyOf(read);
        this.batch = List.copyOf(readBatch);
        this.refresh = in.readBoolean();
        this.indexUuid = in.readOptionalString();
        this.bytes = counted;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVInt(shard);
        out.writeVInt(batch.size());
        for (org.opensearch.serverless.shell.ServerlessNode.BulkOperation item : batch) {
            out.writeString(item.record().id());
            out.writeString(item.record().source());
            out.writeBoolean(item.record().isDeletion());
            out.writeZLong(item.ifSeqNo());
            out.writeVLong(item.ifPrimaryTerm());
            out.writeBoolean(item.requireAbsent());
        }
        out.writeBoolean(refresh);
        out.writeOptionalString(indexUuid);
    }

    /**
     * Returns the uuid of the index incarnation the sender resolved.
     *
     * @return the uuid, or null when the sender did not name one
     */
    public String indexUuid() {
        return indexUuid;
    }

    /**
     * Returns the size of the batch's sources, for pressure accounting and the forward's deadline.
     *
     * <p>Counted once as the request is built rather than by re-encoding every record on the receiving
     * side, which was a second full JSON encode per forwarded batch spent purely on counting.
     *
     * @return the summed source lengths
     */
    public long bytes() {
        return bytes;
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
