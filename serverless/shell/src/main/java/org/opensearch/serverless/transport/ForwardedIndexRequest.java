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
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/** A write being handed to the node that owns the shard. */
public final class ForwardedIndexRequest extends TransportRequest {

    /** The transport action name. */
    /**
     * Named for writing rather than indexing, because it carries deletions too. A delete is routed,
     * forwarded, fenced and logged exactly like an index operation; only the terminal call differs.
     */
    public static final String ACTION = "internal:serverless/document/write";

    private final String index;
    private final int shard;
    private final String id;
    private final String source;
    private final boolean refresh;
    private final boolean deletion;
    private final long ifSeqNo;
    private final long ifPrimaryTerm;

    /**
     * Creates a request.
     *
     * @param index the index
     * @param shard the shard the document routes to
     * @param id the document id
     * @param source the document source
     * @param refresh whether to make the write visible before answering
     */
    public ForwardedIndexRequest(String index, int shard, String id, String source, boolean refresh) {
        this(index, shard, id, source, refresh, false);
    }

    /**
     * Creates a forwarded write.
     *
     * @param index the index
     * @param shard the shard
     * @param id the document id
     * @param source the document source, empty for a deletion
     * @param refresh whether to make the change visible before answering
     * @param deletion whether this removes the document rather than adding it
     */
    public ForwardedIndexRequest(String index, int shard, String id, String source, boolean refresh, boolean deletion) {
        this(index, shard, id, source, refresh, deletion, org.opensearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO, 0L);
    }

    /**
     * Creates a forwarded write that may be conditional.
     *
     * <p>The condition travels with the write rather than being evaluated before it leaves. It has to:
     * only the owner holds the document, so only the owner can compare against it, and a coordinator that
     * checked first would be comparing against a copy it does not have.
     *
     * @param index the index
     * @param shard the shard
     * @param id the document id
     * @param source the document source, empty for a deletion
     * @param refresh whether to make the change visible before answering
     * @param deletion whether this removes the document rather than adding it
     * @param ifSeqNo the sequence number the document must be at, or unassigned for an unconditional write
     * @param ifPrimaryTerm the primary term the document must be at, or 0 when unconditional
     */
    public ForwardedIndexRequest(
        String index,
        int shard,
        String id,
        String source,
        boolean refresh,
        boolean deletion,
        long ifSeqNo,
        long ifPrimaryTerm
    ) {
        this.deletion = deletion;
        this.index = index;
        this.shard = shard;
        this.id = id;
        this.source = source;
        this.refresh = refresh;
        this.ifSeqNo = ifSeqNo;
        this.ifPrimaryTerm = ifPrimaryTerm;
    }

    /**
     * Returns the sequence number the document must currently be at.
     *
     * @return the condition, or unassigned when the write is unconditional
     */
    public long ifSeqNo() {
        return ifSeqNo;
    }

    /**
     * Returns the primary term the document must currently be at.
     *
     * @return the condition, or 0 when the write is unconditional
     */
    public long ifPrimaryTerm() {
        return ifPrimaryTerm;
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public ForwardedIndexRequest(StreamInput in) throws IOException {
        super(in);
        this.index = in.readString();
        this.shard = in.readVInt();
        this.id = in.readString();
        this.source = in.readString();
        this.refresh = in.readBoolean();
        this.deletion = in.readBoolean();
        this.ifSeqNo = in.readZLong();
        this.ifPrimaryTerm = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(index);
        out.writeVInt(shard);
        out.writeString(id);
        out.writeString(source);
        out.writeBoolean(refresh);
        out.writeBoolean(deletion);
        out.writeZLong(ifSeqNo);
        out.writeVLong(ifPrimaryTerm);
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
     * Returns the document id.
     *
     * @return the id
     */
    public String id() {
        return id;
    }

    /**
     * Returns the document source.
     *
     * @return the source
     */
    public String source() {
        return source;
    }

    /**
     * Returns whether this removes the document rather than adding it.
     *
     * @return true for a deletion
     */
    public boolean deletion() {
        return deletion;
    }

    /**
     * Returns whether the write should be refreshed before answering.
     *
     * @return true to refresh
     */
    public boolean refresh() {
        return refresh;
    }
}
