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
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.serverless.store.WalRecord;

import java.io.IOException;
import java.util.List;

/**
 * A writer's answer to a fetch: the acknowledged writes it holds for a shard above a sequence number, the subscription it
 * now pushes the asking node under, whether the records run without a gap from the asking node's commit, and how far it
 * has answered refreshing writes as refreshed.
 */
public final class TailFetchResponse extends TransportResponse {

    /** The {@link #subscription()} of an answer that subscribed nothing. */
    public static final long NONE = -1L;

    private final List<WalRecord> records;
    private final long subscription;
    private final boolean complete;
    private final long claimedThrough;

    /**
     * Creates a response that subscribed nothing and claims nothing.
     *
     * @param records the records, possibly none
     */
    public TailFetchResponse(List<WalRecord> records) {
        this(records, NONE, false, -1L);
    }

    /**
     * Creates a response.
     *
     * @param records the records, possibly none
     * @param subscription the subscription the asking node is pushed under, or {@link #NONE}
     * @param complete whether the records, with the asking node's commit, hold every write the writer has acknowledged
     * @param claimedThrough the highest sequence number a refreshing write may have been answered as refreshed through;
     *     {@link Long#MAX_VALUE} when the answering node cannot say
     */
    public TailFetchResponse(List<WalRecord> records, long subscription, boolean complete, long claimedThrough) {
        this.records = records;
        this.subscription = subscription;
        this.complete = complete;
        this.claimedThrough = claimedThrough;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public TailFetchResponse(StreamInput in) throws IOException {
        this.records = TailRecords.read(in);
        this.subscription = in.readZLong();
        this.complete = in.readBoolean();
        this.claimedThrough = in.readZLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TailRecords.write(out, records);
        out.writeZLong(subscription);
        out.writeBoolean(complete);
        out.writeZLong(claimedThrough);
    }

    /** @return the records */
    public List<WalRecord> records() {
        return records;
    }

    /** @return the subscription the asking node is pushed under, or {@link #NONE} */
    public long subscription() {
        return subscription;
    }

    /** @return whether the records, with the asking node's commit, hold every write the writer has acknowledged */
    public boolean complete() {
        return complete;
    }

    /** @return the highest sequence number a refreshing write may have been answered as refreshed through */
    public long claimedThrough() {
        return claimedThrough;
    }
}
