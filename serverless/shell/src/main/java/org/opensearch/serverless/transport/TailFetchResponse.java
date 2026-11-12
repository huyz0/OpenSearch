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

/** The acknowledged writes a writer holds for a shard above a sequence number. */
public final class TailFetchResponse extends TransportResponse {

    private final List<WalRecord> records;

    /**
     * Creates a response.
     *
     * @param records the records, possibly none
     */
    public TailFetchResponse(List<WalRecord> records) {
        this.records = records;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public TailFetchResponse(StreamInput in) throws IOException {
        this.records = TailRecords.read(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        TailRecords.write(out, records);
    }

    /** @return the records */
    public List<WalRecord> records() {
        return records;
    }
}
