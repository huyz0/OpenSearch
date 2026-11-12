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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * How acknowledged records travel between a writer and its readers: one fixed binary form, so a request reads back to
 * the bytes it was signed over.
 */
final class TailRecords {

    private TailRecords() {}

    static void write(StreamOutput out, List<WalRecord> records) throws IOException {
        out.writeVInt(records.size());
        for (WalRecord record : records) {
            out.writeString(record.id());
            out.writeBoolean(record.isDeletion());
            out.writeString(record.isDeletion() ? "" : record.source());
            out.writeZLong(record.seqNo());
            out.writeZLong(record.primaryTerm());
            out.writeZLong(record.version());
        }
    }

    static List<WalRecord> read(StreamInput in) throws IOException {
        final int count = in.readVInt();
        final List<WalRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            final String id = in.readString();
            final boolean deletion = in.readBoolean();
            final String source = in.readString();
            final long seqNo = in.readZLong();
            final long primaryTerm = in.readZLong();
            final long version = in.readZLong();
            records.add(
                deletion ? WalRecord.deletion(id, seqNo, primaryTerm, version) : new WalRecord(id, source, seqNo, primaryTerm, version)
            );
        }
        return records;
    }
}
