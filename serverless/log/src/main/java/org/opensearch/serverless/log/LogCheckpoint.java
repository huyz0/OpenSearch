/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.log;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * A position in a specific activated log. Not an engine sequence number, S3 ordinal, or proof of
 * commit. Only the log authority can attest that an entry is committed, and only a pinned reader can
 * attest that it is searchable. Deserialized checkpoints must be authenticated/verified before use.
 *
 * @param logId durable history identity
 * @param index log entry index, or -1 for an empty history
 * @param entryTerm term of the referenced entry, not necessarily the current leader's term
 */
@InternalApi
public record LogCheckpoint(ShardLogId logId, long index, long entryTerm) implements Writeable {

    private static final int FORMAT_VERSION = 1;

    /**
     * Validates the coordinate; this does not establish commit authority.
     *
     * @param logId durable history identity
     * @param index entry index, or -1 for an empty history
     * @param entryTerm referenced entry's term
     */
    public LogCheckpoint {
        Objects.requireNonNull(logId);
        if (index < -1 || entryTerm < 0 || (index == -1 && entryTerm != 0)) {
            throw new IllegalArgumentException("checkpoint requires index >= -1 and term >= 0; empty history has term 0");
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte) FORMAT_VERSION);
        logId.writeTo(out);
        out.writeZLong(index);
        out.writeVLong(entryTerm);
    }

    /**
     * Reads a bounded versioned coordinate. It must be verified before making freshness decisions.
     *
     * @param in encoded checkpoint
     * @return decoded coordinate
     * @throws IOException on malformed or unsupported coordinates
     */
    public static LogCheckpoint readFrom(StreamInput in) throws IOException {
        final int version = Byte.toUnsignedInt(in.readByte());
        if (version != FORMAT_VERSION) {
            throw new IOException("unsupported log checkpoint format " + version);
        }
        final ShardLogId logId = ShardLogId.readFrom(in);
        final long index = in.readZLong();
        final long term = in.readVLong();
        try {
            return new LogCheckpoint(logId, index, term);
        } catch (IllegalArgumentException e) {
            throw new IOException("invalid log checkpoint", e);
        }
    }
}
