/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.manifest;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

/**
 * The write-ahead-log position covered by a commit: everything up to and including this position,
 * written under {@code writerEpoch}, is durably reflected in the commit and its chunks become
 * eligible for deletion once the manifest is published (rfc-serverless-opensearch.md &sect;6.4).
 */
public final class WalPosition implements Writeable {

    private final String writerEpoch;
    private final long offset;

    public WalPosition(String writerEpoch, long offset) {
        this.writerEpoch = Objects.requireNonNull(writerEpoch, "writerEpoch");
        this.offset = offset;
    }

    public WalPosition(StreamInput in) throws IOException {
        this(in.readString(), in.readVLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(writerEpoch);
        out.writeVLong(offset);
    }

    public String writerEpoch() {
        return writerEpoch;
    }

    public long offset() {
        return offset;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WalPosition)) return false;
        WalPosition that = (WalPosition) o;
        return offset == that.offset && writerEpoch.equals(that.writerEpoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(writerEpoch, offset);
    }

    @Override
    public String toString() {
        return "WalPosition{epoch='" + writerEpoch + "', offset=" + offset + '}';
    }
}
