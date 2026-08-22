/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.nodecapacity;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The full cluster-wide node-autoscaling signal: writer and reader pools reported independently,
 * since they scale on different signals and different timescales (node autoscaling design doc,
 * "The signal") -- the plugin-computed half of the contract with an external control plane; see
 * {@code NodeCapacitySignalService} for how this is computed and {@code
 * org.opensearch.serverless.storage.nodecapacity.action.RestNodeCapacityAction} for how it's served.
 */
public final class NodeCapacitySignal implements Writeable, ToXContentObject {

    private final RoleCapacitySignal writer;
    private final RoleCapacitySignal reader;

    /** An empty-but-valid signal, used before the first evaluation completes. */
    public static NodeCapacitySignal empty() {
        RoleCapacitySignal empty = new RoleCapacitySignal(List.of(), 0, List.of(), 0, Map.of());
        return new NodeCapacitySignal(empty, empty);
    }

    /**
     * Creates a signal.
     *
     * @param writer the writer role's capacity signal.
     * @param reader the reader role's capacity signal.
     */
    public NodeCapacitySignal(RoleCapacitySignal writer, RoleCapacitySignal reader) {
        this.writer = writer;
        this.reader = reader;
    }

    /**
     * Deserializes a signal.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link NodeCapacitySignal}.
     */
    public NodeCapacitySignal(StreamInput in) throws IOException {
        this.writer = new RoleCapacitySignal(in);
        this.reader = new RoleCapacitySignal(in);
    }

    /** @param out stream to write this signal's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writer.writeTo(out);
        reader.writeTo(out);
    }

    /** The writer role's capacity signal. */
    public RoleCapacitySignal writer() {
        return writer;
    }

    /** The reader role's capacity signal. */
    public RoleCapacitySignal reader() {
        return reader;
    }

    /**
     * @param builder the builder to append this signal's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("writer");
        writer.toXContent(builder, params);
        builder.field("reader");
        reader.toXContent(builder, params);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeCapacitySignal that = (NodeCapacitySignal) o;
        return Objects.equals(writer, that.writer) && Objects.equals(reader, that.reader);
    }

    @Override
    public int hashCode() {
        return Objects.hash(writer, reader);
    }
}
