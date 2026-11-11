/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.transport;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.serverless.store.CommitManifest;

import java.io.IOException;

/**
 * A writer's newest commit for a shard, or nothing if it does not write the shard. Carried as the register's own JSON,
 * which holds every field a reader needs; the binary form leaves some out on purpose.
 */
public final class NewestCommitResponse extends TransportResponse {

    private final CommitManifest commit;

    /**
     * Creates a response.
     *
     * @param commit the commit, or null
     */
    public NewestCommitResponse(CommitManifest commit) {
        this.commit = commit;
    }

    /**
     * Reads a response off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public NewestCommitResponse(StreamInput in) throws IOException {
        final BytesReference bytes = in.readOptionalBytesReference();
        this.commit = bytes == null ? null : CommitManifest.fromStream(bytes.streamInput());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBytesReference(commit == null ? null : commit.toBytes());
    }

    /** @return the commit, or null if the node does not write the shard */
    public CommitManifest commit() {
        return commit;
    }
}
