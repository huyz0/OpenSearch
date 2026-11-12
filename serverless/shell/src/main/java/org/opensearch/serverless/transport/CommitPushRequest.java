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
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;

/** A writer telling a search node that it has just published a shard's commit; see {@link CommitPush}. */
public final class CommitPushRequest extends TransportRequest {

    /** The transport action name. */
    public static final String ACTION = "internal:serverless/commit/push";

    private final String indexName;
    private final String indexUuid;
    private final int shard;
    private final CommitManifest commit;
    /**
     * The commit as it travels, kept as read: the request is signed over its serialized form, and a manifest written out
     * again need not come out byte for byte the same.
     */
    private final BytesReference commitBytes;

    /**
     * Creates a request.
     *
     * @param indexName the index
     * @param indexUuid the index's uuid
     * @param shard the shard number
     * @param commit the commit just published
     */
    public CommitPushRequest(String indexName, String indexUuid, int shard, CommitManifest commit) {
        this.indexName = indexName;
        this.indexUuid = indexUuid;
        this.shard = shard;
        this.commit = commit;
        try {
            this.commitBytes = commit.toBytes();
        } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /**
     * Reads a request off the wire.
     *
     * @param in the stream
     * @throws IOException if reading fails
     */
    public CommitPushRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.indexUuid = in.readString();
        this.shard = in.readVInt();
        this.commitBytes = in.readBytesReference();
        this.commit = CommitManifest.fromStream(commitBytes.streamInput());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeString(indexUuid);
        out.writeVInt(shard);
        out.writeBytesReference(commitBytes);
    }

    /** @return the index */
    public String indexName() {
        return indexName;
    }

    /** @return the index's uuid */
    public String indexUuid() {
        return indexUuid;
    }

    /** @return the shard number */
    public int shard() {
        return shard;
    }

    /** @return the commit just published */
    public CommitManifest commit() {
        return commit;
    }
}
