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
import java.util.LinkedHashMap;
import java.util.Map;

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
    /** Blocks of the commit's new files, by cache key, read from the writer's own copies; empty if none ride along. */
    private final Map<String, byte[]> blocks;
    /** The mapping and settings versions of the descriptor the writer holds, or -1 if it did not say. */
    private final long mappingVersion;
    private final long settingsVersion;

    /**
     * Creates a request.
     *
     * @param indexName the index
     * @param indexUuid the index's uuid
     * @param shard the shard number
     * @param commit the commit just published
     */
    public CommitPushRequest(String indexName, String indexUuid, int shard, CommitManifest commit) {
        this(indexName, indexUuid, shard, commit, Map.of(), -1L, -1L);
    }

    /**
     * Creates a request that carries some of the commit's blocks.
     *
     * @param indexName the index
     * @param indexUuid the index's uuid
     * @param shard the shard number
     * @param commit the commit just published
     * @param blocks blocks of the commit's new files, by cache key
     * @param mappingVersion the mapping version of the descriptor the writer holds, or -1
     * @param settingsVersion the settings version of the descriptor the writer holds, or -1
     */
    public CommitPushRequest(
        String indexName,
        String indexUuid,
        int shard,
        CommitManifest commit,
        Map<String, byte[]> blocks,
        long mappingVersion,
        long settingsVersion
    ) {
        this.blocks = blocks;
        this.mappingVersion = mappingVersion;
        this.settingsVersion = settingsVersion;
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
        final int count = in.readVInt();
        this.blocks = new LinkedHashMap<>(count);
        for (int i = 0; i < count; i++) {
            blocks.put(in.readString(), in.readByteArray());
        }
        this.mappingVersion = in.readZLong();
        this.settingsVersion = in.readZLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeString(indexUuid);
        out.writeVInt(shard);
        out.writeBytesReference(commitBytes);
        out.writeVInt(blocks.size());
        for (Map.Entry<String, byte[]> block : blocks.entrySet()) {
            out.writeString(block.getKey());
            out.writeByteArray(block.getValue());
        }
        out.writeZLong(mappingVersion);
        out.writeZLong(settingsVersion);
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

    /** @return the mapping version of the descriptor the writer holds, or -1 if it did not say */
    public long mappingVersion() {
        return mappingVersion;
    }

    /** @return the settings version of the descriptor the writer holds, or -1 if it did not say */
    public long settingsVersion() {
        return settingsVersion;
    }

    /** @return blocks of the commit's new files, by cache key; empty if none came */
    public Map<String, byte[]> blocks() {
        return blocks;
    }
}
