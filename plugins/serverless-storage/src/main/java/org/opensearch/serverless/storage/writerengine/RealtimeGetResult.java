/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

/**
 * The outcome of a real-time {@code _get} routed directly to a shard's writer engine
 * (rfc-serverless-opensearch.md &sect;8: "{@code _get} by document id can optionally route to the
 * writer shard for true realtime gets (the writer has the live version map)").
 *
 * <p>Deliberately existence/version/seqNo only, not the document's {@code _source}: extracting
 * {@code _source} needs a {@code MapperService}/{@code ShardGetService}-level context this
 * plugin's {@link ShardActivityRegistry} (a plain node-local engine lookup, with no {@code
 * IndexShard} reference) does not carry. This is the real-time <em>existence and version</em>
 * check the writer's live version map actually answers authoritatively that a stale reader copy
 * cannot -- see {@link ObjectStoreWriterEngine}'s own javadoc for why its version map is
 * authoritative and a reader engine's is not (a reader engine has none at all).
 */
public final class RealtimeGetResult {

    private final boolean exists;
    private final long version;
    private final long seqNo;
    private final long primaryTerm;

    /**
     * Creates a result.
     *
     * @param exists whether the document currently exists per the writer's live version map
     * @param version the document's current version, or {@link org.opensearch.common.lucene.uid.Versions#NOT_FOUND} if it doesn't exist
     * @param seqNo the document's current sequence number, meaningless if {@code exists} is {@code false}
     * @param primaryTerm the document's current primary term, meaningless if {@code exists} is {@code false}
     */
    public RealtimeGetResult(boolean exists, long version, long seqNo, long primaryTerm) {
        this.exists = exists;
        this.version = version;
        this.seqNo = seqNo;
        this.primaryTerm = primaryTerm;
    }

    /** Whether the document currently exists per the writer's live version map. */
    public boolean exists() {
        return exists;
    }

    /** The document's current version. */
    public long version() {
        return version;
    }

    /** The document's current sequence number, meaningless if {@link #exists()} is {@code false}. */
    public long seqNo() {
        return seqNo;
    }

    /** The document's current primary term, meaningless if {@link #exists()} is {@code false}. */
    public long primaryTerm() {
        return primaryTerm;
    }
}
