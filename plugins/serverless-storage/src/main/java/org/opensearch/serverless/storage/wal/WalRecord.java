/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import java.util.Objects;

/**
 * One operation buffered by the node-level WAL service (rfc-serverless-opensearch.md
 * &sect;6.4). The chunk format treats {@code payload} as opaque bytes: it does not know or care
 * whether it holds a serialized {@code Translog.Operation} or, per rfc-serverless-opensearch.md
 * &sect;12, ciphertext produced by per-record envelope encryption keyed to {@code indexUuid}.
 * That separation is deliberate &mdash; the WAL wire format and the encryption policy are
 * independent concerns.
 *
 * <p>{@code primaryTerm} is a necessary, but on its own <b>not sufficient</b>, piece of fencing
 * under this WAL service's actual sharing model: one {@code writerEpoch} (and its chunk stream)
 * spans every writer shard on the node for as long as the node's own WAL service component stays
 * up, mixing many shards' (and, over time, many terms') records together in the same chunks -- the
 * entire reason a node-level WAL service exists over a per-shard one is exactly this cross-shard
 * batching (rfc-serverless-opensearch.md &sect;6.4's cost-sanity argument). That rules out "fence a
 * superseded writer by discarding its epoch directory" (a shared epoch can't be discarded without
 * also fencing every other shard using it). A per-record {@code primaryTerm} filter (see {@link
 * WalChunkReader#filterByShardAndMinimumTerm}) correctly excludes records from a term that never
 * validly held the lease at all, but cannot by itself exclude a record legitimately tagged with a
 * once-valid term that was actually written <em>after</em> that term was superseded (a writer
 * unaware it has just been fenced can keep appending for a real window) -- see that method's own
 * javadoc for why closing this gap needs either an append-time fencing check (not implemented) or
 * a cutoff tied to the actual moment of lease transfer, not to term identity alone.
 */
public final class WalRecord {

    private final String indexUuid;
    private final int shardId;
    private final long primaryTerm;
    private final long seqNo;
    private final byte[] payload;

    /**
     * Creates a WAL record for one buffered operation.
     *
     * @param indexUuid the UUID of the index the operation belongs to
     * @param shardId the shard the operation belongs to
     * @param primaryTerm the primary term the writer believed was current when this record was appended
     * @param seqNo the operation's sequence number
     * @param payload the opaque operation bytes, possibly per-record ciphertext
     */
    public WalRecord(String indexUuid, int shardId, long primaryTerm, long seqNo, byte[] payload) {
        this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid");
        this.shardId = shardId;
        this.primaryTerm = primaryTerm;
        this.seqNo = seqNo;
        this.payload = Objects.requireNonNull(payload, "payload");
    }

    /** Returns the UUID of the index this record's operation belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** Returns the shard this record's operation belongs to. */
    public int shardId() {
        return shardId;
    }

    /** Returns the primary term the writer believed was current when this record was appended. */
    public long primaryTerm() {
        return primaryTerm;
    }

    /** Returns this record's operation sequence number. */
    public long seqNo() {
        return seqNo;
    }

    /** Returns the opaque operation bytes, possibly per-record ciphertext. */
    public byte[] payload() {
        return payload;
    }

    /**
     * Returns whether this record was appended for the given index UUID and shard.
     *
     * @param indexUuid the index UUID to compare against
     * @param shardId the shard to compare against
     */
    public boolean belongsTo(String indexUuid, int shardId) {
        return this.indexUuid.equals(indexUuid) && this.shardId == shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WalRecord)) return false;
        WalRecord that = (WalRecord) o;
        return shardId == that.shardId
            && primaryTerm == that.primaryTerm
            && seqNo == that.seqNo
            && indexUuid.equals(that.indexUuid)
            && java.util.Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indexUuid, shardId, primaryTerm, seqNo);
        result = 31 * result + java.util.Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "WalRecord{index='"
            + indexUuid
            + "', shard="
            + shardId
            + ", term="
            + primaryTerm
            + ", seqNo="
            + seqNo
            + ", payloadBytes="
            + payload.length
            + '}';
    }
}
