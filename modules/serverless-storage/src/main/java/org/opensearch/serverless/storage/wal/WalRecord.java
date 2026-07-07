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
 */
public final class WalRecord {

    private final String indexUuid;
    private final int shardId;
    private final long seqNo;
    private final byte[] payload;

    public WalRecord(String indexUuid, int shardId, long seqNo, byte[] payload) {
        this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid");
        this.shardId = shardId;
        this.seqNo = seqNo;
        this.payload = Objects.requireNonNull(payload, "payload");
    }

    public String indexUuid() {
        return indexUuid;
    }

    public int shardId() {
        return shardId;
    }

    public long seqNo() {
        return seqNo;
    }

    public byte[] payload() {
        return payload;
    }

    public boolean belongsTo(String indexUuid, int shardId) {
        return this.indexUuid.equals(indexUuid) && this.shardId == shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WalRecord)) return false;
        WalRecord that = (WalRecord) o;
        return shardId == that.shardId
            && seqNo == that.seqNo
            && indexUuid.equals(that.indexUuid)
            && java.util.Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(indexUuid, shardId, seqNo);
        result = 31 * result + java.util.Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "WalRecord{index='" + indexUuid + "', shard=" + shardId + ", seqNo=" + seqNo + ", payloadBytes=" + payload.length + '}';
    }
}
