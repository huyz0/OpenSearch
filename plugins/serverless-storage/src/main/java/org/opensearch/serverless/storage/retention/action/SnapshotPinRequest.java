/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.retention.PitrRetentionPolicy;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Names a shard by (indexUuid, shardId) and a {@code snapshotId} for {@link SnapshotPinAction}. */
public class SnapshotPinRequest extends ActionRequest {

    private final String indexUuid;
    private final int shardId;
    private final String snapshotId;

    /**
     * Creates a request.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param snapshotId the snapshot name to pin the shard's current manifest generation under.
     */
    public static SnapshotPinRequest expiring(String indexUuid, int shardId, String snapshotId, long expiresAtMillis) {
        SnapshotPinRequest request = new SnapshotPinRequest(indexUuid, shardId, snapshotId);
        request.expiresAtMillis = expiresAtMillis;
        return request;
    }

    /** When the pin this takes stops holding its generation, or {@link PinRecord#NEVER_EXPIRES}. */
    public long expiresAtMillis() {
        return expiresAtMillis;
    }

    private long expiresAtMillis = PinRecord.NEVER_EXPIRES;

    public SnapshotPinRequest(String indexUuid, int shardId, String snapshotId) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.snapshotId = snapshotId;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link SnapshotPinRequest}.
     */
    public SnapshotPinRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.snapshotId = in.readString();
        this.expiresAtMillis = in.readLong();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeString(snapshotId);
        out.writeLong(expiresAtMillis);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexUuid == null || indexUuid.isEmpty()) {
            validationException = addValidationError("indexUuid is required", validationException);
        } else if (isWellFormedIndexUuid(indexUuid) == false) {
            validationException = addValidationError(INDEX_UUID_CHARSET_ERROR, validationException);
        }
        if (shardId < 0) {
            validationException = addValidationError("shardId must be >= 0", validationException);
        }
        if (snapshotId == null || snapshotId.isEmpty()) {
            validationException = addValidationError("snapshotId is required", validationException);
        } else if (snapshotId.equals(PitrRetentionPolicy.PITR_PIN_ID)) {
            // "pitr" is the reserved pinId PitrRetentionReconciler uses for its own internal
            // pins (see that class's own javadoc, and DurablePinRegistry#replacePin's "strips
            // every other pin sharing this pinId" semantics). A snapshot pin under this exact
            // name would either wipe out every legitimate PITR-window pin on this shard right
            // now, or get silently deleted itself the next time the reconciler ticks and treats
            // it as its own stale entry -- neither of which the caller would ever be told about.
            validationException = addValidationError(
                "snapshotId [" + PitrRetentionPolicy.PITR_PIN_ID + "] is reserved for internal PITR retention and cannot be used",
                validationException
            );
        }
        return validationException;
    }

    /** The message reported for an {@code indexUuid} carrying characters no real uuid contains. */
    static final String INDEX_UUID_CHARSET_ERROR = "indexUuid must contain only [A-Za-z0-9_-]";

    /**
     * Whether {@code indexUuid} is drawn from the alphabet a real index UUID is drawn from --
     * OpenSearch generates them as unpadded URL-safe base64, so letters, digits, {@code -} and
     * {@code _} and nothing else (the {@code _na_} placeholder also fits). Shared by the other two
     * retention requests in this package.
     *
     * <p>Defence in depth, not the primary control: the authoritative check is
     * {@code TransportSnapshotPinAction#requireRealShard}, which resolves the uuid against cluster
     * metadata and so answers "does this name a real shard", which no character test can. This one
     * exists because the uuid ends up as a blob-path segment
     * ({@code BlobPath.cleanPath().add(indexUuid)}) resolved without normalisation, and a boundary
     * that refuses {@code /} and {@code ..} outright is worth having independently of whether the
     * metadata lookup downstream is ever bypassed or reordered.
     */
    static boolean isWellFormedIndexUuid(String indexUuid) {
        for (int i = 0; i < indexUuid.length(); i++) {
            char c = indexUuid.charAt(i);
            if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
                continue;
            }
            return false;
        }
        return true;
    }

    /** UUID of the index the shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** The snapshot name to pin the shard's current manifest generation under. */
    public String snapshotId() {
        return snapshotId;
    }
}
