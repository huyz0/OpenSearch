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

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Names a shard by (indexUuid, shardId) and what to restore it to, which is one of two things.
 *
 * <p>A {@code snapshotId} names a pin: restore to the generation someone deliberately held. A {@code
 * restoreToMillis} names an instant: restore to whichever generation was current then, resolved by {@link
 * org.opensearch.serverless.storage.retention.PitrRestoreResolution}. Exactly one is required, and asking
 * for both is refused rather than silently preferring one -- they can disagree, and a caller who supplied
 * both has not decided what they want.
 */
public class SnapshotRestoreRequest extends ActionRequest {

    /** The value of {@link #restoreToMillis} that means "not a point-in-time restore". */
    public static final long NO_INSTANT = -1L;

    private final String indexUuid;
    private final int shardId;
    private final String snapshotId;
    private final long restoreToMillis;

    /**
     * Creates a request.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param snapshotId the snapshot name whose pinned generation the shard's head should be restored to.
     */
    public SnapshotRestoreRequest(String indexUuid, int shardId, String snapshotId) {
        this(indexUuid, shardId, snapshotId, NO_INSTANT);
    }

    /**
     * Creates a point-in-time request: restore this shard to whatever generation was current at
     * {@code restoreToMillis}.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     * @param restoreToMillis the instant to restore to, in epoch millis.
     */
    public static SnapshotRestoreRequest toInstant(String indexUuid, int shardId, long restoreToMillis) {
        return new SnapshotRestoreRequest(indexUuid, shardId, null, restoreToMillis);
    }

    private SnapshotRestoreRequest(String indexUuid, int shardId, String snapshotId, long restoreToMillis) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.snapshotId = snapshotId;
        this.restoreToMillis = restoreToMillis;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link SnapshotRestoreRequest}.
     */
    public SnapshotRestoreRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.snapshotId = in.readOptionalString();
        this.restoreToMillis = in.readLong();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeOptionalString(snapshotId);
        out.writeLong(restoreToMillis);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexUuid == null || indexUuid.isEmpty()) {
            validationException = addValidationError("indexUuid is required", validationException);
        }
        if (shardId < 0) {
            validationException = addValidationError("shardId must be >= 0", validationException);
        }
        boolean named = snapshotId != null && snapshotId.isEmpty() == false;
        boolean timed = restoreToMillis != NO_INSTANT;
        if (named == false && timed == false) {
            validationException = addValidationError("one of snapshotId or restoreToMillis is required", validationException);
        }
        if (named && timed) {
            validationException = addValidationError(
                "snapshotId and restoreToMillis cannot both be set: a pin and an instant can resolve to "
                    + "different generations, so this asks for one or the other",
                validationException
            );
        }
        if (timed && restoreToMillis < 0) {
            validationException = addValidationError("restoreToMillis must be >= 0", validationException);
        }
        return validationException;
    }

    /** UUID of the index the shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** The snapshot name whose pinned generation the shard's head should be restored to, or null. */
    public String snapshotId() {
        return snapshotId;
    }

    /** The instant to restore to in epoch millis, or {@link #NO_INSTANT} for a pin-named restore. */
    public long restoreToMillis() {
        return restoreToMillis;
    }

    /** Whether this request names an instant rather than a pin. */
    public boolean restoresToInstant() {
        return restoreToMillis != NO_INSTANT;
    }
}
