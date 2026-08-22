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

/** Names an index by its name and a {@code snapshotId} for {@link IndexSnapshotRestoreAction}. */
public class IndexSnapshotRestoreRequest extends ActionRequest {

    /** The value of {@link #restoreToMillis} that means "not a point-in-time restore". */
    public static final long NO_INSTANT = SnapshotRestoreRequest.NO_INSTANT;

    private final String indexName;
    private final String snapshotId;
    private final long restoreToMillis;

    /**
     * Creates a point-in-time request: restore every shard to whatever generation was current then.
     *
     * @param indexName name of the index to restore.
     * @param restoreToMillis the instant to restore to, in epoch millis.
     */
    public static IndexSnapshotRestoreRequest toInstant(String indexName, long restoreToMillis) {
        return new IndexSnapshotRestoreRequest(indexName, null, restoreToMillis);
    }

    /**
     * Creates a request.
     *
     * @param indexName name of the index to restore, every shard, to {@code snapshotId}.
     * @param snapshotId the snapshot name to restore every shard from.
     */
    public IndexSnapshotRestoreRequest(String indexName, String snapshotId) {
        this(indexName, snapshotId, NO_INSTANT);
    }

    private IndexSnapshotRestoreRequest(String indexName, String snapshotId, long restoreToMillis) {
        this.indexName = indexName;
        this.snapshotId = snapshotId;
        this.restoreToMillis = restoreToMillis;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link IndexSnapshotRestoreRequest}.
     */
    public IndexSnapshotRestoreRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.snapshotId = in.readOptionalString();
        this.restoreToMillis = in.readLong();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeOptionalString(snapshotId);
        out.writeLong(restoreToMillis);
    }

    /** The instant to restore to in epoch millis, or {@link #NO_INSTANT} for a pin-named restore. */
    public long restoreToMillis() {
        return restoreToMillis;
    }

    /** Whether this request names an instant rather than a pin. */
    public boolean restoresToInstant() {
        return restoreToMillis != NO_INSTANT;
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexName == null || indexName.isEmpty()) {
            validationException = addValidationError("indexName is required", validationException);
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

    /** Name of the index to restore every shard of. */
    public String indexName() {
        return indexName;
    }

    /** The snapshot name to restore every shard from. */
    public String snapshotId() {
        return snapshotId;
    }
}
