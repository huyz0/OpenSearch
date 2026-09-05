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
import org.opensearch.serverless.storage.retention.PitrRetentionPolicy;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Names an index by its name and a {@code snapshotId} for {@link IndexSnapshotReleaseAction}. */
public class IndexSnapshotReleaseRequest extends ActionRequest {

    private final String indexName;
    private final String snapshotId;

    /**
     * Creates a request.
     *
     * @param indexName name of the index to release {@code snapshotId} from, every shard.
     * @param snapshotId the snapshot name whose pins should be released.
     */
    public IndexSnapshotReleaseRequest(String indexName, String snapshotId) {
        this.indexName = indexName;
        this.snapshotId = snapshotId;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link IndexSnapshotReleaseRequest}.
     */
    public IndexSnapshotReleaseRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.snapshotId = in.readString();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeString(snapshotId);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexName == null || indexName.isEmpty()) {
            validationException = addValidationError("indexName is required", validationException);
        }
        if (snapshotId == null || snapshotId.isEmpty()) {
            validationException = addValidationError("snapshotId is required", validationException);
        } else if (SnapshotPinRequest.isWellFormedSnapshotId(snapshotId) == false) {
            // See SnapshotPinRequest#isWellFormedSnapshotId: this string becomes a pin id and a blob-name
            // suffix, so an unrestricted alphabet is a path and a namespace the caller picks.
            validationException = addValidationError(SnapshotPinRequest.SNAPSHOT_ID_CHARSET_ERROR, validationException);
        } else if (snapshotId.startsWith(SnapshotPinRequest.DEEP_SNAPSHOT_PIN_ID_PREFIX)) {
            // The deep-snapshot export mints its own per-shard pins under this prefix and releases them in
            // a finally block. A caller allowed to name a snapshot into that namespace could remove the pin
            // holding a copy in progress, or leave one behind that the export will never release. The
            // per-shard action deliberately does NOT reject this prefix -- that is the path the export
            // itself uses -- so the refusal belongs on the operator-facing entry points.
            validationException = addValidationError(
                SnapshotPinRequest.reservedSnapshotIdError(SnapshotPinRequest.DEEP_SNAPSHOT_PIN_ID_PREFIX),
                validationException
            );
        } else if (snapshotId.equals(PitrRetentionPolicy.PITR_PIN_ID)) {
            // See SnapshotReleaseRequest's own validate() for why this exact name is reserved.
            validationException = addValidationError(
                "snapshotId [" + PitrRetentionPolicy.PITR_PIN_ID + "] is reserved for internal PITR retention and cannot be used",
                validationException
            );
        }
        return validationException;
    }

    /** Name of the index to release every shard's pin on. */
    public String indexName() {
        return indexName;
    }

    /** The snapshot name whose pins should be released. */
    public String snapshotId() {
        return snapshotId;
    }
}
