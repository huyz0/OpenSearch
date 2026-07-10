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
