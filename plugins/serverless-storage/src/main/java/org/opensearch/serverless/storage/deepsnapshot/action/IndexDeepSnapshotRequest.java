/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.deepsnapshot.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Names an index by its name (resolved from cluster state or, for a gated index, its descriptor --
 * see {@link TransportIndexDeepSnapshotAction}), the repository to copy it into, and the snapshot
 * name to copy it under.
 *
 * <p>A {@link ClusterManagerNodeRequest}, unlike every action in {@code retention.action}: those are
 * pure object-store I/O and need no specific node, but {@link
 * org.opensearch.repositories.Repository#finalizeSnapshot} submits a cluster state update and fails
 * {@code NotClusterManagerException} anywhere else, so the index-level orchestration this request
 * drives has to run where that call can succeed. The per-shard byte copy underneath does not share
 * that constraint -- see {@link ShardDeepSnapshotRequest}'s own javadoc.
 */
public class IndexDeepSnapshotRequest extends ClusterManagerNodeRequest<IndexDeepSnapshotRequest> {

    private final String indexName;
    private final String repositoryName;
    private final String snapshotName;

    /**
     * Creates a request.
     *
     * @param indexName the index to copy, every shard.
     * @param repositoryName the repository to copy it into.
     * @param snapshotName the snapshot name to copy it under.
     */
    public IndexDeepSnapshotRequest(String indexName, String repositoryName, String snapshotName) {
        this.indexName = indexName;
        this.repositoryName = repositoryName;
        this.snapshotName = snapshotName;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link IndexDeepSnapshotRequest}.
     */
    public IndexDeepSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.repositoryName = in.readString();
        this.snapshotName = in.readString();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeString(repositoryName);
        out.writeString(snapshotName);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexName == null || indexName.isEmpty()) {
            validationException = addValidationError("indexName is required", validationException);
        }
        if (repositoryName == null || repositoryName.isEmpty()) {
            validationException = addValidationError("repositoryName is required", validationException);
        }
        if (snapshotName == null || snapshotName.isEmpty()) {
            validationException = addValidationError("snapshotName is required", validationException);
        }
        return validationException;
    }

    /** The index to copy, every shard. */
    public String indexName() {
        return indexName;
    }

    /** The repository to copy it into. */
    public String repositoryName() {
        return repositoryName;
    }

    /** The snapshot name to copy it under. */
    public String snapshotName() {
        return snapshotName;
    }
}
