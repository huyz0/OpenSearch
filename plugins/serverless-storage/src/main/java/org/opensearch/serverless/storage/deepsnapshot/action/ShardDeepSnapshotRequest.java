/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.deepsnapshot.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Names one shard, the repository to copy it into, and the snapshot identity to copy it under.
 *
 * <p>The snapshot name and uuid are carried on every shard request rather than generated per shard,
 * because {@link org.opensearch.snapshots.SnapshotId} equality is by both fields and {@link
 * org.opensearch.repositories.Repository#finalizeSnapshot} must be called with the exact same
 * identity every {@code snapshotShard} call used -- {@link IndexDeepSnapshotAction}'s transport
 * action generates the uuid once and threads it through every shard.
 */
public class ShardDeepSnapshotRequest extends ActionRequest {

    private final String indexName;
    private final String indexUuid;
    private final int shardId;
    private final String repositoryName;
    private final String snapshotName;
    private final String snapshotUuid;

    /**
     * Creates a request.
     *
     * @param indexName the index name, needed only for {@link org.opensearch.repositories.IndexId} and
     *                   {@link org.opensearch.core.index.shard.ShardId}, both of which carry it for
     *                   readability though the uuid is what identifies anything.
     * @param indexUuid UUID of the index the shard belongs to, which is what the object store is keyed by.
     * @param shardId the shard number within {@code indexUuid}.
     * @param repositoryName the repository to copy this shard's currently-pinned commit into.
     * @param snapshotName the snapshot name, shared by every shard of one {@link IndexDeepSnapshotAction} call.
     * @param snapshotUuid the snapshot uuid, likewise shared, generated once by the index-level action.
     */
    public ShardDeepSnapshotRequest(
        String indexName,
        String indexUuid,
        int shardId,
        String repositoryName,
        String snapshotName,
        String snapshotUuid
    ) {
        this.indexName = indexName;
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.repositoryName = repositoryName;
        this.snapshotName = snapshotName;
        this.snapshotUuid = snapshotUuid;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardDeepSnapshotRequest}.
     */
    public ShardDeepSnapshotRequest(StreamInput in) throws IOException {
        super(in);
        this.indexName = in.readString();
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
        this.repositoryName = in.readString();
        this.snapshotName = in.readString();
        this.snapshotUuid = in.readString();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexName);
        out.writeString(indexUuid);
        out.writeVInt(shardId);
        out.writeString(repositoryName);
        out.writeString(snapshotName);
        out.writeString(snapshotUuid);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indexName == null || indexName.isEmpty()) {
            validationException = addValidationError("indexName is required", validationException);
        }
        if (indexUuid == null || indexUuid.isEmpty()) {
            validationException = addValidationError("indexUuid is required", validationException);
        }
        if (shardId < 0) {
            validationException = addValidationError("shardId must be >= 0", validationException);
        }
        if (repositoryName == null || repositoryName.isEmpty()) {
            validationException = addValidationError("repositoryName is required", validationException);
        }
        if (snapshotName == null || snapshotName.isEmpty()) {
            validationException = addValidationError("snapshotName is required", validationException);
        }
        if (snapshotUuid == null || snapshotUuid.isEmpty()) {
            validationException = addValidationError("snapshotUuid is required", validationException);
        }
        return validationException;
    }

    /** The index name, carried for readability in the {@link org.opensearch.repositories.IndexId}/{@code ShardId} this builds. */
    public String indexName() {
        return indexName;
    }

    /** UUID of the index the shard belongs to. */
    public String indexUuid() {
        return indexUuid;
    }

    /** The shard number within {@link #indexUuid()}. */
    public int shardId() {
        return shardId;
    }

    /** The repository to copy this shard's currently-pinned commit into. */
    public String repositoryName() {
        return repositoryName;
    }

    /** The snapshot name, shared across every shard of one index-level request. */
    public String snapshotName() {
        return snapshotName;
    }

    /** The snapshot uuid, shared across every shard of one index-level request. */
    public String snapshotUuid() {
        return snapshotUuid;
    }
}
