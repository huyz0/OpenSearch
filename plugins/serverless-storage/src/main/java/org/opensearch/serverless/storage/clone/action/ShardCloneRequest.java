/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Names a source and target shard by (indexUuid, shardId) for {@link ShardCloneAction}. */
public class ShardCloneRequest extends ActionRequest {

    private final String sourceIndexUuid;
    private final int sourceShardId;
    private final String targetIndexUuid;
    private final int targetShardId;

    /**
     * Creates a request.
     *
     * @param sourceIndexUuid the index being cloned from.
     * @param sourceShardId the shard number within {@code sourceIndexUuid}.
     * @param targetIndexUuid the brand-new index the clone creates.
     * @param targetShardId the shard number within {@code targetIndexUuid}.
     */
    public ShardCloneRequest(String sourceIndexUuid, int sourceShardId, String targetIndexUuid, int targetShardId) {
        this.sourceIndexUuid = sourceIndexUuid;
        this.sourceShardId = sourceShardId;
        this.targetIndexUuid = targetIndexUuid;
        this.targetShardId = targetShardId;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardCloneRequest}.
     */
    public ShardCloneRequest(StreamInput in) throws IOException {
        super(in);
        this.sourceIndexUuid = in.readString();
        this.sourceShardId = in.readVInt();
        this.targetIndexUuid = in.readString();
        this.targetShardId = in.readVInt();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceIndexUuid);
        out.writeVInt(sourceShardId);
        out.writeString(targetIndexUuid);
        out.writeVInt(targetShardId);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndexUuid == null || sourceIndexUuid.isEmpty()) {
            validationException = addValidationError("source indexUuid is required", validationException);
        } else if (isWellFormedIndexUuid(sourceIndexUuid) == false) {
            validationException = addValidationError("source " + INDEX_UUID_CHARSET_ERROR, validationException);
        }
        if (targetIndexUuid == null || targetIndexUuid.isEmpty()) {
            validationException = addValidationError("target indexUuid is required", validationException);
        } else if (isWellFormedIndexUuid(targetIndexUuid) == false) {
            validationException = addValidationError("target " + INDEX_UUID_CHARSET_ERROR, validationException);
        }
        if (sourceShardId < 0) {
            validationException = addValidationError("source shardId must be >= 0", validationException);
        }
        if (targetShardId < 0) {
            validationException = addValidationError("target shardId must be >= 0", validationException);
        }
        if (sourceIndexUuid != null
            && targetIndexUuid != null
            && sourceIndexUuid.equals(targetIndexUuid)
            && sourceShardId == targetShardId) {
            // ShardCloner#clone writes the target's manifest (generation 1) before its head CAS
            // check runs -- for a self-clone that write would collide with/clobber the shard's own
            // genuine first commit before the CAS could reject the operation.
            validationException = addValidationError("source and target must not name the same shard", validationException);
        }
        return validationException;
    }

    /** The message reported for an {@code indexUuid} carrying characters no real uuid contains. */
    static final String INDEX_UUID_CHARSET_ERROR = "indexUuid must contain only [A-Za-z0-9_-]";

    /**
     * Whether {@code indexUuid} is drawn from the alphabet a real index UUID is drawn from --
     * OpenSearch generates them as unpadded URL-safe base64, so letters, digits, {@code -} and
     * {@code _} and nothing else (the {@code _na_} placeholder also fits).
     *
     * <p>Defence in depth, not the primary control: the authoritative check is
     * {@code TransportShardCloneAction#requireRealShard}, which resolves each uuid against cluster
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

    /** The index being cloned from. */
    public String sourceIndexUuid() {
        return sourceIndexUuid;
    }

    /** The shard number within {@link #sourceIndexUuid()}. */
    public int sourceShardId() {
        return sourceShardId;
    }

    /** The brand-new index the clone creates. */
    public String targetIndexUuid() {
        return targetIndexUuid;
    }

    /** The shard number within {@link #targetIndexUuid()}. */
    public int targetShardId() {
        return targetShardId;
    }
}
