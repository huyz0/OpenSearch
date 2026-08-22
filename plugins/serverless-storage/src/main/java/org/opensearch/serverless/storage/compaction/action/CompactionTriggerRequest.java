/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Names a shard by (indexUuid, shardId) for {@link CompactionTriggerAction}. */
public class CompactionTriggerRequest extends ActionRequest {

    private final String indexUuid;
    private final int shardId;

    /**
     * Creates a request.
     *
     * @param indexUuid UUID of the index the shard belongs to.
     * @param shardId the shard number within {@code indexUuid}.
     */
    public CompactionTriggerRequest(String indexUuid, int shardId) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link CompactionTriggerRequest}.
     */
    public CompactionTriggerRequest(StreamInput in) throws IOException {
        super(in);
        this.indexUuid = in.readString();
        this.shardId = in.readVInt();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(indexUuid);
        out.writeVInt(shardId);
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
     * {@code TransportCompactionTriggerAction#requireRealShard}, which resolves the uuid against
     * cluster metadata and so answers "does this name a real shard", which no character test can.
     * This one exists because the uuid ends up as a blob-path segment
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
}
