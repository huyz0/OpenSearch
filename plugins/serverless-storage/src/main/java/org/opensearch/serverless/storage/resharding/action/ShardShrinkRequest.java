/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import static org.opensearch.action.ValidateActions.addValidationError;

/** Names every source shard being merged, plus the brand-new target shard, for {@link ShardShrinkAction}. */
public class ShardShrinkRequest extends ActionRequest {

    private final List<ShardRef> sources;
    private final String targetIndexUuid;
    private final int targetShardId;

    /**
     * Creates a request.
     *
     * @param sources every shard being merged; should have at least two entries (a single-source
     *                "shrink" is just a clone, and is rejected -- use {@code ShardCloneAction} for that).
     * @param targetIndexUuid the brand-new index this shrink creates.
     * @param targetShardId the shard number within {@code targetIndexUuid}.
     */
    public ShardShrinkRequest(List<ShardRef> sources, String targetIndexUuid, int targetShardId) {
        this.sources = sources;
        this.targetIndexUuid = targetIndexUuid;
        this.targetShardId = targetShardId;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardShrinkRequest}.
     */
    public ShardShrinkRequest(StreamInput in) throws IOException {
        super(in);
        this.sources = in.readList(ShardRef::new);
        this.targetIndexUuid = in.readString();
        this.targetShardId = in.readVInt();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeList(sources);
        out.writeString(targetIndexUuid);
        out.writeVInt(targetShardId);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sources == null || sources.size() < 2) {
            validationException = addValidationError(
                "sources must contain at least two shards (a single-source shrink is just a clone)",
                validationException
            );
        } else {
            for (ShardRef source : sources) {
                if (source.indexUuid() == null || source.indexUuid().isEmpty()) {
                    validationException = addValidationError("every source indexUuid is required", validationException);
                }
                if (source.shardId() < 0) {
                    validationException = addValidationError("every source shardId must be >= 0", validationException);
                }
            }
            if (new HashSet<>(sources).size() != sources.size()) {
                // A duplicated (indexUuid, shardId) would be materialized and merged twice with no
                // document-level dedup at the Lucene addIndexes layer -- every document in that
                // source would end up duplicated in the target.
                validationException = addValidationError("sources must not contain duplicate (indexUuid, shardId) entries", validationException);
            }
        }
        if (targetIndexUuid == null || targetIndexUuid.isEmpty()) {
            validationException = addValidationError("target indexUuid is required", validationException);
        }
        if (targetShardId < 0) {
            validationException = addValidationError("target shardId must be >= 0", validationException);
        }
        return validationException;
    }

    /** Every shard being merged. */
    public List<ShardRef> sources() {
        return sources;
    }

    /** The brand-new index this shrink creates. */
    public String targetIndexUuid() {
        return targetIndexUuid;
    }

    /** The shard number within {@link #targetIndexUuid()}. */
    public int targetShardId() {
        return targetShardId;
    }
}
