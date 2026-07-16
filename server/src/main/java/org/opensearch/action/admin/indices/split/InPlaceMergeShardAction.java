/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.split;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedRequest;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Operator-triggered in-place shard merge (dynamic-partitioning-plan.md Phase 2 item 2.1) -- the
 * reverse of {@link InPlaceSplitShardAction}. Reverses an earlier split by folding that split's
 * children back into the single parent shard, closing core's public API for
 * {@link org.opensearch.cluster.metadata.MetadataInPlaceMergeShardService}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class InPlaceMergeShardAction extends ActionType<AcknowledgedResponse> {

    public static final InPlaceMergeShardAction INSTANCE = new InPlaceMergeShardAction();
    public static final String NAME = "indices:admin/shards/merge_in_place";

    private InPlaceMergeShardAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /**
     * Request to merge an earlier split's children back into their parent shard in place.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class Request extends AcknowledgedRequest<Request> {

        private String index;
        private int parentShardId;

        public Request(StreamInput in) throws IOException {
            super(in);
            index = in.readString();
            parentShardId = in.readVInt();
        }

        public Request() {}

        /**
         * @param index the index whose split is being reversed.
         * @param parentShardId the parent shard number, within {@code index}, whose children are being merged back.
         */
        public Request(String index, int parentShardId) {
            this.index = index;
            this.parentShardId = parentShardId;
        }

        /** The index whose split is being reversed. */
        public String index() {
            return index;
        }

        /** The parent shard number, within {@link #index()}, whose children are being merged back. */
        public int parentShardId() {
            return parentShardId;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (index == null || index.isEmpty()) {
                validationException = addValidationError("index is missing", validationException);
            }
            if (parentShardId < 0) {
                validationException = addValidationError("parentShardId must be >= 0", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeVInt(parentShardId);
        }
    }
}
