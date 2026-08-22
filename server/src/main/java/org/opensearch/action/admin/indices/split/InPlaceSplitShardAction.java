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
 * Operator-triggered in-place shard split: closes core's
 * previously-missing public API for {@link org.opensearch.cluster.metadata.MetadataInPlaceSplitShardService},
 * which existed with no REST/transport action reaching it at all -- a gap found directly by
 * auditing what could actually reach that service.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class InPlaceSplitShardAction extends ActionType<AcknowledgedResponse> {

    public static final InPlaceSplitShardAction INSTANCE = new InPlaceSplitShardAction();
    public static final String NAME = "indices:admin/shards/split_in_place";

    private InPlaceSplitShardAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /**
     * Request to split one shard of an index in place into {@code splitInto} child shards.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static class Request extends AcknowledgedRequest<Request> {

        private String index;
        private int shardId;
        private int splitInto;

        public Request(StreamInput in) throws IOException {
            super(in);
            index = in.readString();
            shardId = in.readVInt();
            splitInto = in.readVInt();
        }

        public Request() {}

        /**
         * @param index the index whose shard is being split.
         * @param shardId the shard number, within {@code index}, to split.
         * @param splitInto how many child shards to split {@code shardId} into; must be {@code >= 2}.
         */
        public Request(String index, int shardId, int splitInto) {
            this.index = index;
            this.shardId = shardId;
            this.splitInto = splitInto;
        }

        /** The index whose shard is being split. */
        public String index() {
            return index;
        }

        /** The shard number, within {@link #index()}, to split. */
        public int shardId() {
            return shardId;
        }

        /** How many child shards {@link #shardId()} is being split into. */
        public int splitInto() {
            return splitInto;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (index == null || index.isEmpty()) {
                validationException = addValidationError("index is missing", validationException);
            }
            if (shardId < 0) {
                validationException = addValidationError("shardId must be >= 0", validationException);
            }
            if (splitInto < 2) {
                validationException = addValidationError("splitInto must be >= 2", validationException);
            }
            return validationException;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(index);
            out.writeVInt(shardId);
            out.writeVInt(splitInto);
        }
    }
}
