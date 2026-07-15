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
import java.util.List;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Names everything {@link OrchestrateShardSplitAction}'s chained sequence needs: the source index,
 * the real target index names to create (caller-supplied, same "operator always names targets"
 * contract {@link ProvisionSplitTargetsRequest} already commits to), the alias search traffic
 * should cut over to, and whether to also enable write-partition-routing for that same alias.
 */
public class OrchestrateShardSplitRequest extends ActionRequest {

    private final String sourceIndexName;
    private final List<String> targetIndexNames;
    private final String routingAliasName;
    private final boolean enableWriteRouting;

    /**
     * Creates a request.
     *
     * @param sourceIndexName the index being split; must currently have exactly 1 shard.
     * @param targetIndexNames the real target index names to create, in partition order; must name
     *                         at least 2 partitions.
     * @param routingAliasName the alias every target is cut over into once split.
     * @param enableWriteRouting whether to also assign write-partition-routing for {@code routingAliasName}
     *                           once the search-only cutover completes.
     */
    public OrchestrateShardSplitRequest(
        String sourceIndexName,
        List<String> targetIndexNames,
        String routingAliasName,
        boolean enableWriteRouting
    ) {
        this.sourceIndexName = sourceIndexName;
        this.targetIndexNames = targetIndexNames;
        this.routingAliasName = routingAliasName;
        this.enableWriteRouting = enableWriteRouting;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link OrchestrateShardSplitRequest}.
     */
    public OrchestrateShardSplitRequest(StreamInput in) throws IOException {
        super(in);
        this.sourceIndexName = in.readString();
        this.targetIndexNames = in.readStringList();
        this.routingAliasName = in.readString();
        this.enableWriteRouting = in.readBoolean();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(sourceIndexName);
        out.writeStringCollection(targetIndexNames);
        out.writeString(routingAliasName);
        out.writeBoolean(enableWriteRouting);
    }

    /** @return validation errors, or {@code null} if the request is well-formed. */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (sourceIndexName == null || sourceIndexName.isEmpty()) {
            validationException = addValidationError("sourceIndexName is required", validationException);
        }
        if (targetIndexNames == null || targetIndexNames.size() < 2) {
            validationException = addValidationError("targetIndexNames must name at least 2 partitions", validationException);
        }
        if (routingAliasName == null || routingAliasName.isEmpty()) {
            validationException = addValidationError("routingAliasName is required", validationException);
        }
        return validationException;
    }

    /** The index being split. */
    public String sourceIndexName() {
        return sourceIndexName;
    }

    /** The real target index names to create, in partition order. */
    public List<String> targetIndexNames() {
        return targetIndexNames;
    }

    /** The alias every target is cut over into once split. */
    public String routingAliasName() {
        return routingAliasName;
    }

    /** Whether to also assign write-partition-routing for {@link #routingAliasName()}. */
    public boolean enableWriteRouting() {
        return enableWriteRouting;
    }
}
