/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Reports which stage {@link OrchestrateShardSplitAction}'s chained sequence reached -- every
 * field {@code true} means every stage genuinely completed (whether by doing the work this call,
 * or by finding it already done on a resumed retry); a response with a later stage {@code false}
 * always means an exception was also raised for the failing stage, since this response is only
 * ever returned via a successful listener callback for the stages that did complete, or omitted
 * entirely in favor of {@code onFailure} once a stage can't proceed.
 *
 * <p>{@link #sourceFenced()} is reported separately because fencing is no longer unconditional. It
 * is applied only after write routing has actually been enabled, so that the alias is a working
 * write entry point before the source stops being one; a caller that leaves {@code
 * enable_write_routing} at its default of {@code false} gets {@code write_routing_enabled: false}
 * and {@code source_fenced: false}, and the source index stays writable. See finding R-7 in {@link
 * TransportOrchestrateShardSplitAction#writeRoutingStage} for the failure this replaced, and {@link
 * org.opensearch.serverless.storage.resharding.SourceSplitFenceMetadata}'s own javadoc for what
 * fencing does and does not close.
 */
public class OrchestrateShardSplitResponse extends ActionResponse implements ToXContentObject {

    private final boolean targetsProvisioned;
    private final boolean allTargetsSplit;
    private final boolean cutover;
    private final boolean writeRoutingEnabled;
    private final boolean sourceFenced;

    /**
     * Creates a response.
     *
     * @param targetsProvisioned whether every named target index now exists.
     * @param allTargetsSplit whether every named target has a published split head.
     * @param cutover whether the search-only alias cutover completed and was acknowledged.
     * @param writeRoutingEnabled whether write-partition-routing was also assigned (always {@code false}
     *                            if the request didn't ask for it).
     * @param sourceFenced whether the source index was fenced against direct writes (only ever
     *                     {@code true} when {@code writeRoutingEnabled} is, so the dataset always
     *                     retains a writable entry point).
     */
    public OrchestrateShardSplitResponse(
        boolean targetsProvisioned,
        boolean allTargetsSplit,
        boolean cutover,
        boolean writeRoutingEnabled,
        boolean sourceFenced
    ) {
        this.targetsProvisioned = targetsProvisioned;
        this.allTargetsSplit = allTargetsSplit;
        this.cutover = cutover;
        this.writeRoutingEnabled = writeRoutingEnabled;
        this.sourceFenced = sourceFenced;
    }

    /**
     * Deserializes a response.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link OrchestrateShardSplitResponse}.
     */
    public OrchestrateShardSplitResponse(StreamInput in) throws IOException {
        super(in);
        this.targetsProvisioned = in.readBoolean();
        this.allTargetsSplit = in.readBoolean();
        this.cutover = in.readBoolean();
        this.writeRoutingEnabled = in.readBoolean();
        this.sourceFenced = in.readBoolean();
    }

    /** @param out stream to write this response's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(targetsProvisioned);
        out.writeBoolean(allTargetsSplit);
        out.writeBoolean(cutover);
        out.writeBoolean(writeRoutingEnabled);
        out.writeBoolean(sourceFenced);
    }

    /** Whether every named target index now exists. */
    public boolean targetsProvisioned() {
        return targetsProvisioned;
    }

    /** Whether every named target has a published split head. */
    public boolean allTargetsSplit() {
        return allTargetsSplit;
    }

    /** Whether the search-only alias cutover completed. */
    public boolean cutover() {
        return cutover;
    }

    /** Whether write-partition-routing was also assigned. */
    public boolean writeRoutingEnabled() {
        return writeRoutingEnabled;
    }

    /** Whether the source index was fenced against direct writes. */
    public boolean sourceFenced() {
        return sourceFenced;
    }

    /**
     * @param builder the builder to append this response's fields to.
     * @param params unused.
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("targets_provisioned", targetsProvisioned)
            .field("all_targets_split", allTargetsSplit)
            .field("cutover", cutover)
            .field("write_routing_enabled", writeRoutingEnabled)
            .field("source_fenced", sourceFenced)
            .endObject();
    }
}
