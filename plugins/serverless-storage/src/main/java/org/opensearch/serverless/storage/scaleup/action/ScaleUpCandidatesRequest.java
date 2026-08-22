/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaleup.action;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Requests a cluster-wide {@link ScaleUpCandidatesAction} evaluation. Always fans out to every
 * data node, same "no reason to scope to some nodes, the whole point is a complete cluster-wide
 * picture" reasoning as {@code org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesRequest}.
 */
public class ScaleUpCandidatesRequest extends BaseNodesRequest<ScaleUpCandidatesRequest> {

    /** Sentinel meaning "use {@code ServerlessStoragePlugin.SERVERLESS_STORAGE_SCALE_UP_QPM_THRESHOLD_SETTING}'s current value." */
    public static final long USE_DEFAULT_QPM_THRESHOLD = -1L;
    /** Sentinel meaning "use {@code ServerlessStoragePlugin.SERVERLESS_STORAGE_SCALE_UP_MAX_SEARCH_REPLICAS_SETTING}'s current value." */
    public static final int USE_DEFAULT_MAX_SEARCH_REPLICAS = -1;

    private final long qpmThreshold;
    private final int maxSearchReplicas;

    /** Requests an evaluation using this node's currently configured default thresholds. */
    public ScaleUpCandidatesRequest() {
        this(USE_DEFAULT_QPM_THRESHOLD, USE_DEFAULT_MAX_SEARCH_REPLICAS);
    }

    /**
     * Requests an evaluation using caller-supplied thresholds, overriding this node's configured defaults.
     *
     * @param qpmThreshold the queries-per-minute a reader copy must exceed to count toward
     *                     {@link ScaleUpCandidateEntry#candidate()}, or {@link #USE_DEFAULT_QPM_THRESHOLD}.
     * @param maxSearchReplicas the maximum {@code index.number_of_search_replicas} an index may
     *                          still be expanded past, or {@link #USE_DEFAULT_MAX_SEARCH_REPLICAS}.
     */
    public ScaleUpCandidatesRequest(long qpmThreshold, int maxSearchReplicas) {
        super(new String[0]);
        this.qpmThreshold = qpmThreshold;
        this.maxSearchReplicas = maxSearchReplicas;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ScaleUpCandidatesRequest}.
     */
    public ScaleUpCandidatesRequest(StreamInput in) throws IOException {
        super(in);
        this.qpmThreshold = in.readZLong();
        this.maxSearchReplicas = in.readVInt();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeZLong(qpmThreshold);
        out.writeVInt(maxSearchReplicas);
    }

    /**
     * Rejects any override strictly less than the "use default" sentinel: {@code qpmThreshold}/
     * {@code maxSearchReplicas} feed directly into {@code ScaleUpCandidatesResponse}'s {@code
     * queriesPerMinute() > qpmThreshold} candidate check -- an unvalidated negative value other than
     * the sentinel (e.g. a typo'd {@code ?qpm_threshold=-100}) would make every shard on every node
     * satisfy that comparison, since query rates are never negative, flagging the entire cluster as
     * a scale-up candidate.
     */
    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (qpmThreshold < USE_DEFAULT_QPM_THRESHOLD) {
            validationException = addValidationError("qpmThreshold must be >= -1", validationException);
        }
        if (maxSearchReplicas < USE_DEFAULT_MAX_SEARCH_REPLICAS) {
            validationException = addValidationError("maxSearchReplicas must be >= -1", validationException);
        }
        return validationException;
    }

    /** The caller-supplied query-rate threshold override, or {@link #USE_DEFAULT_QPM_THRESHOLD}. */
    public long qpmThreshold() {
        return qpmThreshold;
    }

    /** The caller-supplied replica-cap override, or {@link #USE_DEFAULT_MAX_SEARCH_REPLICAS}. */
    public int maxSearchReplicas() {
        return maxSearchReplicas;
    }
}
