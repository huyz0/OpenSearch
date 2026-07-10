/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Requests a cluster-wide {@link ScaleToZeroCandidatesAction} evaluation. Always fans out to every
 * data node in the cluster -- unlike {@link org.opensearch.action.support.nodes.BaseNodesRequest}'s
 * general "optionally scope to specific node ids" contract, a caller has no reason to ask this
 * question about only some nodes, since the whole point is a complete cluster-wide picture.
 */
public class ScaleToZeroCandidatesRequest extends BaseNodesRequest<ScaleToZeroCandidatesRequest> {

    /** Sentinel meaning "use {@code ServerlessStoragePlugin.SERVERLESS_STORAGE_SCALE_TO_ZERO_IDLE_THRESHOLD_SETTING}'s current value." */
    public static final long USE_DEFAULT_IDLE_THRESHOLD = -1L;
    /** Sentinel meaning "use {@code ServerlessStoragePlugin.SERVERLESS_STORAGE_SCALE_TO_ZERO_LAG_THRESHOLD_SETTING}'s current value." */
    public static final long USE_DEFAULT_LAG_THRESHOLD = -1L;

    private final long idleThresholdMillis;
    private final long lagThreshold;

    /**
     * Requests an evaluation using this node's currently configured default thresholds.
     */
    public ScaleToZeroCandidatesRequest() {
        this(USE_DEFAULT_IDLE_THRESHOLD, USE_DEFAULT_LAG_THRESHOLD);
    }

    /**
     * Requests an evaluation using caller-supplied thresholds, overriding this node's configured defaults.
     *
     * @param idleThresholdMillis how idle (in millis) a writer copy must be to count toward
     *                            {@link ScaleToZeroCandidateEntry#candidate()}, or {@link #USE_DEFAULT_IDLE_THRESHOLD}.
     * @param lagThreshold the largest manifest-generation lag a reader copy may have and still
     *                     count as "caught up," or {@link #USE_DEFAULT_LAG_THRESHOLD}.
     */
    public ScaleToZeroCandidatesRequest(long idleThresholdMillis, long lagThreshold) {
        super(new String[0]);
        this.idleThresholdMillis = idleThresholdMillis;
        this.lagThreshold = lagThreshold;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ScaleToZeroCandidatesRequest}.
     */
    public ScaleToZeroCandidatesRequest(StreamInput in) throws IOException {
        super(in);
        this.idleThresholdMillis = in.readZLong();
        this.lagThreshold = in.readZLong();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeZLong(idleThresholdMillis);
        out.writeZLong(lagThreshold);
    }

    /** The caller-supplied idle threshold override, or {@link #USE_DEFAULT_IDLE_THRESHOLD}. */
    public long idleThresholdMillis() {
        return idleThresholdMillis;
    }

    /** The caller-supplied lag threshold override, or {@link #USE_DEFAULT_LAG_THRESHOLD}. */
    public long lagThreshold() {
        return lagThreshold;
    }
}
