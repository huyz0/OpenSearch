/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.support.nodes.BaseNodesRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Requests a cluster-wide {@link ShardSplitCandidatesAction} evaluation. Always fans out to every
 * data node, same "no reason to scope to some nodes, the whole point is a complete cluster-wide
 * picture" reasoning as {@code org.opensearch.serverless.storage.scaletozero.action.ScaleToZeroCandidatesRequest}.
 */
public class ShardSplitCandidatesRequest extends BaseNodesRequest<ShardSplitCandidatesRequest> {

    /** Sentinel meaning "use {@code ServerlessStoragePlugin.SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_WPM_THRESHOLD_SETTING}'s current value." */
    public static final long USE_DEFAULT_WPM_THRESHOLD = -1L;

    /** Sentinel meaning "use {@code ServerlessStoragePlugin.SERVERLESS_STORAGE_RESHARDING_SPLIT_CANDIDATE_SIZE_THRESHOLD_BYTES_SETTING}'s current value." */
    public static final long USE_DEFAULT_SIZE_THRESHOLD_BYTES = -1L;

    private final long writesPerMinuteThreshold;
    private final long sizeThresholdBytes;

    /** Requests an evaluation using this node's currently configured default thresholds. */
    public ShardSplitCandidatesRequest() {
        this(USE_DEFAULT_WPM_THRESHOLD, USE_DEFAULT_SIZE_THRESHOLD_BYTES);
    }

    /**
     * Requests an evaluation using a caller-supplied write-rate threshold, overriding this node's
     * configured default write-rate threshold while leaving the size threshold at its default.
     *
     * @param writesPerMinuteThreshold the writes-per-minute a writer copy must exceed to count
     *                                 toward {@link ShardSplitCandidateEntry#writeRateCandidate()},
     *                                 or {@link #USE_DEFAULT_WPM_THRESHOLD}.
     */
    public ShardSplitCandidatesRequest(long writesPerMinuteThreshold) {
        this(writesPerMinuteThreshold, USE_DEFAULT_SIZE_THRESHOLD_BYTES);
    }

    /**
     * Requests an evaluation using caller-supplied thresholds, overriding this node's configured defaults.
     *
     * @param writesPerMinuteThreshold the writes-per-minute a writer copy must exceed to count
     *                                 toward {@link ShardSplitCandidateEntry#writeRateCandidate()},
     *                                 or {@link #USE_DEFAULT_WPM_THRESHOLD}.
     * @param sizeThresholdBytes the size in bytes a writer copy must exceed to count toward {@link
     *                           ShardSplitCandidateEntry#sizeCandidate()}, or {@link
     *                           #USE_DEFAULT_SIZE_THRESHOLD_BYTES}.
     */
    public ShardSplitCandidatesRequest(long writesPerMinuteThreshold, long sizeThresholdBytes) {
        super(new String[0]);
        this.writesPerMinuteThreshold = writesPerMinuteThreshold;
        this.sizeThresholdBytes = sizeThresholdBytes;
    }

    /**
     * Deserializes a request.
     *
     * @param in stream positioned at a previously-{@link #writeTo}-written {@link ShardSplitCandidatesRequest}.
     */
    public ShardSplitCandidatesRequest(StreamInput in) throws IOException {
        super(in);
        this.writesPerMinuteThreshold = in.readZLong();
        this.sizeThresholdBytes = in.readZLong();
    }

    /** @param out stream to write this request's fields to. */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeZLong(writesPerMinuteThreshold);
        out.writeZLong(sizeThresholdBytes);
    }

    /** The caller-supplied write-rate threshold override, or {@link #USE_DEFAULT_WPM_THRESHOLD}. */
    public long writesPerMinuteThreshold() {
        return writesPerMinuteThreshold;
    }

    /** The caller-supplied size threshold override, or {@link #USE_DEFAULT_SIZE_THRESHOLD_BYTES}. */
    public long sizeThresholdBytes() {
        return sizeThresholdBytes;
    }
}
