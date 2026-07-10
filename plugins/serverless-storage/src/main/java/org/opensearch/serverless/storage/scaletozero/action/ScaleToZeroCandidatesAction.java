/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.scaletozero.action;

import org.opensearch.action.ActionType;

/**
 * Cluster-wide scale-to-zero candidate policy evaluator (rfc-serverless-opensearch.md
 * &sect;7.3/&sect;10): fans out {@link org.opensearch.serverless.storage.writerengine.action.NodeIdleShardsAction}
 * and {@link org.opensearch.serverless.storage.readerengine.action.NodeManifestLagAction} across
 * every data node (unlike either of those, which are deliberately single-node-scoped), merges
 * each shard's writer-idle and reader-freshness signal by {@code (indexUuid, shardId)}, and
 * applies this plugin's idle/lag thresholds to flag which shards are worth a real
 * suspension/scale-to-zero controller's attention.
 *
 * <p>Deliberately the "policy" half only, never the "mechanism" half: this action never closes,
 * suspends, or reactivates a shard -- it is read-only, matching how {@code CompactionSchedulerTask}
 * separates "decide a compaction is due" from "run one." See this package's {@code package-info.java}
 * for why the real suspend/reactivate mechanism is out of scope here.
 */
public class ScaleToZeroCandidatesAction extends ActionType<ScaleToZeroCandidatesResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final ScaleToZeroCandidatesAction INSTANCE = new ScaleToZeroCandidatesAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/scale_to_zero/candidates";

    private ScaleToZeroCandidatesAction() {
        super(NAME, ScaleToZeroCandidatesResponse::new);
    }
}
