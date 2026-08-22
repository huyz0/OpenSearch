/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.retention.action;

import org.opensearch.action.ActionType;

/**
 * The user-facing entry point for inspecting one shard's retention/GC state on demand
 * (rfc-serverless-opensearch.md &sect;6.5/&sect;14): manifest and bundle counts, how many of each
 * are currently deletable under {@code ManifestRetentionPolicy}/{@code BundleReferenceCounter}'s
 * own rules, durable pin counts (PITR and total), and this node's configured retention windows --
 * everything an operator would otherwise have to piece together from logs or by reasoning about
 * {@code GcSchedulerTask}/{@code PitrRetentionSchedulerTask}'s own background schedules, which
 * this plugin's other observability actions (idle shards, manifest lag, scale-to-zero candidates)
 * already give a real REST surface to but retention/GC never did.
 *
 * <p>Deliberately read-only, same as every other {@code Node*Action}/stats-shaped action in this
 * plugin: it recomputes the same dry-run policy decisions {@code GcSchedulerTask}'s real sweep
 * would make, but never deletes anything itself -- unlike {@link
 * org.opensearch.serverless.storage.compaction.action.CompactionTriggerAction}, which is
 * deliberately a real on-demand trigger, this is purely observability.
 */
public class ShardRetentionStatsAction extends ActionType<ShardRetentionStatsResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final ShardRetentionStatsAction INSTANCE = new ShardRetentionStatsAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:monitor/serverless/storage/shard/retention_stats";

    private ShardRetentionStatsAction() {
        super(NAME, ShardRetentionStatsResponse::new);
    }
}
