/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;

/**
 * Enables real write-side routing for a split-target alias (rfc-serverless-opensearch.md &sect;16
 * Phase 4's "real write-side partition routing" gap): assigns each named target index a partition
 * slot for {@code aliasName}, recorded in {@link
 * org.opensearch.serverless.storage.resharding.WritePartitionRoutingMetadata} on each target's own
 * {@code IndexMetadata}. {@link
 * org.opensearch.serverless.storage.resharding.WritePartitionRoutingActionFilter} is what actually
 * consults this assignment on the indexing hot path, rewriting an {@code IndexRequest} against
 * {@code aliasName} to the one real target partition a document's routing key belongs to, using the
 * same {@link org.opensearch.serverless.storage.resharding.RoutingPartitionFilter} hash already used
 * read-side.
 *
 * <p><b>Deliberately a separate, explicit opt-in step from {@link CutoverSplitRoutingAction}, not a
 * flag on it</b>: the existing action's own alias-fan-out cutover is search-only and already proven
 * safe (every target holds the pre-split shard's full document set until physical rewrite happens,
 * so a search fan-out across all targets is correct even before any write routing exists). Enabling
 * write routing is a distinct, higher-stakes step -- once assigned, a target index expects to
 * receive <em>only</em> its own partition's writes, and {@link
 * org.opensearch.serverless.storage.resharding.WritePartitionRoutingActionFilter} enforces that by
 * rejecting direct writes to an assigned target index that bypass the alias, so this must never be
 * silently bundled into an operation whose primary job is enabling reads.
 *
 * <p><b>No auto-split trigger wired to this yet, deliberately</b>: {@code writesPerMinute()} tracking
 * exists (rfc-serverless-opensearch.md &sect;16 Phase 4) but nothing calls this action automatically.
 * A real auto-split controller needs to coordinate physical rewrite, search cutover, and write
 * cutover as one resumable sequence -- real design and failure-mode work beyond what this first,
 * manually-triggered increment covers.
 */
public class EnableWritePartitionRoutingAction extends ActionType<AcknowledgedResponse> {

    /** Singleton instance, matching the shape every other {@link ActionType} in this plugin uses. */
    public static final EnableWritePartitionRoutingAction INSTANCE = new EnableWritePartitionRoutingAction();

    /** This action's registered transport name. */
    public static final String NAME = "cluster:admin/serverless/storage/resharding/enable_write_partition_routing";

    private EnableWritePartitionRoutingAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
