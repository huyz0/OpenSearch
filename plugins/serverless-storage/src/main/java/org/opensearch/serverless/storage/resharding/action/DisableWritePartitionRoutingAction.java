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
 * The rollback primitive {@link EnableWritePartitionRoutingAction} was missing
 * (rfc-serverless-opensearch.md &sect;16 Phase 4's write-side partition-routing gap, tracked in
 * <code>write-routing-and-term-authority-progress.md</code>'s Effort A5): clears the write-routing
 * assignment from every named target index, restoring them to ordinary directly-writable indices.
 *
 * <p><b>Why this is the right-sized rollback, not a broader reconciliation feature</b>: once enabled,
 * every document written through a write-routing alias already landed in the one real partition its
 * routing key determined at write time -- there is nothing incorrect about where those documents
 * are. What was actually missing was simpler: no supported way existed to turn routing back off, so
 * an operator who enabled it had no path back to ordinary single-index writes short of directly
 * mutating cluster state. This action closes exactly that gap. A broader "move documents already
 * written to one partition into a different one" is a real data-migration problem, unrelated to
 * routing itself, and remains out of scope here.
 *
 * <p>Once disabled, {@link org.opensearch.serverless.storage.resharding.WritePartitionRoutingActionFilter}
 * naturally stops both rewriting writes against these targets (no assignment left to resolve a
 * partition from) and fencing their direct writes (no assignment left to consider them targets of).
 */
public class DisableWritePartitionRoutingAction extends ActionType<AcknowledgedResponse> {

    /** Singleton instance, matching the shape every other {@link ActionType} in this plugin uses. */
    public static final DisableWritePartitionRoutingAction INSTANCE = new DisableWritePartitionRoutingAction();

    /** This action's registered transport name. */
    public static final String NAME = "cluster:admin/serverless/storage/resharding/disable_write_partition_routing";

    private DisableWritePartitionRoutingAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
