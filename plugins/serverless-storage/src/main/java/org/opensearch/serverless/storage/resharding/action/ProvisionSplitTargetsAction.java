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
 * Creates the real OpenSearch target indices a split needs, one per named partition, inheriting
 * settings and mappings from the source index -- the auto-provisioning primitive {@code
 * ShardSplitter#split}/{@code ShardSplitCandidateEntry}'s own javadoc already documented this
 * plugin was missing (rfc-serverless-opensearch.md &sect;16 Phase 4, tracked in
 * <code>write-routing-and-term-authority-progress.md</code>'s Effort A).
 *
 * <p><b>Deliberately mirrors core's own {@code POST /{index}/_split/{target}} contract exactly,
 * not an earlier draft that generated names algorithmically</b>: core's real `_split` API
 * (`TransportResizeAction`) always requires the caller to supply the target index name in the URL
 * path -- core never invents one, even though it could. Splitting further, core's own code
 * explicitly refuses to auto-calculate the target shard count for a split (`assert
 * resizeRequest.getResizeType() != ResizeType.SPLIT : "split must specify the number of shards
 * explicitly"` -- `TransportResizeAction`), even though it does auto-calculate shard count for
 * shrink. That is a real, deliberate design signal from core's own maintainers: even the team that
 * owns this whole feature treats "how many pieces, named what" as something only the caller should
 * decide for a split, not something core should guess. This action follows that same contract:
 * every target index name is caller-supplied, in full, every call -- there is no algorithm here
 * deciding a naming scheme or a partition count on its own.
 *
 * <p>Still deliberately a separate, explicit, operator-triggered action, not wired to any
 * threshold or scheduler -- the same "signal exists, no auto action" boundary this plugin already
 * draws for `ShardSplitCandidatesAction`'s own advisory `writesPerMinute()` signal.
 */
public class ProvisionSplitTargetsAction extends ActionType<AcknowledgedResponse> {

    /** Singleton instance, matching the shape every other {@link ActionType} in this plugin uses. */
    public static final ProvisionSplitTargetsAction INSTANCE = new ProvisionSplitTargetsAction();

    /** This action's registered transport name. */
    public static final String NAME = "cluster:admin/serverless/storage/resharding/provision_split_targets";

    private ProvisionSplitTargetsAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
