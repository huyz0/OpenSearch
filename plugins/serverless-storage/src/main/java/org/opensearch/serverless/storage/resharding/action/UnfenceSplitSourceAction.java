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
 * Removes a split source index's write fence, making it directly writable again -- the inverse of
 * {@link FenceSplitSourceAction}, and the escape hatch whose absence made an accidental or
 * half-finished fence unrecoverable (see {@link
 * org.opensearch.serverless.storage.resharding.SourceSplitFenceMetadata}'s own javadoc for the full
 * discussion of why an unfence primitive is not optional).
 *
 * <p>Deliberately <b>not</b> wired into {@link TransportOrchestrateShardSplitAction} anywhere: a
 * fence applied after a real cutover is correct and must stay, so unfencing is only ever an explicit
 * operator decision. It is exactly the pair {@link EnableWritePartitionRoutingAction} /
 * {@link DisableWritePartitionRoutingAction} already forms for the target side of the same split.
 */
public class UnfenceSplitSourceAction extends ActionType<AcknowledgedResponse> {

    /** Singleton instance, matching the shape every other {@link ActionType} in this plugin uses. */
    public static final UnfenceSplitSourceAction INSTANCE = new UnfenceSplitSourceAction();

    /** This action's registered transport name. */
    public static final String NAME = "cluster:admin/serverless/storage/resharding/unfence_split_source";

    private UnfenceSplitSourceAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
