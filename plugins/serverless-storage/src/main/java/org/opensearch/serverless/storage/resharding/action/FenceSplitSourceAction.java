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
 * Marks a split source index fenced -- see {@link org.opensearch.serverless.storage.resharding.SourceSplitFenceMetadata}'s
 * own javadoc for the full design rationale and honest limitation. Wired into {@link
 * TransportOrchestrateShardSplitAction} immediately after a successful {@link CutoverSplitRoutingAction},
 * so every orchestrated split fences its source automatically; also callable directly for a source
 * whose split was completed manually (the same "operator can also invoke each stage directly" shape
 * every other action in this orchestration sequence already has).
 */
public class FenceSplitSourceAction extends ActionType<AcknowledgedResponse> {

    /** Singleton instance, matching the shape every other {@link ActionType} in this plugin uses. */
    public static final FenceSplitSourceAction INSTANCE = new FenceSplitSourceAction();

    /** This action's registered transport name. */
    public static final String NAME = "cluster:admin/serverless/storage/resharding/fence_split_source";

    private FenceSplitSourceAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
