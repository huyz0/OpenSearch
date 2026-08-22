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
 * The opt-in, operator-triggered "retire a shrink's source index" action rfc-serverless-opensearch.md
 * &sect;16 Phase 5's own status note left explicitly out of scope: {@code ShardShrinker#shrink} only
 * ever creates the merged target, deliberately never touching, deleting, or modifying any source.
 * {@link RetireShrinkSourceRequest} names the source and the shrink target it claims to have been
 * merged into; {@link TransportRetireShrinkSourceAction} verifies that target genuinely has a
 * published manifest before deleting the source -- never automatic, always an explicit, separate
 * operator decision, the same "never auto-deletes anything it didn't itself just create" caution
 * {@code ShardCloner#deleteClone} already applies.
 */
public class RetireShrinkSourceAction extends ActionType<AcknowledgedResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final RetireShrinkSourceAction INSTANCE = new RetireShrinkSourceAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/resharding/retire_shrink_source";

    private RetireShrinkSourceAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
