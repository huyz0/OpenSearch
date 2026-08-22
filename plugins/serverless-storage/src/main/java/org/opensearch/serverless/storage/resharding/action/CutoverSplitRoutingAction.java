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
 * The opt-in, operator-triggered "route client traffic to a shard split's real targets" action --
 * rfc-serverless-opensearch.md &sect;16 Phase 4's own scoped-first-version answer to its "real
 * routing cutover remains undesigned, not just unimplemented" gap.
 *
 * <p><b>Deliberately search-only, not a full write-routing solution</b>: a split target is a
 * wholly separate index identity holding a disjoint partition of the source's document space (see
 * {@code ShardSplitter}'s own doc-routing partition filter). A single core alias spanning multiple
 * indices already correctly fans a <em>search</em> out across every aliased index and unions the
 * results -- exactly right here, since each target's partition is disjoint, so nothing needs to
 * decide which target a search should hit. A <em>write</em>, by contrast, needs to land in exactly
 * one target, which core's own multi-index alias routing has no generic way to decide for an
 * arbitrary N-way custom partition function without per-document routing information this plugin
 * does not yet expose through any client-facing mechanism -- real, separate design work, correctly
 * left out of this first increment rather than half-built.
 *
 * <p><b>Deliberately additive, never destructive</b>: {@link TransportCutoverSplitRoutingAction}
 * only ever creates or updates the named alias to point at the given targets -- it never touches,
 * modifies, or deletes the source index a split came from. A real client-facing cutover under the
 * source's <em>own original name</em> requires that name to first be freed (core forbids an alias
 * sharing a name with a real index), which requires deleting the source -- a distinct, separate,
 * already-existing, explicitly verified operation ({@link RetireShrinkSourceAction}'s own "confirm
 * the replacement genuinely exists, then delete" pattern), not something bundled into this action.
 * An operator combines the two: cut client traffic over to a fresh alias name first (this action,
 * fully reversible, zero data risk), confirm it, then separately retire the source once satisfied.
 */
public class CutoverSplitRoutingAction extends ActionType<AcknowledgedResponse> {

    /** The single shared instance -- {@link ActionType}s are stateless, so one instance serves every request. */
    public static final CutoverSplitRoutingAction INSTANCE = new CutoverSplitRoutingAction();
    /** The transport action name this action is registered under. */
    public static final String NAME = "cluster:admin/serverless/storage/resharding/cutover_split_routing";

    private CutoverSplitRoutingAction() {
        super(NAME, AcknowledgedResponse::new);
    }
}
