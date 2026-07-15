/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding.action;

import org.opensearch.action.ActionType;

/**
 * Chains {@link ProvisionSplitTargetsAction} -&gt; {@link ShardSplitAction} (once per target) -&gt;
 * {@link CutoverSplitRoutingAction} -&gt; (optionally) {@link EnableWritePartitionRoutingAction} into
 * one resumable, still explicitly operator-triggered sequence -- the A15 gap
 * <code>write-routing-and-term-authority-progress.md</code> and this RFC's own &sect;16 Phase 4
 * notes both flagged as remaining after {@link ProvisionSplitTargetsAction} closed the
 * auto-provisioning blocker: "chaining `ProvisionSplitTargetsAction` -&gt; the existing
 * per-partition `ShardSplitAction` calls -&gt; `CutoverSplitRoutingAction` -&gt;
 * `EnableWritePartitionRoutingAction` into one resumable sequence."
 *
 * <p><b>Still explicitly operator-triggered, not wired to any threshold or scheduler</b> -- the
 * same "signal exists, no auto action" boundary this plugin draws for
 * {@link ShardSplitCandidatesAction}'s own advisory {@code writesPerMinute()} signal everywhere
 * else. This action only removes the *manual four-call sequencing* burden, not the operator
 * decision to split at all.
 *
 * <p><b>Resumable, not merely retriable</b>: re-issuing the exact same request after a partial
 * failure (e.g. targets were provisioned but the split step failed partway through) does not
 * error out on the already-completed steps. Provisioning is skipped if every named target already
 * exists (and fails clearly, not silently, if only *some* do -- an inconsistent state this action
 * refuses to guess its way out of). Each per-target split is skipped if that target already has a
 * published head (the same "already has a published head; refusing to clone onto it" condition
 * {@code ShardCloner#clone} itself refuses a second attempt against). The alias cutover and the
 * optional write-routing assignment are both naturally idempotent already ({@code
 * IndicesAliasesRequest}'s add action, and {@code WritePartitionRoutingMetadata}'s own
 * overwrite-in-place assignment), so no special-casing is needed for either.
 *
 * <p>Deliberately scoped to a single-shard source splitting into single-shard targets -- the same
 * shape {@link ProvisionSplitTargetsAction} and {@link ShardSplitAction} already commit to
 * (`number_of_shards` fixed at 1 per target, matching {@code ShardSplitter}'s own per-shard model,
 * structurally different from core's own multi-shard resize). A multi-shard source is refused with
 * a clear error rather than guessing how each of its shards should map onto the target set.
 */
public class OrchestrateShardSplitAction extends ActionType<OrchestrateShardSplitResponse> {

    /** Singleton instance, matching the shape every other {@link ActionType} in this plugin uses. */
    public static final OrchestrateShardSplitAction INSTANCE = new OrchestrateShardSplitAction();

    /** This action's registered transport name. */
    public static final String NAME = "cluster:admin/serverless/storage/resharding/orchestrate_split";

    private OrchestrateShardSplitAction() {
        super(NAME, OrchestrateShardSplitResponse::new);
    }
}
