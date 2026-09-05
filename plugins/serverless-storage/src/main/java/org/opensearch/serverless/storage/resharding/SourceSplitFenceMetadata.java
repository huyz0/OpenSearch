/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.HashMap;
import java.util.Map;

/**
 * Reads/writes a split source index's write-fencing marker, stored as ordinary {@link IndexMetadata}
 * custom data -- the exact same extension-point shape {@link WritePartitionRoutingMetadata} already
 * uses for a target's write-routing assignment, applied here to the source side of a split instead.
 *
 * <p><b>What this closes, and what it does not.</b> Before this existed, nothing in {@code
 * TransportOrchestrateShardSplitAction}'s split -&gt; cutover -&gt; write-routing sequence stopped a
 * client from continuing to write directly to the source index's own name, forever, with no error
 * anywhere -- a silently, permanently diverging copy that {@code RetireShrinkSourceAction} would
 * eventually delete along with whatever it held. Once {@code FenceSplitSourceAction} marks a source
 * fenced (wired into orchestration immediately after cutover succeeds, see {@code
 * TransportOrchestrateShardSplitAction#cutoverStage}), {@link
 * org.opensearch.serverless.storage.resharding.WritePartitionRoutingActionFilter} rejects any further
 * direct write against it, the same "fence a direct write against metadata already marked
 * superseded" shape that filter already applies to a write-routing target's own direct writes.
 *
 * <p><b>This narrows the gap, it does not eliminate it.</b> Fencing only takes effect once cutover
 * has already completed -- {@link org.opensearch.serverless.storage.resharding.action.ShardSplitAction}
 * still clones the source's state as of a single point-in-time generation with nothing stopping
 * writes between that clone point and cutover completing; any document written to the source in that
 * window is still captured only by the source, not by any split target. Closing that earlier window
 * completely needs either true write-blocking synchronized with the clone itself, or a dual-write
 * bridge mirroring writes to both source and targets until cutover -- both remain the "genuinely new,
 * separate mechanism" rfc-serverless-opensearch.md &sect;16 Phase 4 already described as out of scope.
 * What this closes is the much longer-lived, permanently-open window this fixes: from cutover onward,
 * for as long as the source index continues to exist before eventual retirement.
 *
 * <p><b>There is an unfence primitive, and there has to be.</b> The first increment of this class
 * deliberately shipped without one, on the reasoning that fencing only ever fires after a successful
 * cutover and so has no safe rollback scenario. That reasoning was wrong in a way that could not be
 * recovered from: a fence is a <em>permanent, cluster-state-durable write block</em> on a real
 * dataset, and an operator who ends up with one they did not intend -- because a later stage of the
 * orchestration failed, because the superseding alias has no write index, or simply because they
 * fenced the wrong index -- had exactly two options, deleting the index or hand-editing cluster
 * state. A mechanism whose only escape hatch is data loss is not a safe mechanism, however narrow
 * the window that produces it. {@link #withoutFence} and {@code UnfenceSplitSourceAction} exist for
 * that reason; unfencing is an explicit operator action, never automatic, so the normal
 * post-cutover fence still behaves exactly as before.
 */
public final class SourceSplitFenceMetadata {

    private SourceSplitFenceMetadata() {}

    /** The {@link IndexMetadata} custom-data key this source's fence marker is stored under. */
    public static final String FENCE_CUSTOM_TYPE = "serverless_storage_resharding_fenced_source";

    private static final String SUPERSEDING_ALIAS_MAP_KEY = "superseding_alias";

    /**
     * Returns {@code indexMetadata} marked as fenced, recording the alias that now serves as the
     * real entry point in its place.
     *
     * @param indexMetadata the source index to fence.
     * @param supersedingAliasName the write-routing/cutover alias split targets are now reachable through.
     */
    public static IndexMetadata withFence(IndexMetadata indexMetadata, String supersedingAliasName) {
        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
        Map<String, String> map = new HashMap<>();
        map.put(SUPERSEDING_ALIAS_MAP_KEY, supersedingAliasName);
        builder.putCustom(FENCE_CUSTOM_TYPE, map);
        return builder.build();
    }

    /**
     * Returns {@code indexMetadata} with any fence marker removed, restoring it as a writable index.
     * Returns the argument unchanged (same reference) when it was not fenced, so a caller can use
     * reference identity to decide whether a cluster-state update is needed at all.
     *
     * @param indexMetadata the source index to unfence.
     */
    public static IndexMetadata withoutFence(IndexMetadata indexMetadata) {
        if (isFencedSource(indexMetadata) == false) {
            return indexMetadata;
        }
        IndexMetadata.Builder builder = IndexMetadata.builder(indexMetadata);
        builder.removeCustom(FENCE_CUSTOM_TYPE);
        return builder.build();
    }

    /** Whether {@code indexMetadata} is currently fenced as a split source. */
    public static boolean isFencedSource(IndexMetadata indexMetadata) {
        return indexMetadata.getCustomData(FENCE_CUSTOM_TYPE) != null;
    }

    /**
     * The alias that now serves as this fenced source's real entry point, or {@code null} if
     * {@code indexMetadata} isn't fenced.
     *
     * @param indexMetadata the index metadata to read the fence marker from.
     */
    public static String supersedingAlias(IndexMetadata indexMetadata) {
        Map<String, String> custom = indexMetadata.getCustomData(FENCE_CUSTOM_TYPE);
        return custom == null ? null : custom.get(SUPERSEDING_ALIAS_MAP_KEY);
    }
}
