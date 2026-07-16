/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.split.InPlaceSplitShardAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidateEntry;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesAction;
import org.opensearch.serverless.storage.util.SustainedCandidateTracker;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * The "do the work" half of dynamic-partitioning-plan.md Phase 1 item 1.2: turns {@link
 * org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesAction}'s
 * (Task 56, extended with a size signal by Phase 1 item 1.1) advisory candidate list into real
 * calls to Phase 0's {@link InPlaceSplitShardAction} -- the first automated consumer of that
 * signal, closing the "policy without a mechanism yet" gap {@link ShardSplitCandidateEntry}'s own
 * javadoc used to describe (now stale, since Phase 0 landed the real mechanism).
 *
 * <p>Mirrors {@code org.opensearch.serverless.storage.scaleup.ReaderReplicaExpansionCoordinator}'s
 * shape closely -- same "sustained-duration hysteresis" (a shard must be flagged a candidate on
 * {@code requiredConsecutiveTicks} consecutive evaluations before this actually acts on it, not
 * just once) and same "illustrative per-tick budget" (rate-limits how many splits this fires in one
 * evaluation, busiest candidates first) reasoning, for the same reasons: avoiding a flapping
 * reaction to one noisy tick, and avoiding a burst of cluster-state-mutating calls all at once.
 *
 * <p><b>Split-point selection (Phase 1 item 1.3)</b>: always splits into exactly 2 children, a
 * simple midpoint bisection of the current {@link org.opensearch.cluster.metadata.ShardRange} --
 * matches core's own {@code SplitShardsMetadata.Builder#splitShard}'s default equal-subdivision
 * shape. No attempt is made to target a specific hot sub-range for the split-for-heat case: as the
 * plan's own item 1.3 notes, DynamoDB's real split-point-for-heat algorithm is not confidently
 * documented publicly, so this deliberately doesn't guess at one -- hysteresis (wait, re-measure
 * after the split, split again next tick if still hot) is the honest first cut here, not a
 * targeted-range heuristic.
 *
 * <p><b>Guards against re-triggering an already-in-flight or already-split shard</b>: {@link
 * #triggerCandidates} checks {@link SplitShardsMetadata#isSplitOfShardInProgress}/{@link
 * SplitShardsMetadata#isSplitParent} against the caller-supplied {@link ClusterState} before
 * issuing a split, and clears that shard's streak either way -- a shard {@link
 * ShardSplitCandidatesAction} still reports as a signal source (its writer engine hasn't
 * deregistered from {@code ShardActivityRegistry} yet) but that core has already retired via a
 * just-committed split must not be split again.
 */
public final class InPlaceSplitTriggerCoordinator {

    private static final Logger logger = LogManager.getLogger(InPlaceSplitTriggerCoordinator.class);

    /** Every automatically triggered split divides a shard into exactly this many children -- see this class's own "Split-point selection" javadoc. */
    public static final int SPLIT_INTO = 2;

    private final Client client;
    private final int maxSplitsPerTick;
    private final SustainedCandidateTracker<ShardSplitCandidateEntry> tracker;

    /**
     * Creates a coordinator.
     *
     * @param client dispatches the {@link InPlaceSplitShardAction} that actually splits a shard.
     * @param requiredConsecutiveTicks how many consecutive evaluations in a row a shard must be
     *                                 flagged a candidate on before it is actually split -- see this
     *                                 class's own "sustained-duration hysteresis" javadoc. Values
     *                                 {@code <= 1} restore the original single-tick behavior.
     * @param maxSplitsPerTick the maximum number of distinct shards this coordinator will actually
     *                         split in one call -- see this class's own "illustrative per-tick
     *                         budget" javadoc. Values {@code <= 0} mean unlimited.
     */
    public InPlaceSplitTriggerCoordinator(Client client, int requiredConsecutiveTicks, int maxSplitsPerTick) {
        this.client = client;
        this.maxSplitsPerTick = maxSplitsPerTick;
        this.tracker = new SustainedCandidateTracker<>(requiredConsecutiveTicks);
    }

    private static String key(ShardSplitCandidateEntry entry) {
        return entry.indexUuid() + "/" + entry.shardId();
    }

    /**
     * Splits every distinct sustained candidate shard that isn't already mid-split or already a
     * split parent, bounded by {@link #maxSplitsPerTick}. Busiest shards (by {@link
     * ShardSplitCandidateEntry#writesPerMinute()}) win the budget first, same priority order {@code
     * ReaderReplicaExpansionCoordinator} uses for its own budget.
     *
     * @param candidates one evaluation's full merged shard list -- every shard this evaluation
     *                   observed, not just the ones currently flagged a candidate, so this method
     *                   can correctly reset the streak of a shard that stopped qualifying and drop
     *                   the streak of a shard no longer reported at all.
     * @param state the cluster state to check each candidate's current split status against,
     *              before issuing a real split request.
     */
    public void triggerCandidates(List<ShardSplitCandidateEntry> candidates, ClusterState state) {
        List<ShardSplitCandidateEntry> sustainedCandidates = tracker.filterSustained(
            candidates,
            InPlaceSplitTriggerCoordinator::key,
            ShardSplitCandidateEntry::candidate
        );

        // Busiest shard first, so a tight budget is spent on whichever candidates need it most.
        sustainedCandidates.sort(Comparator.comparingLong(ShardSplitCandidateEntry::writesPerMinute).reversed());

        // Already-handled shards (in flight or already split) don't consume the per-tick budget at
        // all -- filter them out (clearing their streak) before the budget-limited selection below,
        // same as leaving them out of it entirely.
        List<ShardSplitCandidateEntry> actionable = new ArrayList<>();
        for (ShardSplitCandidateEntry entry : sustainedCandidates) {
            if (alreadySplitOrInFlight(state, entry)) {
                tracker.clearStreak(key(entry)); // core has already handled this shard -- start fresh if it recurs.
            } else {
                actionable.add(entry);
            }
        }

        // Budget exhausted for this tick -- deliberately leave the remaining candidates' streaks
        // intact rather than clearing them, so they keep their "already sustained" status and
        // highest priority into the next tick instead of having to re-qualify from scratch. Same
        // choice ReaderReplicaExpansionCoordinator makes.
        tracker.selectWithBudget(actionable, maxSplitsPerTick, entry -> true, entry -> {
            tracker.clearStreak(key(entry)); // acted on -- start counting fresh.
            triggerSplit(entry);
            return true;
        });
    }

    private boolean alreadySplitOrInFlight(ClusterState state, ShardSplitCandidateEntry entry) {
        // Metadata#index(String) looks up by index *name*, not uuid -- confirm the uuid still
        // matches too, in case the name has since been reused by an unrelated index (e.g. the
        // original was deleted and a new one created with the same name between this evaluation
        // being computed and this coordinator acting on it).
        IndexMetadata indexMetadata = state.metadata().index(entry.indexName());
        if (indexMetadata == null || entry.indexUuid().equals(indexMetadata.getIndexUUID()) == false) {
            return true; // index gone (or replaced) since this evaluation was computed -- nothing to split.
        }
        SplitShardsMetadata splitShardsMetadata = indexMetadata.getSplitShardsMetadata();
        return splitShardsMetadata.isSplitOfShardInProgress(entry.shardId()) || splitShardsMetadata.isSplitParent(entry.shardId());
    }

    private void triggerSplit(ShardSplitCandidateEntry entry) {
        InPlaceSplitShardAction.Request request = new InPlaceSplitShardAction.Request(entry.indexName(), entry.shardId(), SPLIT_INTO);
        client.execute(InPlaceSplitShardAction.INSTANCE, request, ActionListener.wrap(response -> onSplitResponse(entry, response), e -> {
            logger.warn(
                () -> new org.apache.logging.log4j.message.ParameterizedMessage(
                    "automatic in-place split failed for index [{}] shard [{}]",
                    entry.indexName(),
                    entry.shardId()
                ),
                e
            );
        }));
    }

    private void onSplitResponse(ShardSplitCandidateEntry entry, AcknowledgedResponse response) {
        if (response.isAcknowledged()) {
            logger.info(
                "automatically split index [{}] shard [{}] into {} children (write_rate_candidate={}, size_candidate={})",
                entry.indexName(),
                entry.shardId(),
                SPLIT_INTO,
                entry.writeRateCandidate(),
                entry.sizeCandidate()
            );
        } else {
            logger.warn("automatic in-place split for index [{}] shard [{}] was not acknowledged", entry.indexName(), entry.shardId());
        }
    }
}
