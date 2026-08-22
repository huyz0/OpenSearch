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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.admin.indices.split.InPlaceMergeShardAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidateEntry;
import org.opensearch.serverless.storage.util.SustainedCandidateTracker;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * The merge-side "do the work" half of dynamic-partitioning-plan.md Phase 2 item 2.3: the automatic
 * counterpart to {@link InPlaceSplitTriggerCoordinator}, turning the same cluster-wide per-shard
 * write-rate/size signal (surfaced by {@code
 * org.opensearch.serverless.storage.resharding.action.ShardSplitCandidatesAction}) into real calls
 * to Phase 2 item 2.1's operator {@link InPlaceMergeShardAction} -- merging a committed split's full
 * sibling pair back into their parent once the pair has become collectively small/quiet enough that
 * undoing the split makes sense, the inverse of why it was split.
 *
 * <p><b>What a "merge candidate" is (unlike split's single-shard signal).</b> Split needs only one
 * shard's own write-rate/size to cross a threshold. Merge is about a <em>pair</em>: for each parent
 * shard whose split has committed ({@link SplitShardsMetadata#canMergeChildrenBackToParent(int)}
 * true) and that has exactly two direct children (this whole feature is scoped to the
 * full-sibling-pair case -- see {@link SplitShardsMetadata.Builder#mergeChildrenBackToParent(int)}),
 * this sums both children's {@link ShardSplitCandidateEntry#writesPerMinute()} and {@link
 * ShardSplitCandidateEntry#shardSizeInBytes()} and flags the pair a candidate when both combined
 * sums sit at or below the merge thresholds. Eligibility (committed, unsplit-further, exactly-two)
 * is checked against the caller-supplied {@link ClusterState}; the quietness signal comes from the
 * candidate entries.
 *
 * <p><b>Hysteresis, keyed by the parent.</b> Reuses the shared {@link SustainedCandidateTracker}
 * exactly as split does, but keyed by the <em>parent</em> shard id -- the stable identity across a
 * merge decision (the children are what get retired). A pair must be flagged quiet on {@code
 * requiredConsecutiveTicks} consecutive evaluations before this actually merges it, and a tight
 * per-tick budget rate-limits how many pairs merge in one evaluation.
 *
 * <p><b>Anti-flap design, and a known limitation.</b> The danger unique to the merge side: split's
 * own auto-trigger just split a hot/large shard, and if merge's auto-trigger saw the resulting pair
 * dip below the merge threshold before write patterns stabilized, the two could flap
 * split/merge/split. Two mechanisms guard against this here:
 * <ol>
 *   <li><b>A deliberately large threshold margin.</b> The merge thresholds are set well below the
 *       split thresholds (see {@code
 *       ServerlessStoragePlugin.SERVERLESS_STORAGE_RESHARDING_MERGE_CANDIDATE_COMBINED_WPM_THRESHOLD_SETTING}):
 *       right after a split, the pair's combined write rate/size is still close to what triggered
 *       the split, an order of magnitude above the merge ceiling, so the pair simply isn't a merge
 *       candidate until its real, sustained load has fallen far below what split it.</li>
 *   <li><b>A higher sustained-tick requirement than split.</b> Merge defaults to more consecutive
 *       quiet ticks than split does, so a momentary lull can't provoke a merge.</li>
 * </ol>
 * <ol start="3">
 *   <li><b>A minimum split-commit cool-down.</b> A committed split records its commit instant in
 *       {@link SplitShardsMetadata#getSplitCommitTimestamp(int)}; a pair is not even a merge
 *       candidate until at least {@code minCooldownMillis} has elapsed since then. Unlike the two
 *       signal-based guards above, this is a hard time floor checked <em>before</em> the pair ever
 *       enters the sustained-tick tracker -- time-since-split is not itself a "sustained" signal, so
 *       a pair inside its cool-down is omitted entirely (which also keeps its tracked streak reset).
 *       A split whose commit predates the timestamp field ({@link
 *       SplitShardsMetadata#NO_SPLIT_COMMIT_TIMESTAMP}) fails open -- no floor -- and a non-positive
 *       {@code minCooldownMillis} disables the gate. See dynamic-partitioning-progress.md's "Phase 2
 *       item 2.3" entry.</li>
 * </ol>
 */
public final class InPlaceMergeTriggerCoordinator {

    private static final Logger logger = LogManager.getLogger(InPlaceMergeTriggerCoordinator.class);

    /** Every merge folds back exactly this many siblings -- the full-sibling-pair scope this feature is bounded to. */
    public static final int SIBLING_PAIR_SIZE = 2;

    private final Client client;
    private final int maxMergesPerTick;
    private final long combinedWritesPerMinuteThreshold;
    private final long combinedSizeThresholdBytes;
    private final long minCooldownMillis;
    private final LongSupplier nowMillisSupplier;
    private final SustainedCandidateTracker<MergeCandidate> tracker;

    /**
     * Creates a coordinator.
     *
     * @param client dispatches the {@link InPlaceMergeShardAction} that actually merges a pair back.
     * @param requiredConsecutiveTicks how many consecutive evaluations in a row a pair must be
     *                                 flagged quiet on before it is actually merged -- see this
     *                                 class's own "Hysteresis" javadoc. Values {@code <= 1} restore
     *                                 single-tick behavior.
     * @param maxMergesPerTick the maximum number of distinct sibling pairs this coordinator will
     *                         actually merge in one call. Values {@code <= 0} mean unlimited.
     * @param combinedWritesPerMinuteThreshold a pair's summed children writes-per-minute must be at
     *                                         or below this to count as quiet.
     * @param combinedSizeThresholdBytes a pair's summed children size in bytes must be at or below
     *                                   this to count as quiet.
     * @param minCooldownMillis the minimum time that must elapse after a split commits before its
     *                          pair is eligible to be a merge candidate at all -- a hard time floor
     *                          checked before the sustained-tick tracker. Values {@code <= 0} disable
     *                          the gate.
     */
    public InPlaceMergeTriggerCoordinator(
        Client client,
        int requiredConsecutiveTicks,
        int maxMergesPerTick,
        long combinedWritesPerMinuteThreshold,
        long combinedSizeThresholdBytes,
        long minCooldownMillis
    ) {
        this(
            client,
            requiredConsecutiveTicks,
            maxMergesPerTick,
            combinedWritesPerMinuteThreshold,
            combinedSizeThresholdBytes,
            minCooldownMillis,
            System::currentTimeMillis
        );
    }

    /**
     * Test-visible constructor allowing the "now" clock (used with each pair's split-commit timestamp
     * to evaluate the cool-down floor) to be supplied deterministically. Production uses the other
     * constructor, which pins {@code nowMillisSupplier} to {@link System#currentTimeMillis()} -- the
     * same absolute epoch-millis clock the split commit records with.
     */
    InPlaceMergeTriggerCoordinator(
        Client client,
        int requiredConsecutiveTicks,
        int maxMergesPerTick,
        long combinedWritesPerMinuteThreshold,
        long combinedSizeThresholdBytes,
        long minCooldownMillis,
        LongSupplier nowMillisSupplier
    ) {
        this.client = client;
        this.maxMergesPerTick = maxMergesPerTick;
        this.combinedWritesPerMinuteThreshold = combinedWritesPerMinuteThreshold;
        this.combinedSizeThresholdBytes = combinedSizeThresholdBytes;
        this.minCooldownMillis = minCooldownMillis;
        this.nowMillisSupplier = nowMillisSupplier;
        this.tracker = new SustainedCandidateTracker<>(requiredConsecutiveTicks);
    }

    private static String signalKey(String indexUuid, int shardId) {
        return indexUuid + "/" + shardId;
    }

    private static String candidateKey(MergeCandidate candidate) {
        return candidate.indexUuid + "/" + candidate.parentShardId;
    }

    /**
     * Merges every distinct sustained-quiet sibling pair, bounded by {@code maxMergesPerTick}.
     * Quietest pairs (lowest combined writes-per-minute) win the budget first -- the mirror of
     * split's "busiest first" priority, since the quietest pair is the one most clearly worth
     * reclaiming.
     *
     * @param observedShards one evaluation's full merged per-shard signal list (every writer shard
     *                       observed cluster-wide), the raw material for each pair's combined signal.
     * @param state the cluster state whose {@link SplitShardsMetadata} decides which parents are
     *              committed, unsplit-further, exactly-two-child merge candidates.
     */
    public void triggerCandidates(List<ShardSplitCandidateEntry> observedShards, ClusterState state) {
        Map<String, ShardSplitCandidateEntry> signalByShard = new LinkedHashMap<>();
        for (ShardSplitCandidateEntry entry : observedShards) {
            signalByShard.put(signalKey(entry.indexUuid(), entry.shardId()), entry);
        }

        List<MergeCandidate> pairs = findEligiblePairs(state, signalByShard);

        List<MergeCandidate> sustained = tracker.filterSustained(pairs, InPlaceMergeTriggerCoordinator::candidateKey, c -> c.quiet);

        // Quietest pair first, so a tight budget is spent reclaiming whichever pair is most clearly idle.
        sustained.sort(Comparator.comparingLong((MergeCandidate c) -> c.combinedWritesPerMinute));

        // Budget exhausted for this tick -- deliberately leave the remaining pairs' streaks intact,
        // same choice InPlaceSplitTriggerCoordinator makes, so they keep their sustained status into
        // the next tick rather than re-qualifying from scratch.
        tracker.selectWithBudget(sustained, maxMergesPerTick, c -> true, c -> {
            tracker.clearStreak(candidateKey(c)); // acted on -- start counting fresh.
            triggerMerge(c);
            return true;
        });
    }

    /**
     * Every committed, unsplit-further, exactly-two-child split parent anywhere in {@code state},
     * each carried with the combined signal of its two children and whether that signal is quiet
     * enough to merge. A pair whose two children aren't both currently reporting a signal is omitted
     * entirely (rather than assumed quiet) -- without both children's size we can't safely conclude
     * the pair fits back inside one shard, and omission correctly resets its tracked streak.
     */
    private List<MergeCandidate> findEligiblePairs(ClusterState state, Map<String, ShardSplitCandidateEntry> signalByShard) {
        List<MergeCandidate> pairs = new ArrayList<>();
        for (IndexMetadata indexMetadata : state.metadata().indices().values()) {
            SplitShardsMetadata splitShardsMetadata = indexMetadata.getSplitShardsMetadata();
            String indexUuid = indexMetadata.getIndexUUID();
            String indexName = indexMetadata.getIndex().getName();
            for (int parentShardId : splitShardsMetadata.getSplitParentShardIds()) {
                // Committed (not mid-split) and no child split further -- the exact split-level
                // precondition mergeChildrenBackToParent enforces, queried here without mutating.
                if (splitShardsMetadata.canMergeChildrenBackToParent(parentShardId) == false) {
                    continue;
                }
                // Hard split-commit cool-down floor, checked before the sustained-tick tracker ever sees
                // this pair: within its cool-down window a just-split pair isn't a candidate at all, no matter
                // how quiet its combined signal looks -- the strongest anti-flap guard, since it prevents a
                // split immediately followed by a merge even if write patterns dip right after the split.
                if (withinSplitCommitCooldown(splitShardsMetadata, parentShardId)) {
                    continue;
                }
                Set<Integer> childShardIds = splitShardsMetadata.getChildShardIdsOfParent(parentShardId);
                if (childShardIds.size() != SIBLING_PAIR_SIZE) {
                    continue; // only full sibling pairs are in scope -- see class javadoc.
                }

                long combinedWpm = 0;
                long combinedSize = 0;
                boolean bothChildrenReporting = true;
                for (int childShardId : childShardIds) {
                    ShardSplitCandidateEntry childSignal = signalByShard.get(signalKey(indexUuid, childShardId));
                    if (childSignal == null
                        || childSignal.writesPerMinute() == ShardSplitCandidateEntry.UNKNOWN
                        || childSignal.shardSizeInBytes() == ShardSplitCandidateEntry.UNKNOWN) {
                        bothChildrenReporting = false;
                        break;
                    }
                    combinedWpm += childSignal.writesPerMinute();
                    combinedSize += childSignal.shardSizeInBytes();
                }
                if (bothChildrenReporting == false) {
                    continue;
                }

                boolean quiet = combinedWpm <= combinedWritesPerMinuteThreshold && combinedSize <= combinedSizeThresholdBytes;
                pairs.add(new MergeCandidate(indexUuid, indexName, parentShardId, combinedWpm, combinedSize, quiet));
            }
        }
        return pairs;
    }

    /**
     * Whether {@code parentShardId}'s split committed too recently to be a merge candidate yet. Fails open
     * on a disabled gate ({@code minCooldownMillis <= 0}) or an unrecorded timestamp ({@link
     * SplitShardsMetadata#NO_SPLIT_COMMIT_TIMESTAMP}, e.g. a split that committed on a cluster-state old
     * enough to predate the field): in both cases there is no floor and the pair is allowed through.
     */
    private boolean withinSplitCommitCooldown(SplitShardsMetadata splitShardsMetadata, int parentShardId) {
        if (minCooldownMillis <= 0) {
            return false;
        }
        long committedAt = splitShardsMetadata.getSplitCommitTimestamp(parentShardId);
        if (committedAt == SplitShardsMetadata.NO_SPLIT_COMMIT_TIMESTAMP) {
            return false;
        }
        return nowMillisSupplier.getAsLong() - committedAt < minCooldownMillis;
    }

    private void triggerMerge(MergeCandidate candidate) {
        InPlaceMergeShardAction.Request request = new InPlaceMergeShardAction.Request(candidate.indexName, candidate.parentShardId);
        client.execute(
            InPlaceMergeShardAction.INSTANCE,
            request,
            ActionListener.wrap(response -> onMergeResponse(candidate, response), e -> {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "automatic in-place merge failed for index [{}] parent shard [{}]",
                        candidate.indexName,
                        candidate.parentShardId
                    ),
                    e
                );
            })
        );
    }

    private void onMergeResponse(MergeCandidate candidate, AcknowledgedResponse response) {
        if (response.isAcknowledged()) {
            logger.info(
                "automatically merged index [{}] children of parent shard [{}] back (combined_writes_per_minute={}, combined_size_bytes={})",
                candidate.indexName,
                candidate.parentShardId,
                candidate.combinedWritesPerMinute,
                candidate.combinedSizeBytes
            );
        } else {
            logger.warn(
                "automatic in-place merge for index [{}] parent shard [{}] was not acknowledged",
                candidate.indexName,
                candidate.parentShardId
            );
        }
    }

    /** One eligible sibling pair for a tick: its parent identity, its combined signal, and whether that signal is quiet. */
    private static final class MergeCandidate {
        private final String indexUuid;
        private final String indexName;
        private final int parentShardId;
        private final long combinedWritesPerMinute;
        private final long combinedSizeBytes;
        private final boolean quiet;

        private MergeCandidate(
            String indexUuid,
            String indexName,
            int parentShardId,
            long combinedWritesPerMinute,
            long combinedSizeBytes,
            boolean quiet
        ) {
            this.indexUuid = indexUuid;
            this.indexName = indexName;
            this.parentShardId = parentShardId;
            this.combinedWritesPerMinute = combinedWritesPerMinute;
            this.combinedSizeBytes = combinedSizeBytes;
            this.quiet = quiet;
        }
    }
}
