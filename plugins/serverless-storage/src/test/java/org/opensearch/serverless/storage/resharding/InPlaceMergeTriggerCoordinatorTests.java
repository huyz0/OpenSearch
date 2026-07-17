/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.split.InPlaceMergeShardAction;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.serverless.storage.resharding.action.ShardSplitCandidateEntry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.Client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class InPlaceMergeTriggerCoordinatorTests extends OpenSearchTestCase {

    private static final long WPM_THRESHOLD = 2_000L;
    private static final long SIZE_THRESHOLD = 4L * 1024 * 1024 * 1024;

    private Client client;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        client = mock(Client.class);
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onResponse(new AcknowledgedResponse(true));
            return null;
        }).when(client).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    private InPlaceMergeTriggerCoordinator coordinator(int requiredConsecutiveTicks, int maxMergesPerTick) {
        return new InPlaceMergeTriggerCoordinator(client, requiredConsecutiveTicks, maxMergesPerTick, WPM_THRESHOLD, SIZE_THRESHOLD);
    }

    /** An index whose root shard {@code parentShardId} has been split into {@code splitInto} children AND committed. */
    private static IndexMetadata committedSplitIndex(String indexUuid, String indexName, int rootShards, int parentShardId, int splitInto) {
        IndexMetadata base = IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
                    .build()
            )
            .numberOfShards(rootShards)
            .numberOfReplicas(0)
            .build();
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(base.getSplitShardsMetadata());
        List<ShardRange> childRanges = builder.splitShard(parentShardId, splitInto);
        Set<Integer> childIds = new HashSet<>();
        childRanges.forEach(r -> childIds.add(r.shardId()));
        builder.updateSplitMetadataForChildShards(parentShardId, childIds);
        return IndexMetadata.builder(base).splitShardsMetadata(builder.build()).build();
    }

    /** A further split: after {@code parentShardId} is split+committed, one of its children is itself split+committed. */
    private static IndexMetadata furtherSplitChildIndex(String indexUuid, String indexName, int parentShardId) {
        IndexMetadata committed = committedSplitIndex(indexUuid, indexName, 1, parentShardId, 2);
        int childToSplitFurther = committed.getSplitShardsMetadata().getChildShardIdsOfParent(parentShardId).iterator().next();
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(committed.getSplitShardsMetadata());
        List<ShardRange> grandchildRanges = builder.splitShard(childToSplitFurther, 2);
        Set<Integer> grandchildIds = new HashSet<>();
        grandchildRanges.forEach(r -> grandchildIds.add(r.shardId()));
        builder.updateSplitMetadataForChildShards(childToSplitFurther, grandchildIds);
        return IndexMetadata.builder(committed).splitShardsMetadata(builder.build()).build();
    }

    private static ClusterState stateOf(IndexMetadata... indices) {
        Metadata.Builder metadata = Metadata.builder();
        for (IndexMetadata indexMetadata : indices) {
            metadata.put(indexMetadata, false);
        }
        return ClusterState.builder(new ClusterName("test")).metadata(metadata.build()).build();
    }

    private static ShardSplitCandidateEntry signal(String indexUuid, String indexName, int shardId, long wpm, long sizeBytes) {
        return new ShardSplitCandidateEntry(indexUuid, shardId, indexName, wpm, sizeBytes, false, false);
    }

    /** Signal entries for both children of {@code parentShardId}, each carrying the given per-child wpm/size. */
    private static List<ShardSplitCandidateEntry> childSignals(
        IndexMetadata indexMetadata,
        int parentShardId,
        long perChildWpm,
        long perChildSize
    ) {
        List<ShardSplitCandidateEntry> entries = new ArrayList<>();
        for (int childShardId : indexMetadata.getSplitShardsMetadata().getChildShardIdsOfParent(parentShardId)) {
            entries.add(signal(indexMetadata.getIndexUUID(), indexMetadata.getIndex().getName(), childShardId, perChildWpm, perChildSize));
        }
        return entries;
    }

    public void testMergesQuietSustainedPair() {
        IndexMetadata index = committedSplitIndex("uuid-1", "my-index", 1, 0, 2);
        coordinator(1, 0).triggerCandidates(childSignals(index, 0, 100L, 1024L), stateOf(index));
        verify(client, times(1)).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    public void testMergeRequestTargetsCorrectIndexAndParentShard() {
        IndexMetadata index = committedSplitIndex("uuid-1", "my-index", 3, 2, 2);
        coordinator(1, 0).triggerCandidates(childSignals(index, 2, 100L, 1024L), stateOf(index));

        org.mockito.ArgumentCaptor<InPlaceMergeShardAction.Request> captor = org.mockito.ArgumentCaptor.forClass(
            InPlaceMergeShardAction.Request.class
        );
        verify(client).execute(eq(InPlaceMergeShardAction.INSTANCE), captor.capture(), any());
        assertEquals("my-index", captor.getValue().index());
        assertEquals(2, captor.getValue().parentShardId());
    }

    public void testDoesNotMergeWhenCombinedWriteRateAboveThreshold() {
        IndexMetadata index = committedSplitIndex("uuid-1", "my-index", 1, 0, 2);
        // Each child at 1_500/min -> combined 3_000, above the 2_000 ceiling.
        coordinator(1, 0).triggerCandidates(childSignals(index, 0, 1_500L, 1024L), stateOf(index));
        verify(client, never()).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    public void testDoesNotMergeWhenCombinedSizeAboveThreshold() {
        IndexMetadata index = committedSplitIndex("uuid-1", "my-index", 1, 0, 2);
        // Each child at 3 GiB -> combined 6 GiB, above the 4 GiB ceiling.
        coordinator(1, 0).triggerCandidates(childSignals(index, 0, 10L, 3L * 1024 * 1024 * 1024), stateOf(index));
        verify(client, never()).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    public void testDoesNotMergeAnInProgressSplit() {
        // Split started but never committed -> not a merge candidate.
        IndexMetadata base = IndexMetadata.builder("my-index")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_INDEX_UUID, "uuid-1")
                    .build()
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        SplitShardsMetadata.Builder builder = new SplitShardsMetadata.Builder(base.getSplitShardsMetadata());
        List<ShardRange> childRanges = builder.splitShard(0, 2); // in progress, not committed
        IndexMetadata index = IndexMetadata.builder(base).splitShardsMetadata(builder.build()).build();

        List<ShardSplitCandidateEntry> entries = new ArrayList<>();
        childRanges.forEach(r -> entries.add(signal("uuid-1", "my-index", r.shardId(), 10L, 1024L)));

        coordinator(1, 0).triggerCandidates(entries, stateOf(index));
        verify(client, never()).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    public void testDoesNotMergeWhenAChildHasBeenSplitFurther() {
        IndexMetadata index = furtherSplitChildIndex("uuid-1", "my-index", 0);
        // Report a quiet signal for every currently-active shard, so only the further-split
        // precondition (not a missing signal) can be what blocks the merge.
        List<ShardSplitCandidateEntry> entries = new ArrayList<>();
        index.getSplitShardsMetadata()
            .getActiveShardIterator()
            .forEachRemaining(shardId -> entries.add(signal("uuid-1", "my-index", shardId, 10L, 1024L)));

        coordinator(1, 0).triggerCandidates(entries, stateOf(index));
        verify(client, never()).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    public void testDoesNotMergeWhenAChildIsNotReporting() {
        IndexMetadata index = committedSplitIndex("uuid-1", "my-index", 1, 0, 2);
        // Only one of the two children reports a signal -- the pair is not fully observed.
        int oneChild = index.getSplitShardsMetadata().getChildShardIdsOfParent(0).iterator().next();
        List<ShardSplitCandidateEntry> onlyOne = List.of(signal("uuid-1", "my-index", oneChild, 10L, 1024L));

        coordinator(1, 0).triggerCandidates(onlyOne, stateOf(index));
        verify(client, never()).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    public void testDoesNotMergeWhenAChildSizeSignalIsUnknown() {
        IndexMetadata index = committedSplitIndex("uuid-1", "my-index", 1, 0, 2);
        List<ShardSplitCandidateEntry> entries = new ArrayList<>();
        boolean first = true;
        for (int childShardId : index.getSplitShardsMetadata().getChildShardIdsOfParent(0)) {
            long size = first ? ShardSplitCandidateEntry.UNKNOWN : 1024L;
            entries.add(signal("uuid-1", "my-index", childShardId, 10L, size));
            first = false;
        }
        coordinator(1, 0).triggerCandidates(entries, stateOf(index));
        verify(client, never()).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    public void testMergesOnlyOnceTheStreakReachesRequiredConsecutiveTicks() {
        IndexMetadata index = committedSplitIndex("uuid-1", "my-index", 1, 0, 2);
        ClusterState state = stateOf(index);
        List<ShardSplitCandidateEntry> quiet = childSignals(index, 0, 100L, 1024L);
        InPlaceMergeTriggerCoordinator coordinator = coordinator(3, 0);

        coordinator.triggerCandidates(quiet, state);
        coordinator.triggerCandidates(quiet, state);
        verify(client, never()).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());

        coordinator.triggerCandidates(quiet, state);
        verify(client, times(1)).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    public void testAGapInQuietnessResetsTheStreak() {
        IndexMetadata index = committedSplitIndex("uuid-1", "my-index", 1, 0, 2);
        ClusterState state = stateOf(index);
        List<ShardSplitCandidateEntry> quiet = childSignals(index, 0, 100L, 1024L);
        List<ShardSplitCandidateEntry> loud = childSignals(index, 0, 5_000L, 1024L); // combined 10_000 > threshold
        InPlaceMergeTriggerCoordinator coordinator = coordinator(3, 0);

        coordinator.triggerCandidates(quiet, state);
        coordinator.triggerCandidates(quiet, state);
        coordinator.triggerCandidates(loud, state); // resets the streak
        coordinator.triggerCandidates(quiet, state);
        coordinator.triggerCandidates(quiet, state);

        // Two quiet ticks after the reset is a streak of 2, still short of 3.
        verify(client, never()).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    public void testPerTickBudgetLimitsHowManyPairsMerge() {
        IndexMetadata a = committedSplitIndex("uuid-a", "index-a", 1, 0, 2);
        IndexMetadata b = committedSplitIndex("uuid-b", "index-b", 1, 0, 2);
        List<ShardSplitCandidateEntry> entries = new ArrayList<>();
        entries.addAll(childSignals(a, 0, 100L, 1024L));
        entries.addAll(childSignals(b, 0, 200L, 1024L));

        coordinator(1, 1).triggerCandidates(entries, stateOf(a, b));
        verify(client, times(1)).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    public void testPerTickBudgetPrioritizesTheQuietestPairFirst() {
        IndexMetadata quiet = committedSplitIndex("uuid-quiet", "quiet-index", 1, 0, 2);
        IndexMetadata lessQuiet = committedSplitIndex("uuid-less", "less-quiet-index", 1, 0, 2);
        List<ShardSplitCandidateEntry> entries = new ArrayList<>();
        entries.addAll(childSignals(quiet, 0, 10L, 1024L));       // combined 20/min -- quietest
        entries.addAll(childSignals(lessQuiet, 0, 500L, 1024L));  // combined 1_000/min

        coordinator(1, 1).triggerCandidates(entries, stateOf(quiet, lessQuiet));

        org.mockito.ArgumentCaptor<InPlaceMergeShardAction.Request> captor = org.mockito.ArgumentCaptor.forClass(
            InPlaceMergeShardAction.Request.class
        );
        verify(client, times(1)).execute(eq(InPlaceMergeShardAction.INSTANCE), captor.capture(), any());
        assertEquals(
            "with only budget for one, the quietest pair's index must be the one that actually merges",
            "quiet-index",
            captor.getValue().index()
        );
    }

    public void testNonPositiveBudgetMeansUnlimited() {
        IndexMetadata a = committedSplitIndex("uuid-a", "index-a", 1, 0, 2);
        IndexMetadata b = committedSplitIndex("uuid-b", "index-b", 1, 0, 2);
        List<ShardSplitCandidateEntry> entries = new ArrayList<>();
        entries.addAll(childSignals(a, 0, 100L, 1024L));
        entries.addAll(childSignals(b, 0, 200L, 1024L));

        coordinator(1, 0).triggerCandidates(entries, stateOf(a, b));
        verify(client, times(2)).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }

    public void testNoCandidatesWhenNoIndexIsSplit() {
        coordinator(1, 0).triggerCandidates(List.of(), ClusterState.builder(new ClusterName("test")).build());
        verify(client, never()).execute(eq(InPlaceMergeShardAction.INSTANCE), any(InPlaceMergeShardAction.Request.class), any());
    }
}
