/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Metadata for tracking shard split operations on an index.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SplitShardsMetadata extends AbstractDiffable<SplitShardsMetadata> implements ToXContentFragment {
    private static final int MINIMUM_RANGE_LENGTH_THRESHOLD = 1000;

    /**
     * Sentinel returned by {@link #getSplitCommitTimestamp(int)} when no split-commit timestamp is recorded
     * for a shard id -- because it isn't a committed split parent at all, or because the split committed on a
     * node/cluster-state old enough to predate the {@code splitCommitTimestamps} field (see wire-format gating
     * on {@link Version#V_3_8_0}). A caller gating on elapsed-time-since-commit must treat this as "no floor"
     * (fail open), never as "committed at epoch 0".
     */
    public static final long NO_SPLIT_COMMIT_TIMESTAMP = -1L;

    private static final String KEY_ROOT_SHARDS_TO_ALL_CHILDREN = "root_shards_to_all_children";
    private static final String KEY_NUMBER_OF_ROOT_SHARDS = "num_of_root_shards";
    private static final String KEY_PARENT_TO_CHILD_SHARDS = "parent_to_child_shards";
    private static final String KEY_MAX_SHARD_ID = "max_shard_id";
    private static final String KEY_IN_PROGRESS_SPLIT_SHARD_IDS = "in_progress_split_shard_id";
    private static final String KEY_ACTIVE_SHARD_IDS = "active_shard_ids";
    private static final String KEY_SPLIT_COMMIT_TIMESTAMPS = "split_commit_timestamps";

    // Following fields are upadated only after split completion and are used to service active shards request.
    // Root shard id to flat list of all child shards under root.
    private final ShardRange[][] rootShardsToAllChildren;
    private final int maxShardId;
    private final Set<Integer> activeShardIds;

    // Following fields can store temporary information about in progress child shards along with info about
    // split completed shards.
    // Mapping of a parent shard ID to children.
    private final Map<Integer, ShardRange[]> parentToChildShards;
    private final Set<Integer> inProgressSplitShardIds;

    // Parent shard id -> epoch millis at which that shard's split committed (all children reached STARTED and
    // were promoted to active). Recorded by MetadataInPlaceSplitShardCommitService at commit time so a merge
    // trigger can enforce a minimum cool-down since the split before considering the pair for re-merge.
    // Keyed by parent shard id, consistent with parentToChildShards, and dropped when the parent is merged
    // back (Builder#mergeChildrenBackToParent).
    private final Map<Integer, Long> splitCommitTimestamps;

    SplitShardsMetadata(
        ShardRange[][] rootShardsToAllChildren,
        Map<Integer, ShardRange[]> parentToChildShards,
        Set<Integer> inProgressSplitShardIds,
        Set<Integer> activeShardIds,
        int maxShardId
    ) {
        this(rootShardsToAllChildren, parentToChildShards, inProgressSplitShardIds, activeShardIds, maxShardId, Collections.emptyMap());
    }

    SplitShardsMetadata(
        ShardRange[][] rootShardsToAllChildren,
        Map<Integer, ShardRange[]> parentToChildShards,
        Set<Integer> inProgressSplitShardIds,
        Set<Integer> activeShardIds,
        int maxShardId,
        Map<Integer, Long> splitCommitTimestamps
    ) {

        this.rootShardsToAllChildren = rootShardsToAllChildren;
        this.parentToChildShards = Collections.unmodifiableMap(parentToChildShards);
        this.maxShardId = maxShardId;
        this.inProgressSplitShardIds = Collections.unmodifiableSet(inProgressSplitShardIds);
        this.activeShardIds = activeShardIds;
        this.splitCommitTimestamps = Collections.unmodifiableMap(splitCommitTimestamps);
    }

    public SplitShardsMetadata(StreamInput in) throws IOException {
        int numberOfRootShards = in.readVInt();
        this.rootShardsToAllChildren = new ShardRange[numberOfRootShards][];
        for (int i = 0; i < numberOfRootShards; i++) {
            this.rootShardsToAllChildren[i] = in.readOptionalArray(ShardRange::new, ShardRange[]::new);
        }
        this.maxShardId = in.readVInt();
        this.inProgressSplitShardIds = Collections.unmodifiableSet(in.readSet(StreamInput::readInt));
        this.activeShardIds = Collections.unmodifiableSet(in.readSet(StreamInput::readInt));
        this.parentToChildShards = Collections.unmodifiableMap(
            in.readMap(StreamInput::readInt, i -> i.readArray(ShardRange::new, ShardRange[]::new))
        );
        // splitCommitTimestamps is a V_3_8_0+ addition. Reading from an older node's stream (which never
        // wrote this map) must leave it empty rather than attempting a read that would corrupt the stream.
        // The whole SplitShardsMetadata blob is itself only ever serialized between V_3_6_0+ nodes (gated in
        // IndexMetadata), so this inner gate is what keeps a mixed V_3_6_0/V_3_7_x <-> V_3_8_0 cluster safe.
        if (in.getVersion().onOrAfter(Version.V_3_8_0)) {
            this.splitCommitTimestamps = Collections.unmodifiableMap(in.readMap(StreamInput::readInt, StreamInput::readLong));
        } else {
            this.splitCommitTimestamps = Collections.emptyMap();
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(rootShardsToAllChildren.length);
        for (ShardRange[] rootShardsToAllChild : rootShardsToAllChildren) {
            out.writeOptionalArray(rootShardsToAllChild);
        }
        out.writeVInt(this.maxShardId);
        out.writeCollection(this.inProgressSplitShardIds, StreamOutput::writeInt);
        out.writeCollection(this.activeShardIds, StreamOutput::writeInt);
        out.writeMap(this.parentToChildShards, StreamOutput::writeInt, StreamOutput::writeArray);
        // See the constructor's note: only write the new field to V_3_8_0+ peers.
        if (out.getVersion().onOrAfter(Version.V_3_8_0)) {
            out.writeMap(this.splitCommitTimestamps, StreamOutput::writeInt, StreamOutput::writeLong);
        }
    }

    public int getShardIdOfHash(int rootShardId, int hash) {
        // First check if we have child shards against this root shard.
        if (rootShardsToAllChildren[rootShardId] == null) {
            return rootShardId;
        }

        ShardRange[] existingChildShards = rootShardsToAllChildren[rootShardId];
        ShardRange shardRange = binarySearchShards(existingChildShards, hash);
        assert shardRange != null;

        return shardRange.shardId();
    }

    private ShardRange binarySearchShards(ShardRange[] childShards, int hash) {
        int low = 0, high = childShards.length - 1;
        while (low <= high) {
            int mid = low + (high - low) / 2;
            ShardRange midShard = childShards[mid];
            if (midShard.contains(hash)) {
                return midShard;
            } else if (hash < midShard.start()) {
                high = mid - 1;
            } else {
                low = mid + 1;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        StringBuilder parentToChildMap = new StringBuilder();
        for (Map.Entry<Integer, ShardRange[]> entry : parentToChildShards.entrySet()) {
            parentToChildMap.append("[");
            parentToChildMap.append(entry.getKey()).append("=").append(Arrays.toString(entry.getValue()));
            parentToChildMap.append("]");
        }
        return "SplitShardsMetadata{"
            + "rootShardsToAllChildren="
            + Arrays.toString(rootShardsToAllChildren)
            + ", maxShardId="
            + maxShardId
            + ", activeShardIds="
            + activeShardIds
            + ", parentToChildShards="
            + parentToChildMap
            + ", inProgressSplitShardIds="
            + inProgressSplitShardIds
            + ", splitCommitTimestamps="
            + splitCommitTimestamps
            + '}';
    }

    public int getNumberOfRootShards() {
        return rootShardsToAllChildren.length;
    }

    public int getNumberOfShards() {
        return activeShardIds.size();
    }

    public List<Integer> getRootShards() {
        List<Integer> rootShardList = new ArrayList<>();
        for (int i = 0; i < rootShardsToAllChildren.length; i++) {
            rootShardList.add(i);
        }
        return rootShardList;
    }

    public ShardRange[] getChildShardsOfParent(int shardId) {
        if (parentToChildShards.containsKey(shardId) == false) {
            return null;
        }

        ShardRange[] childShards = new ShardRange[parentToChildShards.get(shardId).length];
        int childShardIdx = 0;
        for (ShardRange childShard : parentToChildShards.get(shardId)) {
            childShards[childShardIdx++] = childShard;
        }
        return childShards;
    }

    /**
     * The hash range of {@code shardId}, if it is a split child -- in progress or already committed --
     * or {@code null} if it is not a split child at all (an ordinary, never-split shard). Unlike
     * {@link #getParentAndRangeOfChild(int)} (deliberately scoped to only the in-progress window, for
     * recovery), this covers a child's entire lifetime: the read path needs a committed child's own
     * range for as long as its bundles remain logically-filtered rather than physically rewritten
     * (see this plugin's own {@code InPlaceSplitFilteringDirectoryReader}), which is indefinitely in
     * this increment, not just until commit.
     */
    public ShardRange getRangeOfShard(int shardId) {
        for (ShardRange[] childRanges : parentToChildShards.values()) {
            for (ShardRange childRange : childRanges) {
                if (childRange.shardId() == shardId) {
                    return childRange;
                }
            }
        }
        for (ShardRange[] childRanges : rootShardsToAllChildren) {
            if (childRanges == null) {
                continue;
            }
            for (ShardRange childRange : childRanges) {
                if (childRange.shardId() == shardId) {
                    return childRange;
                }
            }
        }
        return null;
    }

    /**
     * Reverse of {@link #getChildShardsOfParent(int)}: given a shard ID that is currently a
     * not-yet-committed child of an in-progress split, returns its parent's shard ID and its own
     * {@link ShardRange}. Returns {@code null} if {@code childShardId} isn't a child of any
     * currently in-progress split -- in particular, this does <em>not</em> resolve parentage for a
     * child whose split has already committed (use {@link #getRootShards()}/the {@code
     * rootShardsToAllChildren} lineage for that case instead; this method only serves the recovery
     * window between a child's {@link ShardRange} being reserved and the split being committed).
     */
    public Tuple<Integer, ShardRange> getParentAndRangeOfChild(int childShardId) {
        for (Map.Entry<Integer, ShardRange[]> entry : parentToChildShards.entrySet()) {
            if (inProgressSplitShardIds.contains(entry.getKey()) == false) {
                continue;
            }
            for (ShardRange childRange : entry.getValue()) {
                if (childRange.shardId() == childShardId) {
                    return new Tuple<>(entry.getKey(), childRange);
                }
            }
        }
        return null;
    }

    public Set<Integer> getChildShardIdsOfParent(int shardId) {
        Set<Integer> childShardIds = new HashSet<>();
        if (parentToChildShards.containsKey(shardId) == false) {
            return childShardIds;
        }

        for (ShardRange childShard : parentToChildShards.get(shardId)) {
            childShardIds.add(childShard.shardId());
        }
        return childShardIds;
    }

    public int inProgressChildShardsCount() {
        int total = 0;
        for (Integer parent : inProgressSplitShardIds) {
            total += parentToChildShards.get(parent).length;
        }
        return total;
    }

    public Iterator<Integer> getActiveShardIterator() {
        return new HashSet<>(activeShardIds).iterator();
    }

    // Visible for testing
    static void validateShardRanges(int shardId, ShardRange[] shardRanges) {
        Integer start = null;
        int lowerBound = Integer.MIN_VALUE;
        int upperBound = Integer.MAX_VALUE;
        for (ShardRange shardRange : shardRanges) {
            validateBounds(shardRange, start, lowerBound);
            long rangeEnd = shardRange.end();
            long rangeLength = rangeEnd - shardRange.start() + 1;
            if (rangeLength < MINIMUM_RANGE_LENGTH_THRESHOLD) {
                throw new IllegalArgumentException(
                    "Shard range from "
                        + shardRange.start()
                        + " to "
                        + shardRange.end()
                        + " is below shard range threshold of "
                        + MINIMUM_RANGE_LENGTH_THRESHOLD
                );
            }

            start = shardRange.end();
        }

        if (start == null) {
            throw new IllegalArgumentException("No shard range defined for child shards of shard " + shardId);
        }

        if (start != upperBound) {
            throw new IllegalArgumentException(
                "Shard range from " + (start + 1) + " to " + upperBound + " is missing from the list of shard ranges"
            );
        }
    }

    private static void validateBounds(ShardRange shardRange, Integer start, long parentStart) {
        if (start == null) {
            if (shardRange.start() != parentStart) {
                throw new IllegalArgumentException(
                    "Shard range from " + parentStart + " to " + (shardRange.start() - 1) + " is missing from the list of shard ranges"
                );
            }
        } else if (shardRange.start() != start + 1) {
            String errorMessage;
            if (shardRange.start() < start + 1) {
                errorMessage = "Shard range overlaps from " + shardRange.start() + " to " + start;
            } else {
                errorMessage = "Shard range from "
                    + (start + 1)
                    + " to "
                    + (shardRange.start() - 1)
                    + " is missing from the list of shard ranges";
            }
            throw new IllegalArgumentException(errorMessage);
        }
    }

    /**
     * Builder for {@link SplitShardsMetadata}.
     *
     * @opensearch.experimental
     */
    public static class Builder {
        private final ShardRange[][] rootShardsToAllChildren;
        private final Map<Integer, ShardRange[]> parentToChildShards;
        private int maxShardId;
        private final Set<Integer> inProgressSplitShardIds;
        private final Set<Integer> activeShardIds;
        private final Map<Integer, Long> splitCommitTimestamps;

        public Builder(int numberOfShards) {
            maxShardId = numberOfShards - 1;
            rootShardsToAllChildren = new ShardRange[numberOfShards][];
            parentToChildShards = new HashMap<>();
            inProgressSplitShardIds = new HashSet<>();
            activeShardIds = new HashSet<>();
            splitCommitTimestamps = new HashMap<>();
            for (int i = 0; i < numberOfShards; i++) {
                activeShardIds.add(i);
            }
        }

        public Builder(SplitShardsMetadata splitShardsMetadata) {
            this.maxShardId = splitShardsMetadata.maxShardId;
            this.splitCommitTimestamps = new HashMap<>(splitShardsMetadata.splitCommitTimestamps);

            this.rootShardsToAllChildren = new ShardRange[splitShardsMetadata.rootShardsToAllChildren.length][];
            Set<Integer> activeShardIds = new HashSet<>();
            for (int i = 0; i < splitShardsMetadata.rootShardsToAllChildren.length; i++) {
                if (splitShardsMetadata.rootShardsToAllChildren[i] != null) {
                    this.rootShardsToAllChildren[i] = new ShardRange[splitShardsMetadata.rootShardsToAllChildren[i].length];
                    int j = 0;
                    for (ShardRange childShard : splitShardsMetadata.rootShardsToAllChildren[i]) {
                        this.rootShardsToAllChildren[i][j++] = childShard;
                        activeShardIds.add(childShard.shardId());
                    }
                } else {
                    activeShardIds.add(i);
                }
            }

            this.parentToChildShards = new HashMap<>();
            for (Integer parentShardId : splitShardsMetadata.parentToChildShards.keySet()) {
                // Getting a copy of child shards for this parent.
                ShardRange[] childShards = splitShardsMetadata.getChildShardsOfParent(parentShardId);
                this.parentToChildShards.put(parentShardId, childShards);
            }

            inProgressSplitShardIds = new HashSet<>(splitShardsMetadata.inProgressSplitShardIds);
            this.activeShardIds = activeShardIds;
        }

        /**
         * Create metadata of new child shards for the provided shard id.
         * @param splitShardId Shard id to split
         * @param numberOfChildren Number of child shards this shard is going to have.
         */
        public List<ShardRange> splitShard(int splitShardId, int numberOfChildren) {
            if (numberOfChildren < 2) {
                throw new IllegalArgumentException("Cannot split shard [" + splitShardId + "] into fewer than 2 children.");
            }
            if (inProgressSplitShardIds.contains(splitShardId) || parentToChildShards.containsKey(splitShardId)) {
                throw new IllegalArgumentException("Split of shard [" + splitShardId + "] is already in progress or completed.");
            }

            Tuple<Integer, ShardRange> shardTuple = findRootAndShard(splitShardId, rootShardsToAllChildren);
            if (shardTuple == null) {
                throw new IllegalArgumentException("Invalid shard id provided for splitting");
            }
            ShardRange parentShard = shardTuple.v2();

            long rangeSize = ((long) parentShard.end() - parentShard.start() + 1) / numberOfChildren;
            if (rangeSize <= MINIMUM_RANGE_LENGTH_THRESHOLD) {
                throw new IllegalArgumentException("Cannot split shard [" + splitShardId + "] further.");
            }

            Set<Integer> inProgressChildShardIds = getInProgressChildShardIds();
            inProgressSplitShardIds.add(splitShardId);

            List<Integer> allConsumedShardIds = new ArrayList<>();
            for (int i = 0; i < rootShardsToAllChildren.length; i++) {
                if (rootShardsToAllChildren[i] == null) {
                    allConsumedShardIds.add(i);
                }
            }
            for (Integer splitId : parentToChildShards.keySet()) {
                allConsumedShardIds.add(splitId);
                for (ShardRange shard : parentToChildShards.get(splitId)) {
                    allConsumedShardIds.add(shard.shardId());
                }
            }

            Set<Integer> shardIdHoles = findHoles(allConsumedShardIds);
            List<ShardRange> newChildShardsList = new ArrayList<>();
            long start = parentShard.start();

            int nextChildShardId = maxShardId, childShardId;
            for (int i = 0; i < numberOfChildren; ++i) {
                if (shardIdHoles.isEmpty()) {
                    nextChildShardId++;
                    while (inProgressChildShardIds.contains(nextChildShardId)) {
                        assert activeShardIds.contains(nextChildShardId) == false;
                        nextChildShardId++;
                    }
                    childShardId = nextChildShardId;
                } else {
                    childShardId = shardIdHoles.iterator().next();
                    shardIdHoles.remove(childShardId);
                    nextChildShardId = Math.max(nextChildShardId, childShardId);
                }
                assert !inProgressChildShardIds.contains(childShardId);
                assert !activeShardIds.contains(childShardId);
                inProgressChildShardIds.add(childShardId);

                long end = i == numberOfChildren - 1 ? parentShard.end() : start + rangeSize - 1;
                ShardRange childShard = new ShardRange(childShardId, (int) start, (int) end);
                newChildShardsList.add(childShard);
                start = end + 1;
            }

            ShardRange[] newShardRanges = newChildShardsList.toArray(new ShardRange[0]);

            // Get existing childShardRanges under rootShard
            List<ShardRange> shardsUnderRoot = rootShardsToAllChildren[shardTuple.v1()] == null
                ? new ArrayList<>()
                : new ArrayList<>(Arrays.asList(rootShardsToAllChildren[shardTuple.v1()]));
            shardsUnderRoot.remove(shardTuple.v2()); // Remove parent shard range
            shardsUnderRoot.addAll(List.of(newShardRanges)); // Add child shard range
            ShardRange[] newShardsUnderRoot = shardsUnderRoot.toArray(new ShardRange[0]);
            Arrays.sort(newShardsUnderRoot);

            validateShardRanges(splitShardId, newShardsUnderRoot);

            parentToChildShards.put(splitShardId, newShardRanges);
            return Collections.unmodifiableList(newChildShardsList);
        }

        private Set<Integer> findHoles(List<Integer> allConsumedShardIds) {
            Set<Integer> gaps = new TreeSet<>();

            if (allConsumedShardIds == null || allConsumedShardIds.size() <= 1) {
                return gaps;
            }

            Collections.sort(allConsumedShardIds);
            for (int i = 1; i < allConsumedShardIds.size(); i++) {
                int current = allConsumedShardIds.get(i);
                int previous = allConsumedShardIds.get(i - 1);

                if (current - previous > 1) {
                    for (int j = previous + 1; j < current; j++) {
                        gaps.add(j);
                    }
                }
            }

            return gaps;
        }

        public void updateSplitMetadataForChildShards(int sourceShardId, Set<Integer> newChildShardIds) {
            updateSplitMetadataForChildShards(sourceShardId, newChildShardIds, NO_SPLIT_COMMIT_TIMESTAMP);
        }

        /**
         * Commits a split and records the epoch-millis instant at which it committed, keyed by the parent
         * shard id. The production commit path ({@code MetadataInPlaceSplitShardCommitService}) passes a real
         * timestamp here; the no-timestamp overload above (used by tests and any caller that doesn't care)
         * records {@link #NO_SPLIT_COMMIT_TIMESTAMP}, i.e. no cool-down floor. Passing
         * {@link #NO_SPLIT_COMMIT_TIMESTAMP} explicitly clears any prior recorded timestamp for the parent.
         */
        public void updateSplitMetadataForChildShards(int sourceShardId, Set<Integer> newChildShardIds, long commitTimestamp) {
            Tuple<Integer, ShardRange> shardRangeTuple = findRootAndShard(sourceShardId, rootShardsToAllChildren);

            assert inProgressSplitShardIds.contains(sourceShardId);
            assert newChildShardIds.size() == parentToChildShards.get(sourceShardId).length;
            for (ShardRange childShard : parentToChildShards.get(sourceShardId)) {
                assert newChildShardIds.contains(childShard.shardId());
            }

            List<ShardRange> shardsUnderRoot = rootShardsToAllChildren[shardRangeTuple.v1()] == null
                ? new ArrayList<>()
                : new ArrayList<>(Arrays.asList(rootShardsToAllChildren[shardRangeTuple.v1()]));
            shardsUnderRoot.remove(shardRangeTuple.v2());
            shardsUnderRoot.addAll(Arrays.asList(parentToChildShards.get(sourceShardId)));
            ShardRange[] newShardsUnderRoot = shardsUnderRoot.toArray(new ShardRange[0]);
            Arrays.sort(newShardsUnderRoot);
            validateShardRanges(shardRangeTuple.v1(), newShardsUnderRoot);

            int currentMaxShardId = maxShardId;
            for (Integer newChildId : newChildShardIds) {
                assert activeShardIds.contains(newChildId) == false;
                activeShardIds.add(newChildId);
                currentMaxShardId = Math.max(currentMaxShardId, newChildId);
            }

            activeShardIds.remove(sourceShardId);
            maxShardId = currentMaxShardId;
            rootShardsToAllChildren[shardRangeTuple.v1()] = newShardsUnderRoot;
            inProgressSplitShardIds.remove(sourceShardId);
            if (commitTimestamp == NO_SPLIT_COMMIT_TIMESTAMP) {
                splitCommitTimestamps.remove(sourceShardId);
            } else {
                splitCommitTimestamps.put(sourceShardId, commitTimestamp);
            }
        }

        private Set<Integer> getInProgressChildShardIds() {
            Set<Integer> inProgressChildShardIds = new HashSet<>();
            for (Integer inProgressParent : inProgressSplitShardIds) {
                assert parentToChildShards.containsKey(inProgressParent);
                ShardRange[] childShards = parentToChildShards.get(inProgressParent);
                for (ShardRange childShard : childShards) {
                    inProgressChildShardIds.add(childShard.shardId());
                }
            }
            return inProgressChildShardIds;
        }

        public void cancelSplit(int sourceShardId) {
            assert inProgressSplitShardIds.contains(sourceShardId);
            inProgressSplitShardIds.remove(sourceShardId);
            parentToChildShards.remove(sourceShardId);
            // An in-progress split never recorded a commit timestamp, but clear defensively so a re-used
            // parent id can't inherit a stale one.
            splitCommitTimestamps.remove(sourceShardId);
        }

        /**
         * Reverses an already-committed split, merging every one of {@code parentShardId}'s children
         * back into a single active shard again -- dynamic-partitioning-plan.md Phase 2 item 2.1's
         * "undo my own split" in-place merge, deliberately scoped to exactly the flat, non-nested case
         * a first increment can support: {@code parentShardId} must itself be an original root shard
         * (not itself a child produced by an earlier split), and every one of its children must still
         * be active and unsplit -- if any child has itself been split further, this shard's children no
         * longer form a simple contiguous partition of the parent's own original range, and reversing
         * that would require the harder, not-yet-designed nested-merge case (see
         * dynamic-partitioning-progress.md's "Phase 2 item 2.1" entry for why that's explicitly out of
         * scope here). Unlike {@link #cancelSplit}, which only ever undoes a still-*in-progress* split
         * (this plugin's own allocation-failure rollback path), this undoes a split that has already
         * committed and been serving traffic.
         *
         * @param parentShardId the root shard whose full, unsplit-further child set should be merged
         *                      back into it.
         * @throws IllegalArgumentException if {@code parentShardId} isn't a root shard, isn't currently
         *                                  split, is still mid-split, or has a child that's itself been
         *                                  split further.
         */
        public void mergeChildrenBackToParent(int parentShardId) {
            if (parentShardId < 0 || parentShardId >= rootShardsToAllChildren.length) {
                throw new IllegalArgumentException(
                    "Shard [" + parentShardId + "] is not an original root shard; nested in-place merge is not supported"
                );
            }
            if (inProgressSplitShardIds.contains(parentShardId)) {
                throw new IllegalArgumentException("Split of shard [" + parentShardId + "] is still in progress");
            }
            // parentToChildShards records exactly the *direct* children this shard's own split
            // produced, unaffected by any later split of one of those children -- rootShardsToAllChildren
            // is the wrong list to validate/remove against here, since a nested split of one direct
            // child grows *this* root's entry with that child's own grandchildren too.
            ShardRange[] children = parentToChildShards.get(parentShardId);
            if (children == null || children.length == 0) {
                throw new IllegalArgumentException("Shard [" + parentShardId + "] has not been split");
            }
            for (ShardRange child : children) {
                if (activeShardIds.contains(child.shardId()) == false || inProgressSplitShardIds.contains(child.shardId())) {
                    throw new IllegalArgumentException(
                        "Child shard [" + child.shardId() + "] of [" + parentShardId + "] has itself been split further (or is "
                            + "mid-split); nested in-place merge is not supported"
                    );
                }
            }
            for (ShardRange child : children) {
                activeShardIds.remove(child.shardId());
            }
            activeShardIds.add(parentShardId);
            rootShardsToAllChildren[parentShardId] = null;
            parentToChildShards.remove(parentShardId);
            // The split that produced these children is being undone; its commit timestamp is no longer
            // meaningful (and the parent id is active-and-unsplit again).
            splitCommitTimestamps.remove(parentShardId);
        }

        public SplitShardsMetadata build() {
            return new SplitShardsMetadata(
                this.rootShardsToAllChildren,
                this.parentToChildShards,
                this.inProgressSplitShardIds,
                this.activeShardIds,
                this.maxShardId,
                this.splitCommitTimestamps
            );
        }
    }

    private static Tuple<Integer, ShardRange> findRootAndShard(int shardId, ShardRange[][] rootShardsToAllChildren) {
        ShardRange[] allChildren;
        for (int rootShardId = 0; rootShardId < rootShardsToAllChildren.length; rootShardId++) {
            allChildren = rootShardsToAllChildren[rootShardId];
            if (allChildren != null) {
                for (ShardRange shardUnderRoot : allChildren) {
                    if (shardUnderRoot.shardId() == shardId) {
                        return new Tuple<>(rootShardId, shardUnderRoot);
                    }
                }
            }
        }

        if (shardId < rootShardsToAllChildren.length && rootShardsToAllChildren[shardId] == null) {
            // We are splitting a root shard in this case.
            return new Tuple<>(shardId, new ShardRange(shardId, Integer.MIN_VALUE, Integer.MAX_VALUE));
        }

        throw new IllegalArgumentException("Shard ID doesn't exist in the current list of shard ranges");
    }

    public Set<Integer> getInProgressSplitShardIds() {
        return inProgressSplitShardIds;
    }

    /**
     * Epoch-millis timestamp at which {@code parentShardId}'s split committed (its children were promoted to
     * active), or {@link #NO_SPLIT_COMMIT_TIMESTAMP} if none is recorded -- either because the shard isn't a
     * committed split parent, or because the split committed on a cluster old enough to predate this field
     * (see {@link Version#V_3_8_0} wire gating). A merge-trigger cool-down gate must treat the sentinel as
     * "no floor" (fail open), not as a commit at epoch 0.
     */
    public long getSplitCommitTimestamp(int parentShardId) {
        return splitCommitTimestamps.getOrDefault(parentShardId, NO_SPLIT_COMMIT_TIMESTAMP);
    }

    public boolean isSplitOfShardInProgress(int shardId) {
        return inProgressSplitShardIds.contains(shardId);
    }

    public boolean isSplitParent(int shardId) {
        return activeShardIds.contains(shardId) == false && parentToChildShards.containsKey(shardId);
    }

    /**
     * Every shard ID that is currently recorded as a split parent -- i.e. every key in the
     * parent-to-children map, covering both still-in-progress splits and already-committed ones.
     * Callers that only want mergeable (committed, unsplit-further) parents should pair this with
     * {@link #canMergeChildrenBackToParent(int)}. Returned as a fresh copy, so mutating it can't
     * disturb this immutable instance.
     */
    public Set<Integer> getSplitParentShardIds() {
        return new HashSet<>(parentToChildShards.keySet());
    }

    /**
     * Non-mutating counterpart to {@link Builder#mergeChildrenBackToParent(int)}: reports whether an
     * in-place merge of {@code parentShardId}'s children back into it would satisfy every split-level
     * precondition that primitive enforces, {@code true} only if it would (so a caller -- e.g. an
     * automatic merge-trigger policy -- can screen candidate parents without provoking the primitive's
     * {@link IllegalArgumentException} just to discover ineligibility). Mirrors that primitive's own
     * checks exactly: {@code parentShardId} must be an original root shard, must currently be split,
     * must not still be mid-split, and none of its direct children may have been split further (or be
     * mid-split themselves). Deliberately does <em>not</em> check per-child routing liveness (started,
     * non-relocating primaries) -- that's a routing-table concern the merge service validates
     * separately, not a {@link SplitShardsMetadata} one.
     */
    public boolean canMergeChildrenBackToParent(int parentShardId) {
        if (parentShardId < 0 || parentShardId >= rootShardsToAllChildren.length) {
            return false;
        }
        if (inProgressSplitShardIds.contains(parentShardId)) {
            return false;
        }
        ShardRange[] children = parentToChildShards.get(parentShardId);
        if (children == null || children.length == 0) {
            return false;
        }
        for (ShardRange child : children) {
            if (activeShardIds.contains(child.shardId()) == false || inProgressSplitShardIds.contains(child.shardId())) {
                return false;
            }
        }
        return true;
    }

    public boolean isRecoveringChild(int shardId, int parentShardId) {
        if (!inProgressSplitShardIds.contains(parentShardId)) {
            return false;
        }

        for (ShardRange childShard : parentToChildShards.get(parentShardId)) {
            if (childShard.shardId() == shardId) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SplitShardsMetadata)) return false;

        SplitShardsMetadata that = (SplitShardsMetadata) o;

        if (maxShardId != that.maxShardId) return false;
        if (!inProgressSplitShardIds.equals(that.inProgressSplitShardIds)) return false;
        if (!Arrays.deepEquals(rootShardsToAllChildren, that.rootShardsToAllChildren)) return false;
        if (!activeShardIds.equals(that.activeShardIds)) return false;
        if (!splitCommitTimestamps.equals(that.splitCommitTimestamps)) return false;
        if (parentToChildShards.size() != that.parentToChildShards.size()) return false;
        for (Integer key : parentToChildShards.keySet()) {
            if (!Arrays.deepEquals(parentToChildShards.get(key), that.parentToChildShards.get(key))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = Arrays.deepHashCode(rootShardsToAllChildren);
        for (Map.Entry<Integer, ShardRange[]> entry : parentToChildShards.entrySet()) {
            result = 31 * result + Objects.hash(entry.getKey(), Arrays.deepHashCode(entry.getValue()));
        }
        result = 31 * result + maxShardId;
        result = 31 * result + inProgressSplitShardIds.hashCode();
        result = 31 * result + activeShardIds.hashCode();
        result = 31 * result + splitCommitTimestamps.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(KEY_NUMBER_OF_ROOT_SHARDS, rootShardsToAllChildren.length);
        builder.field(KEY_MAX_SHARD_ID, maxShardId);
        if (!inProgressSplitShardIds.isEmpty()) {
            builder.field(KEY_IN_PROGRESS_SPLIT_SHARD_IDS, new ArrayList<>(inProgressSplitShardIds));
        }
        builder.field(KEY_ACTIVE_SHARD_IDS, new ArrayList<>(activeShardIds));
        builder.startObject(KEY_ROOT_SHARDS_TO_ALL_CHILDREN);
        for (int rootShardId = 0; rootShardId < rootShardsToAllChildren.length; rootShardId++) {
            ShardRange[] childShards = rootShardsToAllChildren[rootShardId];
            if (childShards != null) {
                builder.startArray(String.valueOf(rootShardId));
                for (ShardRange childShard : childShards) {
                    childShard.toXContent(builder, params);
                }
                builder.endArray();
            }
        }
        builder.endObject();

        builder.startObject(KEY_PARENT_TO_CHILD_SHARDS);
        for (Integer parentShardId : parentToChildShards.keySet()) {
            builder.startArray(String.valueOf(parentShardId));
            for (ShardRange childShard : parentToChildShards.get(parentShardId)) {
                childShard.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();

        if (!splitCommitTimestamps.isEmpty()) {
            builder.startObject(KEY_SPLIT_COMMIT_TIMESTAMPS);
            for (Map.Entry<Integer, Long> entry : splitCommitTimestamps.entrySet()) {
                builder.field(String.valueOf(entry.getKey()), entry.getValue());
            }
            builder.endObject();
        }

        return builder;
    }

    public static SplitShardsMetadata parse(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        int maxShardId = -1;
        Set<Integer> inProgressSplitShardIds = new HashSet<>();
        Set<Integer> activeShardIds = new HashSet<>();
        ShardRange[][] rootShardsToAllChildren = null;
        Map<Integer, ShardRange[]> tempShardIdToChildShards = new HashMap<>();
        Map<Integer, Long> splitCommitTimestamps = new HashMap<>();
        int numberOfRootShards = -1;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (KEY_MAX_SHARD_ID.equals(currentFieldName)) {
                    maxShardId = parser.intValue();
                } else if (KEY_NUMBER_OF_ROOT_SHARDS.equals(currentFieldName)) {
                    numberOfRootShards = parser.intValue();
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (KEY_ROOT_SHARDS_TO_ALL_CHILDREN.equals(currentFieldName)) {
                    Map<Integer, ShardRange[]> rootShards = parseShardsMap(parser);
                    rootShardsToAllChildren = new ShardRange[numberOfRootShards][];
                    for (Map.Entry<Integer, ShardRange[]> entry : rootShards.entrySet()) {
                        rootShardsToAllChildren[entry.getKey()] = entry.getValue();
                    }
                } else if (KEY_PARENT_TO_CHILD_SHARDS.equals(currentFieldName)) {
                    tempShardIdToChildShards = parseShardsMap(parser);
                } else if (KEY_SPLIT_COMMIT_TIMESTAMPS.equals(currentFieldName)) {
                    splitCommitTimestamps = parseSplitCommitTimestamps(parser);
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (KEY_IN_PROGRESS_SPLIT_SHARD_IDS.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        inProgressSplitShardIds.add(parser.intValue());
                    }
                } else if (KEY_ACTIVE_SHARD_IDS.equals(currentFieldName)) {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        activeShardIds.add(parser.intValue());
                    }
                }
            }
        }

        return new SplitShardsMetadata(
            rootShardsToAllChildren,
            tempShardIdToChildShards,
            inProgressSplitShardIds,
            activeShardIds,
            maxShardId,
            splitCommitTimestamps
        );
    }

    private static Map<Integer, Long> parseSplitCommitTimestamps(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        Map<Integer, Long> timestamps = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                assert currentFieldName != null;
                timestamps.put(Integer.parseInt(currentFieldName), parser.longValue());
            }
        }
        return timestamps;
    }

    private static Map<Integer, ShardRange[]> parseShardsMap(XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        Map<Integer, ShardRange[]> shardsMap = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                List<ShardRange> childShardRanges = new ArrayList<>();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    ShardRange shardRange = ShardRange.parse(parser);
                    childShardRanges.add(shardRange);
                }
                assert currentFieldName != null;
                Integer parentShard = Integer.parseInt(currentFieldName);
                shardsMap.put(parentShard, childShardRanges.toArray(new ShardRange[0]));
            }
        }

        return shardsMap;
    }

    public static Diff<SplitShardsMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(SplitShardsMetadata::new, in);
    }

}
