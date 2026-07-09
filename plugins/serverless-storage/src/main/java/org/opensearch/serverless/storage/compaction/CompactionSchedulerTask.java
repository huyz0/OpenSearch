/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Optional;

/**
 * Runs the compaction service on a fixed schedule for one shard -- the piece
 * rfc-serverless-opensearch.md &sect;16 Phase 4.5 flagged as still missing: {@link
 * CompactionPolicy}, {@link CompactionRebaseExecutor}, and {@link LuceneMergeCompactionPublisher}
 * all existed and were correct, but nothing decided *when* to invoke them, for either the
 * "quiescent shard with no active writer" case this service originally targeted, or (since the
 * publish protocol both a writer and this task use was proven safe under real concurrent,
 * continuous contention -- see {@link #maybeCompact(String, int, ShardStateStore, BlobContainerManifestStore,
 * ObjectStoreCommitMaterializer, ObjectStoreCommitPublisher, CompactionPolicy, CompactionRebaseExecutor)}'s own
 * javadoc) the "busy writer accepts an
 * offload handoff" case &sect;16 Phase 4.5 separately called out.
 *
 * <p>Every tick: read the live {@link ShardHead}; skip entirely (no compaction attempt) only if the
 * shard has never been activated or has never published anything. Otherwise read the manifest at
 * the current published generation, derive {@link CompactionPolicy}'s inputs from it via {@link
 * ManifestSegmentMetrics} (no bundle needs to be opened), and run one {@link
 * CompactionRebaseExecutor#publish} attempt if the policy says the shard is a candidate --
 * regardless of whether a writer's lease is currently held, since {@link CompactionPolicy#shouldCompact}'s
 * own thresholds are what actually decide whether this shard's segments are fragmented enough to be
 * worth touching, not lease presence.
 *
 * <p>Per {@link CompactionRebaseExecutor}'s own safety argument, a tick that skips, fails, or loses
 * every rebase attempt is never worse than a no-op: nothing is deleted or corrupted, and the next
 * scheduled tick simply reevaluates from the (possibly by-then-different) live state. A failure here
 * is therefore logged-and-swallowed, never worth failing anything over -- this task has no engine to
 * fail in the first place, since it runs independently of whether any writer is active.
 */
public final class CompactionSchedulerTask implements Closeable {

    private final String indexUuid;
    private final int shardId;
    private final ShardStateStore shardStateStore;
    private final BlobContainerManifestStore manifestStore;
    private final CompactionPolicy policy;
    private final CompactionRebaseExecutor rebaseExecutor;
    private final ObjectStoreCommitMaterializer materializer;
    private final ObjectStoreCommitPublisher commitPublisher;
    private final Scheduler.Cancellable task;

    /**
     * Schedules background compaction for one shard, ticking on the given interval.
     *
     * @param threadPool      thread pool used to schedule the recurring compaction check
     * @param interval        delay between successive compaction ticks
     * @param indexUuid       UUID of the index the shard belongs to
     * @param shardId         id of the shard within the index
     * @param shardStateStore store used to read the shard's live head
     * @param manifestStore   store used to read the manifest at the shard's currently published generation
     * @param materializer    materializes a commit manifest's segments into a real Lucene directory
     * @param commitPublisher publishes a merged commit as a new manifest
     * @param policy          decides whether the shard is a compaction candidate, and how many segments to merge to
     * @param rebaseExecutor  runs the rebase-on-conflict publish attempt once the policy says the shard is a candidate
     */
    public CompactionSchedulerTask(
        ThreadPool threadPool,
        TimeValue interval,
        String indexUuid,
        int shardId,
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ObjectStoreCommitPublisher commitPublisher,
        CompactionPolicy policy,
        CompactionRebaseExecutor rebaseExecutor
    ) {
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.shardStateStore = shardStateStore;
        this.manifestStore = manifestStore;
        this.materializer = materializer;
        this.commitPublisher = commitPublisher;
        this.policy = policy;
        this.rebaseExecutor = rebaseExecutor;
        this.task = threadPool.scheduleWithFixedDelay(this::maybeCompactSafely, interval, ThreadPool.Names.GENERIC);
    }

    private void maybeCompactSafely() {
        try {
            maybeCompact(indexUuid, shardId, shardStateStore, manifestStore, materializer, commitPublisher, policy, rebaseExecutor);
        } catch (IOException e) {
            // See class javadoc: swallow and let the next scheduled tick reevaluate.
        }
    }

    /**
     * The compaction service's actual per-tick decision, factored out as a static method so both
     * this task's own recurring schedule and a one-off on-demand trigger (see {@code
     * org.opensearch.serverless.storage.compaction.action.TransportCompactionTriggerAction}) share
     * exactly one implementation rather than two copies that could drift -- the safety argument in
     * this class's own javadoc (never worse than a no-op) applies identically to either caller.
     *
     * @param indexUuid UUID of the index the shard belongs to
     * @param shardId id of the shard within the index
     * @param shardStateStore store used to read and CAS the shard's live head
     * @param manifestStore store used to read the manifest at the shard's currently published generation
     * @param materializer materializes a commit manifest's segments into a real Lucene directory
     * @param commitPublisher publishes a merged commit as a new manifest
     * @param policy decides whether the shard is a compaction candidate, and how many segments to merge to
     * @param rebaseExecutor runs the rebase-on-conflict publish attempt once the policy says the shard is a candidate
     * @return whether the shard was a compaction candidate and a publish attempt was actually made
     *         (regardless of whether that attempt ultimately succeeded, was abandoned, or exhausted
     *         its retries -- see {@link RebaseResult} for that finer-grained outcome)
     */
    public static boolean maybeCompact(
        String indexUuid,
        int shardId,
        ShardStateStore shardStateStore,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ObjectStoreCommitPublisher commitPublisher,
        CompactionPolicy policy,
        CompactionRebaseExecutor rebaseExecutor
    ) throws IOException {
        Optional<VersionedShardHead> current = shardStateStore.get(indexUuid, shardId);
        if (current.isEmpty()) {
            return false; // never activated -- nothing to compact
        }

        ShardHead head = current.get().head();
        // Deliberately does NOT skip just because a writer's lease is held (rfc-serverless-opensearch.md
        // &sect;16 Phase 4.5's "offload handoffs from busy writers" note): the publish protocol both
        // sides use -- always recomputing the target generation live from the current head inside a
        // CAS-retry loop, never a caller-supplied or locally-cached generation number -- was proven
        // safe under real concurrent, continuous contention before this method's gate was ever
        // relaxed (see CompactionRebaseExecutorTests#testConcurrentRebaseExecutorsNeverLoseAnUpdate
        // and the formally-verified SpecDecoupled/ShardHeadDecoupled.cfg). A lost CAS race here is
        // never worse than wasted work: rebase-and-retry (already tested) or exhaust attempts and
        // walk away leaving the writer's own publish exactly as it was (also already tested). The
        // real gate against pointlessly competing with a healthy, actively-merging writer is
        // CompactionPolicy#shouldCompact's own segment-count/size/delete-ratio thresholds below,
        // which a writer whose own local merges are keeping up naturally stays under -- an
        // explicitly *busy* writer (accumulating small segments faster than its own merges clear
        // them) is exactly the case those thresholds are meant to catch regardless of who holds the
        // lease.
        if (head.latestManifestGeneration() == 0) {
            return false; // ShardHead#initial()'s sentinel -- nothing has ever been published yet
        }

        CommitManifest currentManifest = manifestStore.readManifest(head.primaryTerm(), head.latestManifestGeneration());
        ManifestSegmentMetrics metrics = ManifestSegmentMetrics.from(currentManifest);
        boolean candidate = policy.shouldCompact(
            metrics.segmentCount,
            metrics.totalBytes,
            metrics.largestSingleSegmentBytes,
            metrics.estimatedDeleteRatio
        );
        if (candidate == false) {
            return false;
        }

        LuceneMergeCompactionPublisher publisher = new LuceneMergeCompactionPublisher(
            indexUuid,
            shardId,
            manifestStore,
            materializer,
            commitPublisher,
            policy
        );
        rebaseExecutor.publish(indexUuid, shardId, publisher);
        return true;
    }

    @Override
    public void close() {
        task.cancel();
    }
}
