/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;

import java.io.IOException;
import java.util.Optional;

/**
 * The decision {@link ObjectStoreCommitPublisher} deliberately does not make: whether a just-packaged
 * commit is allowed to become the shard's officially visible head (rfc-serverless-metadata-plane.md
 * &sect;4/&sect;7). This is the term-fencing check that keeps a writer that has been superseded (its
 * lease expired and another node took over) from publishing a generation after the fact.
 *
 * <p>The target generation is <b>not</b> supplied by the caller's local Lucene state. It is always
 * computed live, inside the retry loop, as {@code currentHead.latestManifestGeneration() + 1} --
 * mirroring exactly how the compaction service (LuceneMergeCompactionPublisher) already numbers its
 * own publications. This closes a class of bug formally verified in
 * {@code plugins/serverless-storage/formal/ShardHead.tla} (`PublishDecoupled`/`SpecDecoupled`,
 * `ShardHeadDecoupled.cfg`): with a caller-supplied generation entangled with local Lucene state, a
 * writer resuming after a compactor advanced the head could either be wrongly fenced out (see the
 * git history of this class for the pre-decoupling version) or -- the narrower risk this decoupling
 * additionally closes -- have its own generation number coincidentally collide with a different
 * actor's, which the old caller-supplied-generation design could not distinguish since {@link
 * ShardHead} carries no manifest-identity field. With live computation there is no external
 * generation to collide: every successful publish, by construction, lands at the current head's
 * generation plus exactly one.
 */
public final class ObjectStoreCommitHeadPublisher {

    private final ObjectStoreCommitPublisher commitPublisher;
    private final ShardStateStore shardStateStore;

    public ObjectStoreCommitHeadPublisher(ObjectStoreCommitPublisher commitPublisher, ShardStateStore shardStateStore) {
        this.commitPublisher = commitPublisher;
        this.shardStateStore = shardStateStore;
    }

    /**
     * Packages {@code segmentInfos} into a bundle+manifest via {@link ObjectStoreCommitPublisher} at
     * a generation computed live from the shard's current head, then attempts to publish it as the
     * shard's new head under {@code primaryTerm}. On a lost CAS race (another compaction or another
     * publish attempt by this same writer winning first), the generation is recomputed from the
     * freshly re-read head and the commit is packaged again at the new generation -- so more than
     * one manifest/bundle pair may be written to object storage for a single call under contention.
     * Every manifest not referenced by the head that ultimately wins the CAS is simply unreferenced
     * garbage, eligible for the same GC path as any other orphaned bundle (see {@link
     * ObjectStoreCommitPublisher}'s own class javadoc) -- never a correctness issue, since bundles
     * and manifests are addressed only through a successfully published head.
     *
     * @return {@code true} if this commit's content is now reflected in the shard's published head;
     *         {@code false} if a different term currently holds the head, meaning this writer has
     *         been fenced out and must stop writing.
     */
    public boolean publishCommitAsHead(
        Directory directory,
        SegmentInfos segmentInfos,
        String indexUuid,
        int shardId,
        long primaryTerm,
        long maxSeqNo,
        long localCheckpoint,
        WalPosition walPosition,
        long mappingVersion,
        PruningStats pruningStats
    ) throws IOException {
        for (;;) {
            Optional<VersionedShardHead> current = shardStateStore.get(indexUuid, shardId);

            long currentGeneration;
            ShardHead currentHead;
            Optional<Long> currentVersion;
            if (current.isEmpty()) {
                currentGeneration = 0L;
                currentHead = null;
                currentVersion = Optional.empty();
            } else {
                VersionedShardHead versioned = current.get();
                currentHead = versioned.head();
                if (currentHead.primaryTerm() != primaryTerm) {
                    // A different term already holds the head -- this writer has been fenced out and
                    // must not publish, regardless of whether that term is higher or (should be
                    // impossible under correct lease handling) lower.
                    return false;
                }
                currentGeneration = currentHead.latestManifestGeneration();
                currentVersion = Optional.of(versioned.version());
            }

            // Always the next slot after the *live* head, exactly like the compactor -- never
            // derived from this writer's own local Lucene generation counter, which is what let a
            // stale writer either get wrongly fenced out or (narrower risk) collide with a different
            // actor's generation number under the old design. See this class's own javadoc.
            long targetGeneration = currentGeneration + 1;

            CommitManifest manifest = commitPublisher.publishCommit(
                directory,
                segmentInfos,
                indexUuid,
                shardId,
                primaryTerm,
                targetGeneration,
                maxSeqNo,
                localCheckpoint,
                walPosition,
                mappingVersion,
                pruningStats
            );

            ShardHead newHead = currentHead == null
                ? new ShardHead(primaryTerm, null, 0L, manifest.generation())
                : currentHead.withPublishedGeneration(manifest.generation());
            if (shardStateStore.compareAndSet(indexUuid, shardId, currentVersion, newHead) == CasResult.SUCCESS) {
                return true;
            }
            // Lost the race (another compaction or another publish attempt winning first) -- reread
            // the live head and retry with a freshly computed generation.
        }
    }

    /**
     * The latest durably-published manifest for this shard, if any -- what a writer activating
     * (under a new or resumed term) reads to determine where WAL replay must resume from (the
     * manifest's own {@link WalPosition}) and what Lucene generation local recovery should already
     * reflect. Empty only when this shard has never published a manifest at all (a brand new
     * shard, nothing yet to catch up on beyond the whole WAL from the start).
     */
    public Optional<CommitManifest> readLatestManifest(String indexUuid, int shardId) throws IOException {
        Optional<VersionedShardHead> current = shardStateStore.get(indexUuid, shardId);
        if (current.isEmpty()) {
            return Optional.empty();
        }
        ShardHead head = current.get().head();
        return Optional.of(commitPublisher.readManifest(head.primaryTerm(), head.latestManifestGeneration()));
    }
}
