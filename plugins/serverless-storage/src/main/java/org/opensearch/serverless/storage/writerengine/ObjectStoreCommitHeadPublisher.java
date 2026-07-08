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
 */
public final class ObjectStoreCommitHeadPublisher {

    private final ObjectStoreCommitPublisher commitPublisher;
    private final ShardStateStore shardStateStore;

    public ObjectStoreCommitHeadPublisher(ObjectStoreCommitPublisher commitPublisher, ShardStateStore shardStateStore) {
        this.commitPublisher = commitPublisher;
        this.shardStateStore = shardStateStore;
    }

    /**
     * Packages {@code segmentInfos} into a bundle+manifest via {@link ObjectStoreCommitPublisher},
     * then attempts to publish it as the shard's new head under {@code primaryTerm}.
     *
     * @return {@code true} if this generation is now (or already was) reflected in the shard's
     *         published head; {@code false} if a different term currently holds the head, meaning
     *         this writer has been fenced out and must stop writing.
     */
    public boolean publishCommitAsHead(
        Directory directory,
        SegmentInfos segmentInfos,
        String indexUuid,
        int shardId,
        long primaryTerm,
        long generation,
        long maxSeqNo,
        long localCheckpoint,
        WalPosition walPosition,
        long mappingVersion,
        PruningStats pruningStats
    ) throws IOException {
        CommitManifest manifest = commitPublisher.publishCommit(
            directory,
            segmentInfos,
            indexUuid,
            shardId,
            primaryTerm,
            generation,
            maxSeqNo,
            localCheckpoint,
            walPosition,
            mappingVersion,
            pruningStats
        );

        for (;;) {
            Optional<VersionedShardHead> current = shardStateStore.get(indexUuid, shardId);
            if (current.isEmpty()) {
                ShardHead newHead = new ShardHead(primaryTerm, null, 0L, manifest.generation());
                if (shardStateStore.compareAndSet(indexUuid, shardId, Optional.empty(), newHead) == CasResult.SUCCESS) {
                    return true;
                }
                continue;
            }

            VersionedShardHead versioned = current.get();
            ShardHead currentHead = versioned.head();
            if (currentHead.primaryTerm() != primaryTerm) {
                // A different term already holds the head -- this writer has been fenced out and
                // must not publish, regardless of whether that term is higher or (should be
                // impossible under correct lease handling) lower.
                return false;
            }
            if (manifest.generation() < currentHead.latestManifestGeneration()) {
                // Something else has already published a *strictly newer* generation under this
                // same term. The primary-term check above can never catch this: the compaction
                // service (LuceneMergeCompactionPublisher) computes its own next generation as
                // currentHead.latestManifestGeneration() + 1, entirely independent of this
                // writer's local Lucene generation counter -- so a compactor running several
                // cycles while this writer was idle can push latestManifestGeneration ahead of
                // whatever this writer's own next local commit computes. If that happens and this
                // branch is skipped, the caller believes its write succeeded while the actual head
                // content is the compactor's, not this commit's -- a genuine silent-data-loss bug,
                // not a hypothetical. Treat it exactly like being fenced out: this writer's notion
                // of "current state" is stale and it must stop, not keep publishing against
                // assumptions that no longer hold.
                return false;
            }
            if (manifest.generation() == currentHead.latestManifestGeneration()) {
                // Already published at exactly this generation -- the intended case is a retried
                // call after a prior attempt's CAS actually succeeded (see
                // ObjectStoreCommitHeadPublisherTests#testRetriedPublicationOfAnAlreadyPublishedGenerationIsANoOpSuccess),
                // which is genuinely safe: nothing in this class's own call pattern (one publish
                // attempt per local flush, never retried with the same manifest after a failure)
                // can trigger it any other way today. Known, narrower residual risk left
                // unresolved: this equality check cannot distinguish "my own retried publish" from
                // "a different actor coincidentally computed the same generation number for
                // different content," since ShardHead carries no manifest-identity field to check
                // against -- only reachable if a writer's local-Lucene-derived generation and a
                // compactor's currentHead-derived generation land on the exact same integer, which
                // requires the writer's generation numbering to stay entangled with local Lucene
                // state at all, the deeper design issue &sect;16's still-open lease-offload
                // negotiation item is really about.
                return true;
            }

            ShardHead newHead = currentHead.withPublishedGeneration(manifest.generation());
            if (shardStateStore.compareAndSet(indexUuid, shardId, Optional.of(versioned.version()), newHead) == CasResult.SUCCESS) {
                return true;
            }
            // Lost the race (another compaction or the same writer's retry publishing concurrently) -- reread and retry.
        }
    }
}
