/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.migration;

import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

import java.io.IOException;
import java.util.Optional;

/**
 * Adopts an already-locally-recovered index's current Lucene commit into serverless storage
 * (rfc-serverless-opensearch.md &sect;16 Phase 6: "Conversion of existing remote-store indices to
 * the bundle format... conversion is manifest synthesis plus optional re-bundling, not data
 * re-upload") -- deliberately indifferent to <em>how</em> that commit's files became locally
 * present. By the time a shard is open and recoverable at all, its store directory holds an
 * ordinary valid Lucene commit whether that recovery path was classic peer/translog recovery, a
 * core remote-store restore, or a snapshot-mount import: none of that history is visible to, or
 * needed by, this class. This makes both migration directions &sect;16 Phase 6 names ("classic to
 * bundle format" and "snapshot-mount import") the exact same mechanical operation from here on:
 * package whatever is already local via {@link ObjectStoreCommitPublisher#publishCommit} (a single
 * bundle upload plus a single manifest write -- no Lucene re-indexing, no re-reading the source
 * repository's own on-disk format) and CAS it in as the shard's first-ever serverless-storage head.
 *
 * <p><b>What this deliberately does not do</b>: resolve a real, currently-open {@code IndexShard}'s
 * local {@code Store} from a shard id alone (needs new {@code IndicesService} wiring this plugin
 * has never used, to actually reach a live shard from a transport action), or read a classic
 * repository's/remote-store's own on-disk metadata format directly (an alternative that could avoid
 * requiring the shard be locally recovered first, at the cost of depending on those formats'
 * internals). Both are real future work; this class is the packaging mechanism either eventual
 * caller would use once it has a {@link Directory}/{@link SegmentInfos} pair in hand -- see the
 * class-level status note in the RFC's own Phase 6 section for the fuller picture.
 */
public final class ClassicIndexMigrator {

    private ClassicIndexMigrator() {}

    /**
     * Packages {@code directory}'s current commit ({@code segmentInfos}) and CASes it in as
     * {@code (indexUuid, shardId)}'s first-ever serverless-storage manifest, at generation 1 under
     * {@code primaryTerm}.
     *
     * @param directory the already-locally-recovered Lucene directory holding the commit to adopt
     * @param segmentInfos the local commit to package -- {@code directory}'s current committed state
     * @param indexUuid the UUID of the index this shard belongs to
     * @param shardId the shard number within {@code indexUuid}
     * @param primaryTerm the primary term to publish the adopted manifest under
     * @param maxSeqNo the maximum sequence number covered by this commit
     * @param localCheckpoint the local checkpoint covered by this commit
     * @param commitPublisher packages and uploads the bundle plus manifest
     * @param shardStateStore CASed to make the adopted manifest this shard's serverless-storage head
     * @return the manifest describing the newly adopted commit
     * @throws IllegalStateException if {@code (indexUuid, shardId)} already has a serverless-storage
     *         head -- migration only ever applies once, to a shard with none; a shard already
     *         active under serverless storage has nothing to adopt
     * @throws IOException if packaging/uploading the commit, or the head CAS itself, fails
     */
    public static CommitManifest migrate(
        Directory directory,
        SegmentInfos segmentInfos,
        String indexUuid,
        int shardId,
        long primaryTerm,
        long maxSeqNo,
        long localCheckpoint,
        ObjectStoreCommitPublisher commitPublisher,
        ShardStateStore shardStateStore
    ) throws IOException {
        Optional<VersionedShardHead> existingHead = shardStateStore.get(indexUuid, shardId);
        if (existingHead.isPresent()) {
            throw new IllegalStateException(
                "shard ("
                    + indexUuid
                    + "/"
                    + shardId
                    + ") already has a serverless-storage head at generation "
                    + existingHead.get().head().latestManifestGeneration()
                    + " -- migration only applies once, to a shard with no existing head"
            );
        }

        long generation = 1L;
        CommitManifest manifest = commitPublisher.publishCommit(
            directory,
            segmentInfos,
            indexUuid,
            shardId,
            primaryTerm,
            generation,
            maxSeqNo,
            localCheckpoint,
            null,
            0,
            PruningStats.empty()
        );

        ShardHead head = new ShardHead(primaryTerm, null, 0L, generation);
        CasResult result = shardStateStore.compareAndSet(indexUuid, shardId, Optional.empty(), head);
        if (result != CasResult.SUCCESS) {
            throw new IllegalStateException(
                "lost the race activating the migrated shard head for ("
                    + indexUuid
                    + "/"
                    + shardId
                    + ") -- another writer must have activated concurrently"
            );
        }
        return manifest;
    }
}
