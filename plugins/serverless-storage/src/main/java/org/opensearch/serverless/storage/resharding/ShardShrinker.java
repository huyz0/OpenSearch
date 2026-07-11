/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The inverse of {@link ShardSplitter}: merges several existing shards' current document sets into
 * one brand-new target shard identity (rfc-serverless-opensearch.md &sect;16 Phase 5's "resharding-by-copy
 * (split/shrink ...)" -- the "shrink" half). Unlike split, this is deliberately <em>not</em> a
 * zero-copy operation: split's target shares the pre-split shard's own segment files because it
 * starts as a strict logical subset of one already-coherent Lucene commit, but merging several
 * independent shards' document spaces into a single new shard fundamentally requires a real Lucene
 * merge -- there is no manifest schema trick that lets one commit's {@code segments_N} file
 * describe segments drawn from several unrelated directories without actually combining them. This
 * is the same "no document is re-parsed, only segment files combined" merge {@link
 * org.opensearch.serverless.storage.compaction.LuceneMergeCompactionPublisher} already uses for
 * compaction, just across shards instead of within one.
 *
 * <p><b>No pin needed on any source, unlike {@link ShardSplitter}</b>: {@link ShardSplitter#split}
 * needs a source-generation pin because its target's manifest keeps referencing the source's own
 * bundle files. {@link #shrink} never does that -- {@link IndexWriter#addIndexes(Directory...)}
 * writes entirely fresh segment files into the target's own directory, so the target's published
 * bundle is immediately self-sufficient and never depends on any source shard's bundles staying
 * alive. No source shard is touched, deleted, or otherwise modified by this call.
 *
 * <p><b>Deliberately conservative merged metadata</b>: the sources being merged are independent
 * shard identities with their own, mutually-meaningless sequence-number spaces -- there is no
 * coherent single "the merged max seq no" the way one shard's own commit history has. This takes
 * the maximum {@code maxSeqNo}/{@code mappingVersion} across sources (a safe upper bound, matching
 * how {@code ManifestRetentionPolicy} and friends already treat these fields as monotonic
 * watermarks, never as an exact merged-history reconstruction) and does not attempt to carry
 * forward any source's WAL position at all (there is no coherent one).
 */
public final class ShardShrinker {

    private ShardShrinker() {}

    /** One source shard being merged: its currently materializable manifest, resolved by the caller. */
    public record ShrinkSource(CommitManifest manifest, ObjectStoreCommitMaterializer materializer) {

        /**
         * Creates a shrink source.
         *
         * @param manifest this source's currently published manifest.
         * @param materializer materializes {@code manifest} into a real Lucene directory, already
         *                      resolved to read through any bundle-fallback chain this source itself needs.
         */
        public ShrinkSource {
        }

        /** This source's currently published manifest. */
        @Override
        public CommitManifest manifest() {
            return manifest;
        }

        /** Materializes {@link #manifest()} into a real Lucene directory. */
        @Override
        public ObjectStoreCommitMaterializer materializer() {
            return materializer;
        }
    }

    /**
     * Merges every source's current document set into a brand-new target shard identity.
     *
     * @param sources every shard being merged; must be non-empty.
     * @param targetIndexUuid the brand-new index this shrink creates; must not already have a
     *                        published head, the same put-if-absent refusal {@link
     *                        org.opensearch.serverless.storage.clone.ShardCloner#clone} already gives.
     * @param targetShardId the shard number within {@code targetIndexUuid}.
     * @param targetShardStateStore where the target shard's new head is published.
     * @param targetCommitPublisher publishes the merged result as the target's first commit manifest.
     * @param nowMillis the target manifest's {@code createdAtMillis} -- passed in rather than read
     *                  internally so this class stays trivially deterministic to test.
     * @throws IOException if {@code sources} is empty, materializing any source fails, or the
     *                      target already has a published head.
     */
    public static void shrink(
        List<ShrinkSource> sources,
        String targetIndexUuid,
        int targetShardId,
        ShardStateStore targetShardStateStore,
        ObjectStoreCommitPublisher targetCommitPublisher,
        long nowMillis
    ) throws IOException {
        if (sources.isEmpty()) {
            throw new IOException("shrink requires at least one source shard, got none");
        }

        List<Directory> sourceDirectories = new ArrayList<>(sources.size());
        try {
            long mergedMaxSeqNo = -1;
            long mergedMappingVersion = 0;
            for (ShrinkSource source : sources) {
                Directory sourceDirectory = new ByteBuffersDirectory();
                sourceDirectories.add(sourceDirectory);
                source.materializer().materialize(source.manifest(), sourceDirectory);
                mergedMaxSeqNo = Math.max(mergedMaxSeqNo, source.manifest().maxSeqNo());
                mergedMappingVersion = Math.max(mergedMappingVersion, source.manifest().mappingVersion());
            }

            try (Directory targetDirectory = new ByteBuffersDirectory()) {
                try (IndexWriter writer = new IndexWriter(targetDirectory, new IndexWriterConfig())) {
                    writer.addIndexes(sourceDirectories.toArray(new Directory[0]));
                    writer.commit();
                }
                SegmentInfos mergedInfos = SegmentInfos.readLatestCommit(targetDirectory);

                CommitManifest targetManifest = targetCommitPublisher.publishCommit(
                    targetDirectory,
                    mergedInfos,
                    targetIndexUuid,
                    targetShardId,
                    1,
                    1,
                    mergedMaxSeqNo,
                    mergedMaxSeqNo,
                    null,
                    mergedMappingVersion,
                    PruningStats.empty()
                );

                CasResult result = targetShardStateStore.compareAndSet(
                    targetIndexUuid,
                    targetShardId,
                    Optional.empty(),
                    new ShardHead(1, null, 0L, targetManifest.generation())
                );
                if (result != CasResult.SUCCESS) {
                    throw new IOException(
                        "target shard "
                            + targetIndexUuid
                            + "/"
                            + targetShardId
                            + " already has a published head; refusing to shrink onto it"
                    );
                }
            }
        } finally {
            for (Directory sourceDirectory : sourceDirectories) {
                sourceDirectory.close();
            }
        }
    }
}
