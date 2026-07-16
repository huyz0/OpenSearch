/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.resharding;

import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.engine.Engine;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * The data half of an in-place shard <em>merge</em> (dynamic-partitioning-plan.md Phase 2 item 2.1):
 * folds the current, authoritative document sets of an earlier split's two (in general N) full
 * children back into the single revived parent's local Lucene commit. The reverse of {@link
 * ShardSplitter}, and the sibling-pair-scoped counterpart of {@link ShardShrinker} -- but subtly
 * different from both, for a reason worth stating precisely.
 *
 * <p><b>Why this is a real Lucene merge and not a metadata operation.</b> An early spike assumed a
 * full-sibling-pair merge could revive the parent for free: take one surviving child's own local
 * store and simply drop its {@link InPlaceSplitFilteringDirectoryReader} range filter, since both
 * children were cloned from the same parent bundle. That holds only at the instant a split commits,
 * with zero post-split writes. Once the children serve traffic each becomes an independent writer
 * primary with its own blob container/manifest, and routing sends each new document to exactly one
 * child by hash -- so each child accumulates its own disjoint post-split segments. Picking one child
 * as the survivor and dropping its filter would silently lose the other child's post-split writes.
 * A correct merge must fold <em>both</em> children's current segment sets together.
 *
 * <p><b>Why the union needs no bespoke reconciliation.</b> The one genuinely hard-looking case is
 * post-split deletes/updates of a pre-split document that lives in a shared, immutable base segment:
 * each child records that as its own per-child {@code liveDocs} against the shared segment, so the
 * two children reference the same base segment with divergent delete state that a plain manifest
 * concatenation cannot express. This class sidesteps that entirely by not concatenating manifests at
 * all: it materializes each child, wraps it in {@link InPlaceSplitFilteringDirectoryReader} using
 * that child's own {@link ShardRange}, and folds the <em>filtered</em> readers together via a single
 * {@link IndexWriter#addIndexes(CodecReader...)}. Because hash routing partitions documents into
 * disjoint ranges, each filtered reader yields exactly that child's authoritative slice -- a base
 * document survives only through the one child whose range owns its hash (respecting that child's own
 * deletes via {@code liveDocs}), and every post-split document is in-range in exactly the child that
 * wrote it. The union therefore has no double-counting and no cross-child version conflict to
 * resolve, so a plain {@code addIndexes} of the filtered readers is already correct.
 *
 * <p><b>The soft-deletes field must be configured</b> ({@link Lucene#SOFT_DELETES_FIELD}, the same
 * constant every real OpenSearch index actually indexes with) even though this writer never itself
 * soft-deletes anything: {@code addIndexes} throws {@code IllegalArgumentException} the moment the
 * source segments it copies carry that field and the target's config does not acknowledge it -- the
 * exact latent bug {@link org.opensearch.serverless.storage.compaction.LuceneMergeCompactionPublisher}
 * already had to fix for compaction against soft-deleted content.
 */
public final class InPlaceSiblingMerger {

    private InPlaceSiblingMerger() {}

    /**
     * One child being merged back into the parent: its current published manifest, a read path that
     * can fetch that manifest's bundle files, and the child's own hash range.
     *
     * @param manifest the child's current published commit manifest.
     * @param readPath fetches {@code manifest}'s bundle files; must resolve both the child's own
     *                 post-split bundles and (for a child that never published a post-split commit and
     *                 so still references the parent's cloned-by-reference base bundle) the parent's.
     * @param range the child's own hash range -- the filter that restricts this child's materialized
     *              reader to exactly its authoritative document slice.
     */
    public record MergeChild(CommitManifest manifest, BundleFileReader readPath, ShardRange range) {}

    /**
     * Folds every child's authoritative document slice into {@code targetDirectory} as a single fresh
     * Lucene commit, carrying the commit user data a freshly-revived primary needs ({@link
     * Engine#HISTORY_UUID_KEY}, {@link SequenceNumbers#LOCAL_CHECKPOINT_KEY}/{@link
     * SequenceNumbers#MAX_SEQ_NO}, {@link Engine#MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID}) -- the same
     * keys {@code Store#createEmpty} seeds, so the engine can open the result. The translog UUID is
     * left for the caller to associate afterward (via {@code Store#associateIndexWithNewTranslog}),
     * exactly as {@code WriterEngineFactory#recoverInPlaceSplitLocalStore} does for its own case.
     *
     * <p>The merged {@code maxSeqNo}/{@code localCheckpoint} is the maximum across children -- a safe
     * monotonic watermark, not an exact merged-history reconstruction, matching {@link ShardShrinker}'s
     * own treatment of independent sources' mutually-meaningless sequence-number spaces. A fresh
     * {@code HISTORY_UUID} is minted because the revived parent is a new history, not a continuation of
     * either child's.
     *
     * @param children every child being merged; must be non-empty.
     * @param targetDirectory the (empty) directory to fold the merged commit into -- the revived
     *                        parent's own local store directory in production, a bare directory in tests.
     * @return the merged {@code maxSeqNo} (== the merged local checkpoint), for the caller to bootstrap
     *         the parent's new translog at.
     * @throws IOException if {@code children} is empty, materializing any child fails, or the merge
     *                      write fails.
     */
    public static long merge(List<MergeChild> children, Directory targetDirectory) throws IOException {
        if (children.isEmpty()) {
            throw new IOException("in-place merge requires at least one child, got none");
        }

        List<DirectoryReader> filteredReaders = new ArrayList<>(children.size());
        List<Directory> childDirectories = new ArrayList<>(children.size());
        try {
            List<CodecReader> codecReaders = new ArrayList<>();
            long mergedMaxSeqNo = SequenceNumbers.NO_OPS_PERFORMED;
            for (MergeChild child : children) {
                Directory childDirectory = new ByteBuffersDirectory();
                childDirectories.add(childDirectory);
                new ObjectStoreCommitMaterializer(child.readPath()).materialize(child.manifest(), childDirectory);

                // A plain DirectoryReader.open exposes soft-deleted docs (a post-split update records
                // the superseded old version as soft-deleted, not hard-deleted) as still-live -- the
                // range filter below starts from getLiveDocs() and would wrongly re-admit that stale
                // version. Wrapping in the soft-deletes-aware reader first hard-excludes them, exactly
                // as the engine's own read path does before applying the same range filter.
                DirectoryReader rawReader = new SoftDeletesDirectoryReaderWrapper(
                    DirectoryReader.open(childDirectory),
                    Lucene.SOFT_DELETES_FIELD
                );
                // Filtering to the child's own range makes each child contribute exactly its
                // authoritative slice; the union across disjoint ranges therefore never double-counts
                // a shared base document (see this class's own javadoc for the full argument).
                DirectoryReader filteredReader = new InPlaceSplitFilteringDirectoryReader(rawReader, child.range());
                filteredReaders.add(filteredReader);
                for (LeafReaderContext leafContext : filteredReader.leaves()) {
                    codecReaders.add(SlowCodecReaderWrapper.wrap(leafContext.reader()));
                }
                mergedMaxSeqNo = Math.max(mergedMaxSeqNo, child.manifest().maxSeqNo());
            }

            IndexWriterConfig config = new IndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
                .setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            try (IndexWriter writer = new IndexWriter(targetDirectory, config)) {
                if (codecReaders.isEmpty() == false) {
                    writer.addIndexes(codecReaders.toArray(new CodecReader[0]));
                }
                Map<String, String> userData = new HashMap<>();
                userData.put(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID());
                userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(mergedMaxSeqNo));
                userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(mergedMaxSeqNo));
                userData.put(Engine.MAX_UNSAFE_AUTO_ID_TIMESTAMP_COMMIT_ID, "-1");
                writer.setLiveCommitData(userData.entrySet());
                writer.commit();
            }
            return mergedMaxSeqNo;
        } finally {
            IOUtils.closeWhileHandlingException(filteredReaders);
            IOUtils.closeWhileHandlingException(childDirectories);
        }
    }

    /**
     * A {@link BundleFileReader} that tries each delegate in order, falling through to the next only
     * when a bundle file is not found in the current one. Its use in the merge path: a child that has
     * published at least one post-split commit re-packed every live segment (base + its own) into a
     * self-contained bundle in its own container, so its own container alone resolves everything; a
     * child that never published a post-split commit still references the parent's cloned-by-reference
     * base bundle, which lives in the parent's container -- so the child container is tried first and
     * the parent container serves as fallback.
     */
    public static final class FallbackBundleFileReader implements BundleFileReader {

        private final List<BundleFileReader> delegates;

        /**
         * @param delegates the readers to try in order; earlier entries take precedence.
         */
        public FallbackBundleFileReader(List<BundleFileReader> delegates) {
            this.delegates = List.copyOf(delegates);
        }

        @Override
        public byte[] readFile(String bundleName, org.opensearch.serverless.storage.format.BundleFileEntry entry) throws IOException {
            IOException last = null;
            for (BundleFileReader delegate : delegates) {
                try {
                    return delegate.readFile(bundleName, entry);
                } catch (IOException e) {
                    last = e;
                }
            }
            throw last != null ? last : new IOException("no delegate could read bundle file " + bundleName + "/" + entry.name());
        }
    }
}
