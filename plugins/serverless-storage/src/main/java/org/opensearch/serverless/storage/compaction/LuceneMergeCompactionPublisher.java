/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.compaction;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.Randomness;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

/**
 * The real Lucene merge {@link CompactionPublisher} the compaction service was missing
 * (rfc-serverless-opensearch.md &sect;7.4): materializes the shard's current published commit,
 * folds its segments together via a real {@code IndexWriter} merge (not a re-index -- no document
 * is re-parsed, only segment files are combined), and republishes the merged result as a new
 * commit manifest.
 *
 * <p><b>The merge happens in place, on the materialized source commit, and not into a fresh empty
 * directory.</b> That was not a stylistic choice. Merging into an empty {@code Directory} means the
 * merged commit is always Lucene generation 1 -- {@code segments_1} -- no matter what generation
 * its source was at, and Lucene's segment counter restarts, so successive compactions recycle
 * segment names ({@code _3}, then {@code _0}). A reader materializes every generation into one
 * shared, additive directory and lets Lucene resolve "the commit" as the highest-generation
 * {@code segments_N} present, so a compaction published on top of a reader sitting at
 * {@code segments_3} produced three distinct failures, verified against real Lucene 10.5 rather
 * than inferred: the compacted commit was silently never selected while the reader reported itself
 * caught up; the compacted {@code segments_1} was not even fetched, because it is exactly the same
 * 155 bytes as the writer's own {@code segments_1} and the materializer trusted equal lengths; and
 * after a second compaction the recycled {@code _0.*} names overwrote the writer's originals in
 * place, so the next fresh {@code DirectoryReader.open} -- a node restart, a relocation -- failed
 * the shard with {@code CorruptIndexException} permanently. Opening the {@code IndexWriter} on the
 * materialized source instead makes the merged commit's generation strictly greater than its
 * source's and its segment names disjoint from its source's, because both come from state Lucene
 * persists in the segments file it just read. (The reader side is hardened independently, and has
 * to be, since a writer's own next commit can still choose the same generation number: {@code
 * ObjectStoreCommitMaterializer} now decides "already present" on content rather than length and
 * removes superseded commit pointers, so the manifest -- not whichever generation number happens to
 * be numerically largest -- decides what is open.)
 *
 * <p><b>The merge runs against a real filesystem directory, not {@code ByteBuffersDirectory}.</b>
 * The previous version materialized the whole shard into heap and merged it into a second heap
 * directory: peak resident of roughly twice the shard's size, on a search node's heap, behind no
 * circuit breaker, for a task that is scheduled inside every reader engine as well as the writer's.
 * A 20&nbsp;GB shard was an unconditional OOM of a node that was concurrently serving queries. A
 * temporary directory costs disk instead, is deleted in {@link #close()}, and removes the OOM class
 * outright.
 *
 * <p>The merge target is size-tiered, not always "one segment regardless of size"
 * (rfc-serverless-opensearch.md &sect;16 Phase 4.5's "still open" item): {@link
 * CompactionPolicy#targetSegmentCount} turns the source manifest's total byte size into a target
 * segment count such that each resulting segment stays under the policy's {@code
 * maxTargetBundleSizeBytes}, and {@link IndexWriter#forceMerge(int)} is asked to merge down to
 * that many segments rather than unconditionally down to one. A small shard still ends up as one
 * segment (its target segment count is 1 either way); a shard whose total size already exceeds
 * one target bundle now stays multiple segments after compaction instead of being forced into one
 * oversized segment.
 *
 * <p><b>A compaction that would change nothing abandons rather than republishing.</b> When the
 * source is already at or under the target segment count, {@code forceMerge} does nothing, {@code
 * IndexWriter#commit} finds no pending changes and writes no commit, and the "merged" {@code
 * SegmentInfos} is literally the source's. Publishing that would re-upload the whole shard into a
 * fresh bundle, orphan the previous one, and burn a manifest generation, all to produce a commit
 * identical to the one already published -- and it would do so on every tick, forever, on any shard
 * whose policy keeps calling it a candidate. {@link #computeNewHead} returns
 * {@link Optional#empty()} instead, which {@link CompactionRebaseExecutor} already treats as an
 * abandoned attempt.
 *
 * <p><b>A rebase retry redoes the merge whenever, and only whenever, the source actually changed.</b>
 * {@link CompactionRebaseExecutor} re-reads the live head fresh on every retry, and a CAS usually
 * fails because the head's content genuinely changed underneath it (a writer's publish, or another
 * compactor's merge landing first) -- in which case reusing the previous attempt's merged bytes
 * under a new generation would silently republish stale content and discard whatever the other
 * party just wrote. That is a correctness bug, not a missed optimization, and it is why this class
 * used to redo the entire materialize-merge sequence unconditionally. But the head can also move
 * without the <em>commit</em> moving in any way this merge depends on, and then an unconditional
 * redo means {@code maxAttempts} full-shard downloads and full-shard merges per tick per replica.
 * The distinguishing test is exact and cheap: if the freshly-read source manifest's file map is
 * equal to the one the cached merge was computed from, the merge input is byte-for-byte the same
 * thing and reusing it republishes exactly what a redo would have produced. Anything else -- any
 * added, removed, or rebound file -- discards the cache and merges again.
 *
 * <p><b>A losing rebase attempt's already-uploaded bundle is genuinely orphaned, and this class
 * cannot clean it up itself.</b> {@code publishWithBundleNameCollisionRetry} below uploads the
 * merged bundle <em>before</em> {@link CompactionRebaseExecutor} attempts the head CAS that would
 * make it live -- the same bundle-before-manifest-before-head ordering used everywhere else in
 * this codebase, so a crash never leaves a published manifest pointing at bytes that aren't
 * durably there. If that CAS then loses the race, the bundle this attempt just uploaded is
 * unreferenced by any manifest, but this class's container is deliberately delete-denied
 * (rfc-serverless-opensearch.md &sect;15, see {@code RestrictingBlobContainer}'s own javadoc:
 * "the compaction service needs GET+PUT but no DELETE (deletion stays with GC)") -- reclaiming it
 * is {@code GcSchedulerTask}'s own per-shard bundle sweep's job, not this class's, exactly the same
 * way the bundle-name-collision "stuck leftover" case just above relies on that same sweep rather
 * than deleting the leftover directly. Both are bounded (at most {@code maxAttempts} orphaned
 * bundles per rebase-and-retry cycle, not unbounded), but only actually reclaimed if GC is enabled
 * on this shard -- see {@code GcSchedulerTask}'s own class javadoc for that setting.
 *
 * <p><b>Unsticks a permanently-colliding target generation on its own, without needing delete
 * permission or a manual operator action.</b> {@code ObjectStoreCommitPublisher#publishCommit}'s
 * bundle name is deterministic per {@code (indexUuid, shardId, primaryTerm, generation)}; on a
 * quiescent shard whose head never advances (no writer commit ever moves it past the collision),
 * every future compaction attempt recomputes the exact same target name. If an earlier attempt's
 * bundle upload succeeded but a later step (the manifest write, or the shard-head CAS) faulted
 * before that attempt could complete, {@code writeBundle}'s own collision guard correctly refuses
 * to trust or overwrite the mismatched leftover -- but on its own, that just converts the failure
 * from silent corruption to a loud, permanently-repeating one. This class now catches exactly that
 * collision and retries the upload (not the merge -- the already-merged bytes are reused as-is,
 * since a bundle-name retry has nothing to do with the shard head having changed) under a fresh
 * random suffix via {@code ObjectStoreCommitPublisher}'s {@code bundleNameSuffix} overload, up to
 * {@link #MAX_BUNDLE_NAME_COLLISION_RETRIES} times. This doesn't need compaction's own delete
 * permission (deliberately withheld, see rfc-serverless-opensearch.md &sect;15) because it never
 * touches the stuck leftover at all -- it just stops targeting it.
 */
public final class LuceneMergeCompactionPublisher implements CompactionPublisher, AutoCloseable {

    private static final org.apache.logging.log4j.Logger logger = org.apache.logging.log4j.LogManager.getLogger(
        LuceneMergeCompactionPublisher.class
    );

    /**
     * Bounds the bundle-name-collision retry loop in {@link #computeNewHead}. A fresh random
     * 64-bit suffix colliding with a previous attempt's own random suffix is astronomically
     * unlikely; this bound exists only so a (hypothetical, never-observed) run of bad luck fails
     * loudly rather than looping forever.
     */
    private static final int MAX_BUNDLE_NAME_COLLISION_RETRIES = 5;

    private final String indexUuid;
    private final int shardId;
    private final BlobContainerManifestStore manifestStore;
    private final ObjectStoreCommitMaterializer materializer;
    private final ObjectStoreCommitPublisher commitPublisher;
    private final CompactionPolicy policy;

    /**
     * The temporary directory the current cached merge lives in, or {@code null} when there is
     * none. Deleted and recreated whenever the merge has to be redone, and finally in {@link
     * #close()} -- an instance of this class lives for exactly one {@link
     * CompactionRebaseExecutor#publish} loop, so nothing outlives that.
     */
    /**
     * Where merge scratch lives. Explicit rather than the JVM temp directory because a merge stages a
     * whole shard's worth of segments, and the platform default is routinely a small tmpfs; the node's
     * own data path is sized for shard-sized data by definition. Defaulted for callers that do not care
     * (tests), supplied by the plugin in production.
     */
    private final Path mergeWorkRoot;

    private Path mergeWorkPath;
    private Directory mergeDirectory;
    private SegmentInfos cachedMergedInfos;
    /** The source manifest's file map the cached merge was computed from; see the class javadoc for why equality of this is the reuse test. */
    private Map<String, FileReference> cachedSourceFiles;

    /**
     * Creates a publisher that materializes, merges, and republishes the given shard's current commit.
     *
     * @param indexUuid       UUID of the index the shard belongs to
     * @param shardId         id of the shard within the index
     * @param manifestStore   store used to read the source commit manifest
     * @param materializer    materializes a commit manifest's segments into a real Lucene directory
     * @param commitPublisher publishes the merged result as a new commit manifest
     * @param policy          used to compute the target segment count for the merge
     */
    /**
     * Creates a publisher that stages its merge under an explicit root.
     *
     * @param indexUuid the index
     * @param shardId the shard
     * @param manifestStore the manifest store
     * @param materializer materializes the source commit locally
     * @param commitPublisher publishes the merged commit
     * @param policy decides what to merge into what
     * @param mergeWorkRoot directory to stage the merge under; created if absent
     */
    public LuceneMergeCompactionPublisher(
        String indexUuid,
        int shardId,
        BlobContainerManifestStore manifestStore,
        ObjectStoreCommitMaterializer materializer,
        ObjectStoreCommitPublisher commitPublisher,
        CompactionPolicy policy,
        Path mergeWorkRoot
    ) {
        this.mergeWorkRoot = mergeWorkRoot;
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.manifestStore = manifestStore;
        this.materializer = materializer;
        this.commitPublisher = commitPublisher;
        this.policy = policy;
    }

    @Override
    public Optional<ShardHead> computeNewHead(ShardHead currentHead) {
        try {
            CommitManifest sourceManifest = manifestStore.readManifest(currentHead.primaryTerm(), currentHead.latestManifestGeneration());
            SegmentInfos mergedInfos = mergeOrReuse(sourceManifest);
            if (mergedNothing(mergedInfos, sourceManifest)) {
                // forceMerge had nothing to do -- the source is already at or under the target
                // segment count -- so IndexWriter#commit found no pending changes and wrote no new
                // commit at all, leaving mergedInfos as literally the source commit. Publishing it
                // anyway would re-upload the entire shard into a fresh bundle, orphan the old one,
                // and burn a manifest generation, to produce a commit byte-identical to the one
                // already published. Abandoning is the honest answer, and CompactionRebaseExecutor
                // already has a first-class outcome for it. Found by this class's own test asserting
                // that a second compaction of an already-compacted shard keeps advancing the Lucene
                // generation -- it does not, because there is nothing to advance.
                return Optional.empty();
            }
            long newGeneration = currentHead.latestManifestGeneration() + 1;

            CommitManifest newManifest = publishWithBundleNameCollisionRetry(
                mergeDirectory,
                mergedInfos,
                currentHead.primaryTerm(),
                newGeneration,
                sourceManifest
            );

            return Optional.of(currentHead.withPublishedGeneration(newManifest.generation()));
        } catch (IOException e) {
            throw new UncheckedIOException("compaction merge failed for " + indexUuid + "/" + shardId, e);
        }
    }

    /**
     * Returns the merged commit's {@link SegmentInfos}, redoing the materialize-and-merge only when
     * {@code sourceManifest}'s file map differs from the one the cached merge was built from -- see
     * this class's own javadoc for why exact file-map equality is the right, and the only safe,
     * reuse test.
     */
    private SegmentInfos mergeOrReuse(CommitManifest sourceManifest) throws IOException {
        if (cachedMergedInfos != null && sourceManifest.files().equals(cachedSourceFiles)) {
            return cachedMergedInfos;
        }
        discardCachedMerge();

        long totalBytes = sourceManifest.files().values().stream().mapToLong(FileReference::length).sum();
        int targetSegmentCount = policy.targetSegmentCount(totalBytes);

        // A real filesystem directory, not a heap one -- see this class's own javadoc for what the
        // heap version actually cost. Created under the JVM's temp dir and removed in close().
        // Under an explicit root rather than the JVM temp directory: a merge stages a whole shard's
        // worth of segments, and the JVM default is routinely a small tmpfs that a large shard fills.
        // The root is created if absent, and a stale directory from a process that died mid-merge is
        // removed by the sweep below rather than by deleteOnExit -- which could only ever remove an
        // empty directory, and only at a clean exit, so it never removed a leaked merge at all.
        Files.createDirectories(mergeWorkRoot);
        removeStaleMergeWork();
        mergeWorkPath = Files.createTempDirectory(mergeWorkRoot, "compaction-" + indexUuid + "-" + shardId + "-");
        mergeDirectory = FSDirectory.open(mergeWorkPath);
        materializer.materialize(sourceManifest, mergeDirectory);

        // The soft-deletes field must be configured here (matching the constant every real
        // OpenSearch index actually indexes with, per Lucene.SOFT_DELETES_FIELD) even though this
        // writer never itself soft-deletes anything -- an IndexWriter opened over segments that
        // carry that field throws IllegalArgumentException ("this index has [...] as soft-deletes
        // already but soft-deletes field is not configured in IWC") the moment it touches them. A
        // real, previously latent bug: no existing test exercised compaction against any
        // soft-deleted source content, so this had never been caught before.
        IndexWriterConfig mergeConfig = new IndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        // Opened on the materialized source itself (CREATE_OR_APPEND, the default), so both the
        // commit generation and the segment-name counter continue from what the source commit
        // recorded rather than restarting at 1 and _0. That is the whole point -- see the class
        // javadoc for the three failures the restart-from-empty version produced.
        try (IndexWriter writer = new IndexWriter(mergeDirectory, mergeConfig)) {
            writer.forceMerge(targetSegmentCount);
            writer.commit();
        }
        cachedMergedInfos = SegmentInfos.readLatestCommit(mergeDirectory);
        cachedSourceFiles = sourceManifest.files();
        return cachedMergedInfos;
    }

    /**
     * Whether the "merged" commit is just the source commit again.
     *
     * <p>Compared on the segments file name <em>and</em> the file set, not on either alone: the name
     * alone would miss a merge that produced a same-named commit (it cannot, but relying on that is
     * relying on a Lucene implementation detail), and the file set alone would miss a commit that
     * renamed nothing but was genuinely rewritten. Both being equal means there is nothing to
     * publish that is not already published.
     */
    private static boolean mergedNothing(SegmentInfos mergedInfos, CommitManifest sourceManifest) throws IOException {
        return mergedInfos.getSegmentsFileName().equals(sourceManifest.segmentsFileName())
            && mergedInfos.files(true).equals(sourceManifest.files().keySet());
    }

    private void discardCachedMerge() {
        cachedMergedInfos = null;
        cachedSourceFiles = null;
        IOUtils.closeWhileHandlingException(mergeDirectory);
        mergeDirectory = null;
        if (mergeWorkPath != null) {
            try {
                IOUtils.rm(mergeWorkPath);
            } catch (IOException | RuntimeException removalFailure) {
                // A leftover temp directory wastes disk until the OS or the next restart clears it;
                // never worth failing a compaction attempt over.
                // (No logger on this class by design -- the caller's own tick already logs.)
                assert removalFailure != null;
            }
            mergeWorkPath = null;
        }
    }

    /**
     * Releases the merge scratch directory. An instance of this class is constructed for exactly
     * one {@link CompactionRebaseExecutor#publish} loop and must be closed after it, or the temp
     * directory holding a whole shard's worth of segment files stays on disk until the OS clears it.
     */
    /**
     * Removes scratch left by a process that died mid-merge.
     *
     * <p>Only directories this class names, and only for this shard, so a concurrent compaction of a
     * different shard under the same root is never touched. Best effort: a leftover that survives is
     * wasted disk, and the next compaction of this shard tries again.
     */
    private void removeStaleMergeWork() {
        String prefix = "compaction-" + indexUuid + "-" + shardId + "-";
        try (java.util.stream.Stream<Path> entries = Files.list(mergeWorkRoot)) {
            for (Path entry : entries.toList()) {
                if (entry.getFileName().toString().startsWith(prefix)) {
                    try {
                        IOUtils.rm(entry);
                    } catch (IOException e) {
                        logger.debug("could not remove stale compaction scratch [" + entry + "]", e);
                    }
                }
            }
        } catch (IOException e) {
            logger.debug("could not list compaction scratch under [" + mergeWorkRoot + "]", e);
        }
    }

    @Override
    public void close() {
        discardCachedMerge();
    }

    /**
     * Publishes the already-merged {@code mergedDirectory}/{@code mergedInfos} under the normal
     * deterministic bundle name first; if and only if that fails with {@code writeBundle}'s own
     * documented collision message (a previous attempt's mismatched leftover permanently occupying
     * that name), retries the exact same already-merged bytes under a fresh random suffix instead
     * of redoing the merge -- a bundle-name collision has nothing to do with the shard head having
     * changed, so there's nothing to re-materialize or re-merge. Any other {@link IOException}
     * (a genuine fault, not this specific collision) propagates immediately, unretried, exactly as
     * before this method existed.
     */
    private CommitManifest publishWithBundleNameCollisionRetry(
        Directory mergedDirectory,
        SegmentInfos mergedInfos,
        long primaryTerm,
        long newGeneration,
        CommitManifest sourceManifest
    ) throws IOException {
        String bundleNameSuffix = "";
        for (int attempt = 0; attempt <= MAX_BUNDLE_NAME_COLLISION_RETRIES; attempt++) {
            try {
                // verifyIdempotentContent=false: this class's own javadoc documents that redoing
                // the merge on every retry is not byte-deterministic, so a genuine prior attempt of
                // this class's own can legitimately land at the same generation with different
                // content -- content verification would wrongly reject that as a foreign write. See
                // ObjectStoreCommitPublisher#publishCommit's own verifyIdempotentContent-overload
                // javadoc for the full reasoning.
                return commitPublisher.publishCommit(
                    mergedDirectory,
                    mergedInfos,
                    indexUuid,
                    shardId,
                    primaryTerm,
                    newGeneration,
                    sourceManifest.maxSeqNo(),
                    sourceManifest.localCheckpoint(),
                    sourceManifest.walPosition(),
                    sourceManifest.mappingVersion(),
                    sourceManifest.pruningStats(),
                    bundleNameSuffix,
                    false
                );
            } catch (IOException e) {
                if (attempt == MAX_BUNDLE_NAME_COLLISION_RETRIES || isBundleNameCollision(e) == false) {
                    throw e;
                }
                bundleNameSuffix = "-r" + Long.toHexString(Randomness.get().nextLong());
            }
        }
        // Unreachable: the loop above always either returns or throws.
        throw new IllegalStateException("unreachable");
    }

    /** Walks {@code error}'s cause chain looking for {@code BlobContainerBundleStore#writeBundle}'s own documented collision-safety message. */
    private static boolean isBundleNameCollision(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains("already exists with different real content")) {
                return true;
            }
        }
        return false;
    }
}
