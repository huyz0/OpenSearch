/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.format.SegmentBundle;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Turns one local Lucene commit into the object-store-native artifacts a shard needs to be
 * durable independent of local disk (rfc-serverless-opensearch.md &sect;6.2/&sect;6.3): every
 * file the commit's {@link SegmentInfos} references gets packed into a single {@link
 * SegmentBundle} blob, and a {@link CommitManifest} describing where each file landed inside it
 * is built and written alongside it.
 *
 * <p>This is deliberately just the "package this commit" step -- it does not decide whether a
 * generation is safe to publish as the shard's new head (that requires the shard-head CAS in
 * {@link org.opensearch.serverless.storage.shardstate.ShardStateStore}, which needs term-fencing
 * information this class has no reason to know about) and it does not touch the local {@link
 * Directory} at all; the caller decides when to invoke it (e.g. from an {@code InternalEngine}
 * subclass's {@code commitIndexWriter} override, immediately after the local Lucene commit
 * completes) and what to do with the resulting manifest.
 */
public final class ObjectStoreCommitPublisher {

    private final BlobContainerBundleStore bundleStore;
    private final BlobContainerManifestStore manifestStore;

    /**
     * Creates a publisher that packages commits into bundles via {@code bundleStore} and describes
     * them in manifests via {@code manifestStore}.
     *
     * @param bundleStore where packaged segment bundles are uploaded
     * @param manifestStore where commit manifests are written and read back
     */
    public ObjectStoreCommitPublisher(BlobContainerBundleStore bundleStore, BlobContainerManifestStore manifestStore) {
        this.bundleStore = bundleStore;
        this.manifestStore = manifestStore;
    }

    /**
     * Packs every file referenced by {@code segmentInfos} into one bundle, uploads it, builds the
     * corresponding {@link CommitManifest}, writes that too, and returns it. Both writes are
     * atomic-or-absent (via {@code writeBlobAtomic}) but are two separate blobs; a crash between
     * them leaves an orphaned bundle, which is a correctness non-issue (bundles are addressed only
     * through a written manifest, so an unreferenced bundle is simply eligible for GC) rather than
     * a corruption -- never the reverse (a manifest referencing a bundle that failed to write).
     *
     * <p>Idempotent under retry, but only for a genuine retry of the exact same content: a
     * (primaryTerm, generation) pair is meant to uniquely identify one Lucene commit, so if a
     * manifest for it was already published (e.g. the caller timed out waiting for a first call
     * that actually succeeded), this returns that existing manifest unchanged rather than
     * re-uploading and overwriting the bundle -- but only after {@link #requireSameContent}
     * confirms the existing manifest actually describes {@code segmentInfos}' own files, not some
     * unrelated write that happens to occupy the same generation (see that method's own javadoc).
     * One caller, {@code LuceneMergeCompactionPublisher}, needs this content check disabled -- see
     * the {@code verifyIdempotentContent} overload further down for why.
     *
     * @param directory the local Lucene {@link Directory} holding the files referenced by {@code segmentInfos}
     * @param segmentInfos the local Lucene commit to package
     * @param indexUuid the index this shard belongs to
     * @param shardId the shard this commit belongs to
     * @param primaryTerm the primary term this commit is published under
     * @param generation the manifest generation this commit is published at
     * @param maxSeqNo the maximum sequence number covered by this commit
     * @param localCheckpoint the local checkpoint covered by this commit
     * @param walPosition the WAL position this commit's manifest should record
     * @param mappingVersion the mapping version in effect for this commit
     * @param pruningStats pruning statistics to record in the manifest
     * @return the manifest describing the packaged commit
     */
    public CommitManifest publishCommit(
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
        return publishCommit(
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
            pruningStats,
            ""
        );
    }

    /**
     * Same as {@link #publishCommit(Directory, SegmentInfos, String, int, long, long, long, long,
     * WalPosition, long, PruningStats)}, additionally appending {@code bundleNameSuffix}
     * to the otherwise-deterministic {@code (indexUuid, shardId, primaryTerm, generation)} bundle
     * name. Every ordinary caller passes {@code ""} (via the other overloads) and gets the exact
     * same deterministic name as before -- this exists only for {@code
     * org.opensearch.serverless.storage.compaction.LuceneMergeCompactionPublisher}'s own retry path,
     * which needs a fresh, never-yet-written bundle name when the deterministic one is permanently
     * stuck colliding with a previous failed attempt's mismatched content (see that class's own
     * javadoc for why compaction, unlike a writer's retry, can never assume re-publishing the same
     * generation produces byte-identical content). The manifest-existence idempotency check above
     * is keyed on {@code (primaryTerm, generation)} alone, never on {@code bundleNameSuffix}, so this
     * parameter cannot affect whether an already-fully-published generation short-circuits.
     *
     * <p>Delegates with content verification enabled -- see the {@code verifyIdempotentContent}
     * overload's own javadoc for why {@code LuceneMergeCompactionPublisher} specifically needs to
     * call that overload directly with it disabled, instead of this one.
     *
     * @param directory the local Lucene {@link Directory} holding the files referenced by {@code segmentInfos}
     * @param segmentInfos the local Lucene commit to package
     * @param indexUuid the index this shard belongs to
     * @param shardId the shard this commit belongs to
     * @param primaryTerm the primary term this commit is published under
     * @param generation the manifest generation this commit is published at
     * @param maxSeqNo the maximum sequence number covered by this commit
     * @param localCheckpoint the local checkpoint covered by this commit
     * @param walPosition the WAL position this commit's manifest should record
     * @param mappingVersion the mapping version in effect for this commit
     * @param pruningStats pruning statistics to record in the manifest
     * @param bundleNameSuffix appended verbatim to the deterministic bundle name; {@code ""} for the
     *                         normal deterministic name every other caller uses.
     * @return the manifest describing the packaged commit
     */
    public CommitManifest publishCommit(
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
        PruningStats pruningStats,
        String bundleNameSuffix
    ) throws IOException {
        return publishCommit(
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
            pruningStats,
            bundleNameSuffix,
            true
        );
    }

    /**
     * Same as {@link #publishCommit(Directory, SegmentInfos, String, int, long, long, long, long,
     * WalPosition, long, PruningStats, String)}, with {@code verifyIdempotentContent}
     * controlling whether the idempotency short-circuit below trusts an existing manifest by
     * identity ({@code (primaryTerm, generation)} alone) or requires it to also describe the exact
     * same file content this call is about to publish (see {@link #requireSameContent}'s own
     * javadoc for why the latter matters).
     *
     * <p>{@code false} is for {@code LuceneMergeCompactionPublisher} alone, called directly rather
     * than through any other overload here: that class's own javadoc documents, as a deliberate
     * design property rather than an oversight, that redoing its merge on every retry is
     * <em>not</em> byte-deterministic, so two of its own attempts at the exact same {@code
     * (primaryTerm, generation)} can legitimately differ -- content verification would reject
     * compaction's own legitimate self-retry (an already-published, equally valid prior merge
     * attempt of its own) exactly as if it were a genuinely foreign write, defeating the
     * idempotency this short-circuit exists to provide for that caller. Every other caller keeps
     * content verification enabled, since for them a differing existing manifest really does mean
     * a foreign write, not their own retry -- see {@code requireSameContent}'s own javadoc for the
     * danger that protects against.
     *
     * @param verifyIdempotentContent {@code true} (every caller except {@code
     *                                 LuceneMergeCompactionPublisher}) to require an existing
     *                                 manifest to match this call's own content before trusting it
     *                                 as an idempotent retry; {@code false} to trust it by identity
     *                                 alone, matching this method's behavior before that check existed.
     * @return the manifest describing the packaged commit
     */
    public CommitManifest publishCommit(
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
        PruningStats pruningStats,
        String bundleNameSuffix,
        boolean verifyIdempotentContent
    ) throws IOException {
        return publishCommit(
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
            pruningStats,
            bundleNameSuffix,
            verifyIdempotentContent,
            null
        );
    }

    /**
     * Same as the {@code verifyIdempotentContent} overload, plus <b>delta bundling</b>: when {@code
     * deltaBase} is non-{@code null}, only the files of {@code segmentInfos} that {@code deltaBase}
     * does not already describe are read and packed into this publication's bundle; the rest keep
     * their existing {@link FileReference}s, pointing into the bundles they already live in.
     *
     * <p><b>Why this matters.</b> Without it, every publication read {@code segmentInfos.files(true)}
     * -- <em>all</em> files of the commit, not the new ones -- fully into heap, and {@code
     * BundleWriter.write} then allocated a second array holding header + every file concatenated. So
     * each publish was a full-shard rewrite: at a 1 s refresh interval a 10 GiB shard uploaded 10 GiB
     * per second of publication, every retained generation stored another full copy, two full-shard
     * byte arrays sat on the flush thread outside any circuit breaker, and -- worst -- {@code
     * BundleWriter} throws {@code IllegalArgumentException("bundle too large")} at {@code
     * Integer.MAX_VALUE}, so a shard crossing ~2 GiB of committed segments failed on <em>every</em>
     * flush, permanently, with no recovery path. rfc-serverless-opensearch.md &sect;6.2 specifies "the
     * new files of one or more commits"; this is what implements that, and it is also the premise
     * {@code BundleReferenceCounter} already assumes (bundle liveness is a reference count over
     * manifests, so a carried-forward reference is exactly what keeps an older bundle live).
     *
     * <p><b>When a delta base is sound.</b> A file reference may be carried forward only if the file
     * name in this commit and the file name in {@code deltaBase} necessarily denote the same bytes.
     * Within one continuously-running local {@code IndexWriter} that is guaranteed by Lucene's own
     * file naming (the segment counter is monotone and persisted in {@code segments_N}; a name is
     * never reused with different content). Across <em>different</em> Lucene indexes for the same
     * shard -- a compactor's merged commit, a previous writer's index -- it is not: {@code _0.cfs}
     * can be entirely different bytes. Callers must therefore only pass a manifest they themselves
     * published from the very same live {@code IndexWriter} (see {@code
     * ObjectStoreWriterEngine#lastPublishedManifest} and the head check in {@code
     * ObjectStoreCommitHeadPublisher}). Local file length is verified as a cheap extra guard; any
     * mismatch simply drops that file back into the delta rather than trusting the base.
     *
     * @param deltaBase a manifest published by this same caller from this same local index, whose
     *                  file references may be carried forward; {@code null} (every caller other than
     *                  the writer engine's own steady-state publish) packs the full commit exactly as
     *                  before.
     * @return the manifest describing the packaged commit
     */
    public CommitManifest publishCommit(
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
        PruningStats pruningStats,
        String bundleNameSuffix,
        boolean verifyIdempotentContent,
        CommitManifest deltaBase
    ) throws IOException {
        Collection<String> fileNames = segmentInfos.files(true);
        Map<String, FileReference> carriedForward = new LinkedHashMap<>();
        List<BundleFileContent> contents = new java.util.ArrayList<>(fileNames.size());
        for (String fileName : fileNames) {
            FileReference reusable = reusableReference(directory, deltaBase, fileName);
            if (reusable != null) {
                carriedForward.put(fileName, reusable);
            } else {
                contents.add(new BundleFileContent(fileName, readFile(directory, fileName)));
            }
        }

        // The candidate's content, by name, independent of which bundle each file ends up in: a
        // carried-forward file's (length, checksum) is already recorded in the base manifest, and a
        // freshly-read file's is computed from the bytes just read. This is what the idempotency
        // check compares, so delta bundling does not weaken it.
        Map<String, long[]> candidateDigest = new LinkedHashMap<>();
        carriedForward.forEach((name, reference) -> candidateDigest.put(name, new long[] { reference.length(), reference.checksum() }));
        for (BundleFileContent file : contents) {
            candidateDigest.put(file.name(), new long[] { file.content().length, checksum(file.content()) });
        }

        if (manifestStore.manifestExists(primaryTerm, generation)) {
            CommitManifest existing = manifestStore.readManifest(primaryTerm, generation);
            if (verifyIdempotentContent) {
                requireSameContent(primaryTerm, generation, candidateDigest, existing);
            }
            return existing;
        }

        String bundleName = BlobContainerBundleStore.NAME_PREFIX
            + indexUuid
            + "-"
            + shardId
            + "-"
            + primaryTerm
            + "-"
            + generation
            + bundleNameSuffix;

        Map<String, FileReference> files = new LinkedHashMap<>(carriedForward);
        if (contents.isEmpty() == false) {
            SegmentBundle bundle;
            try {
                bundle = bundleStore.writeBundle(bundleName, contents);
            } catch (IOException e) {
                // A bundle name is derived from exactly the same (indexUuid, shardId, primaryTerm,
                // generation) tuple the manifest name is, so a foreign actor occupying this
                // generation collides here first, and reports it as a prose IOException from the
                // format layer. Re-probe the manifest to tell that lost race apart from a genuine
                // store failure, and give the race its own type so the head publisher's retry loop
                // can act on it instead of the engine dying. The probe costs a request only on this
                // rare error path.
                if (manifestStore.manifestExists(primaryTerm, generation)) {
                    throw new ManifestGenerationCollisionException(
                        primaryTerm,
                        generation,
                        "bundle [" + bundleName + "] is already occupied by another actor's content",
                        e
                    );
                }
                throw e;
            }
            bundle.entries()
                .forEach((name, entry) -> files.put(name, new FileReference(bundleName, entry.offset(), entry.length(), entry.checksum())));
        }

        long totalDocCount = segmentInfos.totalMaxDoc();
        long deletedDocCount = 0;
        for (SegmentCommitInfo segmentCommitInfo : segmentInfos.asList()) {
            deletedDocCount += segmentCommitInfo.getDelCount() + segmentCommitInfo.getSoftDelCount();
        }

        CommitManifest manifest = new CommitManifest(
            indexUuid,
            shardId,
            primaryTerm,
            generation,
            segmentInfos.getSegmentsFileName(),
            files,
            maxSeqNo,
            localCheckpoint,
            walPosition,
            mappingVersion,
            pruningStats,
            System.currentTimeMillis(),
            totalDocCount,
            deletedDocCount
        );
        try {
            manifestStore.writeManifest(manifest);
        } catch (IOException e) {
            // writeManifest uses writeBlobAtomic(..., failIfAlreadyExists=true), so a foreign actor
            // that took this generation between the probe above and now surfaces here. Same
            // treatment as the bundle case: distinguish a lost race from a real failure, and give
            // the race its own type. (Also closes the probe's own time-of-check/time-of-use window,
            // which the probe alone never could.)
            if (manifestStore.manifestExists(primaryTerm, generation) == false) {
                throw e;
            }
            CommitManifest existing = manifestStore.readManifest(primaryTerm, generation);
            if (verifyIdempotentContent == false || sameContent(candidateDigest, existing)) {
                return existing;
            }
            throw new ManifestGenerationCollisionException(primaryTerm, generation, "a manifest with different content already exists", e);
        }
        return manifest;
    }

    /**
     * The base manifest's {@link FileReference} for {@code fileName} if it is safe to carry forward
     * unchanged, or {@code null} if this file must be read and bundled fresh. See the delta-bundling
     * overload's javadoc for why a length check is the right cheap guard here and why the caller's
     * choice of {@code deltaBase} is what actually carries the correctness argument.
     */
    private static FileReference reusableReference(Directory directory, CommitManifest deltaBase, String fileName) {
        if (deltaBase == null) {
            return null;
        }
        FileReference reference = deltaBase.files().get(fileName);
        if (reference == null) {
            return null;
        }
        try {
            if (directory.fileLength(fileName) != reference.length()) {
                return null;
            }
        } catch (IOException e) {
            // Can't cheaply confirm it -- fall back to bundling the file, which is always correct.
            return null;
        }
        return reference;
    }

    /**
     * Reads back a previously published manifest by its exact (primaryTerm, generation) key.
     *
     * @param primaryTerm the primary term the manifest was published under
     * @param generation the manifest generation to read
     * @return the manifest published at that (primaryTerm, generation)
     */
    public CommitManifest readManifest(long primaryTerm, long generation) throws IOException {
        return manifestStore.readManifest(primaryTerm, generation);
    }

    /**
     * Deletes a manifest this caller wrote and then failed to install as the shard's head, so it
     * does not linger in the container as a manifest that is discoverable by name but never became
     * anything (rfc-serverless-opensearch.md &sect;6.3 keeps "list manifest names, highest term then
     * generation" as a bootstrap/fallback discovery path, and a plain listing has no way to tell a
     * CAS loser apart from the winner).
     *
     * <p><b>Callers must have already proven the head does not point at this manifest.</b> This
     * method deliberately does not check: the caller re-reads the live head as part of its own retry
     * anyway, so making the check here would cost a second read. See {@code
     * ObjectStoreCommitHeadPublisher}'s use, which only calls this after confirming the head moved
     * somewhere else.
     *
     * @param manifest the manifest to remove.
     */
    void deleteUnreferencedManifest(CommitManifest manifest) throws IOException {
        manifestStore.deleteManifests(List.of(manifest));
    }

    private static byte[] readFile(Directory directory, String fileName) throws IOException {
        try (IndexInput input = directory.openInput(fileName, IOContext.READONCE)) {
            long length = input.length();
            // (int) length was a silent truncation for any single file over 2 GiB: the array came
            // out short, readBytes filled only that much, and the bundle then described a file with
            // wrong content and a checksum computed over the truncated bytes -- corruption that
            // would only be discovered when something tried to open the segment. A single Lucene
            // file over 2 GiB is entirely reachable on a large shard after a big merge. Failing
            // loudly here is not a fix for the underlying cap (that needs the streaming bundle
            // writer described in the B1 plan), but it is the difference between a detectable
            // failure and silent corruption.
            if (length > Integer.MAX_VALUE) {
                throw new IOException(
                    "file ["
                        + fileName
                        + "] is "
                        + length
                        + " bytes, which exceeds the "
                        + Integer.MAX_VALUE
                        + "-byte limit the in-memory bundle format can represent; a streaming bundle writer is required for this shard"
                );
            }
            byte[] bytes = new byte[(int) length];
            input.readBytes(bytes, 0, bytes.length);
            return bytes;
        }
    }

    /**
     * Guards {@link #publishCommit}'s idempotency short-circuit: an existing manifest at {@code
     * (primaryTerm, generation)} is only safe to trust as "this caller's own prior attempt" if it
     * actually describes the same file content {@code candidate} is about to publish. Without this
     * check, any caller that computes the same {@code (primaryTerm, generation)} as a completely
     * unrelated, still-in-flight write elsewhere -- e.g. {@code ShardCloner} or {@code
     * ShardShrinker} writing a brand-new target's very first commit at {@code (1, 1)}, which loses
     * its own shard-head CAS afterward -- would silently receive that foreign manifest back and
     * treat it as "my commit was already published," discarding its real content. {@code
     * ShardCloner}/{@code ShardShrinker}'s own best-effort rollback of that stray manifest cannot be
     * relied on to prevent this by itself: their target containers are deliberately delete-denied
     * (rfc-serverless-opensearch.md &sect;15), so the rollback's delete call fails and is swallowed
     * in the normal, production wiring -- this check is what actually keeps the wrong content from
     * ever becoming durably referenced as this shard's head, independent of whether that best-effort
     * cleanup happens to run.
     *
     * <p>Comparing local, already-in-memory checksums against the existing manifest's {@link
     * FileReference#checksum()} values costs no additional object-store round trip beyond the
     * {@code readManifest} the caller already just did -- {@code candidate} was built from the local
     * {@link Directory} before this method is ever reached.
     *
     * <p>Compares by {@code name -> (length, checksum)} rather than by bundle placement, so it is
     * unaffected by delta bundling: a carried-forward file's length and checksum come from the base
     * manifest, a freshly-read file's from the bytes just read, and either way what is compared is
     * the logical content of the commit -- exactly the property that decides whether an existing
     * manifest is this caller's own prior attempt.
     *
     * @param primaryTerm the primary term being published under, used only for the error message.
     * @param generation the manifest generation being published at, used only for the error message.
     * @param candidateDigest this call's own {@code name -> {length, checksum}} view of the commit.
     * @param existing the manifest already found at {@code (primaryTerm, generation)}.
     * @throws ManifestGenerationCollisionException if {@code existing} does not describe the exact
     *         same set of files (by name, length, and checksum) as this call's own content. Typed,
     *         not a bare {@link IOException}, so {@code ObjectStoreCommitHeadPublisher}'s retry loop
     *         can treat it as the lost race it is instead of letting it fail the engine -- see that
     *         exception's own javadoc.
     */
    private static void requireSameContent(long primaryTerm, long generation, Map<String, long[]> candidateDigest, CommitManifest existing)
        throws IOException {
        if (sameContent(candidateDigest, existing) == false) {
            throw new ManifestGenerationCollisionException(
                primaryTerm,
                generation,
                "refusing to treat an existing manifest with different content as this caller's own idempotent retry "
                    + "(a concurrent writer/compaction publish, or a lost clone/shrink/split attempt, occupies this generation)"
            );
        }
    }

    /** Whether {@code existing} describes exactly the same files, by name, length and checksum, as {@code candidateDigest}. */
    private static boolean sameContent(Map<String, long[]> candidateDigest, CommitManifest existing) {
        Map<String, FileReference> existingFiles = existing.files();
        if (candidateDigest.size() != existingFiles.size()) {
            return false;
        }
        for (Map.Entry<String, long[]> entry : candidateDigest.entrySet()) {
            FileReference reference = existingFiles.get(entry.getKey());
            if (reference == null || reference.length() != entry.getValue()[0] || reference.checksum() != entry.getValue()[1]) {
                return false;
            }
        }
        return true;
    }

    /** Computes the CRC32C checksum of {@code data}, matching {@code BundleWriter}'s own per-file checksum. */
    private static long checksum(byte[] data) {
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(data);
        return crc.getValue();
    }
}
