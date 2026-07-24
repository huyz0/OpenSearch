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
            false
        );
    }

    /**
     * Same as {@link #publishCommit(Directory, SegmentInfos, String, int, long, long, long, long,
     * WalPosition, long, PruningStats)}, additionally marking the published manifest {@link
     * CommitManifest#quiescent()} -- a writer's deliberate final commit before scale-to-zero
     * suspension (rfc-serverless-opensearch.md &sect;7.3), used by {@code
     * ObjectStoreWriterEngine#flushAndPublishQuiescent}.
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
     * @param quiescent whether to mark the published manifest as this writer's final commit before suspension.
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
        boolean quiescent
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
            quiescent,
            ""
        );
    }

    /**
     * Same as {@link #publishCommit(Directory, SegmentInfos, String, int, long, long, long, long,
     * WalPosition, long, PruningStats, boolean)}, additionally appending {@code bundleNameSuffix}
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
     * @param quiescent whether to mark the published manifest as this writer's final commit before suspension.
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
        boolean quiescent,
        String bundleNameSuffix
    ) throws IOException {
        Collection<String> fileNames = segmentInfos.files(true);
        List<BundleFileContent> contents = new java.util.ArrayList<>(fileNames.size());
        for (String fileName : fileNames) {
            contents.add(new BundleFileContent(fileName, readFile(directory, fileName)));
        }

        if (manifestStore.manifestExists(primaryTerm, generation)) {
            CommitManifest existing = manifestStore.readManifest(primaryTerm, generation);
            requireSameContent(primaryTerm, generation, contents, existing);
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
        SegmentBundle bundle = bundleStore.writeBundle(bundleName, contents);

        Map<String, FileReference> files = new LinkedHashMap<>();
        bundle.entries()
            .forEach((name, entry) -> files.put(name, new FileReference(bundleName, entry.offset(), entry.length(), entry.checksum())));

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
            quiescent,
            totalDocCount,
            deletedDocCount
        );
        manifestStore.writeManifest(manifest);
        return manifest;
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

    private static byte[] readFile(Directory directory, String fileName) throws IOException {
        try (IndexInput input = directory.openInput(fileName, IOContext.READONCE)) {
            byte[] bytes = new byte[(int) input.length()];
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
     * @param primaryTerm the primary term being published under, used only for the error message.
     * @param generation the manifest generation being published at, used only for the error message.
     * @param candidate this call's own file content, as about to be bundled.
     * @param existing the manifest already found at {@code (primaryTerm, generation)}.
     * @throws IOException if {@code existing} does not describe the exact same set of files (by name,
     *                      length, and checksum) as {@code candidate}.
     */
    private static void requireSameContent(long primaryTerm, long generation, List<BundleFileContent> candidate, CommitManifest existing)
        throws IOException {
        Map<String, FileReference> existingFiles = existing.files();
        boolean matches = candidate.size() == existingFiles.size();
        if (matches) {
            for (BundleFileContent file : candidate) {
                FileReference reference = existingFiles.get(file.name());
                if (reference == null || reference.length() != file.content().length || reference.checksum() != checksum(file.content())) {
                    matches = false;
                    break;
                }
            }
        }
        if (matches == false) {
            throw new IOException(
                "manifest already exists at (primaryTerm="
                    + primaryTerm
                    + ", generation="
                    + generation
                    + ") with different content -- refusing to treat it as this caller's own idempotent retry "
                    + "(likely a foreign write, e.g. a lost clone/shrink/split attempt, occupying this generation)"
            );
        }
    }

    /** Computes the CRC32C checksum of {@code data}, matching {@code BundleWriter}'s own per-file checksum. */
    private static long checksum(byte[] data) {
        java.util.zip.CRC32C crc = new java.util.zip.CRC32C();
        crc.update(data);
        return crc.getValue();
    }
}
