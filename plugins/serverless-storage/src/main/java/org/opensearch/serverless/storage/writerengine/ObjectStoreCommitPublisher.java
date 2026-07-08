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
     * <p>Idempotent under retry: a (primaryTerm, generation) pair uniquely identifies one Lucene
     * commit, so if a manifest for it was already published (e.g. the caller timed out waiting for
     * a first call that actually succeeded), this returns that existing manifest unchanged rather
     * than re-uploading and overwriting the bundle.
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
        if (manifestStore.manifestExists(primaryTerm, generation)) {
            return manifestStore.readManifest(primaryTerm, generation);
        }

        Collection<String> fileNames = segmentInfos.files(true);
        List<BundleFileContent> contents = new java.util.ArrayList<>(fileNames.size());
        for (String fileName : fileNames) {
            contents.add(new BundleFileContent(fileName, readFile(directory, fileName)));
        }

        String bundleName = "bundle-" + indexUuid + "-" + shardId + "-" + primaryTerm + "-" + generation;
        SegmentBundle bundle = bundleStore.writeBundle(bundleName, contents);

        Map<String, FileReference> files = new LinkedHashMap<>();
        bundle.entries()
            .forEach((name, entry) -> files.put(name, new FileReference(bundleName, entry.offset(), entry.length(), entry.checksum())));

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
            System.currentTimeMillis()
        );
        manifestStore.writeManifest(manifest);
        return manifest;
    }

    /** Reads back a previously published manifest by its exact (primaryTerm, generation) key. */
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
}
