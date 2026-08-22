/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The inverse of {@code ObjectStoreCommitPublisher}: given a {@link CommitManifest}, fetches
 * every file it references from object storage (checksum-verified, per rfc-serverless-opensearch.md
 * &sect;6.2/&sect;6.3) and writes it into a target Lucene {@link Directory} so a plain Lucene
 * reader can open the resulting commit.
 *
 * <p>This is a full-materialization strategy: every referenced file is fetched in full before the
 * commit becomes readable, rather than serving reads lazily out of the bundle store with a block
 * cache. That is a deliberate, scoped starting point -- correct and immediately useful for reader
 * shards whose segment set is small enough to fetch upfront -- not the eventual lazy/block-cached
 * object-store-native {@code Directory} the RFC's read path targets, which is separate follow-on
 * work.
 *
 * <p><b>Safe to call more than once against the same, already-populated {@code targetDirectory}</b>
 * -- e.g. {@code ObjectStoreReaderEngine}'s own refresh-to-newer-generation polling
 * (rfc-serverless-opensearch.md &sect;16 Phase 3) materializes a second, newer manifest into the
 * same directory a first {@code open()} call already materialized into. Two successive Lucene
 * commits from the same shard routinely reference the *same* unchanged segment files (Lucene only
 * ever writes a new segment for new data; it does not rewrite an untouched one just because a later
 * commit still references it) -- found by a real refresh test throwing {@code
 * FileAlreadyExistsException} on exactly this overlap, not assumed. Every file already present in
 * {@code targetDirectory} with the length its manifest entry expects is skipped (not re-fetched);
 * see {@link #materialize} for why length, not a full re-checksum, is what's actually verified.
 */
public final class ObjectStoreCommitMaterializer {

    private final BundleFileReader bundleStore;

    /**
     * Creates a materializer backed by {@code bundleStore}.
     *
     * @param bundleStore reads and checksum-verifies files out of object storage
     */
    public ObjectStoreCommitMaterializer(BundleFileReader bundleStore) {
        this.bundleStore = bundleStore;
    }

    /**
     * Fetches and writes every file in {@code manifest.files()} not already present (and correctly
     * sized) in {@code targetDirectory} into it, so that {@code Lucene.readSegmentInfos(targetDirectory)}
     * (and hence any plain Lucene {@code DirectoryReader}) can open the commit afterward. Every
     * newly-fetched file's bytes are verified against the checksum recorded in the manifest as
     * they're fetched, so a corrupt or truncated transfer fails loudly here rather than surfacing as
     * a confusing Lucene-level error later.
     *
     * <p><b>An "already present" file is only trusted if its on-disk length matches the manifest's
     * own record of it.</b> This matters because a prior call into this same {@code targetDirectory}
     * can be interrupted partway through the file loop below (a transient object-store error, or the
     * process dying) -- any file from before the interruption point is fully written and {@code
     * close()}d, but the batch {@link Directory#sync} that would make it durable against a crash
     * only ever runs once, after every file in that call's own loop finished. A subsequent call
     * (this method's own retry-safety contract above) must not treat such a file as done purely
     * because {@link Directory#listAll} lists it -- a length mismatch (the truncated-write
     * signature an interrupted transfer actually leaves) means it's deleted and re-fetched instead
     * of silently trusted. This is a length check, not a full re-checksum: re-reading and
     * re-hashing every already-present file on every call (including the common, unchanged-segment
     * case this method's own "safe to call more than once" contract exists for) would reintroduce
     * real local-disk I/O this skip-if-present path is specifically meant to avoid, for a class of
     * corruption (same length, wrong bytes) that {@code IndexOutput}/Lucene's own per-file checksum
     * footer already catches whenever the segment is actually opened for reading.
     *
     * @param manifest the commit manifest whose files should be present in {@code targetDirectory}
     * @param targetDirectory the Lucene directory to write missing files into
     * @throws IOException if fetching or writing a file fails, or a checksum mismatches
     */
    public void materialize(CommitManifest manifest, Directory targetDirectory) throws IOException {
        Set<String> alreadyPresent = new HashSet<>(Arrays.asList(targetDirectory.listAll()));
        Set<String> newlyWritten = new HashSet<>();
        for (Map.Entry<String, FileReference> entry : manifest.files().entrySet()) {
            String fileName = entry.getKey();
            FileReference ref = entry.getValue();
            if (alreadyPresent.contains(fileName)) {
                if (targetDirectory.fileLength(fileName) == ref.length()) {
                    continue;
                }
                // A prior call's own write into this exact file was interrupted before it could be
                // synced -- the on-disk copy is a truncated (or otherwise incomplete) leftover, not
                // a legitimately reusable one. deleteFile before createOutput below: Lucene's
                // createOutput throws FileAlreadyExistsException rather than overwriting.
                targetDirectory.deleteFile(fileName);
            }
            byte[] bytes = bundleStore.readFile(
                ref.bundleName(),
                new BundleFileEntry(fileName, ref.offset(), ref.length(), ref.checksum())
            );
            try (IndexOutput out = targetDirectory.createOutput(fileName, IOContext.DEFAULT)) {
                out.writeBytes(bytes, bytes.length);
            }
            newlyWritten.add(fileName);
        }
        if (newlyWritten.isEmpty() == false) {
            targetDirectory.sync(newlyWritten);
        }
    }
}
