/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.format;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Persists and retrieves {@link SegmentBundle}s against a real {@link BlobContainer}
 * (rfc-serverless-opensearch.md &sect;6.1/&sect;6.2). This is the seam between the
 * storage-backend-agnostic bundle format and an actual repository (FS, S3, GCS, Azure, ...);
 * the format itself never depends on {@link BlobContainer} so it stays trivially unit-testable
 * in memory, while this class carries the I/O.
 */
public final class BlobContainerBundleStore implements BundleFileReader {

    /** Every bundle this store writes is named with this prefix -- see {@link #writeBundle}'s callers. */
    public static final String NAME_PREFIX = "bundle-";

    private final BlobContainer blobContainer;

    /**
     * Wraps a real backing container.
     *
     * @param blobContainer the shard's blob container to persist and retrieve bundles against.
     */
    public BlobContainerBundleStore(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    /**
     * Packs {@code files} into one bundle and uploads it under {@code bundleName} in a single
     * write. Uses {@link BlobContainer#writeBlobAtomic} rather than plain {@code writeBlob}:
     * bundles must be atomically visible-or-absent, never observable half-written, since readers
     * fetch byte ranges out of them concurrently with no coordination (rfc-serverless-opensearch.md
     * &sect;6.2).
     *
     * <p><b>The upload streams rather than materializing.</b> The bundle's body goes to the
     * container as a sequence of the header followed by each file, so this method never allocates
     * the whole commit a second time and is not bounded by {@code Integer.MAX_VALUE}. What is
     * <em>not</em> here is multipart upload: {@code BlobContainer#writeBlobAtomic} is a single
     * request whatever its length, and the multipart path is a different interface
     * ({@code AsyncMultiStreamBlobContainer}) that the only backend this plugin currently uses
     * ({@code FsBlobContainer}) does not implement -- see &sect;6.2's own status note.
     *
     * <p><b>Idempotent under retry</b>, matching {@code ObjectStoreCommitPublisher#publishCommit}'s
     * own documented contract: {@code bundleName} is fully deterministic per commit ({@code
     * indexUuid}/{@code shardId}/{@code primaryTerm}/{@code generation}), so if it already exists
     * -- e.g. an earlier attempt's bundle upload succeeded but a subsequent step (the manifest
     * write) failed before that attempt could return -- this returns the packed bundle without
     * re-uploading, rather than failing on {@code writeBlobAtomic}'s {@code failIfAlreadyExists}.
     * Caught by a real chaos test (a fault injected between bundle upload and manifest write, then
     * a bare retry) before this existed: the retry hit a raw {@code FileAlreadyExistsException}
     * writing the exact same bundle name a previous, partially-successful attempt had already
     * uploaded.
     *
     * <p><b>That idempotency assumption only holds if a retry always packs byte-identical content
     * under the same name</b> -- true for a plain writer-engine retry (it re-reads the same
     * already-durable local commit), but <b>not</b> true for {@code LuceneMergeCompactionPublisher},
     * whose {@code computeNewHead} redoes a real Lucene merge on every call: two merges of the same
     * logical source produce different bytes (fresh random segment IDs), yet compute the exact same
     * deterministic target {@code bundleName} whenever retried against an unchanged {@code
     * currentHead}. Silently returning the freshly-packed (but not actually re-uploaded) bundle in
     * that case would build a manifest whose checksums/offsets describe content that was never
     * written -- a real corrupted-manifest bug, caught by a sustained multi-operation chaos test
     * before this existed, not by a hand-written failure sequence. This method now compares the
     * existing blob's real per-file entries (read back via {@link #readHeader}) against the
     * freshly-packed ones before trusting the short-circuit -- <b>a length-only check was tried
     * first and found insufficient</b>: Lucene's random segment IDs are fixed-width, so two
     * independent merges of the same logical documents routinely pack to the exact same total
     * length while still differing byte-for-byte, which the same chaos test caught directly. A
     * mismatch throws loudly (never silently returned, never blindly overwritten -- overwriting
     * could also destroy a legitimate concurrent writer's own already-published content) rather
     * than risk corruption. See rfc-serverless-opensearch.md &sect;17's own status note for this
     * section for the larger, deliberately-deferred follow-up: this converts the failure from
     * silent corruption to a loud, detectable one, but does not itself give compaction a way to
     * unstick a permanently-colliding target generation (that needs either delete permission this
     * tier deliberately doesn't have, or a compaction-specific bundle-naming scheme change -- real,
     * separate follow-up work).
     *
     * @param bundleName the name to upload the bundle under.
     * @param files the files to pack into the bundle, in bundle order.
     * @return the packed bundle, with its parsed file-entry map.
     * @throws IOException if the write fails, or if a blob already exists under {@code bundleName}
     *                      whose real entries don't match the content just packed (see above).
     */
    public SegmentBundle writeBundle(String bundleName, List<BundleFileContent> files) throws IOException {
        SegmentBundle bundle = BundleWriter.write(files);
        Map<String, BlobMetadata> existing = blobContainer.listBlobsByPrefix(bundleName);
        BlobMetadata existingMetadata = existing.get(bundleName);
        if (existingMetadata != null) {
            Map<String, BundleFileEntry> existingEntries = readEntriesForIdempotencyCheck(bundleName, bundle.length());
            if (bundle.entries().equals(existingEntries)) {
                return bundle;
            }
            throw new IOException(
                "bundle ["
                    + bundleName
                    + "] already exists with different real content than what was just packed -- "
                    + "refusing to silently trust the mismatch or overwrite it; "
                    + "see BlobContainerBundleStore#writeBundle's own javadoc"
            );
        }
        // Streamed, not materialized: header then each file body in turn, so a publish never
        // allocates a second full copy of the whole commit and never hits the 2 GiB ceiling a
        // byte[] imposes. writeBlobAtomic needs the exact length up front, which the layout knows
        // without holding any of the body. See BundleLayout's own javadoc.
        try (InputStream in = bundle.openStream()) {
            blobContainer.writeBlobAtomic(bundleName, in, bundle.length(), true);
        }
        return bundle;
    }

    /**
     * Reads the entries of an already-present blob at {@code bundleName} for {@link #writeBundle}'s
     * idempotency check, or {@code null} if it cannot be read as a bundle at all.
     *
     * <p><b>The length passed in is the freshly-packed bundle's own plaintext length, deliberately
     * not the length {@code listBlobsByPrefix} reported for the existing blob.</b> When {@code
     * EncryptingBlobContainer} is in the chain -- the configuration this plugin ships for
     * encryption at rest -- the listing reports the <em>ciphertext</em> length (envelope header
     * plus block padding), while every read below goes through the <em>decrypting</em> container
     * and therefore addresses plaintext offsets. Handing the ciphertext length to {@link
     * #readHeader} makes its fallback branch ask for a range that runs past the end of the
     * plaintext, turning a perfectly recoverable retry-after-partial-failure into an opaque
     * out-of-range error. The plaintext length of the bundle just packed is the right expectation:
     * if the existing blob really is the same content (the case this short-circuit exists for), it
     * has exactly that plaintext length.
     *
     * <p>A read failure here is deliberately <em>not</em> propagated: an existing blob that cannot
     * be parsed as a bundle is, for this check's purposes, simply not equal to what was just
     * packed, and the caller's own loud "already exists with different real content" error is a far
     * more useful diagnosis than whatever low-level parse failure produced it -- and it is the
     * message {@code LuceneMergeCompactionPublisher}'s fresh-suffix retry matches on to unstick a
     * permanently-colliding target name.
     *
     * @param bundleName the existing blob to inspect.
     * @param expectedPlaintextLength the plaintext length of the bundle just packed.
     * @return the existing blob's parsed entries, or {@code null} if it is unreadable as a bundle.
     */
    private Map<String, BundleFileEntry> readEntriesForIdempotencyCheck(String bundleName, long expectedPlaintextLength) {
        try {
            return readHeader(bundleName, expectedPlaintextLength).entries();
        } catch (IOException | RuntimeException unreadable) {
            return null;
        }
    }

    /**
     * Fetches and parses just the header of a previously written bundle. Since the header length
     * isn't known up front, this reads a generous prefix (default 64&nbsp;KiB, which comfortably
     * covers realistic segment counts per bundle) and re-fetches with the exact size if the
     * header turns out to be larger than the prefix &mdash; a bundle with a header that large
     * would be a pathological outlier (tens of thousands of files in one bundle).
     *
     * @param bundleName the name of the previously written bundle.
     * @param bundleLength the total length in bytes of the bundle blob.
     * @return the parsed header.
     */
    public BundleHeader readHeader(String bundleName, long bundleLength) throws IOException {
        long prefixLength = Math.min(bundleLength, 64L * 1024);
        byte[] prefix = readRange(bundleName, 0, prefixLength);
        try {
            return BundleReader.parseHeader(prefix);
        } catch (BundleFormatException tooShort) {
            if (prefixLength >= bundleLength) {
                throw tooShort;
            }
            // Header didn't fit in the prefix (or the bundle itself is small enough that the
            // prefix already covered it and parsing genuinely failed) -- retry the header parse
            // against the whole object rather than guessing a bigger prefix size.
            byte[] whole = readRange(bundleName, 0, bundleLength);
            return BundleReader.parseHeader(whole);
        }
    }

    /**
     * Fetches and checksum-verifies exactly the bytes of one file described by {@code entry}.
     *
     * @param bundleName the name of the bundle containing the file.
     * @param entry the file's location and expected checksum within the bundle.
     */
    @Override
    public byte[] readFile(String bundleName, BundleFileEntry entry) throws IOException {
        byte[] raw = readRange(bundleName, entry.offset(), entry.length());
        // extractFile expects the entry's offset to be relative to the start of the array it's
        // given; since we ranged-fetched exactly [offset, offset+length), rebase the entry to 0.
        BundleFileEntry rebased = new BundleFileEntry(entry.name(), 0, entry.length(), entry.checksum());
        return BundleReader.extractFile(raw, rebased);
    }

    /**
     * Every bundle name currently present in this shard's container -- the "all known bundles"
     * input {@link org.opensearch.serverless.storage.gc.BundleReferenceCounter#computeDeletableBundles}
     * needs, listed by the {@link #NAME_PREFIX} every bundle this class writes shares, the same
     * listing idiom {@code BlobContainerManifestStore#listManifests} already uses for manifests.
     *
     * @return every bundle name currently present in this shard's container.
     */
    public Set<String> listBundleNames() throws IOException {
        return blobContainer.listBlobsByPrefix(NAME_PREFIX).keySet();
    }

    /**
     * Deletes exactly the named bundles, ignoring any that are already absent (a retried or
     * partially-completed prior sweep must not fail on that account) -- see {@code
     * BlobContainer#deleteBlobsIgnoringIfNotExists}. Callers are responsible for having already
     * proven these bundles are unreferenced by any retained manifest (rfc-serverless-opensearch.md
     * &sect;6.5); this class has no opinion on that decision, only on how the delete is issued.
     *
     * @param bundleNames the bundle names to delete.
     */
    public void deleteBundles(Collection<String> bundleNames) throws IOException {
        if (bundleNames.isEmpty()) {
            return;
        }
        blobContainer.deleteBlobsIgnoringIfNotExists(List.copyOf(bundleNames));
    }

    private byte[] readRange(String bundleName, long position, long length) throws IOException {
        if (length > Integer.MAX_VALUE) {
            throw new IOException("range too large to read in one call: " + length + " bytes");
        }
        try (InputStream in = openRange(bundleName, position, length)) {
            return in.readAllBytes();
        }
    }

    /**
     * Opens exactly the given byte range of a bundle, unbuffered and with no checksum
     * verification (unlike {@link #readFile}, which verifies a whole logical file's checksum in
     * one shot -- this format has no sub-file checksum, so a caller reading an arbitrary block
     * range, such as {@code org.opensearch.serverless.storage.readerengine.LazyBundleIndexInput},
     * cannot verify per-block). Signature deliberately matches {@code
     * org.opensearch.index.store.remote.utils.TransferManager.StreamReader} exactly, so a method
     * reference to this can be passed directly as one.
     *
     * @param bundleName the name of the bundle to read from.
     * @param position the start offset of the range to read.
     * @param length the length in bytes of the range to read.
     * @return a stream over exactly the requested byte range.
     */
    public InputStream openRange(String bundleName, long position, long length) throws IOException {
        return blobContainer.readBlob(bundleName, position, length);
    }

    /**
     * Streams exactly one file's bytes straight out of the bundle blob, with no intermediate
     * {@code byte[]} at all -- the override {@link BundleFileReader#openFile}'s own javadoc
     * explains the need for: {@link #readFile} above cannot return a file larger than {@code
     * Integer.MAX_VALUE} bytes, and {@link #readRange} refuses one loudly, so without this a
     * shard whose compaction produced a single 2&nbsp;GiB+ stored-fields file can never be
     * materialized or re-compacted again.
     *
     * <p>Unverified by construction, exactly as that interface method documents: the caller
     * computes {@code entry.checksum()} incrementally over the stream. This class's own {@link
     * #readFile} remains the verified, all-at-once path for every caller that can afford it.
     *
     * @param bundleName the name of the bundle containing the file.
     * @param entry the file's location and length within the bundle.
     * @return a stream over exactly this file's bytes.
     */
    @Override
    public InputStream openFile(String bundleName, BundleFileEntry entry) throws IOException {
        return openRange(bundleName, entry.offset(), entry.length());
    }
}
