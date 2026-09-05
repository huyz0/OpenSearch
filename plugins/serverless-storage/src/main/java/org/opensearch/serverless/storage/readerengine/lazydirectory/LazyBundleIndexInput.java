/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.lazydirectory;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;

import java.io.IOException;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * One logical Lucene file, lazily fetched in fixed-size blocks from a single {@code (bundleName,
 * fileOffsetInBundle)} location instead of being materialized to local disk up front
 * (rfc-serverless-opensearch.md &sect;7.2/&sect;9). Deliberately modeled on core's own {@code
 * OnDemandBlockSnapshotIndexInput} -- same base class, same {@link TransferManager}/{@link
 * org.opensearch.index.store.remote.filecache.FileCache} machinery searchable snapshots already
 * uses for exactly this "lazy block-cached remote file" problem -- but simpler: this plugin's
 * bundle format never splits one logical file across more than one blob (see {@code
 * SegmentBundle}'s own format), so {@link #fetchBlock} only ever issues a single-part {@link
 * BlobFetchRequest}, unlike the snapshot version's multi-part chunking logic.
 *
 * <p><b>The block cache key carries this file's full content identity, and is checked on every
 * read.</b> It used to be {@code "<lucene file name>_block_<n>"} and nothing else -- no bundle, no
 * generation, no checksum -- resolved inside a <em>persistent</em>, shard-path-derived directory,
 * and core's {@code TransferManager} short-circuits on {@code Files.exists} without validating
 * anything it finds there. That was a silent wrong-bytes machine in two independent ways. (i) Name
 * reuse across content: when a compaction republishes a file name -- {@code segments_1}, or a
 * recycled {@code _0.si} -- with different bytes, {@code LazyBundleDirectory#advanceToManifest}
 * rebinds the name, but the next {@code fetchBlock} looked up a key that was already on disk
 * holding the <em>previous</em> file's bytes, and got the old commit back for a file it had just
 * re-resolved. (ii) Restart: {@code close()} left the block files on disk, so on the next open of
 * that shard on that node the {@code FileCache} was empty but the files were present, and every
 * block read served whatever the previous incarnation left behind. Keying on {@code (bundle,
 * offset, length, CRC32C)} makes the key content-addressed, so a key can only ever name one
 * byte-sequence; both vectors close at once, and a cached block correctly survives a generation
 * advance that did <em>not</em> change the file.
 *
 * <p><b>Per-block integrity, as far as this format permits.</b> Every fetched block is
 * length-checked against what the manifest says it must be, so a short or over-long ranged read
 * fails here rather than reaching Lucene. A file that fits in a single block -- which is most of a
 * segment's metadata files, and every {@code segments_N} -- is additionally verified in full
 * against {@link org.opensearch.serverless.storage.manifest.FileReference#checksum()}, at zero
 * extra object-store requests, because for those files the block <em>is</em> the file. What is
 * still not verifiable is an interior block of a multi-block file: this format has no sub-file
 * checksum, and the two ways to get one -- adding a body-aligned block-checksum table to the
 * bundle header (which needs a per-bundle header GET to consult) or putting one checksum per
 * 1&nbsp;MiB of every file into every manifest -- are a real cost/integrity tradeoff
 * (rfc-serverless-opensearch.md &sect;9's own status note). For those blocks, integrity rests on
 * the transport layer's own transfer checksums and on Lucene's per-file CRC footer where the codec
 * checks it, which is now a documented, bounded gap rather than the whole of the story.
 */
public class LazyBundleIndexInput extends AbstractBlockIndexInput {

    private final TransferManager transferManager;
    private final FSDirectory cacheDirectory;
    private final String bundleName;
    private final String fileName;
    /** Where this logical file's bytes begin inside {@link #bundleName}'s blob. */
    private final long bundleFileOffset;
    /** The full (unsliced) length of this logical file -- needed to size the last block correctly. */
    private final long originalFileSize;
    /** This file's CRC32C as recorded in the manifest -- part of the cache key, and verified whole for single-block files. */
    private final long fileChecksum;
    /**
     * The content-addressed cache-key prefix every block of this file is stored under. Computed
     * once per input rather than per {@link #fetchBlock} call, and carried across {@link
     * #buildSlice}/{@link #clone} so a slice can never disagree with its parent about which bytes
     * it is reading.
     */
    private final String cacheKeyPrefix;

    /**
     * &sect;9's target block granularity: {@code 1 << 20} = 1 MiB regions, not
     * {@link AbstractBlockIndexInput.Builder#DEFAULT_BLOCK_SIZE_SHIFT}'s 8 MiB default (tuned for
     * whole snapshot files, much larger than typical individual Lucene per-segment files this
     * plugin's bundles hold) -- a smaller region means less wasted fetch/cache footprint per file
     * for the same working set, at the cost of more distinct cache entries for a large file.
     */
    static final int BLOCK_SIZE_SHIFT = 20;

    /**
     * Creates a top-level (non-slice) input covering the whole logical file.
     *
     * @param resourceDescription human-readable description used in error messages
     * @param bundleName the blob this logical file's bytes live inside
     * @param fileName the logical Lucene file name
     * @param bundleFileOffset where this file's bytes begin inside {@code bundleName}'s blob
     * @param originalFileSize the full (unsliced) length of this logical file
     * @param fileChecksum this file's CRC32C as recorded in the manifest -- part of the block cache
     *                     key, so a cache entry can only ever name one byte-sequence
     * @param cacheDirectory local on-disk directory {@link TransferManager} writes fetched blocks into
     * @param transferManager fetches and caches blocks on demand
     */
    public LazyBundleIndexInput(
        String resourceDescription,
        String bundleName,
        String fileName,
        long bundleFileOffset,
        long originalFileSize,
        long fileChecksum,
        FSDirectory cacheDirectory,
        TransferManager transferManager
    ) {
        this(
            AbstractBlockIndexInput.builder()
                .resourceDescription(resourceDescription)
                .isClone(false)
                .offset(0L)
                .length(originalFileSize)
                .blockSizeShift(BLOCK_SIZE_SHIFT),
            bundleName,
            fileName,
            bundleFileOffset,
            originalFileSize,
            fileChecksum,
            cacheDirectory,
            transferManager
        );
    }

    private LazyBundleIndexInput(
        AbstractBlockIndexInput.Builder<?> builder,
        String bundleName,
        String fileName,
        long bundleFileOffset,
        long originalFileSize,
        long fileChecksum,
        FSDirectory cacheDirectory,
        TransferManager transferManager
    ) {
        super(builder);
        this.bundleName = bundleName;
        this.fileName = fileName;
        this.bundleFileOffset = bundleFileOffset;
        this.originalFileSize = originalFileSize;
        this.fileChecksum = fileChecksum;
        this.cacheDirectory = cacheDirectory;
        this.transferManager = transferManager;
        this.cacheKeyPrefix = cacheKeyPrefix(bundleName, fileName, bundleFileOffset, originalFileSize, fileChecksum);
    }

    /**
     * The content identity every block of this file is cached under.
     *
     * <p>Deliberately not the raw bundle name concatenated in: bundle names are long (they embed
     * index UUID, shard, primary term, generation and sometimes a random retry suffix), and the
     * result becomes a real filename on a real filesystem with a 255-byte component limit. A
     * 64-bit hash of the bundle name is folded in instead, alongside the three fields that already
     * pin content exactly -- offset, length, and the manifest's CRC32C. Two different
     * byte-sequences colliding would need the same offset, the same length, the same CRC32C
     * <em>and</em> a bundle-name hash collision; the previous key needed only the same Lucene file
     * name, which compaction produces routinely.
     *
     * <p>The Lucene file name is kept at the front purely so a human (or {@code
     * AbstractBlockIndexInput#getOriginalFileName}) can still tell at a glance what a cache file
     * belongs to.
     */
    private static String cacheKeyPrefix(String bundleName, String fileName, long offset, long length, long checksum) {
        StringBuilder key = new StringBuilder(fileName.length() + 48);
        for (int i = 0; i < fileName.length(); i++) {
            char c = fileName.charAt(i);
            key.append(Character.isLetterOrDigit(c) || c == '.' || c == '-' || c == '_' ? c : '_');
        }
        key.append("__b").append(Long.toHexString(bundleName.hashCode() & 0xFFFFFFFFL));
        key.append('_').append(Long.toHexString(offset));
        key.append('_').append(Long.toHexString(length));
        key.append('_').append(Long.toHexString(checksum));
        return key.toString();
    }

    @Override
    protected LazyBundleIndexInput buildSlice(String sliceDescription, long offset, long length) {
        LazyBundleIndexInput slice = new LazyBundleIndexInput(
            AbstractBlockIndexInput.builder()
                .blockSizeShift(blockSizeShift)
                .isClone(true)
                .offset(this.offset + offset)
                .length(length)
                .resourceDescription(sliceDescription),
            bundleName,
            fileName,
            bundleFileOffset,
            originalFileSize,
            fileChecksum,
            cacheDirectory,
            transferManager
        );
        if (onClone != null) {
            slice.setOnClone(onClone);
            onClone.accept(slice);
        }
        return slice;
    }

    @Override
    protected IndexInput fetchBlock(int blockId) throws IOException {
        // Content-addressed, NOT getBlockFileName(fileName, blockId) -- see this class's own
        // javadoc for the two ways the bare-file-name key served wrong bytes.
        String blockFileName = getBlockFileName(cacheKeyPrefix, blockId);
        long blockStart = getBlockStart(blockId);
        long blockLength = getActualBlockSize(blockId, blockSizeShift, originalFileSize);

        BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
            .blobParts(List.of(new BlobFetchRequest.BlobPart(bundleName, bundleFileOffset + blockStart, blockLength)))
            .directory(cacheDirectory)
            .fileName(blockFileName)
            .build();
        IndexInput block = transferManager.fetchBlob(blobFetchRequest);
        boolean verified = false;
        try {
            verifyBlock(block, blockId, blockLength);
            verified = true;
            return block;
        } finally {
            if (verified == false) {
                // A block that failed verification must not be handed back, and must not leak its
                // reference count either -- the caller never sees it, so it can never close it.
                org.opensearch.common.util.io.IOUtils.closeWhileHandlingException(block);
            }
        }
    }

    /**
     * Checks that what came back is actually this file's block, and not whatever happened to be
     * sitting at that path.
     *
     * <p>The length check is unconditional and cheap: {@code TransferManager} serves any file
     * already present at the request path without validating it against the request, so a
     * truncated fetch, a partially-written block left by a crash, or -- before the key became
     * content-addressed -- a different file entirely, all arrive here looking like a normal block.
     *
     * <p>The full CRC32C check runs only when this file fits in a single block, which is the case
     * for essentially every per-segment metadata file and for every {@code segments_N} -- exactly
     * the files whose corruption is most damaging, since a wrong {@code segments_N} silently
     * selects a different commit. For those files the block <em>is</em> the file, so the manifest's
     * own recorded checksum applies directly and costs nothing extra to check. Interior blocks of a
     * large file have no checksum in this format to check against; see the class javadoc.
     */
    private void verifyBlock(IndexInput block, int blockId, long expectedBlockLength) throws IOException {
        if (block.length() != expectedBlockLength) {
            throw new IOException(
                "block "
                    + blockId
                    + " of file '"
                    + fileName
                    + "' in bundle '"
                    + bundleName
                    + "' is "
                    + block.length()
                    + " bytes, but the manifest says it must be "
                    + expectedBlockLength
                    + " -- refusing to serve it"
            );
        }
        boolean fileFitsInOneBlock = blockId == 0 && expectedBlockLength == originalFileSize;
        if (fileFitsInOneBlock == false) {
            return;
        }
        IndexInput verifier = block.clone();
        verifier.seek(0L);
        CRC32C crc = new CRC32C();
        byte[] buffer = new byte[(int) Math.min(64L * 1024, Math.max(1L, expectedBlockLength))];
        long remaining = expectedBlockLength;
        while (remaining > 0) {
            int chunk = (int) Math.min(buffer.length, remaining);
            verifier.readBytes(buffer, 0, chunk);
            crc.update(buffer, 0, chunk);
            remaining -= chunk;
        }
        if (crc.getValue() != fileChecksum) {
            throw new IOException(
                "checksum mismatch for file '"
                    + fileName
                    + "' in bundle '"
                    + bundleName
                    + "': manifest says "
                    + fileChecksum
                    + " but the fetched bytes hash to "
                    + crc.getValue()
                    + " -- refusing to serve them"
            );
        }
    }

    @Override
    public LazyBundleIndexInput clone() {
        LazyBundleIndexInput clone = buildSlice("clone", 0L, this.length);
        clone.cloneBlock(this);
        return clone;
    }
}
