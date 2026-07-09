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
 * <p><b>No per-block checksum verification</b>: {@code BlobContainerBundleStore#readFile}
 * verifies a whole logical file's checksum in one shot, but this format has no sub-file checksum
 * to verify an arbitrary block range against. A caller that needs end-to-end integrity for
 * partially-fetched files still gets it for free from the transport layer ({@code BlobContainer}
 * implementations themselves checksum their own transfers) but not from this plugin's own format
 * the way a fully-materialized read does -- a known, documented tradeoff of this first lazy-read
 * slice, not yet closed.
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

    /**
     * &sect;9's target block granularity: {@code 1 << 20} = 1 MiB regions, not
     * {@link AbstractBlockIndexInput.Builder#DEFAULT_BLOCK_SIZE_SHIFT}'s 8 MiB default (tuned for
     * whole snapshot files, much larger than typical individual Lucene per-segment files this
     * plugin's bundles hold) -- a smaller region means less wasted fetch/cache footprint per file
     * for the same working set, at the cost of more distinct cache entries for a large file.
     */
    static final int BLOCK_SIZE_SHIFT = 20;

    public LazyBundleIndexInput(
        String resourceDescription,
        String bundleName,
        String fileName,
        long bundleFileOffset,
        long originalFileSize,
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
        FSDirectory cacheDirectory,
        TransferManager transferManager
    ) {
        super(builder);
        this.bundleName = bundleName;
        this.fileName = fileName;
        this.bundleFileOffset = bundleFileOffset;
        this.originalFileSize = originalFileSize;
        this.cacheDirectory = cacheDirectory;
        this.transferManager = transferManager;
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
        String blockFileName = getBlockFileName(fileName, blockId);
        long blockStart = getBlockStart(blockId);
        long blockLength = getActualBlockSize(blockId, blockSizeShift, originalFileSize);

        BlobFetchRequest blobFetchRequest = BlobFetchRequest.builder()
            .blobParts(List.of(new BlobFetchRequest.BlobPart(bundleName, bundleFileOffset + blockStart, blockLength)))
            .directory(cacheDirectory)
            .fileName(blockFileName)
            .build();
        return transferManager.fetchBlob(blobFetchRequest);
    }

    @Override
    public LazyBundleIndexInput clone() {
        LazyBundleIndexInput clone = buildSlice("clone", 0L, this.length);
        clone.cloneBlock(this);
        return clone;
    }
}
