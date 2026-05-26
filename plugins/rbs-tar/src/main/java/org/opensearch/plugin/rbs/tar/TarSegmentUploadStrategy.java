/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.rbs.tar;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.RemoteStoreSegmentStrategy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A segment upload strategy that packages segment files of a refresh batch into a single tar archive.
 * The first entry of the archive is always "index.bin".
 */
public final class TarSegmentUploadStrategy implements RemoteStoreSegmentStrategy {

    @Override
    public void upload(
        final Collection<String> localSegments,
        final Map<String, Long> localSegmentsSizeMap,
        final Directory storeDirectory,
        final RemoteSegmentStoreDirectory remoteDirectory,
        final ActionListener<Void> listener,
        final UploadCallback callback,
        final boolean isLowPriorityUpload,
        final CryptoMetadata cryptoMetadata
    ) throws IOException {
        if (localSegments.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        // Generate unique tar base name and local temporary name
        final String bundleUuid = UUIDs.randomBase64UUID();
        final String bundleName = "rbs_bundle_" + bundleUuid + ".tar";
        final String remoteTarName = remoteDirectory.getNewRemoteSegmentFilename(bundleName);

        // Pre-calculate file offsets in tar archive
        final int indexBinSize = calculateIndexBinSize(localSegments);
        final int indexBinPadding = (512 - (indexBinSize % 512)) % 512;
        long currentOffset = 512L + indexBinSize + indexBinPadding;

        final Map<String, Long> fileOffsets = new HashMap<>();
        final Map<String, String> fileChecksums = new HashMap<>();
        for (final String file : localSegments) {
            currentOffset += 512L; // skip header for this file
            fileOffsets.put(file, currentOffset);

            final String checksumStr = remoteDirectory.getChecksumOfLocalFile(storeDirectory, file);
            fileChecksums.put(file, checksumStr);

            final long length = localSegmentsSizeMap.get(file);
            final long filePadding = (512L - (length % 512L)) % 512L;
            currentOffset += length + filePadding;
        }

        // Build index.bin in memory
        final ByteArrayOutputStream indexBinStream = new ByteArrayOutputStream(indexBinSize);
        try (DataOutputStream dos = new DataOutputStream(indexBinStream)) {
            dos.write(new byte[] { 'S', 'T', 'R', 'I' });
            dos.writeByte(1);
            dos.writeShort(localSegments.size());
            for (final String file : localSegments) {
                final byte[] nameBytes = file.getBytes(StandardCharsets.UTF_8);
                dos.writeShort(nameBytes.length);
                dos.write(nameBytes);
                dos.writeLong(fileOffsets.get(file));
                dos.writeLong(localSegmentsSizeMap.get(file));
                final long checksum = Long.parseLong(fileChecksums.get(file));
                dos.writeLong(checksum);
            }
        }
        final byte[] indexBinBytes = indexBinStream.toByteArray();

        // Write tar archive to temporary local file
        final IndexOutput indexOutput = storeDirectory.createOutput(bundleName, IOContext.DEFAULT);
        final OutputStream os = new OutputStream() {
            @Override
            public void write(final int b) throws IOException {
                indexOutput.writeByte((byte) b);
            }

            @Override
            public void write(final byte[] b, final int off, final int len) throws IOException {
                indexOutput.writeBytes(b, off, len);
            }

            @Override
            public void close() throws IOException {
                indexOutput.close();
            }
        };

        try (TarOutputStream tos = new TarOutputStream(os)) {
            // Write index.bin as the first entry
            tos.putNextEntry("index.bin", indexBinBytes.length);
            tos.write(indexBinBytes);
            tos.closeEntry();

            // Stream each segment file
            final byte[] buf = new byte[8192];
            for (final String file : localSegments) {
                final long length = localSegmentsSizeMap.get(file);
                tos.putNextEntry(file, length);
                try (IndexInput indexInput = storeDirectory.openInput(file, IOContext.READONCE)) {
                    long remaining = length;
                    while (remaining > 0) {
                        final int toRead = (int) Math.min(buf.length, remaining);
                        indexInput.readBytes(buf, 0, toRead);
                        tos.write(buf, 0, toRead);
                        remaining -= toRead;
                    }
                }
                tos.closeEntry();
            }
        }

        // Notify upload starts
        for (final String file : localSegments) {
            callback.onUploadStart(file);
        }

        final Runnable postUploadRunner = () -> {
            try {
                // Register all uploaded segments
                for (final String file : localSegments) {
                    final long length = localSegmentsSizeMap.get(file);
                    final String checksum = fileChecksums.get(file);
                    final String remotePath = remoteTarName + "#" + fileOffsets.get(file);
                    final RemoteSegmentStoreDirectory.UploadedSegmentMetadata metadata =
                        new RemoteSegmentStoreDirectory.UploadedSegmentMetadata(file, remotePath, checksum, length);
                    remoteDirectory.addUploadedSegment(file, metadata);
                }
            } catch (final Exception e) {
                throw new RuntimeException("Exception in post-upload segment registration", e);
            }
        };

        final ActionListener<Void> wrapListener = ActionListener.wrap(resp -> {
            // Success: notify uploader service
            for (final String file : localSegments) {
                callback.onUploadSuccess(file);
            }
            // Cleanup local temp file
            try {
                storeDirectory.deleteFile(bundleName);
            } catch (final IOException e) {
                // Ignore
            }
            listener.onResponse(null);
        }, ex -> {
            // Failure: notify error
            for (final String file : localSegments) {
                callback.onUploadFailure(file, ex);
            }
            // Cleanup local temp file
            try {
                storeDirectory.deleteFile(bundleName);
            } catch (final IOException e) {
                // Ignore
            }
            listener.onFailure(ex);
        });

        final Directory remoteDataDir = remoteDirectory.getDelegate();
        boolean uploaded = false;
        if (remoteDataDir instanceof RemoteDirectory) {
            uploaded = ((RemoteDirectory) remoteDataDir).copyFrom(
                storeDirectory,
                bundleName,
                remoteTarName,
                IOContext.DEFAULT,
                postUploadRunner,
                wrapListener,
                isLowPriorityUpload,
                cryptoMetadata
            );
        }
        if (uploaded == false) {
            try {
                remoteDataDir.copyFrom(storeDirectory, bundleName, remoteTarName, IOContext.DEFAULT);
                postUploadRunner.run();
                wrapListener.onResponse(null);
            } catch (final Exception e) {
                wrapListener.onFailure(e);
            }
        }
    }

    private int calculateIndexBinSize(final Collection<String> files) {
        int size = 7;
        for (final String file : files) {
            final byte[] nameBytes = file.getBytes(StandardCharsets.UTF_8);
            size += 26 + nameBytes.length;
        }
        return size;
    }
}
