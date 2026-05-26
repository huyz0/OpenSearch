/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.lucene.store.Directory;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Strategy interface for segment uploads to remote store.
 */
@ExperimentalApi
public interface RemoteStoreSegmentStrategy {

    /**
     * Uploads the segment files to the remote segment store.
     *
     * @param remoteDirectory remote segment store directory
     * @param shardId shard ID
     * @param context the upload context containing files, directories, checkpoints, and callbacks
     * @param listener listener to handle upload completion
     * @throws IOException in case of I/O error
     */
    void upload(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId, UploadContext context, UploadListener listener)
        throws IOException;

    /**
     * Uploads segment metadata files to the remote metadata store.
     *
     * @param remoteDirectory remote segment store directory
     * @param shardId shard ID
     * @param context the metadata upload context
     * @throws IOException in case of I/O error
     */
    void uploadMetadata(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId, MetadataUploadContext context) throws IOException;

    /**
     * Deletes stale segments that are no longer referenced by active commits.
     *
     * @param remoteDirectory remote segment store directory
     * @param shardId shard ID
     * @param minCommitsToKeep minimum number of commits to keep
     * @throws IOException in case of I/O error
     */
    void deleteStaleSegments(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId, int minCommitsToKeep) throws IOException;

    /**
     * Deletes a single file from the remote segment store.
     *
     * @param remoteDirectory remote segment store directory
     * @param shardId shard ID
     * @param name the name of the file to delete
     * @throws IOException in case of I/O error
     */
    void deleteFile(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId, String name) throws IOException;

    /**
     * Returns a segment metadata reader for the remote segment store strategy.
     *
     * @param remoteDirectory remote segment store directory
     * @param shardId shard ID
     * @return the segment metadata reader
     */
    MetadataReader getMetadataReader(RemoteSegmentStoreDirectory remoteDirectory, ShardId shardId);

    /**
     * Context object containing all the parameters needed to upload segment files.
     */
    @ExperimentalApi
    public static record UploadContext(Collection<SegmentFile> segmentFiles, Directory storeDirectory, ReplicationCheckpoint checkpoint,
        boolean isLowPriorityUpload, CryptoMetadata cryptoMetadata) {

        /**
         * Represents a segment file within an upload context.
         */
        @ExperimentalApi
        public static record SegmentFile(String name, boolean toUpload) {
        }
    }

    /**
     * Listener interface to notify about upload lifecycle events per segment file as well as overall batch completion status.
     */
    @ExperimentalApi
    public interface UploadListener {
        /**
         * Invoked when the upload of a specific segment file begins.
         *
         * @param file the name of the file being uploaded
         */
        void onUploadStart(String file);

        /**
         * Invoked when the upload of a specific segment file completes successfully.
         *
         * @param file the name of the file uploaded
         */
        void onUploadSuccess(String file);

        /**
         * Invoked when the upload of a specific segment file fails.
         *
         * @param file the name of the file that failed to upload
         * @param ex the exception that occurred
         */
        void onUploadFailure(String file, Exception ex);

        /**
         * Invoked when all segment files in the batch have uploaded successfully.
         */
        void onAllUploadsSuccess();

        /**
         * Invoked when the batch upload fails as a whole.
         *
         * @param ex the exception causing the overall batch failure
         */
        void onAllUploadsFailure(Exception ex);
    }

    /**
     * Context object containing all the parameters needed to upload segment metadata.
     */
    @ExperimentalApi
    public static record MetadataUploadContext(Collection<String> activeSegmentFiles, CatalogSnapshot catalogSnapshot,
        Directory storeDirectory, long translogGeneration, ReplicationCheckpoint checkpoint, String nodeId, CheckedFunction<
            CatalogSnapshot,
            byte[],
            IOException> catalogSnapshotToCommitSerializer) {
    }

    /**
     * Interface encapsulating all remote store segment discovery and metadata reading.
     */
    @ExperimentalApi
    public interface MetadataReader {

        /**
         * Reads the latest segment metadata.
         *
         * @return the parsed segment metadata, or null if not found
         * @throws IOException in case of I/O error
         */
        RemoteSegmentMetadata readMetadata() throws IOException;

        /**
         * Reads the segment metadata for a specific commit.
         *
         * @param primaryTerm primary term
         * @param generation commit generation
         * @return the parsed segment metadata, or null if not found
         * @throws IOException in case of I/O error
         */
        RemoteSegmentMetadata readMetadata(long primaryTerm, long generation) throws IOException;

        /**
         * Reads the segment metadata for a specific timestamp.
         *
         * @param timestamp timestamp
         * @return the parsed segment metadata, or null if not found
         * @throws IOException in case of I/O error
         */
        RemoteSegmentMetadata readMetadata(long timestamp) throws IOException;

        /**
         * Reads the segment metadata for a specific file name.
         *
         * @param filename the physical filename of the metadata file
         * @return the parsed segment metadata, or null if not found
         * @throws IOException in case of I/O error
         */
        RemoteSegmentMetadata readMetadata(String filename) throws IOException;

        /**
         * Reads the latest N metadata files.
         *
         * @param count number of metadata files to fetch
         * @return map from metadata filename to metadata object
         * @throws IOException in case of I/O error
         */
        Map<String, RemoteSegmentMetadata> readLatestNMetadata(int count) throws IOException;

        /**
         * Returns the physical metadata filename representing a specific commit.
         *
         * @param primaryTerm primary term
         * @param generation commit generation
         * @return physical filename of the metadata
         * @throws IOException in case of I/O or file discovery error
         */
        String getMetadataFilename(long primaryTerm, long generation) throws IOException;
    }
}
