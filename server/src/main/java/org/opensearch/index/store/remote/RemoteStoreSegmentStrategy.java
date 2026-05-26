/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManager;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;

import java.io.IOException;
import java.util.Map;

/**
 * Strategy interface for segment uploads to remote store.
 */
@ExperimentalApi
public interface RemoteStoreSegmentStrategy {

    /**
     * Callback interface to notify about upload lifecycle events per segment file.
     */
    @ExperimentalApi
    interface UploadCallback {
        void onUploadStart(String file);

        void onUploadSuccess(String file);

        void onUploadFailure(String file, Exception ex);
    }

    /**
     * Factory method called at shard startup to obtain a shard-bound instance of the strategy.
     * The returned instance will hold references to the remote directories and ShardId,
     * allowing all other operations to be parameter-free.
     *
     * @param remoteDirectory the remote segment store directory
     * @param remoteMetadataDirectory the remote metadata directory
     * @param shardId the shard ID
     * @return a shard-bound instance of the strategy
     * @throws IOException in case of I/O error
     */
    RemoteStoreSegmentStrategy getShardInstance(
        RemoteSegmentStoreDirectory remoteDirectory,
        RemoteDirectory remoteMetadataDirectory,
        ShardId shardId
    ) throws IOException;

    /**
     * Uploads the segment files to the remote segment store.
     *
     * @param context the upload context containing files, directories, checkpoints, and callbacks
     * @param listener listener to handle upload completion
     * @throws IOException in case of I/O error
     */
    void upload(UploadContext context, ActionListener<Void> listener) throws IOException;

    /**
     * Uploads segment metadata files to the remote metadata store.
     *
     * @param context the metadata upload context
     * @throws IOException in case of I/O error
     */
    void uploadMetadata(MetadataUploadContext context) throws IOException;

    /**
     * Discovers and parses the latest metadata from the remote metadata/data store.
     * Resolves the list of active remote segment files, segment infos, and checkpoint.
     *
     * @return the remote segment metadata
     * @throws IOException in case of I/O error
     */
    RemoteSegmentMetadata init() throws IOException;

    /**
     * Deletes stale segments that are no longer referenced by active commits.
     *
     * @param minCommitsToKeep minimum number of commits to keep
     * @throws IOException in case of I/O error
     */
    void deleteStaleSegments(int minCommitsToKeep) throws IOException;

    /**
     * Deletes a single file from the remote segment store.
     *
     * @param name the name of the file to delete
     * @throws IOException in case of I/O error
     */
    void deleteFile(String name) throws IOException;

    /**
     * Gets the filename of the metadata file representing a specific commit.
     *
     * @param primaryTerm the primary term
     * @param generation the generation
     * @return the metadata filename
     * @throws IOException in case of I/O error
     */
    String getMetadataFileForCommit(long primaryTerm, long generation) throws IOException;

    /**
     * Reads metadata from a specific filename.
     *
     * @param filename the metadata filename
     * @return the remote segment metadata
     * @throws IOException in case of I/O error
     */
    RemoteSegmentMetadata readMetadata(String filename) throws IOException;

    /**
     * Initializes metadata to a specific timestamp.
     *
     * @param timestamp the timestamp
     * @return the remote segment metadata
     * @throws IOException in case of I/O error
     */
    RemoteSegmentMetadata initializeToSpecificTimestamp(long timestamp) throws IOException;

    /**
     * Initializes metadata to a specific commit.
     *
     * @param primaryTerm the primary term
     * @param commitGeneration the commit generation
     * @param acquirerId the acquirer ID
     * @param mdLockManager the metadata lock manager
     * @return the remote segment metadata
     * @throws IOException in case of I/O error
     */
    RemoteSegmentMetadata initializeToSpecificCommit(
        long primaryTerm,
        long commitGeneration,
        String acquirerId,
        RemoteStoreLockManager mdLockManager
    ) throws IOException;

    /**
     * Reads the latest N metadata files.
     *
     * @param count the number of files to read
     * @return map from filename to remote segment metadata
     * @throws IOException in case of I/O error
     */
    Map<String, RemoteSegmentMetadata> readLatestNMetadataFiles(int count) throws IOException;
}
