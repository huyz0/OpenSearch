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
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.util.Collection;

/**
 * Context object containing all the parameters needed to upload segment files.
 */
@ExperimentalApi
public final class UploadContext {
    private final Collection<String> filesToUpload;
    private final Collection<String> activeSegmentFiles;
    private final Directory storeDirectory;
    private final ReplicationCheckpoint checkpoint;
    private final RemoteStoreSegmentStrategy.UploadCallback callback;
    private final boolean isLowPriorityUpload;
    private final CryptoMetadata cryptoMetadata;

    public UploadContext(
        Collection<String> filesToUpload,
        Collection<String> activeSegmentFiles,
        Directory storeDirectory,
        ReplicationCheckpoint checkpoint,
        RemoteStoreSegmentStrategy.UploadCallback callback,
        boolean isLowPriorityUpload,
        CryptoMetadata cryptoMetadata
    ) {
        this.filesToUpload = filesToUpload;
        this.activeSegmentFiles = activeSegmentFiles;
        this.storeDirectory = storeDirectory;
        this.checkpoint = checkpoint;
        this.callback = callback;
        this.isLowPriorityUpload = isLowPriorityUpload;
        this.cryptoMetadata = cryptoMetadata;
    }

    public Collection<String> getFilesToUpload() {
        return filesToUpload;
    }

    public Collection<String> getActiveSegmentFiles() {
        return activeSegmentFiles;
    }

    public Directory getStoreDirectory() {
        return storeDirectory;
    }

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    public RemoteStoreSegmentStrategy.UploadCallback getCallback() {
        return callback;
    }

    public boolean isLowPriorityUpload() {
        return isLowPriorityUpload;
    }

    public CryptoMetadata getCryptoMetadata() {
        return cryptoMetadata;
    }
}
