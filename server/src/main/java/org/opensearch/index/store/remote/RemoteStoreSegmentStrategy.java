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
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;
import java.util.Collection;
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

    void upload(
        Collection<String> localSegments,
        Map<String, Long> localSegmentsSizeMap,
        Directory storeDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        ActionListener<Void> listener,
        UploadCallback callback,
        boolean isLowPriorityUpload,
        CryptoMetadata cryptoMetadata
    ) throws IOException;
}
