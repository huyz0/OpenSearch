/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * Default implementation of RemoteStoreSegmentStrategy which uploads segments one-by-one.
 */
public final class DefaultRemoteStoreSegmentStrategy implements RemoteStoreSegmentStrategy {

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
        final ActionListener<Collection<Void>> mappedListener = ActionListener.map(listener, resp -> null);
        final GroupedActionListener<Void> batchUploadListener = new GroupedActionListener<>(mappedListener, localSegments.size());

        for (final String localSegment : localSegments) {
            final ActionListener<Void> aggregatedListener = ActionListener.wrap(resp -> {
                callback.onUploadSuccess(localSegment);
                batchUploadListener.onResponse(resp);
            }, ex -> {
                callback.onUploadFailure(localSegment, ex);
                batchUploadListener.onFailure(ex);
            });

            callback.onUploadStart(localSegment);
            remoteDirectory.copyFrom(
                storeDirectory,
                localSegment,
                IOContext.DEFAULT,
                aggregatedListener,
                isLowPriorityUpload,
                cryptoMetadata
            );
        }
    }
}
