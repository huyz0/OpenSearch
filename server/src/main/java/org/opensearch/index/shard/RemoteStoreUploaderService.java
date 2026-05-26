/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.util.UploadListener;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSyncListener;
import org.opensearch.index.store.remote.RemoteStoreSegmentStrategy;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The service essentially acts as a bridge between local segment storage and remote storage,
 * ensuring efficient and reliable segment synchronization while providing comprehensive monitoring and error handling.
 */
public class RemoteStoreUploaderService implements RemoteStoreUploader {

    private final Logger logger;

    private final IndexShard indexShard;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final List<RemoteSyncListener> syncListeners = new ArrayList<>();

    public RemoteStoreUploaderService(IndexShard indexShard, Directory storeDirectory, RemoteSegmentStoreDirectory remoteDirectory) {
        logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.indexShard = indexShard;
        this.storeDirectory = storeDirectory;
        this.remoteDirectory = remoteDirectory;
        // One-time chain walk at construction — register the sync listener from the directory stack
        registerSyncListenersFromDirectory(storeDirectory);
        // Configure strategy supplier on the remote directory
        remoteDirectory.setStrategySupplier(() -> {
            try {
                final Map<String, RemoteStoreSegmentStrategy> segmentStrategies = indexShard.getRemoteStoreSegmentStrategies();
                final RemoteStoreSegmentStrategy baseStrategy;
                if (segmentStrategies != null && indexShard.indexSettings() != null) {
                    final String strategyName = indexShard.indexSettings().getRemoteStoreSegmentStrategy();
                    if ("default".equals(strategyName)) {
                        baseStrategy = new org.opensearch.index.store.remote.DefaultRemoteStoreSegmentStrategy();
                    } else {
                        final RemoteStoreSegmentStrategy matched = segmentStrategies.get(strategyName);
                        baseStrategy = matched != null
                            ? matched
                            : new org.opensearch.index.store.remote.DefaultRemoteStoreSegmentStrategy();
                    }
                } else {
                    baseStrategy = new org.opensearch.index.store.remote.DefaultRemoteStoreSegmentStrategy();
                }
                return baseStrategy.getShardInstance(remoteDirectory, remoteDirectory.getMetadataDirectory(), indexShard.shardId());
            } catch (IOException e) {
                throw new RuntimeException("Failed to initialize remote segment strategy for shard", e);
            }
        });
    }

    /**
     * Registers a listener to be notified after each file is synced to remote.
     *
     * @param listener the listener to register
     */
    public void addSyncListener(RemoteSyncListener listener) {
        if (listener != null) {
            syncListeners.add(listener);
        }
    }

    /**
     * Walks the directory chain once to find and register the first {@link RemoteSyncListener}.
     */
    private void registerSyncListenersFromDirectory(Directory dir) {
        Directory current = dir;
        while (current != null) {
            if (current instanceof RemoteSyncListener) {
                syncListeners.add((RemoteSyncListener) current);
                return;
            }
            if (current instanceof FilterDirectory) {
                current = ((FilterDirectory) current).getDelegate();
            } else {
                break;
            }
        }
    }

    @Override
    public void uploadSegments(
        Collection<String> localSegments,
        Map<String, Long> localSegmentsSizeMap,
        Collection<String> activeFiles,
        ReplicationCheckpoint checkpoint,
        ActionListener<Void> listener,
        Function<Map<String, Long>, UploadListener> uploadListenerFunction,
        boolean isLowPriorityUpload,
        CryptoMetadata cryptoMetadata
    ) {
        if (localSegments.isEmpty()) {
            logger.debug("No new segments to upload in uploadNewSegments");
            listener.onResponse(null);
            return;
        }

        logger.debug("Effective new segments files to upload {}", localSegments);

        final RemoteStoreSegmentStrategy strategy = remoteDirectory.getActiveStrategy();

        final java.util.concurrent.ConcurrentMap<String, UploadListener> statsListeners = new java.util.concurrent.ConcurrentHashMap<>();
        final RemoteStoreSegmentStrategy.UploadCallback callback = new RemoteStoreSegmentStrategy.UploadCallback() {
            @Override
            public void onUploadStart(final String file) {
                final UploadListener statsListener = uploadListenerFunction.apply(localSegmentsSizeMap);
                statsListeners.put(file, statsListener);
                statsListener.beforeUpload(file);
            }

            @Override
            public void onUploadSuccess(final String file) {
                final UploadListener statsListener = statsListeners.remove(file);
                if (statsListener != null) {
                    statsListener.onSuccess(file);
                }
                notifyAfterSyncToRemote(file);
            }

            @Override
            public void onUploadFailure(final String file, final Exception ex) {
                logger.warn(() -> new ParameterizedMessage("Exception: [{}] while uploading segment files", ex), ex);
                if (ex instanceof CorruptIndexException) {
                    indexShard.failShard(ex.getMessage(), ex);
                }
                final UploadListener statsListener = statsListeners.remove(file);
                if (statsListener != null) {
                    statsListener.onFailure(file);
                }
            }
        };

        try {
            strategy.upload(
                new org.opensearch.index.store.remote.UploadContext(
                    localSegments,
                    activeFiles,
                    storeDirectory,
                    checkpoint,
                    callback,
                    isLowPriorityUpload,
                    cryptoMetadata
                ),
                listener
            );
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    private void notifyAfterSyncToRemote(String file) {
        for (RemoteSyncListener listener : syncListeners) {
            listener.afterSyncToRemote(file);
        }
    }
}
