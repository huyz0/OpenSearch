/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.engine;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.DefaultPrimaryOperationPolicy;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.DocumentMetadataResolver;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.InternalTranslogFactory;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.indices.IndexingMemoryController;
import org.opensearch.storage.utils.DirectoryUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Isolates a disposable native preparation engine's resources from a serving engine. This only
 * builds configuration: the host owns engine admission, verified rebuild, term/schema fencing,
 * buffer flushing, SSD watermarks and cleanup. A Lucene indexing-buffer limit is not a total heap cap.
 */
@InternalApi
public final class PrivatePreparationEngineConfig {
    private PrivatePreparationEngineConfig() {}

    /**
     * Builds a private configuration without registering another index service or public shard.
     * The supplied store and translog directory must already exist on a separately owned local path.
     * @param serving public configuration supplying mapper, analyzer and codec context
     * @param privateStore separately owned filesystem store for the same shard identity
     * @param privateTranslogDirectory separately owned local translog directory
     * @param primaryTerm fixed positive term for this fenced preparation context
     * @param indexingBufferBytes maximum initial Lucene indexing buffer, charged to the host's engine budget
     * @param persistedCommittedCheckpoint private checkpoint supplier; never advance beyond private persisted committed data
     * @param privateFailures private-context failure listener, independent of the serving shard
     * @return configuration for an ordinary native primary engine
     * @throws IOException if physical paths overlap or cannot be resolved
     */
    public static EngineConfig create(
        EngineConfig serving,
        Store privateStore,
        Path privateTranslogDirectory,
        long primaryTerm,
        long indexingBufferBytes,
        LongSupplier persistedCommittedCheckpoint,
        Engine.EventListener privateFailures
    ) throws IOException {
        Objects.requireNonNull(serving);
        Objects.requireNonNull(privateStore);
        Objects.requireNonNull(persistedCommittedCheckpoint);
        Objects.requireNonNull(privateFailures);
        if (primaryTerm <= 0 || indexingBufferBytes <= 0 || indexingBufferBytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("invalid private engine term or indexing buffer budget");
        }
        if (privateStore == serving.getStore() || privateStore.shardId().equals(serving.getShardId()) == false) {
            throw new IllegalArgumentException("preparation requires a separate store for the same shard identity");
        }
        if (privateFailures == serving.getEventListener()) {
            throw new IllegalArgumentException("preparation cannot reuse the serving failure listener");
        }
        if (serving.getCommitterFactory() != null
            || serving.getDocumentLookupProvider() != null
            || (serving.getDocumentMetadataResolver() != null && serving.getDocumentMetadataResolver() != DocumentMetadataResolver.NOOP)
            || (serving.getDataFormatRegistry() != null && serving.getDataFormatRegistry().getRegisteredFormats().isEmpty() == false)) {
            throw new IllegalArgumentException("private native preparation does not support custom data formats or document lookup");
        }
        final Path publicIndex = DirectoryUtils.unwrapFSDirectory(serving.getStore().directory()).getDirectory().toRealPath();
        final Path publicTranslog = serving.getTranslogConfig().getTranslogPath().toRealPath();
        final Path privateIndex = DirectoryUtils.unwrapFSDirectory(privateStore.directory()).getDirectory().toRealPath();
        final Path privateTranslog = Objects.requireNonNull(privateTranslogDirectory).toRealPath();
        if (Files.isDirectory(publicTranslog) == false || Files.isDirectory(privateTranslog) == false) {
            throw new IOException("preparation and serving translog paths must be directories");
        }
        requireSeparate(privateIndex, publicIndex);
        requireSeparate(privateIndex, publicTranslog);
        requireSeparate(privateTranslog, publicIndex);
        requireSeparate(privateTranslog, publicTranslog);
        requireSeparate(privateIndex, privateTranslog);

        final var publicSettings = serving.getIndexSettings();
        final var privateNodeSettings = Settings.builder()
            .put(publicSettings.getNodeSettings())
            .put(IndexingMemoryController.INDEX_BUFFER_SIZE_SETTING.getKey(), indexingBufferBytes + "b")
            .build();
        // IndexSettings owns mutable merge policies; a fresh instance prevents sharing/closing them.
        final var privateSettings = new IndexSettings(
            publicSettings.getIndexMetadata(),
            privateNodeSettings,
            publicSettings.getScopedSettings()
        );
        final var translog = new TranslogConfig(
            serving.getShardId(),
            privateTranslog,
            privateSettings,
            serving.getTranslogConfig().getBigArrays(),
            serving.getTranslogConfig().getNodeId(),
            false
        );
        final var mapper = serving.getMapperService();
        final boolean timeSeries = mapper != null
            && mapper.documentMapper() != null
            && mapper.documentMapper().mappers().containsTimeStampField();
        final var configuration = serving.toBuilder()
            .indexSettings(privateSettings)
            .store(privateStore)
            .mergePolicy(privateSettings.getMergePolicy(timeSeries))
            .translogConfig(translog)
            .translogFactory(new InternalTranslogFactory())
            .translogDeletionPolicyFactory(null)
            .primaryOperationPolicy(DefaultPrimaryOperationPolicy.INSTANCE)
            .primaryTermSupplier(() -> primaryTerm)
            .globalCheckpointSupplier(persistedCommittedCheckpoint)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .startedPrimarySupplier(() -> true)
            .readOnlyReplica(false)
            .warmer(null)
            .indexReaderWarmer(null)
            .externalRefreshListener(List.of())
            .internalRefreshListener(List.of())
            .eventListener(privateFailures)
            .queryCache(null)
            .mergedSegmentTransferTracker(null)
            .build();
        configuration.setEnableGcDeletes(serving.isEnableGcDeletes());
        // Core's legacy system-property escape hatch must not silently exceed this private budget.
        if (configuration.getIndexingBufferSize().getBytes() > indexingBufferBytes
            || configuration.getIndexingBufferSize().getBytes() <= 0) {
            throw new IllegalArgumentException("core indexing buffer override exceeds private engine budget");
        }
        return configuration;
    }

    static void requireSeparate(Path left, Path right) throws IOException {
        if (left.startsWith(right) || right.startsWith(left) || Files.isSameFile(left, right)) {
            throw new IOException("preparation index/translog paths must be separate from serving paths and each other");
        }
    }
}
