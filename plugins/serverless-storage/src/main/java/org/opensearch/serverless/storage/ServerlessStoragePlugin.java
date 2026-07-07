/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.readerengine.ReaderEngineFactory;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitHeadPublisher;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.serverless.storage.writerengine.WriterEngineFactory;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Entry point for the object-store-native serverless storage format: segment bundles, commit
 * manifests, WAL, GC, and the writer/reader engines described in {@code rfc-serverless-opensearch.md}.
 *
 * <p>An index only gets an object-store engine if it explicitly opts in via {@link
 * #SERVERLESS_STORAGE_ENABLED_SETTING}; every other index is untouched (returns {@link
 * Optional#empty()}, so the platform's default engine applies), matching Goal 6 of the RFC:
 * classic mode stays default and untouched.
 *
 * <p>The blob container backing an opted-in index is, for now, always a local-filesystem
 * container rooted at {@link #SERVERLESS_STORAGE_BASE_PATH_SETTING} -- there is no real
 * S3/GCS/Azure {@code compareAndSwapRegister} implementation yet (rfc-serverless-opensearch.md's
 * own scope note: real cloud-backed register implementations are unverified without cloud
 * credentials). Swapping in a real repository-backed container later only touches {@link
 * #blobContainerFor}; nothing else in this class or the engine/factory classes it wires together
 * is FS-specific.
 */
public class ServerlessStoragePlugin extends Plugin implements EnginePlugin {

    public static final Setting<Boolean> SERVERLESS_STORAGE_ENABLED_SETTING = Setting.boolSetting(
        "index.serverless_storage.enabled",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final Setting<String> SERVERLESS_STORAGE_BASE_PATH_SETTING = Setting.simpleString(
        "serverless_storage.base_path",
        Setting.Property.NodeScope
    );

    private volatile Path basePath;

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SERVERLESS_STORAGE_ENABLED_SETTING, SERVERLESS_STORAGE_BASE_PATH_SETTING);
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        String configuredBasePath = SERVERLESS_STORAGE_BASE_PATH_SETTING.get(environment.settings());
        if (configuredBasePath.isEmpty() == false) {
            // Environment#resolveRepoFile is the same sanctioned path-resolution seam
            // repository-fs / repository-url use: it refuses to resolve anything outside the
            // node's configured allowed-paths, rather than trusting an arbitrary settings string.
            basePath = environment.resolveRepoFile(configuredBasePath);
        }
        return Collections.emptyList();
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings, ShardRouting shardRouting) {
        if (SERVERLESS_STORAGE_ENABLED_SETTING.get(indexSettings.getSettings()) == false) {
            return Optional.empty();
        }
        if (basePath == null) {
            throw new IllegalStateException(
                "index ["
                    + indexSettings.getIndex().getName()
                    + "] has serverless storage enabled but no ["
                    + SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey()
                    + "] node setting was configured (or it did not resolve to an allowed path)"
            );
        }

        try {
            String indexUuid = indexSettings.getIndex().getUUID();
            int shardIdValue = shardRouting != null ? shardRouting.shardId().getId() : 0;
            // Each shard gets its own child container (rfc-serverless-opensearch.md &sect;6.1's
            // indices/<index-uuid>/<shard>/ layout): CommitManifest#manifestName() is intentionally
            // just <term>-<generation> with no index/shard component, since it assumes the
            // container it lives in is already shard-scoped.
            BlobContainer blobContainer = blobContainerFor(basePath, indexUuid, shardIdValue);
            ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
            BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
            BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);

            boolean isReaderShard = shardRouting != null && shardRouting.isSearchOnly();
            if (isReaderShard) {
                return Optional.of(new ReaderEngineFactory(shardStateStore, manifestStore, new ObjectStoreCommitMaterializer(bundleStore)));
            }
            ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
            return Optional.of(new WriterEngineFactory(new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore)));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static BlobContainer blobContainerFor(Path basePath, String indexUuid, int shardId) throws IOException {
        FsBlobStore blobStore = new FsBlobStore(1024 * 1024, basePath, false);
        BlobPath shardPath = BlobPath.cleanPath().add(indexUuid).add(String.valueOf(shardId));
        return blobStore.blobContainer(shardPath);
    }
}
