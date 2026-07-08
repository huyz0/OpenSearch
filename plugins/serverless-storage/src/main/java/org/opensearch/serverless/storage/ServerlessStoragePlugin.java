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
import org.opensearch.cluster.routing.allocation.decider.AllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.plugins.ClusterPlugin;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileReader;
import org.opensearch.serverless.storage.format.InMemoryPlaintextBundleCache;
import org.opensearch.serverless.storage.format.LocalDiskCachingBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.readerengine.ReaderEngineFactory;
import org.opensearch.serverless.storage.security.EncryptingBlobContainer;
import org.opensearch.serverless.storage.security.EncryptionKeyProvider;
import org.opensearch.serverless.storage.security.StaticEncryptionKeyProvider;
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
import java.util.Base64;
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
 * container rooted at {@link #SERVERLESS_STORAGE_BASE_PATH_SETTING}. S3, GCS, and Azure all have
 * real, tested {@code compareAndSwapRegister} implementations in their own repository plugins
 * (see {@code S3BlobContainer}/{@code GoogleCloudStorageBlobStore}/{@code AzureBlobStore}); this
 * plugin doesn't yet construct one of those concrete containers instead of the local-filesystem
 * one, which is the remaining piece of wiring, not a correctness gap in the register primitive
 * itself. Swapping in a real repository-backed container only touches {@link #blobContainerFor};
 * nothing else in this class or the engine/factory classes it wires together is FS-specific.
 */
public class ServerlessStoragePlugin extends Plugin implements EnginePlugin, ClusterPlugin {

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

    /**
     * Base64-encoded raw AES key bytes (16/24/32 bytes decoded, for AES-128/192/256). A keystore
     * secret, not a plaintext setting, since it's key material -- matches how repository-s3/gcs/
     * azure hold their own credentials. Optional: if unset, blob content is stored unencrypted
     * (today's default, unchanged for every existing deployment of this plugin).
     */
    public static final Setting<SecureString> SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING = SecureSetting.secureString(
        "serverless_storage.encryption_key",
        null
    );

    /**
     * Cap on the in-memory plaintext bundle-file cache in front of {@link LocalDiskCachingBundleStore}
     * (per reader shard). Keeps the hot working set decrypted in heap so most reads skip both disk
     * I/O and, when encryption is enabled, the decrypt the disk tier's ciphertext would otherwise
     * cost on every hit -- see {@link InMemoryPlaintextBundleCache}'s javadoc. Not yet a node
     * setting: a fixed default until real workload data justifies making it tunable.
     */
    private static final long IN_MEMORY_BUNDLE_CACHE_MAX_BYTES = 64L * 1024 * 1024;

    private volatile Path basePath;
    private volatile Path localCacheRoot;
    private volatile EncryptionKeyProvider encryptionKeyProvider;
    private volatile String localNodeId = "unknown-node";
    // One node-local directory instance shared by every shard on this node -- matches the target
    // design's "one node block cache" shape (&sect;9) rather than a per-shard instance, and needs
    // no I/O to construct, so it's safe to build eagerly rather than threading through createComponents.
    private final ShardDirectory shardDirectory = new InMemoryShardDirectory();

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(SERVERLESS_STORAGE_ENABLED_SETTING, SERVERLESS_STORAGE_BASE_PATH_SETTING, SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING);
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
        if (nodeEnvironment != null && nodeEnvironment.nodeDataPaths().length > 0) {
            localCacheRoot = nodeEnvironment.nodeDataPaths()[0].resolve("serverless_storage_cache");
        }
        if (clusterService != null) {
            localNodeId = clusterService.localNode().getId();
        }
        try (SecureString encryptionKey = SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING.get(environment.settings())) {
            if (encryptionKey.length() > 0) {
                byte[] rawKeyBytes = Base64.getDecoder().decode(new String(encryptionKey.getChars()));
                encryptionKeyProvider = StaticEncryptionKeyProvider.fromRawKeyBytes(rawKeyBytes);
            }
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
            if (encryptionKeyProvider != null) {
                // Wrapping here, at the one seam every downstream class already depends on
                // abstractly (BlobContainer), is the entire integration -- see
                // EncryptingBlobContainer's javadoc for the ranged-read tradeoff this implies.
                blobContainer = new EncryptingBlobContainer(blobContainer, encryptionKeyProvider);
            }
            ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
            BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
            BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);

            boolean isReaderShard = shardRouting != null && shardRouting.isSearchOnly();
            if (isReaderShard) {
                BundleFileReader readPath = bundleStore;
                if (localCacheRoot != null) {
                    // The cache layer (rfc-serverless-opensearch.md &sect;9): a reader shard
                    // re-fetches the same bundle files across queries far more often than a writer
                    // re-reads its own recent writes, so caching is wired in for reader shards only.
                    // Not to be confused with the *shard-location* directory tier (metadata-plane
                    // RFC &sect;8/&sect;9/&sect;11, `shardDirectory` below) -- this one caches
                    // bytes, that one caches "which node has this shard open."
                    Path shardCacheDir = localCacheRoot.resolve(indexUuid).resolve(String.valueOf(shardIdValue));
                    // The disk tier holds ciphertext when encryption is enabled (encryptionKeyProvider
                    // non-null), so a reader node's local disk/page cache never holds plaintext at
                    // rest -- see LocalDiskCachingBundleStore's javadoc. The in-memory tier in front
                    // of it is what keeps the actually-hot working set decrypted, so most reads never
                    // pay that decrypt cost repeatedly; only a disk-cache hit that missed this layer
                    // does.
                    BundleFileReader diskCache = new LocalDiskCachingBundleStore(bundleStore, shardCacheDir, encryptionKeyProvider);
                    readPath = new InMemoryPlaintextBundleCache(diskCache, IN_MEMORY_BUNDLE_CACHE_MAX_BYTES);
                }
                return Optional.of(
                    new ReaderEngineFactory(
                        shardStateStore,
                        manifestStore,
                        new ObjectStoreCommitMaterializer(readPath),
                        shardDirectory,
                        localNodeId
                    )
                );
            }
            ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
            return Optional.of(
                new WriterEngineFactory(new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore), shardDirectory, localNodeId)
            );
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static BlobContainer blobContainerFor(Path basePath, String indexUuid, int shardId) throws IOException {
        FsBlobStore blobStore = new FsBlobStore(1024 * 1024, basePath, false);
        BlobPath shardPath = BlobPath.cleanPath().add(indexUuid).add(String.valueOf(shardId));
        return blobStore.blobContainer(shardPath);
    }

    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return Collections.singletonList(new ReaderShardPlacementAllocationDecider());
    }
}
