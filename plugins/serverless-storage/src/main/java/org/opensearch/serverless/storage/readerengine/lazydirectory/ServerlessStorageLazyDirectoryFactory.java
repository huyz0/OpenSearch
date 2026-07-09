/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.readerengine.lazydirectory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.clone.BlobContainerCloneLineageStore;
import org.opensearch.serverless.storage.clone.CloneLineage;
import org.opensearch.serverless.storage.clone.FallbackStreamReader;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;

import java.io.IOException;
import java.util.Optional;

/**
 * {@link IndexStorePlugin.DirectoryFactory} for {@link ServerlessStoragePlugin#LAZY_DIRECTORY_STORE_TYPE}:
 * a search-only (reader) shard copy gets a {@link LazyBundleDirectory} instead of a normal local
 * {@link FSDirectory} -- rfc-serverless-opensearch.md &sect;7.2/&sect;9's lazy, block-cached remote
 * directory, closing the gap that section's own status note left open ("wiring this into {@code
 * ObjectStoreReaderEngine.open()}... needs either rebuilding {@code EngineConfig}... or a
 * {@code ShardRouting}-aware {@code DirectoryFactory} selection seam in core"). Any other shard
 * copy (a writer/primary, or a reader shard on an index that hasn't opted into the lazy directory)
 * gets a completely normal {@link FsDirectoryFactory}-built directory, unaffected by this class's
 * existence -- selecting the {@link ServerlessStoragePlugin#LAZY_DIRECTORY_STORE_TYPE} store type
 * for an index does not, by itself, change how a writer/primary copy on that same index is built.
 *
 * <p>Reads {@link ServerlessStoragePlugin}'s own fields lazily, at {@link #newDirectory} call time
 * rather than at construction time: {@code getDirectoryFactories()} (which constructs this class)
 * is invoked by core during node startup <em>before</em> {@code createComponents} runs (confirmed
 * by reading {@code Node}'s own constructor), so anything this class needs that {@code
 * createComponents} builds (the shared {@link FileCache}, {@code threadPool}) must be read through
 * the still-being-initialized plugin reference, not captured eagerly -- by the time a real shard is
 * actually created, {@code createComponents} has long since run.
 *
 * <p>Also transparently supports a cloned shard's reader (rfc-serverless-opensearch.md &sect;14):
 * {@link #resolveStreamReader} checks the shard's {@link BlobContainerCloneLineageStore} and, only
 * when it really is a clone, wraps the normal {@link TransferManager.StreamReader} with a {@link
 * FallbackStreamReader} that falls back to the clone source's own container -- the lazy-directory
 * counterpart of {@code FallbackBundleFileReader}'s support for the eager materializer path.
 */
public final class ServerlessStorageLazyDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    private final ServerlessStoragePlugin plugin;
    private final FsDirectoryFactory fallback = new FsDirectoryFactory();

    public ServerlessStorageLazyDirectoryFactory(ServerlessStoragePlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        // The two-argument overload has no ShardRouting to decide with -- always safe to fall back
        // to a normal directory here, since every real call site this plugin cares about goes
        // through the three-argument overload below (see IndexStorePlugin#newDirectory's own
        // javadoc for why the default method delegates the other way; this class overrides both
        // so a caller using either overload gets correct, non-crashing behavior).
        return fallback.newDirectory(indexSettings, shardPath);
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath, ShardRouting shardRouting) throws IOException {
        FileCache fileCache = plugin.lazyDirectoryFileCacheForDirectoryFactory();
        if (shardRouting == null || shardRouting.isSearchOnly() == false || fileCache == null) {
            return fallback.newDirectory(indexSettings, shardPath);
        }

        String indexUuid = indexSettings.getIndex().getUUID();
        int shardId = shardRouting.shardId().getId();
        BlobContainer blobContainer = plugin.blobContainerForDirectoryFactory(indexUuid, shardId);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(blobContainer);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(blobContainer);

        Optional<VersionedShardHead> head = shardStateStore.get(indexUuid, shardId);
        if (head.isEmpty() || head.get().head().latestManifestGeneration() == 0) {
            // Same "nothing for a reader to open" condition ReaderEngineFactory already guards --
            // this class hits it earlier, at directory-construction time, since there's genuinely
            // nothing to build a LazyBundleDirectory's file map from yet.
            throw new IOException(
                "no published head for shard " + shardRouting.shardId() + "; nothing for a lazy reader directory to open"
            );
        }
        ShardHead shardHead = head.get().head();
        CommitManifest manifest = manifestStore.readManifest(shardHead.primaryTerm(), shardHead.latestManifestGeneration());

        // A fresh, shard-specific local directory purely for TransferManager's own on-disk block
        // cache -- separate from ShardPath#resolveIndex(), which normal FS-backed shards use for
        // their actual Lucene index files, so there is no risk of this ever colliding with or being
        // mistaken for a real local commit. Owned and closed by the LazyBundleDirectory built
        // around it (see that class's own close() javadoc), not by this factory.
        FSDirectory cacheDirectory = new MMapDirectory(shardPath.resolve("lazy_directory_cache"), SimpleFSLockFactory.INSTANCE);
        TransferManager transferManager = new TransferManager(
            resolveStreamReader(blobContainer, bundleStore, indexUuid, shardId),
            fileCache,
            plugin.threadPoolForDirectoryFactory()
        );
        return new LazyBundleDirectory(manifest, cacheDirectory, transferManager);
    }

    /**
     * A cloned shard's own {@code blobContainer} cannot see bundles that still physically live in
     * its clone source's container (rfc-serverless-opensearch.md &sect;14) -- the same reason
     * {@code ObjectStoreCommitMaterializer}'s eager path needs {@code FallbackBundleFileReader}.
     * Checks {@link BlobContainerCloneLineageStore} (a cheap single-blob read/miss, harmless for
     * the overwhelming majority of shards that were never cloned) and, only when this shard really
     * is a clone, wraps the normal reader with a {@link FallbackStreamReader} that falls back to
     * the source shard's own container.
     */
    private TransferManager.StreamReader resolveStreamReader(
        BlobContainer blobContainer,
        BlobContainerBundleStore bundleStore,
        String indexUuid,
        int shardId
    ) throws IOException {
        Optional<CloneLineage> lineage = new BlobContainerCloneLineageStore(blobContainer).readLineage();
        if (lineage.isEmpty()) {
            return bundleStore::openRange;
        }
        BlobContainer sourceContainer = plugin.blobContainerForDirectoryFactory(
            lineage.get().sourceIndexUuid(),
            lineage.get().sourceShardId()
        );
        BlobContainerBundleStore sourceBundleStore = new BlobContainerBundleStore(sourceContainer);
        return new FallbackStreamReader(bundleStore::openRange, sourceBundleStore::openRange);
    }

    @Override
    public Directory newFSDirectory(
        java.nio.file.Path location,
        org.apache.lucene.store.LockFactory lockFactory,
        IndexSettings indexSettings
    ) throws IOException {
        return fallback.newFSDirectory(location, lockFactory, indexSettings);
    }
}
