/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;

import java.util.Optional;

/**
 * Exercises {@link ShardActivityRegistry} through a real {@link EngineTestCase}-provisioned
 * engine -- both the "tracked" path (a registered, live engine) and the "not tracked" path (a
 * shard nothing was ever registered for), which is the one {@link
 * org.opensearch.serverless.storage.writerengine.action.TransportShardIdleTimeAction} relies on to
 * distinguish "not hosted here" from a real answer.
 */
public class ShardActivityRegistryTests extends EngineTestCase {

    private static final String LOCAL_NODE_ID = "test-node";

    private final ShardDirectory shardDirectory = new InMemoryShardDirectory();
    private Store lastOpenedStore;

    public void testMillisSinceLastActivityIsEmptyForAnUnregisteredShard() {
        ShardActivityRegistry registry = new ShardActivityRegistry();
        Optional<Long> result = registry.millisSinceLastActivity("never-registered-index", 0);
        assertTrue("a shard nothing was ever registered for must read as not tracked", result.isEmpty());
    }

    public void testMillisSinceLastActivityReflectsARegisteredEnginesOwnValue() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );

        Store store = createStore();
        lastOpenedStore = store;
        store.createEmpty(defaultSettings.getIndexVersionCreated().luceneVersion);
        java.nio.file.Path translogPath = createTempDir();
        String translogUuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        store.associateIndexWithNewTranslog(translogUuid);
        EngineConfig engineConfig = config(defaultSettings, store, translogPath, newMergePolicy(), null);

        ObjectStoreWriterEngine engine = new ObjectStoreWriterEngine(
            engineConfig,
            new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore),
            shardDirectory,
            LOCAL_NODE_ID
        );
        engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
        try {
            ShardActivityRegistry registry = new ShardActivityRegistry();
            registry.register(shardId.getIndex().getUUID(), shardId.getId(), engine);

            Optional<Long> result = registry.millisSinceLastActivity(shardId.getIndex().getUUID(), shardId.getId());
            assertTrue("a registered, live engine must be tracked", result.isPresent());
            long delta = Math.abs(engine.millisSinceLastActivity() - result.get());
            assertTrue(
                "the registry must report the same value the engine itself reports, not some transformed or stale copy "
                    + "(delta was "
                    + delta
                    + "ms)",
                delta < 50 // generous tolerance for the two calls not landing in exactly the same millisecond
            );
        } finally {
            IOUtils.close(engine, lastOpenedStore);
        }
    }
}
