/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardRecoveryException;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;

/**
 * The genuine cross-node failover scenario rfc-serverless-opensearch.md &sect;7.1.2 designs for,
 * exercised through the real {@code IndexShard}/{@code StoreRecovery} path (not a hand-built
 * {@code Engine} construction): a writer indexes and durably publishes, a second writer activates
 * against a completely fresh local {@link org.opensearch.index.store.Store} under {@code
 * RecoverySource.Type.EXISTING_STORE} (what an already-written primary's failover actually carries
 * -- confirmed by reading core allocation code directly, not assumed), and must recover via {@link
 * WriterEngineFactory#recoverMissingLocalStore} rather than failing with core's default "shard
 * allocated for local recovery, should exist, but doesn't".
 */
public class WriterEngineFactoryCrossNodeFailoverTests extends IndexShardTestCase {

    private final ShardDirectory shardDirectory = new InMemoryShardDirectory();

    public void testFailoverToACompletelyFreshLocalStoreRecoversViaMaterializationAlone() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        ObjectStoreCommitHeadPublisher headPublisher = new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore);
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer));
        WriterEngineFactory factory = new WriterEngineFactory(headPublisher, shardDirectory, "test-node", null, null, materializer);

        // Writer 1: a first, ordinary activation (a genuinely new shard, so EMPTY_STORE is the
        // realistic recovery source here) indexes a document and durably publishes it.
        IndexShard shard1 = newStartedShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(factory));
        indexDoc(shard1, "_doc", "1");
        flushShard(shard1, true);

        ShardRouting shard1Routing = shard1.routingEntry();
        IndexMetadata indexMetadata = shard1.indexSettings().getIndexMetadata();
        closeShards(shard1); // shard1's own local Store/path is gone -- shard2 gets a brand new one below

        // Writer 2: same logical shard identity (same ShardId, so it shares shard1's manifest/head
        // in the blobContainer above), but EXISTING_STORE recovery against a completely fresh local
        // Store -- newShard(...) always allocates its own fresh path, exactly what a genuinely
        // different node (or a wiped disk on the same one) looks like.
        ShardRouting failoverRouting = ShardRoutingHelper.initWithSameId(
            shard1Routing,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        IndexShard shard2 = newShard(failoverRouting, indexMetadata, null, new EngineBackedIndexerFactory(factory));
        try {
            // Without WriterEngineFactory#recoverMissingLocalStore, this would throw
            // IndexShardRecoveryException("shard allocated for local recovery ... should exist, but
            // doesn't") -- recoverShardFromStore succeeding at all is most of what this test proves.
            recoverShardFromStore(shard2);
            assertDocCount(shard2, 1);
        } finally {
            closeShards(shard2);
        }
    }

    public void testFailoverWithNoPriorManifestFallsBackToTheOrdinaryCoreFailure() throws Exception {
        // No writer has ever published anything for this shard identity -- recoverMissingLocalStore
        // correctly has nothing to materialize and returns false, so core's own, unchanged
        // "should exist, but doesn't" failure is what a genuinely corrupt/lost EXISTING_STORE shard
        // (this plugin's design has no other explanation for that combination) still gets.
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        ObjectStoreCommitHeadPublisher headPublisher = new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore);
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(new BlobContainerBundleStore(blobContainer));
        WriterEngineFactory factory = new WriterEngineFactory(headPublisher, shardDirectory, "test-node", null, null, materializer);

        ShardId noManifestShardId = new ShardId(new Index("no-manifest-idx", UUIDs.base64UUID()), 0);
        ShardRouting routing = TestShardRouting.newShardRouting(
            noManifestShardId,
            "test-node",
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        IndexShard shard = newShard(routing, Settings.EMPTY, new EngineBackedIndexerFactory(factory));
        try {
            expectThrows(IndexShardRecoveryException.class, () -> recoverShardFromStore(shard));
        } finally {
            closeShards(shard);
        }
    }

    /**
     * The other half of this test class's job, unrelated to cross-node failover: proves {@link
     * WriterEngineFactory#ownsRemoteSegmentDurability} always returns {@code true}, the seam
     * {@code IndexShard} now checks (via {@code EngineBackedIndexerFactory#getEngineFactory}) to
     * skip wiring in core's own remote segment-store upload path for a serverless-storage writer
     * shard -- confirmed to actually suppress those uploads end-to-end by
     * {@code ServerlessStorageSearchOnlyReplicaIT}, not just at this unit level.
     */
    public void testOwnsRemoteSegmentDurabilityIsAlwaysTrue() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        WriterEngineFactory factory = new WriterEngineFactory(
            new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore),
            shardDirectory,
            "test-node"
        );
        assertTrue(
            "WriterEngineFactory's own manifest publication already is a serverless-storage writer shard's "
                + "durable remote copy -- core's remote segment-store upload path must never also run",
            factory.ownsRemoteSegmentDurability()
        );
    }
}
