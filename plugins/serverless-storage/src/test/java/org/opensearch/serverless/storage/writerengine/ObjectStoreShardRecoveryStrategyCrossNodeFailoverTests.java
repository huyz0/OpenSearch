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
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;

import java.io.IOException;

/**
 * The genuine cross-node failover scenario rfc-serverless-opensearch.md &sect;7.1.2 designs for,
 * exercised through the real {@code IndexShard}/{@code StoreRecovery} path (not a hand-built
 * {@code Engine} construction): a writer indexes and durably publishes, a second writer activates
 * against a completely fresh local {@link org.opensearch.index.store.Store} under {@code
 * RecoverySource.Type.EXISTING_STORE} (what an already-written primary's failover actually carries
 * -- confirmed by reading core allocation code directly, not assumed), and must recover via {@link
 * ObjectStoreShardRecoveryStrategy}'s {@code EXISTING_STORE} case rather than failing with core's
 * default "shard allocated for local recovery, should exist, but doesn't".
 */
public class ObjectStoreShardRecoveryStrategyCrossNodeFailoverTests extends IndexShardTestCase {

    private final ShardDirectory shardDirectory = new InMemoryShardDirectory();

    /** Everything a writer shard needs, all pointed at one container -- the shape every test below sets up. */
    private record Fixture(BlobContainer blobContainer, WriterEngineFactory engineFactory, ObjectStoreShardRecoveryStrategy strategy) {
    }

    private Fixture newFixture() throws IOException {
        return newFixture(null);
    }

    private Fixture newFixture(EngineNativeSnapshotSupport engineNativeSnapshotSupport) throws IOException {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        WriterEngineFactory engineFactory = new WriterEngineFactory(
            new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore),
            shardDirectory,
            "test-node"
        );
        // One container for every (indexUuid, shardId): these tests only ever exercise one shard
        // identity at a time, and the strategy resolves everything it needs through this one seam.
        ObjectStoreShardRecoveryStrategy strategy = new ObjectStoreShardRecoveryStrategy(
            (indexUuid, shardId) -> blobContainer,
            () -> engineNativeSnapshotSupport
        );
        return new Fixture(blobContainer, engineFactory, strategy);
    }

    public void testFailoverToACompletelyFreshLocalStoreRecoversViaMaterializationAlone() throws Exception {
        Fixture fixture = newFixture();
        this.shardRecoveryStrategy = fixture.strategy();

        // Writer 1: a first, ordinary activation (a genuinely new shard, so EMPTY_STORE is the
        // realistic recovery source here) indexes a document and durably publishes it.
        IndexShard shard1 = newStartedShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(fixture.engineFactory()));
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
        IndexShard shard2 = newShard(failoverRouting, indexMetadata, null, new EngineBackedIndexerFactory(fixture.engineFactory()));
        try {
            // Without the strategy's EXISTING_STORE case, this would throw
            // IndexShardRecoveryException("shard allocated for local recovery ... should exist, but
            // doesn't") -- recoverShardFromStore succeeding at all is most of what this test proves.
            recoverShardFromStore(shard2);
            assertDocCount(shard2, 1);
        } finally {
            closeShards(shard2);
        }
    }

    public void testFailoverWithNoPriorManifestFallsBackToTheOrdinaryCoreFailure() throws Exception {
        // No writer has ever published anything for this shard identity -- the strategy correctly
        // has nothing to materialize and returns false, so core's own, unchanged "should exist, but
        // doesn't" failure is what a genuinely corrupt/lost EXISTING_STORE shard (this plugin's
        // design has no other explanation for that combination) still gets.
        Fixture fixture = newFixture();
        this.shardRecoveryStrategy = fixture.strategy();

        ShardId noManifestShardId = new ShardId(new Index("no-manifest-idx", UUIDs.base64UUID()), 0);
        ShardRouting routing = TestShardRouting.newShardRouting(
            noManifestShardId,
            "test-node",
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        IndexShard shard = newShard(routing, Settings.EMPTY, new EngineBackedIndexerFactory(fixture.engineFactory()));
        try {
            expectThrows(IndexShardRecoveryException.class, () -> recoverShardFromStore(shard));
        } finally {
            closeShards(shard);
        }
    }

    /**
     * The other half of this test class's job, unrelated to cross-node failover: proves {@link
     * ObjectStoreShardRecoveryStrategy#ownsRemoteSegmentDurability} always returns {@code true}, the
     * answer {@code IndexShard} now reads off the shard's own strategy to skip wiring in core's own
     * remote segment-store upload path for a serverless-storage writer shard -- confirmed to actually
     * suppress those uploads end-to-end by {@code ServerlessStorageSearchOnlyReplicaIT}, not just at
     * this unit level.
     */
    public void testOwnsRemoteSegmentDurabilityIsAlwaysTrue() throws Exception {
        assertTrue(
            "this plugin's own manifest publication already is a serverless-storage writer shard's "
                + "durable remote copy -- core's remote segment-store upload path must never also run",
            newFixture().strategy().ownsRemoteSegmentDurability()
        );
    }

    /**
     * Engine-native snapshot capability is the <em>presence</em> of {@link
     * ObjectStoreShardRecoveryStrategy#engineNativeSnapshots()}, not a separate boolean flag beside it.
     * Core's {@code StoreRecovery} uses that presence as a cheap local gate to decide whether it is even
     * worth probing the repository for a pointer blob; when the gate and the restore call were two
     * independently overridable methods, a mismatch between them meant core silently skipped restoring
     * exactly the snapshots this plugin could actually recover. Making one the other's carrier is what
     * removes that failure mode, and these two tests pin it.
     */
    public void testEngineNativeSnapshotsIsEmptyWhenSupportIsDisabled() throws Exception {
        assertTrue(
            "no configured support must present as no capability, so core skips the remote probe entirely",
            newFixture(null).strategy().engineNativeSnapshots().isEmpty()
        );
    }

    public void testEngineNativeSnapshotsIsPresentWhenSupportIsConfigured() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        EngineNativeSnapshotSupport support = new EngineNativeSnapshotSupport((indexUuid, shardId) -> blobContainer);
        assertSame(
            "the very object that restores must be the one whose presence answers the capability question",
            support,
            newFixture(support).strategy().engineNativeSnapshots().orElseThrow()
        );
    }
}
