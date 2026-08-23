/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.ShardRange;
import org.opensearch.cluster.metadata.SplitShardsMetadata;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingHelper;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.EngineBackedIndexerFactory;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.shard.IndexShardTestUtils;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

/**
 * End-to-end proof that a real in-place split child shard actually receives its parent's
 * documents at the *engine* level (not just the object-store bookkeeping level, already covered
 * by {@code MetadataInPlaceSplitShardCommitServiceTests} and this plugin's other tests) --
 * dynamic-partitioning-progress.md's "Task 19" fix. Uses {@link ObjectStoreShardRecoveryStrategy}'s
 * {@code IN_PLACE_SPLIT_SHARD} case via a real {@link IndexShard} recovery, since that's the level
 * the bug (materialization happening too late, or not at all) actually lived at.
 */
public class InPlaceSplitLocalStoreRecoveryTests extends IndexShardTestCase {

    private static final String LOCAL_NODE_ID = "test-node";

    public void testChildEngineActuallyServesParentsDocumentAfterInPlaceSplitRecovery() throws Exception {
        Path baseDir = createTempDir();
        Map<Integer, BlobContainer> containersByShardId = new HashMap<>();
        IntFunction<BlobContainer> resolver = shardIdValue -> containersByShardId.computeIfAbsent(shardIdValue, id -> {
            try {
                FsBlobStore blobStore = new FsBlobStore(1024, baseDir.resolve(String.valueOf(id)), false);
                return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
        ShardDirectory shardDirectory = new InMemoryShardDirectory();

        // Parent (shard 0): a real writer engine, indexing and flushing a real document -- this is
        // what produces a real manifest referencing a real, readable Lucene commit, not a
        // hand-fabricated one no materializer could actually read back.
        BlobContainer parentContainer = resolver.apply(0);
        ShardStateStore parentShardStateStore = new BlobContainerShardStateStore(parentContainer);
        ObjectStoreCommitPublisher parentCommitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(parentContainer),
            new BlobContainerManifestStore(parentContainer)
        );
        WriterEngineFactory parentEngineFactory = new WriterEngineFactory(
            new ObjectStoreCommitHeadPublisher(parentCommitPublisher, parentShardStateStore),
            shardDirectory,
            LOCAL_NODE_ID
        );
        IndexShard parentShard = newStartedShard(true, Settings.EMPTY, new EngineBackedIndexerFactory(parentEngineFactory));
        indexDoc(parentShard, "_doc", "1", "{\"field\":\"value1\"}");
        flushShard(parentShard, true);
        String indexUuid = parentShard.shardId().getIndex().getUUID();

        // Real SplitShardsMetadata recording shard 0 (root) split into 2 children.
        SplitShardsMetadata.Builder splitBuilder = new SplitShardsMetadata.Builder(1);
        List<ShardRange> children = splitBuilder.splitShard(0, 2);
        ShardRange childRange = children.get(0);
        int childShardId = childRange.shardId();

        Settings indexSettingsSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            // Sized to cover the child shard id too, not just root shard 0 -- IndexMetadata's own
            // per-shard primaryTerm array is fixed at this size; SplitShardsMetadata's own separate
            // active-shard-count bookkeeping is what actually reflects the post-split shard count
            // (see dynamic-partitioning-progress.md's "Task 1" entry on why these two intentionally
            // diverge).
            .put(SETTING_NUMBER_OF_SHARDS, childShardId + 1)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, indexUuid)
            .build();
        IndexMetadata childIndexMetadata = IndexMetadata.builder(parentShard.shardId().getIndexName())
            .settings(indexSettingsSettings)
            .splitShardsMetadata(splitBuilder.build())
            // Matches ShardCloner.clone's own hardcoded target ShardHead(primaryTerm=1, ...) for a
            // freshly-cloned shard's first activation -- otherwise core's own engine construction
            // (which separately acquires/renews a writer lease using ITS OWN idea of this shard's
            // current primary term) sees a lease already held under a "newer" term than its own
            // default and refuses to proceed.
            .primaryTerm(childShardId, 1)
            .build();

        BlobContainer childContainer = resolver.apply(childShardId);
        ShardStateStore childShardStateStore = new BlobContainerShardStateStore(childContainer);
        ObjectStoreCommitPublisher childCommitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(childContainer),
            new BlobContainerManifestStore(childContainer)
        );
        WriterEngineFactory childEngineFactory = new WriterEngineFactory(
            new ObjectStoreCommitHeadPublisher(childCommitPublisher, childShardStateStore),
            shardDirectory,
            LOCAL_NODE_ID
        );
        // The strategy, not the engine factory, is what attaches this child to its parent's data --
        // it runs before any engine exists and needs to reach a shard other than the one recovering.
        this.shardRecoveryStrategy = new ObjectStoreShardRecoveryStrategy(
            (resolvedIndexUuid, resolvedShardId) -> resolver.apply(resolvedShardId),
            () -> null
        );

        ShardId childCoreShardId = new ShardId(parentShard.shardId().getIndex(), childShardId);
        ShardRouting childRouting = ShardRouting.newUnassigned(
            childCoreShardId,
            true,
            RecoverySource.InPlaceSplitShardRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
        ).initialize(randomAlphaOfLength(5), null, -1);

        IndexShard childShard = newShard(childRouting, childIndexMetadata, null, new EngineBackedIndexerFactory(childEngineFactory));
        try {
            childShard.markAsRecovering(
                "in-place split",
                new RecoveryState(
                    childShard.routingEntry(),
                    IndexShardTestUtils.getFakeDiscoNode(childShard.routingEntry().currentNodeId()),
                    null
                )
            );

            PlainActionFuture<Boolean> future = new PlainActionFuture<>();
            childShard.recoverFromStore(future);
            assertTrue("recovery of an in-place split child must succeed", future.get());

            updateRoutingEntry(childShard, ShardRoutingHelper.moveToStarted(childShard.routingEntry()));

            childShard.refresh("test");
            try (org.opensearch.index.engine.Engine.Searcher searcher = childShard.acquireSearcher("test")) {
                int numDocs = searcher.getIndexReader().numDocs();
                assertEquals(
                    "the child's own Lucene engine must actually contain the parent's document, not just "
                        + "the object-store manifest -- this is exactly the gap Task 19 fixed",
                    1,
                    numDocs
                );
            }
        } finally {
            closeShards(parentShard, childShard);
        }
    }
}
