/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.clone;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.support.FilterBlobContainer;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class ShardClonerTests extends OpenSearchTestCase {

    private static final String SOURCE_INDEX_UUID = "source-idx";
    private static final String TARGET_INDEX_UUID = "target-idx";
    private static final int SHARD_ID = 0;
    private static final long PRIMARY_TERM = 1;

    private BlobContainer sourceContainer;
    private BlobContainer targetContainer;
    private BlobContainerManifestStore sourceManifestStore;
    private BlobContainerManifestStore targetManifestStore;
    private BlobContainerBundleStore sourceBundleStore;
    private BlobContainerBundleStore targetBundleStore;
    private ShardStateStore sourceShardStateStore;
    private ShardStateStore targetShardStateStore;
    private DurablePinRegistry sourcePinRegistry;
    private BlobContainerCloneLineageStore targetLineageStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore sourceBlobStore = new FsBlobStore(1024, createTempDir(), false);
        sourceContainer = new FsBlobContainer(sourceBlobStore, BlobPath.cleanPath(), sourceBlobStore.path());
        FsBlobStore targetBlobStore = new FsBlobStore(1024, createTempDir(), false);
        targetContainer = new FsBlobContainer(targetBlobStore, BlobPath.cleanPath(), targetBlobStore.path());

        sourceManifestStore = new BlobContainerManifestStore(sourceContainer);
        targetManifestStore = new BlobContainerManifestStore(targetContainer);
        sourceBundleStore = new BlobContainerBundleStore(sourceContainer);
        targetBundleStore = new BlobContainerBundleStore(targetContainer);
        sourceShardStateStore = new BlobContainerShardStateStore(sourceContainer);
        targetShardStateStore = new BlobContainerShardStateStore(targetContainer);
        sourcePinRegistry = new BlobContainerDurablePinRegistry(sourceContainer);
        targetLineageStore = new BlobContainerCloneLineageStore(targetContainer);
    }

    /** Real IndexWriter -> real ObjectStoreCommitPublisher -> real published head, exactly what a live writer shard would have produced. */
    private CommitManifest publishSourceCommit() throws Exception {
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(sourceBundleStore, sourceManifestStore);
        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc1 = new Document();
                doc1.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc1);
                Document doc2 = new Document();
                doc2.add(new StringField("id", "2", Field.Store.YES));
                writer.addDocument(doc2);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                SOURCE_INDEX_UUID,
                SHARD_ID,
                PRIMARY_TERM,
                1,
                1,
                1,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }
        assertEquals(
            CasResult.SUCCESS,
            sourceShardStateStore.compareAndSet(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                java.util.Optional.empty(),
                new ShardHead(PRIMARY_TERM, null, 0L, manifest.generation())
            )
        );
        return manifest;
    }

    public void testCloneRefusesASourceWithNoPublishedManifest() {
        expectThrows(
            java.io.IOException.class,
            () -> ShardCloner.clone(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                sourceManifestStore,
                sourceShardStateStore,
                sourcePinRegistry,
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetManifestStore,
                targetShardStateStore,
                targetLineageStore,
                1L
            )
        );
    }

    public void testCloneRefusesToOverwriteATargetThatAlreadyHasAPublishedHead() throws Exception {
        publishSourceCommit();
        // Simulates the target already being active for a reason that has nothing to do with this
        // clone attempt at all -- an ordinary index creation, or a stale request against an
        // already-active target -- so it has a head but no clone lineage yet.
        assertEquals(
            CasResult.SUCCESS,
            targetShardStateStore.compareAndSet(TARGET_INDEX_UUID, SHARD_ID, java.util.Optional.empty(), ShardHead.initial())
        );
        expectThrows(
            java.io.IOException.class,
            () -> ShardCloner.clone(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                sourceManifestStore,
                sourceShardStateStore,
                sourcePinRegistry,
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetManifestStore,
                targetShardStateStore,
                targetLineageStore,
                1L
            )
        );
        // This attempt can never complete later -- the target will never become inactive again --
        // so it must not leave behind a lineage record that would misdescribe this unrelated,
        // legitimately-active target's real origin, nor a pin that would block the source shard's
        // GC forever for a clone that will never exist.
        assertTrue(
            "a definitively failed clone attempt must not leave a bogus lineage record on the target",
            targetLineageStore.readLineage().isEmpty()
        );
        assertTrue(
            "a definitively failed clone attempt must not leave its pin on the source",
            sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID).isEmpty()
        );
        assertTrue(
            "a definitively failed clone attempt must not leave its stray manifest on the target -- "
                + "ObjectStoreCommitPublisher would otherwise silently hand that manifest's content "
                + "(the SOURCE shard's file references) back to this target's own real first commit later",
            targetManifestStore.listManifests().isEmpty()
        );
    }

    // Regression test for a narrower leak: a failure BEFORE the manifest/lineage are ever written
    // (e.g. the source manifest read itself fails) must still release the pin that was already
    // added -- previously left in place as "harmless extra retention" (still true in the sense
    // that nothing was corrupted), but with no discovery path at all once a later, genuinely
    // different-source attempt takes over the target: deleteClone can only ever see ONE lineage
    // record, so an earlier, never-completed attempt's own pin on its own chosen source becomes
    // permanently unreachable through any mechanism, not just inconvenient to clean up.
    public void testCloneReleasesThePinEvenWhenFailingBeforeTheManifestOrLineageAreWritten() throws Exception {
        publishSourceCommit();
        BlobContainer readFailingSourceContainer = new FilterBlobContainer(sourceContainer) {
            @Override
            public java.io.InputStream readBlob(String blobName) throws java.io.IOException {
                throw new java.io.IOException("simulated transient read failure for " + blobName);
            }

            @Override
            protected BlobContainer wrapChild(BlobContainer child) {
                throw new AssertionError("this test never descends into a child container");
            }
        };
        BlobContainerManifestStore failingSourceManifestStore = new BlobContainerManifestStore(readFailingSourceContainer);

        expectThrows(
            java.io.IOException.class,
            () -> ShardCloner.clone(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                failingSourceManifestStore,
                sourceShardStateStore,
                sourcePinRegistry,
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetManifestStore,
                targetShardStateStore,
                targetLineageStore,
                1L
            )
        );

        assertTrue(
            "the pin added just before the failing read must still be released, not left as "
                + "unreachable garbage",
            sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID).isEmpty()
        );
        assertTrue(targetLineageStore.readLineage().isEmpty());
    }

    public void testClonePinsTheExactSourceGenerationItClonedFrom() throws Exception {
        CommitManifest sourceManifest = publishSourceCommit();
        ShardCloner.clone(
            SOURCE_INDEX_UUID,
            SHARD_ID,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            TARGET_INDEX_UUID,
            SHARD_ID,
            targetManifestStore,
            targetShardStateStore,
            targetLineageStore,
            System.currentTimeMillis()
        );
        Set<PinRecord> pins = sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID);
        assertEquals(1, pins.size());
        PinRecord pin = pins.iterator().next();
        assertEquals(ShardCloner.clonePinId(TARGET_INDEX_UUID, SHARD_ID), pin.pinId());
        assertEquals(sourceManifest.primaryTerm(), pin.primaryTerm());
        assertEquals(sourceManifest.generation(), pin.generation());
    }

    public void testClonedShardIsSearchableThroughTheFallbackReaderWithoutCopyingAnyBundleBytes() throws Exception {
        publishSourceCommit();
        long cloneTimeMillis = System.currentTimeMillis();
        ShardCloner.clone(
            SOURCE_INDEX_UUID,
            SHARD_ID,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            TARGET_INDEX_UUID,
            SHARD_ID,
            targetManifestStore,
            targetShardStateStore,
            targetLineageStore,
            cloneTimeMillis
        );

        // The target's own bundle container is empty -- nothing was copied -- so a materializer
        // that only consulted it would fail to find every file the cloned manifest references.
        assertTrue(
            "clone must not have copied any bundle bytes into the target's own container",
            targetBundleStore.listBundleNames().isEmpty()
        );

        java.util.Optional<org.opensearch.serverless.storage.shardstate.VersionedShardHead> targetHead = targetShardStateStore.get(
            TARGET_INDEX_UUID,
            SHARD_ID
        );
        assertTrue(targetHead.isPresent());
        CommitManifest targetManifest = targetManifestStore.readManifest(
            targetHead.get().head().primaryTerm(),
            targetHead.get().head().latestManifestGeneration()
        );
        assertEquals(TARGET_INDEX_UUID, targetManifest.indexUuid());

        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(
            new FallbackBundleFileReader(targetBundleStore, sourceBundleStore)
        );
        try (Directory materialized = new ByteBuffersDirectory()) {
            materializer.materialize(targetManifest, materialized);
            try (DirectoryReader reader = DirectoryReader.open(materialized)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                TopDocs hits = searcher.search(new TermQuery(new Term("id", "1")), 10);
                assertEquals(1, hits.totalHits.value());
                hits = searcher.search(new TermQuery(new Term("id", "2")), 10);
                assertEquals(1, hits.totalHits.value());
            }
        }
    }

    public void testDeleteCloneRemovesExactlyThisClonesPinAndLineage() throws Exception {
        publishSourceCommit();
        ShardCloner.clone(
            SOURCE_INDEX_UUID,
            SHARD_ID,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            TARGET_INDEX_UUID,
            SHARD_ID,
            targetManifestStore,
            targetShardStateStore,
            targetLineageStore,
            System.currentTimeMillis()
        );
        assertEquals(1, sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID).size());
        assertTrue(targetLineageStore.readLineage().isPresent());

        ShardCloner.deleteClone(TARGET_INDEX_UUID, SHARD_ID, targetLineageStore, (indexUuid, shardId) -> {
            assertEquals(SOURCE_INDEX_UUID, indexUuid);
            assertEquals(Integer.valueOf(SHARD_ID), shardId);
            return sourcePinRegistry;
        });

        assertTrue(
            "deleteClone must release the pin it protected the source generation with",
            sourcePinRegistry.getPins(SOURCE_INDEX_UUID, SHARD_ID).isEmpty()
        );
        assertTrue(
            "deleteClone must remove the lineage record once the pin it points at is gone",
            targetLineageStore.readLineage().isEmpty()
        );
    }

    public void testDeleteCloneOnANeverClonedShardIsANoOp() throws Exception {
        // targetLineageStore's container has no lineage blob at all -- deleteClone must not throw
        // or invoke the resolver, since there is nothing to resolve a source for.
        ShardCloner.deleteClone(TARGET_INDEX_UUID, SHARD_ID, targetLineageStore, (indexUuid, shardId) -> {
            throw new AssertionError("resolver must not be invoked when there is no lineage to act on");
        });
    }

    public void testResolveLineageChainWalksMultipleCloneHops() throws Exception {
        // A three-hop chain: source -> clone1 -> clone2. resolveLineageChain from clone2's own
        // container must return [clone2, clone1, source], not just [clone2, clone1] -- a clone of
        // a clone must still be able to fall back all the way to the original.
        publishSourceCommit();
        FsBlobStore clone1BlobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer clone1Container = new FsBlobContainer(clone1BlobStore, BlobPath.cleanPath(), clone1BlobStore.path());
        String clone1IndexUuid = "clone1-idx";
        ShardCloner.clone(
            SOURCE_INDEX_UUID,
            SHARD_ID,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            clone1IndexUuid,
            SHARD_ID,
            new BlobContainerManifestStore(clone1Container),
            new BlobContainerShardStateStore(clone1Container),
            new BlobContainerCloneLineageStore(clone1Container),
            System.currentTimeMillis()
        );

        FsBlobStore clone2BlobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer clone2Container = new FsBlobContainer(clone2BlobStore, BlobPath.cleanPath(), clone2BlobStore.path());
        String clone2IndexUuid = "clone2-idx";
        ShardCloner.clone(
            clone1IndexUuid,
            SHARD_ID,
            new BlobContainerManifestStore(clone1Container),
            new BlobContainerShardStateStore(clone1Container),
            new BlobContainerDurablePinRegistry(clone1Container),
            clone2IndexUuid,
            SHARD_ID,
            new BlobContainerManifestStore(clone2Container),
            new BlobContainerShardStateStore(clone2Container),
            new BlobContainerCloneLineageStore(clone2Container),
            System.currentTimeMillis()
        );

        java.util.Map<String, BlobContainer> containersByIndexUuid = java.util.Map.of(
            SOURCE_INDEX_UUID,
            sourceContainer,
            clone1IndexUuid,
            clone1Container,
            clone2IndexUuid,
            clone2Container
        );
        java.util.List<BlobContainer> chain = ShardCloner.resolveLineageChain(
            clone2Container,
            clone2IndexUuid,
            SHARD_ID,
            (indexUuid, shardId) -> containersByIndexUuid.get(indexUuid)
        );
        assertEquals(java.util.List.of(clone2Container, clone1Container, sourceContainer), chain);
    }

    public void testResolveLineageChainOnANeverClonedShardReturnsJustItself() throws Exception {
        java.util.List<BlobContainer> chain = ShardCloner.resolveLineageChain(
            sourceContainer,
            SOURCE_INDEX_UUID,
            SHARD_ID,
            (indexUuid, shardId) -> { throw new AssertionError("resolver must not be invoked when there is no lineage"); }
        );
        assertEquals(java.util.List.of(sourceContainer), chain);
    }

    public void testCloneRefusesToSilentlyOverwriteLineageFromADifferentPriorSource() throws Exception {
        // Simulates a clone attempt that pinned its source and wrote lineage, but then failed
        // before/at the head CAS (e.g. lost a race) -- the target's lineage blob survives that
        // failure. A retry with a DIFFERENT source must not silently overwrite it (that would
        // permanently leak the first attempt's pin on its own source); it must fail loudly instead.
        publishSourceCommit();
        targetLineageStore.writeLineage(new CloneLineage("some-other-abandoned-source-idx", SHARD_ID));

        expectThrows(
            java.io.IOException.class,
            () -> ShardCloner.clone(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                sourceManifestStore,
                sourceShardStateStore,
                sourcePinRegistry,
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetManifestStore,
                targetShardStateStore,
                targetLineageStore,
                System.currentTimeMillis()
            )
        );
        // The abandoned lineage must be left untouched, not overwritten, so the pin it points at
        // remains discoverable/releasable later.
        assertEquals("some-other-abandoned-source-idx", targetLineageStore.readLineage().get().sourceIndexUuid());
    }

    public void testFailedCloneDoesNotRollBackADifferentConcurrentAttemptsLineage() throws Exception {
        publishSourceCommit();
        expectThrows(
            java.io.IOException.class,
            () -> ShardCloner.clone(
                SOURCE_INDEX_UUID,
                SHARD_ID,
                sourceManifestStore,
                sourceShardStateStore,
                sourcePinRegistry,
                TARGET_INDEX_UUID,
                SHARD_ID,
                targetManifestStore,
                targetShardStateStore,
                targetLineageStore,
                1L,
                () -> {
                    // writeLineage is write-once (fails outright if the blob already exists -- see
                    // BlobContainerCloneLineageStore#writeLineage's underlying writeBlobAtomic call),
                    // so an ordinary concurrent clone() call can never silently overwrite this
                    // attempt's own just-written lineage. The only way another attempt's record could
                    // land here is an explicit delete followed by a different write, e.g. an
                    // operator-triggered deleteClone racing a different clone() attempt -- simulated
                    // directly here since it can't happen through a second writeLineage call alone.
                    targetLineageStore.deleteLineage();
                    targetLineageStore.writeLineage(new CloneLineage("racer-source-idx", SHARD_ID));
                    throw new java.io.IOException("simulated failure after a concurrent racer already replaced the lineage");
                }
            )
        );
        assertEquals(
            "this attempt's rollback must never touch a lineage record it can tell is no longer its own",
            "racer-source-idx",
            targetLineageStore.readLineage().get().sourceIndexUuid()
        );
    }

    public void testDeleteCloneIsIdempotent() throws Exception {
        publishSourceCommit();
        ShardCloner.clone(
            SOURCE_INDEX_UUID,
            SHARD_ID,
            sourceManifestStore,
            sourceShardStateStore,
            sourcePinRegistry,
            TARGET_INDEX_UUID,
            SHARD_ID,
            targetManifestStore,
            targetShardStateStore,
            targetLineageStore,
            System.currentTimeMillis()
        );
        ShardCloner.deleteClone(TARGET_INDEX_UUID, SHARD_ID, targetLineageStore, (indexUuid, shardId) -> sourcePinRegistry);
        // Second call: lineage is already gone, so this must be a harmless no-op, not a failure.
        ShardCloner.deleteClone(TARGET_INDEX_UUID, SHARD_ID, targetLineageStore, (indexUuid, shardId) -> {
            throw new AssertionError("resolver must not be invoked once lineage is already gone");
        });
    }

}
