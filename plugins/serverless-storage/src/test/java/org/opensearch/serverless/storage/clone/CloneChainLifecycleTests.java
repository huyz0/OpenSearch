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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Deleting the middle of a clone chain, which used to strand the grandchild twice over.
 *
 * <h2>The shape</h2>
 *
 * A is an original. B is a clone of A. C is a clone of B. A pure clone writes no bundles of its own, so C's
 * manifest is a copy of B's file map, which is a copy of A's -- C's segment files physically live in A's
 * bundles, and nothing used to pin A on C's behalf.
 *
 * <p>Deleting index B then did two independent, fatal things: it removed A's {@code clone:B} pin, leaving
 * A's generation superseded, unpinned and free for A's own sweep to delete the bytes C reads; and it deleted
 * B's lineage record, so C's chain walk stopped at a shard holding no bundles at all and every inherited
 * file resolved to {@code NoSuchFileException}. rfc-serverless-opensearch.md &sect;14 names exactly this
 * clone-then-delete-source interleaving as something the GC model must cover; {@code formal/CloneGc.tla}
 * models one hop, and one hop was where the reasoning held.
 */
public class CloneChainLifecycleTests extends OpenSearchTestCase {

    private static final String A = "index-a";
    private static final String B = "index-b";
    private static final String C = "index-c";
    private static final int SHARD_ID = 0;

    private Path root;
    private final Map<String, BlobContainer> containers = new HashMap<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        root = createTempDir();
    }

    private BlobContainer container(String indexUuid, int shardId) throws IOException {
        String key = indexUuid + "/" + shardId;
        BlobContainer existing = containers.get(key);
        if (existing != null) {
            return existing;
        }
        FsBlobStore blobStore = new FsBlobStore(1024, root.resolve(key), false);
        BlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        containers.put(key, container);
        return container;
    }

    private DurablePinRegistry pins(String indexUuid, int shardId) {
        try {
            return new BlobContainerDurablePinRegistry(container(indexUuid, shardId));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /** A real published commit on A, so the chain below is built on genuine bundles rather than synthetic references. */
    private void publishOriginalCommit() throws Exception {
        BlobContainer containerA = container(A, SHARD_ID);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(containerA),
            new BlobContainerManifestStore(containerA)
        );
        try (Directory directory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "1", Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            publisher.publishCommit(
                directory,
                SegmentInfos.readLatestCommit(directory),
                A,
                SHARD_ID,
                1,
                1,
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }
        ShardStateStore headStore = new BlobContainerShardStateStore(containerA);
        assertEquals(CasResult.SUCCESS, headStore.compareAndSet(A, SHARD_ID, Optional.empty(), new ShardHead(1, null, 0L, 1)));
    }

    private void cloneShard(String sourceIndexUuid, String targetIndexUuid) throws Exception {
        BlobContainer sourceContainer = container(sourceIndexUuid, SHARD_ID);
        BlobContainer targetContainer = container(targetIndexUuid, SHARD_ID);
        ShardCloner.clone(
            sourceIndexUuid,
            SHARD_ID,
            new BlobContainerManifestStore(sourceContainer),
            new BlobContainerShardStateStore(sourceContainer),
            new BlobContainerDurablePinRegistry(sourceContainer),
            targetIndexUuid,
            SHARD_ID,
            new BlobContainerManifestStore(targetContainer),
            new BlobContainerShardStateStore(targetContainer),
            new BlobContainerCloneLineageStore(targetContainer),
            System.currentTimeMillis(),
            null,
            null,
            this::container
        );
    }

    public void testCloningACloneAlsoPinsTheOriginalThatActuallyHoldsTheBytes() throws Exception {
        publishOriginalCommit();
        cloneShard(A, B);
        cloneShard(B, C);

        Set<PinRecord> pinsOnA = pins(A, SHARD_ID).getPins(A, SHARD_ID);
        assertTrue(
            "A must carry C's pin as well as B's: C's files physically live in A's bundles, and a pin on B alone "
                + "protects nothing, because B holds no bundles at all",
            pinsOnA.stream().anyMatch(pin -> pin.pinId().equals(ShardCloner.clonePinId(C, SHARD_ID)))
        );
        assertTrue(pinsOnA.stream().anyMatch(pin -> pin.pinId().equals(ShardCloner.clonePinId(B, SHARD_ID))));
    }

    public void testDeletingTheMiddleOfAChainLeavesTheGrandchildBothPinnedAndReachable() throws Exception {
        publishOriginalCommit();
        cloneShard(A, B);
        cloneShard(B, C);

        ShardCloner.deleteClone(B, SHARD_ID, new BlobContainerCloneLineageStore(container(B, SHARD_ID)), this::pins, this::container);

        Set<PinRecord> pinsOnA = pins(A, SHARD_ID).getPins(A, SHARD_ID);
        assertFalse(
            "B is gone, so its own pin must go",
            pinsOnA.stream().anyMatch(pin -> pin.pinId().equals(ShardCloner.clonePinId(B, SHARD_ID)))
        );
        assertTrue(
            "C is still alive and still reads A's bundles -- deleting B must not unpin the only copy of its data",
            pinsOnA.stream().anyMatch(pin -> pin.pinId().equals(ShardCloner.clonePinId(C, SHARD_ID)))
        );

        assertTrue(
            "B's lineage record must survive: it is the only hop that gets C's chain walk back to A, and B holds no "
                + "bundles of its own to serve from",
            new BlobContainerCloneLineageStore(container(B, SHARD_ID)).readLineage().isPresent()
        );
        assertEquals(
            "and the chain must still resolve all the way to the original",
            3,
            ShardCloner.resolveLineageChain(container(C, SHARD_ID), C, SHARD_ID, this::container).size()
        );
    }

    /** The transfer path, for a chain built by a caller that had no ancestor resolver to pin with. */
    public void testDeletingTheMiddleHandsDownADependantPinTakenWithoutAncestorPinning() throws Exception {
        publishOriginalCommit();
        cloneShard(A, B);

        // C clones B the way the older overloads do: the immediate source only, nothing further back.
        BlobContainer containerB = container(B, SHARD_ID);
        BlobContainer containerC = container(C, SHARD_ID);
        ShardCloner.clone(
            B,
            SHARD_ID,
            new BlobContainerManifestStore(containerB),
            new BlobContainerShardStateStore(containerB),
            new BlobContainerDurablePinRegistry(containerB),
            C,
            SHARD_ID,
            new BlobContainerManifestStore(containerC),
            new BlobContainerShardStateStore(containerC),
            new BlobContainerCloneLineageStore(containerC),
            System.currentTimeMillis()
        );
        assertFalse(
            "precondition: without a resolver nothing pinned A on C's behalf",
            pins(A, SHARD_ID).getPins(A, SHARD_ID).stream().anyMatch(pin -> pin.pinId().equals(ShardCloner.clonePinId(C, SHARD_ID)))
        );

        ShardCloner.deleteClone(B, SHARD_ID, new BlobContainerCloneLineageStore(containerB), this::pins, this::container);

        assertTrue(
            "before releasing its own pin, deleting B must hand C's pin down to the shard that actually holds the "
                + "bytes -- otherwise the chain is unprotected for exactly as long as it takes A's sweep to run",
            pins(A, SHARD_ID).getPins(A, SHARD_ID).stream().anyMatch(pin -> pin.pinId().equals(ShardCloner.clonePinId(C, SHARD_ID)))
        );
    }

    /** The clone's manifest is what proves the pin protects the right generation. */
    public void testAClonedManifestStillNamesTheOriginalsBundles() throws Exception {
        publishOriginalCommit();
        cloneShard(A, B);
        cloneShard(B, C);

        CommitManifest cloneOfClone = new BlobContainerManifestStore(container(C, SHARD_ID)).readManifest(1, 1);
        Set<String> bundlesInA = new BlobContainerBundleStore(container(A, SHARD_ID)).listBundleNames();
        assertTrue(
            "this is the fact the whole finding rests on: the grandchild's files are in the original's bundles",
            bundlesInA.containsAll(cloneOfClone.referencedBundles())
        );
        assertTrue(
            "and the middle shard has none of its own",
            new BlobContainerBundleStore(container(B, SHARD_ID)).listBundleNames().isEmpty()
        );
    }
}
