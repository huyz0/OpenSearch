/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

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
import org.opensearch.serverless.storage.security.RegisterDelegatingBlobContainer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.function.Predicate;

public class ObjectStoreCommitHeadPublisherTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "idx";
    private static final int SHARD_ID = 0;

    private ShardStateStore shardStateStore;
    private ObjectStoreCommitHeadPublisher headPublisher;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        shardStateStore = new BlobContainerShardStateStore(blobContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(blobContainer),
            new BlobContainerManifestStore(blobContainer)
        );
        headPublisher = new ObjectStoreCommitHeadPublisher(commitPublisher, shardStateStore);
    }

    private static SegmentInfos commitOneDocument(Directory directory, String id) throws IOException {
        IndexWriterConfig config = new IndexWriterConfig();
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            Document doc = new Document();
            doc.add(new StringField("id", id, Field.Store.YES));
            writer.addDocument(doc);
            writer.commit();
        }
        return SegmentInfos.readLatestCommit(directory);
    }

    public void testFirstEverPublicationActivatesTheShardHeadAtThatTermAtGenerationOne() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitOneDocument(directory, "1");

            boolean published = headPublisher.publishCommitAsHead(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                3,
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            assertTrue(published);
            ShardHead head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(3, head.primaryTerm());
            // The generation is always the live head's + 1, never derived from local Lucene state --
            // the very first publish under a fresh head (generation 0, i.e. ShardHead#initial()'s
            // sentinel) always lands at generation 1, regardless of what segmentInfos.getGeneration()
            // happens to be.
            assertEquals(1, head.latestManifestGeneration());
        }
    }

    public void testSecondPublicationUnderTheSameTermAdvancesTheGenerationByExactlyOne() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos first = commitOneDocument(directory, "1");
            assertTrue(
                headPublisher.publishCommitAsHead(
                    directory,
                    first,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );
            assertEquals(1, shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head().latestManifestGeneration());

            SegmentInfos second = commitOneDocument(directory, "2");
            assertTrue(
                headPublisher.publishCommitAsHead(
                    directory,
                    second,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    1,
                    1,
                    new WalPosition("epoch-0", 1),
                    0,
                    PruningStats.empty()
                )
            );

            ShardHead head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(2, head.latestManifestGeneration());
        }
    }

    public void testPublicationUnderADifferentTermThanTheCurrentHeadIsFencedOut() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitOneDocument(directory, "1");

            // Someone else already activated the shard at term 5 (e.g. this node's lease expired
            // and another node took over) before this writer's commit publication runs.
            assertEquals(
                CasResult.SUCCESS,
                shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), new ShardHead(5, "other-node", 0L, 0L))
            );

            boolean published = headPublisher.publishCommitAsHead(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );

            assertFalse("a stale-term writer must be fenced out, not allowed to publish", published);
            ShardHead head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(5, head.primaryTerm());
            assertEquals(0, head.latestManifestGeneration());
        }
    }

    // Regression test for a real split-brain bug: acquireOrRenewLease deliberately does not advance
    // ShardHead#primaryTerm on lease acquisition (only a real publish does) -- so a node that has
    // acquired the lease under a newer term but has not yet published anything left a stale writer
    // still able to publish under its own older term, fencing solely on primaryTerm. ShardHead#
    // leaseTerm closes this: it advances the instant a lease is acquired/renewed, independent of
    // whether anything has been published under that term yet.
    public void testPublicationIsFencedTheMomentANewerTermAcquiresTheLeaseEvenBeforeItPublishesAnything() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos first = commitOneDocument(directory, "1");
            assertTrue(
                headPublisher.publishCommitAsHead(
                    directory,
                    first,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );
            ShardHead afterFirstPublish = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(1, afterFirstPublish.primaryTerm());
            assertEquals(1, afterFirstPublish.leaseTerm());

            // Node B is promoted to term 2 and acquires the lease, but has not published anything
            // yet -- primaryTerm on the head is still 1.
            assertTrue(headPublisher.acquireOrRenewLease(INDEX_UUID, SHARD_ID, 2, "node-b", Long.MAX_VALUE));
            ShardHead afterAcquire = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(
                "primaryTerm must stay pointing at the last real manifest, not the lease acquirer's term",
                1,
                afterAcquire.primaryTerm()
            );
            // 3, not 2: this is a TAKEOVER (the head's lease holder was null, i.e. nobody's), and a
            // takeover advances the fencing token strictly -- max(existing leaseTerm, acquiring term)
            // + 1 -- rather than merely to the acquirer's own term. That strictness is what makes the
            // token monotonic for a gated index, whose primary term is a compile-time constant and so
            // can never advance on its own. See ShardHead#withTakenOverLease.
            assertEquals("leaseTerm must advance immediately on acquisition, ahead of any publish", 3, afterAcquire.leaseTerm());

            // Node A, still unaware it has been superseded (e.g. partitioned from the cluster
            // manager), tries to publish under its own stale term 1. This must be rejected
            // immediately -- not only once node B gets around to publishing its own first commit.
            SegmentInfos second = commitOneDocument(directory, "2");
            boolean staleWriterPublished = headPublisher.publishCommitAsHead(
                directory,
                second,
                INDEX_UUID,
                SHARD_ID,
                1,
                1,
                1,
                new WalPosition("epoch-0", 1),
                0,
                PruningStats.empty()
            );

            assertFalse(
                "a writer superseded by a newer term's lease acquisition must be fenced out immediately, "
                    + "not only after the new writer's first publish",
                staleWriterPublished
            );
            ShardHead headAfterStaleAttempt = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(1, headAfterStaleAttempt.primaryTerm());
            assertEquals(1, headAfterStaleAttempt.latestManifestGeneration());
        }
    }

    public void testPublicationAfterAConcurrentCompactionUnderTheSameTermSucceedsAtTheNextLiveGeneration() throws Exception {
        // With the writer's generation numbering decoupled from local Lucene state (formally
        // verified in plugins/serverless-storage/formal/ShardHead.tla's PublishDecoupled/
        // SpecDecoupled, ShardHeadDecoupled.cfg), a compactor advancing the head no longer fences the
        // writer out: the writer simply computes its target as the live head's generation + 1, same
        // as the compactor itself does, and lands its own content one slot after whatever the
        // compactor last published.
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos first = commitOneDocument(directory, "1");
            assertTrue(
                headPublisher.publishCommitAsHead(
                    directory,
                    first,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );

            // Simulate a compactor advancing the head well past this writer's first publish, under
            // the same term (exactly what LuceneMergeCompactionPublisher's rebase-on-CAS publish
            // does, independently of this writer's local commit history).
            VersionedShardHead afterFirstPublish = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
            long compactedGeneration = afterFirstPublish.head().latestManifestGeneration() + 100;
            ShardHead compactedHead = afterFirstPublish.head().withPublishedGeneration(compactedGeneration);
            assertEquals(
                CasResult.SUCCESS,
                shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.of(afterFirstPublish.version()), compactedHead)
            );

            SegmentInfos second = commitOneDocument(directory, "2");
            boolean published = headPublisher.publishCommitAsHead(
                directory,
                second,
                INDEX_UUID,
                SHARD_ID,
                1,
                1,
                1,
                new WalPosition("epoch-0", 1),
                0,
                PruningStats.empty()
            );

            assertTrue("a writer resuming after a concurrent compaction must succeed at the next live generation", published);
            ShardHead head = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(compactedGeneration + 1, head.latestManifestGeneration());
        }
    }

    public void testReadLatestManifestWithPinReleasesItsOwnPinIfTheReadAfterwardFails() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore localShardStateStore = new BlobContainerShardStateStore(rawContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(rawContainer),
            new BlobContainerManifestStore(rawContainer)
        );
        ObjectStoreCommitHeadPublisher localHeadPublisher = new ObjectStoreCommitHeadPublisher(commitPublisher, localShardStateStore);
        DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(rawContainer);

        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos first = commitOneDocument(directory, "1");
            assertTrue(
                localHeadPublisher.publishCommitAsHead(
                    directory,
                    first,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );
            VersionedShardHead afterPublish = localShardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();

            // Plant a head pointing one generation past what was actually published -- no manifest
            // blob exists there -- so the read that follows the pin below is guaranteed to fail with
            // IOException, letting this test prove the pin gets released rather than orphaned.
            ShardHead bogusHead = afterPublish.head().withPublishedGeneration(afterPublish.head().latestManifestGeneration() + 1);
            assertEquals(
                CasResult.SUCCESS,
                localShardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.of(afterPublish.version()), bogusHead)
            );

            expectThrows(
                IOException.class,
                () -> localHeadPublisher.readLatestManifestWithPin(INDEX_UUID, SHARD_ID, pinRegistry, "test-snap-uuid")
            );

            assertTrue(
                "a pin registered right before a failed read must be released, not orphaned",
                pinRegistry.getPins(INDEX_UUID, SHARD_ID).isEmpty()
            );
        }
    }

    // Regression test: readLatestManifestWithPin previously used DurablePinRegistry#addPin, which
    // treats pins with the same pinId but a different generation as distinct entries -- so a
    // retried snapshot request (same pinId) landing after a newer generation had published in
    // between left TWO pins under one pinId, leaking the stale generation's pin (protected from GC
    // forever) until this pinId is eventually released outright. replacePin fixes this by making the
    // new generation the sole pin under that pinId.
    public void testReadLatestManifestWithPinReplacesAStalePinFromAnEarlierGenerationUnderTheSamePinId() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore localShardStateStore = new BlobContainerShardStateStore(rawContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(rawContainer),
            new BlobContainerManifestStore(rawContainer)
        );
        ObjectStoreCommitHeadPublisher localHeadPublisher = new ObjectStoreCommitHeadPublisher(commitPublisher, localShardStateStore);
        DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(rawContainer);
        String pinId = "test-snap-uuid";

        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos first = commitOneDocument(directory, "1");
            assertTrue(
                localHeadPublisher.publishCommitAsHead(
                    directory,
                    first,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );
            // Simulates the FIRST attempt of a snapshot request that pinned generation 1, whose
            // response the client never received (so it will retry under the same pinId).
            Optional<CommitManifest> firstAttempt = localHeadPublisher.readLatestManifestWithPin(INDEX_UUID, SHARD_ID, pinRegistry, pinId);
            assertTrue(firstAttempt.isPresent());
            assertEquals(1, pinRegistry.getPins(INDEX_UUID, SHARD_ID).size());

            // A newer commit publishes in between, advancing the head to generation 2.
            SegmentInfos second = commitOneDocument(directory, "2");
            assertTrue(
                localHeadPublisher.publishCommitAsHead(
                    directory,
                    second,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    1,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );

            // The client retries under the SAME pinId, now reading the newer generation.
            Optional<CommitManifest> retry = localHeadPublisher.readLatestManifestWithPin(INDEX_UUID, SHARD_ID, pinRegistry, pinId);
            assertTrue(retry.isPresent());

            assertEquals(
                "the retry must replace the stale generation-1 pin, not accumulate a second pin under the same pinId",
                1,
                pinRegistry.getPins(INDEX_UUID, SHARD_ID).size()
            );
            org.opensearch.serverless.storage.retention.PinRecord onlyPin = pinRegistry.getPins(INDEX_UUID, SHARD_ID).iterator().next();
            assertEquals(pinId, onlyPin.pinId());
            assertEquals(2, onlyPin.generation());
        }
    }

    /**
     * Fails the first {@code writeBlobAtomic}/{@code writeBlob} call whose blob name matches {@code
     * failWhen}, then passes every subsequent call through -- simulates rfc-serverless-opensearch.md
     * &sect;17's "kill writer mid-bundle-upload" / "mid-manifest-write" chaos cases via a single hard
     * exception, same shape as {@code GcSchedulerTaskTests}' own {@code FaultInjectingBlobContainer}.
     */
    private static final class OneShotFailingBlobContainer extends RegisterDelegatingBlobContainer {

        private final Predicate<String> failWhen;
        private boolean failed;

        OneShotFailingBlobContainer(BlobContainer delegate, Predicate<String> failWhen) {
            super(delegate);
            this.failWhen = failWhen;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new OneShotFailingBlobContainer(child, failWhen);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            maybeFail(blobName);
            super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            maybeFail(blobName);
            super.writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        private void maybeFail(String blobName) throws IOException {
            if (failed == false && failWhen.test(blobName)) {
                failed = true;
                throw new IOException("injected fault writing [" + blobName + "]");
            }
        }
    }

    public void testAKilledBundleUploadLeavesTheHeadUntouchedAndARetrySucceeds() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainer faultyContainer = new OneShotFailingBlobContainer(
            rawContainer,
            blobName -> blobName.startsWith(BlobContainerBundleStore.NAME_PREFIX)
        );
        ShardStateStore faultyShardStateStore = new BlobContainerShardStateStore(rawContainer);
        ObjectStoreCommitHeadPublisher faultyHeadPublisher = new ObjectStoreCommitHeadPublisher(
            new ObjectStoreCommitPublisher(new BlobContainerBundleStore(faultyContainer), new BlobContainerManifestStore(faultyContainer)),
            faultyShardStateStore
        );

        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitOneDocument(directory, "1");

            IOException failure = expectThrows(
                IOException.class,
                () -> faultyHeadPublisher.publishCommitAsHead(
                    directory,
                    segmentInfos,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );
            assertTrue(failure.getMessage(), failure.getMessage().contains(BlobContainerBundleStore.NAME_PREFIX));

            // The head must be completely untouched by a publish that never got past the bundle
            // upload -- no orphaned/half-published head, matching ObjectStoreCommitPublisher's own
            // "a crash between them leaves an orphaned bundle, never the reverse" invariant.
            assertTrue("a killed bundle upload must never create a head", faultyShardStateStore.get(INDEX_UUID, SHARD_ID).isEmpty());

            // A retry (the injected fault only fires once) must succeed cleanly and land at generation 1.
            boolean published = faultyHeadPublisher.publishCommitAsHead(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            assertTrue("a retry after the injected fault must succeed", published);
            ShardHead head = faultyShardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(1, head.latestManifestGeneration());
        }
    }

    public void testAKilledManifestWriteLeavesTheHeadUntouchedAndARetrySucceeds() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainer faultyContainer = new OneShotFailingBlobContainer(
            rawContainer,
            blobName -> blobName.startsWith(BlobContainerBundleStore.NAME_PREFIX) == false
        );
        ShardStateStore faultyShardStateStore = new BlobContainerShardStateStore(rawContainer);
        ObjectStoreCommitHeadPublisher faultyHeadPublisher = new ObjectStoreCommitHeadPublisher(
            new ObjectStoreCommitPublisher(new BlobContainerBundleStore(faultyContainer), new BlobContainerManifestStore(faultyContainer)),
            faultyShardStateStore
        );

        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos segmentInfos = commitOneDocument(directory, "1");

            IOException failure = expectThrows(
                IOException.class,
                () -> faultyHeadPublisher.publishCommitAsHead(
                    directory,
                    segmentInfos,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );
            assertFalse(failure.getMessage().contains(BlobContainerBundleStore.NAME_PREFIX));

            // The bundle is now a real, harmless orphan (never referenced by any manifest) and the
            // head is still completely untouched -- exactly ObjectStoreCommitPublisher's own
            // documented "orphaned bundle... never the reverse" safety property, not a corrupted or
            // half-visible state.
            assertTrue("a killed manifest write must never create a head", faultyShardStateStore.get(INDEX_UUID, SHARD_ID).isEmpty());

            boolean published = faultyHeadPublisher.publishCommitAsHead(
                directory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                0,
                0,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            assertTrue("a retry after the injected fault must succeed", published);
            ShardHead head = faultyShardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow().head();
            assertEquals(1, head.latestManifestGeneration());
        }
    }

    /**
     * The append this whole feature depends on: a successful publish that supersedes a real prior head
     * must record exactly that supersession, with the *old* head's own identity -- not the new one's, and
     * not a name it merely guessed at.
     */
    public void testASuccessfulPublicationThatSupersedesAPriorHeadAppendsExactlyOneCandidateForIt() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore localShardStateStore = new BlobContainerShardStateStore(blobContainer);
        FsBlobStore candidateLogBlobStore = new FsBlobStore(1024, createTempDir(), false);
        org.opensearch.serverless.storage.gc.BlobGcCandidateLog gcCandidateLog =
            new org.opensearch.serverless.storage.gc.BlobGcCandidateLog(candidateLogBlobStore::blobContainer, BlobPath.cleanPath());
        ObjectStoreCommitHeadPublisher publisherWithLog = new ObjectStoreCommitHeadPublisher(
            new ObjectStoreCommitPublisher(new BlobContainerBundleStore(blobContainer), new BlobContainerManifestStore(blobContainer)),
            localShardStateStore,
            gcCandidateLog
        );

        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos first = commitOneDocument(directory, "1");
            assertTrue(
                publisherWithLog.publishCommitAsHead(
                    directory,
                    first,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );
            assertTrue(
                "the first-ever publish supersedes nothing -- there was no prior head to name",
                gcCandidateLog.entriesSince(null).isEmpty()
            );

            SegmentInfos second = commitOneDocument(directory, "2");
            assertTrue(
                publisherWithLog.publishCommitAsHead(
                    directory,
                    second,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    1,
                    1,
                    new WalPosition("epoch-0", 1),
                    0,
                    PruningStats.empty()
                )
            );

            java.util.List<org.opensearch.serverless.storage.gc.BlobGcCandidateLog.LoggedCandidate> pending = gcCandidateLog.entriesSince(
                null
            );
            assertEquals("the second publish must record exactly one supersession", 1, pending.size());
            org.opensearch.serverless.storage.gc.GcCandidate candidate = pending.get(0).candidate();
            assertEquals(INDEX_UUID, candidate.indexUuid());
            assertEquals(SHARD_ID, candidate.shardId());
            assertEquals("the candidate must name the OLD head's own identity, not the new one's", 1, candidate.primaryTerm());
            assertEquals(1, candidate.generation());
        }
    }

    /**
     * The bug the IT wiring test actually caught: {@code acquireOrRenewLease} can put a lease-only head at
     * generation 0 in place before any commit is ever published (see that method's own javadoc). The first
     * real publish afterward sees a non-null {@code currentHead} -- but generation 0 was never a real
     * manifest, so this must not append a candidate naming it. {@code currentHead != null} alone is exactly
     * the wrong condition; only a currentGeneration greater than zero means something real was superseded.
     */
    public void testAFirstPublicationAfterOnlyALeaseAcquisitionAppendsNoCandidate() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        ShardStateStore localShardStateStore = new BlobContainerShardStateStore(blobContainer);
        FsBlobStore candidateLogBlobStore = new FsBlobStore(1024, createTempDir(), false);
        org.opensearch.serverless.storage.gc.BlobGcCandidateLog gcCandidateLog =
            new org.opensearch.serverless.storage.gc.BlobGcCandidateLog(candidateLogBlobStore::blobContainer, BlobPath.cleanPath());
        ObjectStoreCommitHeadPublisher publisherWithLog = new ObjectStoreCommitHeadPublisher(
            new ObjectStoreCommitPublisher(new BlobContainerBundleStore(blobContainer), new BlobContainerManifestStore(blobContainer)),
            localShardStateStore,
            gcCandidateLog
        );

        // Puts a lease-only ShardHead(primaryTerm=1, generation=0) in place, exactly as a writer engine
        // activating does before it has published anything of its own.
        assertTrue(publisherWithLog.acquireOrRenewLease(INDEX_UUID, SHARD_ID, 1, "node-a", System.currentTimeMillis() + 30_000));

        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos first = commitOneDocument(directory, "1");
            assertTrue(
                publisherWithLog.publishCommitAsHead(
                    directory,
                    first,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );
        }

        assertTrue(
            "the lease-only placeholder head is not a real manifest -- nothing was actually superseded, so " + "nothing must be appended",
            gcCandidateLog.entriesSince(null).isEmpty()
        );
    }

    /** A {@code null} log (the default, and the off-by-default configuration) must not be reached for at all. */
    public void testANullGcCandidateLogIsNeverDereferenced() throws Exception {
        try (Directory directory = new ByteBuffersDirectory()) {
            SegmentInfos first = commitOneDocument(directory, "1");
            assertTrue(
                headPublisher.publishCommitAsHead(
                    directory,
                    first,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    0,
                    0,
                    new WalPosition("epoch-0", 0),
                    0,
                    PruningStats.empty()
                )
            );
            SegmentInfos second = commitOneDocument(directory, "2");
            // headPublisher (from setUp) was built with the two-argument constructor, so its
            // gcCandidateLog is null -- this succeeding at all, without a NullPointerException, is the
            // assertion.
            assertTrue(
                headPublisher.publishCommitAsHead(
                    directory,
                    second,
                    INDEX_UUID,
                    SHARD_ID,
                    1,
                    1,
                    1,
                    new WalPosition("epoch-0", 1),
                    0,
                    PruningStats.empty()
                )
            );
        }
    }
}
