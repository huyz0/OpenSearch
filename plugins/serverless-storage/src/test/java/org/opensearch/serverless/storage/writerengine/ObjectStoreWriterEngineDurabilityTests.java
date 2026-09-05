/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.writerengine;

import org.apache.lucene.index.Term;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.VersionType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineTestCase;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.serverless.storage.directory.InMemoryShardDirectory;
import org.opensearch.serverless.storage.directory.ShardDirectory;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.wal.WalChunkService;
import org.opensearch.serverless.storage.wal.WalRecord;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Engine-level regression tests for report findings D2 (the WAL replay term floor of
 * {@code currentTerm - 1} dropped acknowledged records once two term bumps happened without an
 * intervening publish), D3 (a lease renewal that answered "you have been superseded" was discarded,
 * so a fenced writer kept acknowledging writes), D7 (WAL shard registration was best-effort, so a
 * writer could write chunks that WAL GC's safety bound could not see), and R1 (any transient
 * object-store error on the publish path failed the engine outright, with no retry anywhere).
 */
public class ObjectStoreWriterEngineDurabilityTests extends EngineTestCase {

    private static final String LOCAL_NODE_ID = "test-node";

    private final ShardDirectory shardDirectory = new InMemoryShardDirectory();

    private BlobContainer newContainer() throws IOException {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    private ObjectStoreCommitPublisher commitPublisher(BlobContainer container) {
        return new ObjectStoreCommitPublisher(new BlobContainerBundleStore(container), new BlobContainerManifestStore(container));
    }

    /**
     * Opens a writer engine over a freshly created store, running local translog recovery, and
     * returns both so the caller can close them.
     */
    private Opened openEngine(ShardStateStore shardStateStore, ObjectStoreCommitPublisher publisher, WalChunkService walChunkService)
        throws Exception {
        return openEngine(shardStateStore, publisher, walChunkService, true);
    }

    /**
     * @param recoverFromTranslog {@code false} constructs the engine but skips local translog
     *                            recovery, which is what a test wanting to observe {@code
     *                            replayWalOperations()} in isolation needs: recovery ends in a
     *                            {@code flush}, and a flush that publishes would move this shard's
     *                            head under the new term and change the very manifest the replay
     *                            floor is derived from.
     */
    private Opened openEngine(
        ShardStateStore shardStateStore,
        ObjectStoreCommitPublisher publisher,
        WalChunkService walChunkService,
        boolean recoverFromTranslog
    ) throws Exception {
        Store store = createStore();
        store.createEmpty(defaultSettings.getIndexVersionCreated().luceneVersion);
        Path translogPath = createTempDir();
        String translogUuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        store.associateIndexWithNewTranslog(translogUuid);
        EngineConfig engineConfig = config(defaultSettings, store, translogPath, newMergePolicy(), null);
        ObjectStoreWriterEngine engine = new ObjectStoreWriterEngine(
            engineConfig,
            new ObjectStoreCommitHeadPublisher(publisher, shardStateStore),
            shardDirectory,
            LOCAL_NODE_ID,
            null,
            walChunkService
        );
        if (recoverFromTranslog) {
            engine.translogManager().recoverFromTranslog(translogHandler, engine.getProcessedLocalCheckpoint(), Long.MAX_VALUE);
        }
        return new Opened(engine, store);
    }

    private record Opened(ObjectStoreWriterEngine engine, Store store) implements AutoCloseable {
        @Override
        public void close() throws IOException {
            IOUtils.close(engine, store);
        }
    }

    private void index(ObjectStoreWriterEngine engine, String id) throws Exception {
        ParsedDocument doc = testParsedDocument(id, null, testDocumentWithTextField(), SOURCE, null);
        engine.index(
            new Engine.Index(
                new Term("_id", org.opensearch.index.mapper.Uid.encodeId(id)),
                doc,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                primaryTerm.get(),
                Versions.MATCH_ANY,
                VersionType.INTERNAL,
                Engine.Operation.Origin.PRIMARY,
                System.nanoTime(),
                -1,
                false,
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                0
            )
        );
    }

    private void appendWalRecord(WalChunkService walChunkService, long term, long seqNo) throws Exception {
        Translog.Index op = new Translog.Index(String.valueOf(seqNo), seqNo, term, ("doc-" + seqNo).getBytes("UTF-8"));
        BytesStreamOutput out = new BytesStreamOutput();
        Translog.Operation.writeOperation(out, op);
        walChunkService.append(
            new WalRecord(shardId.getIndex().getUUID(), shardId.getId(), term, seqNo, BytesReference.toBytes(out.bytes()))
        );
        walChunkService.flush();
    }

    /**
     * <b>D2.</b> Two term bumps without an intervening publish -- a relocation followed by a restart,
     * or simply two consecutive failed activations -- used to make every record from the last
     * publishing term invisible to replay, because the floor was computed as {@code currentTerm - 1}
     * rather than from the manifest the replay actually starts at. Acknowledged data silently gone,
     * and the manifest's own {@code WalPosition} then let WAL GC delete the chunks holding it.
     */
    public void testReplayKeepsRecordsFromTheLastPublishingTermAcrossTwoTermBumps() throws Exception {
        BlobContainer shardContainer = newContainer();
        BlobContainer walContainer = newContainer();
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(shardContainer);
        ObjectStoreCommitPublisher publisher = commitPublisher(shardContainer);
        WalChunkService walChunkService = new WalChunkService(walContainer, "node-epoch-0");

        long term1 = primaryTerm.get();
        try (Opened opened = openEngine(shardStateStore, publisher, walChunkService)) {
            index(opened.engine(), "1");
            opened.engine().flush(true, true);
        }
        // Acknowledged under term1, durable in the WAL, never covered by any manifest.
        appendWalRecord(walChunkService, term1, 1);

        // Term bump #1: a writer activates, replays, and dies before publishing anything.
        primaryTerm.set(term1 + 1);
        try (Opened opened = openEngine(shardStateStore, publisher, walChunkService, false)) {
            assertEquals("the intermediate writer itself must see the record", 1, opened.engine().replayWalOperations().size());
        }

        // Term bump #2, still with no manifest published under term1 + 1.
        primaryTerm.set(term1 + 2);
        try (Opened opened = openEngine(shardStateStore, publisher, walChunkService, false)) {
            List<Translog.Operation> replayed = opened.engine().replayWalOperations();
            assertEquals(
                "the term-"
                    + term1
                    + " record must still replay: the floor is the last published manifest's term, "
                    + "not an arithmetic guess one term back from the current one",
                1,
                replayed.size()
            );
            assertEquals(1L, replayed.get(0).seqNo());
        }
    }

    /**
     * <b>D3.</b> A lease renewal that returns {@code false} is durable, published evidence that
     * another node has taken this shard. It used to be discarded, so this writer went on accepting
     * and acknowledging writes -- records the new writer's activation cutoff then excluded by design,
     * i.e. acknowledged and lost -- until it next happened to flush, which with {@code
     * index.refresh_interval: -1} can be arbitrarily long. Failing the engine bounds that window to
     * one renewal interval.
     */
    public void testALeaseRenewalThatReportsSupersessionFailsTheEngine() throws Exception {
        BlobContainer shardContainer = newContainer();
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(shardContainer);
        ObjectStoreCommitPublisher publisher = commitPublisher(shardContainer);

        try (Opened opened = openEngine(shardStateStore, publisher, null)) {
            assertFalse("a freshly activated writer must not be failed", opened.engine().isEngineFailedForTesting());

            // Another node takes this shard over: the head now records a different lease holder under
            // a strictly higher fencing token. (Written directly rather than via acquireOrRenewLease,
            // because that method correctly REFUSES a takeover while this engine's own lease is still
            // live -- which is the acquire-time half of the same fencing, tested separately below.)
            takeOverLease(shardStateStore, "other-node");

            opened.engine().renewLeaseForTesting();
            assertTrue(
                "a superseded writer must fail its engine rather than keep acknowledging writes it can never publish",
                opened.engine().isEngineFailedForTesting()
            );
        }
    }

    /** A renewal that succeeds must leave the engine alone -- proof the check above is a real comparison, not always-fail. */
    public void testASuccessfulLeaseRenewalLeavesTheEngineRunning() throws Exception {
        BlobContainer shardContainer = newContainer();
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(shardContainer);
        ObjectStoreCommitPublisher publisher = commitPublisher(shardContainer);

        try (Opened opened = openEngine(shardStateStore, publisher, null)) {
            opened.engine().renewLeaseForTesting();
            assertFalse(opened.engine().isEngineFailedForTesting());
        }
    }

    /**
     * <b>D7.</b> {@code WalGcSchedulerTask} computes its deletable bound over <em>registered</em>
     * shards only, and chunk sequences are one global counter -- so a writer whose registration never
     * landed can have its chunks deleted while it still needs them for replay. Registration used to
     * be best-effort (one attempt, exception swallowed, engine came up and wrote anyway); it is now a
     * precondition, so a writer that cannot register refuses to activate at all.
     */
    public void testAWriterThatCannotRegisterInTheWalShardRegistryRefusesToActivate() throws Exception {
        BlobContainer shardContainer = newContainer();
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(shardContainer);
        ObjectStoreCommitPublisher publisher = commitPublisher(shardContainer);
        // Registers are still readable (so the activation WAL-position snapshot succeeds); only blob
        // writes fail, which is exactly what WalShardRegistry#register does.
        WalChunkService walChunkService = new WalChunkService(new WriteRejectingBlobContainer(newContainer()), "node-epoch-0");

        Store store = createStore();
        try {
            store.createEmpty(defaultSettings.getIndexVersionCreated().luceneVersion);
            Path translogPath = createTempDir();
            String translogUuid = Translog.createEmptyTranslog(translogPath, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
            store.associateIndexWithNewTranslog(translogUuid);
            EngineConfig engineConfig = config(defaultSettings, store, translogPath, newMergePolicy(), null);

            EngineException failure = expectThrows(
                EngineException.class,
                () -> new ObjectStoreWriterEngine(
                    engineConfig,
                    new ObjectStoreCommitHeadPublisher(publisher, shardStateStore),
                    shardDirectory,
                    LOCAL_NODE_ID,
                    null,
                    walChunkService
                )
            );
            assertTrue(
                "the failure must say why activating anyway is unsafe, got: " + failure.getMessage(),
                failure.getMessage().contains("WAL shard registry")
            );
        } finally {
            IOUtils.close(store);
        }
    }

    /**
     * <b>R1.</b> A transient object-store failure during publication must be retried, not fatal.
     * There was no retry or backoff anywhere on this path, and {@code commitIndexWriter} caught every
     * exception and called {@code failEngine} -- so one 503 killed the shard, and under a brownout
     * every writer shard on every node failed within one refresh interval.
     */
    public void testATransientPublishFailureIsRetriedRatherThanFailingTheEngine() throws Exception {
        BlobContainer raw = newContainer();
        FailNAtomicWritesBlobContainer flaky = new FailNAtomicWritesBlobContainer(raw, 2);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(flaky);
        ObjectStoreCommitPublisher publisher = commitPublisher(flaky);

        try (Opened opened = openEngine(shardStateStore, publisher, null)) {
            index(opened.engine(), "1");
            opened.engine().flush(true, true);

            assertFalse("a publish that eventually succeeded must not have failed the engine", opened.engine().isEngineFailedForTesting());
            assertTrue(
                "the retried publish must have actually installed a head",
                shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId()).isPresent()
            );
        }
    }

    /**
     * <b>R1, the other half.</b> Even when the retry budget is exhausted, the engine must survive: an
     * unpublished commit is already durable locally, so the honest response is a failed flush that a
     * later one retries, not a destroyed shard and the reallocation stampede
     * rfc-serverless-opensearch.md &sect;13 says must not happen.
     */
    public void testAnExhaustedPublishRetryBudgetFailsTheFlushButNotTheEngine() throws Exception {
        BlobContainer raw = newContainer();
        FailNAtomicWritesBlobContainer alwaysFailing = new FailNAtomicWritesBlobContainer(raw, Integer.MAX_VALUE);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(alwaysFailing);
        ObjectStoreCommitPublisher publisher = commitPublisher(alwaysFailing);

        try (Opened opened = openEngine(shardStateStore, publisher, null)) {
            index(opened.engine(), "1");
            expectThrows(Exception.class, () -> opened.engine().flush(true, true));

            assertFalse(
                "a transient I/O failure -- however persistent -- is not a fencing verdict and must not fail the engine",
                opened.engine().isEngineFailedForTesting()
            );
            assertTrue(
                "the attempt budget must actually have been spent, not abandoned after one try",
                alwaysFailing.attempts.get() >= ObjectStoreWriterEngine.PUBLISH_ATTEMPTS
            );
        }
    }

    /** Installs a head whose lease is held by {@code otherNodeId} under a strictly higher fencing token. */
    private void takeOverLease(ShardStateStore shardStateStore, String otherNodeId) throws IOException {
        VersionedShardHead current = shardStateStore.get(shardId.getIndex().getUUID(), shardId.getId()).orElseThrow();
        ShardHead taken = current.head().withTakenOverLease(otherNodeId, System.currentTimeMillis() + 30_000, primaryTerm.get());
        assertEquals(
            CasResult.SUCCESS,
            shardStateStore.compareAndSet(shardId.getIndex().getUUID(), shardId.getId(), java.util.Optional.of(current.version()), taken)
        );
    }

    /**
     * <b>P-1 (control-plane finding F1), publish half.</b> Fencing used to be
     * {@code currentHead.leaseTerm() > primaryTerm}. Every gated shard's primary term is the
     * compile-time constant {@code IndexDescriptor.FIRST_PRIMARY_TERM = 1} and nothing advances it, so
     * that comparison was {@code 1 > 1} -- false forever, the refusal branch dead code, and two nodes
     * could both publish complete but divergent manifest lineages onto the same head. A displaced
     * writer's publish must now be refused on the lease token it acquired under, and refusal must fail
     * the engine rather than be retried.
     */
    public void testAPublishByAWriterWhoseLeaseWasTakenOverIsFencedAndFailsTheEngine() throws Exception {
        BlobContainer shardContainer = newContainer();
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(shardContainer);
        ObjectStoreCommitPublisher publisher = commitPublisher(shardContainer);

        try (Opened opened = openEngine(shardStateStore, publisher, null)) {
            index(opened.engine(), "1");
            opened.engine().flush(true, true);
            assertFalse(opened.engine().isEngineFailedForTesting());

            takeOverLease(shardStateStore, "other-node");

            index(opened.engine(), "2");
            expectThrows(Exception.class, () -> opened.engine().flush(true, true));
            assertTrue(
                "a writer whose tenancy the head no longer recognises must be fenced out, not allowed to "
                    + "publish a second lineage on top of the node that displaced it",
                opened.engine().isEngineFailedForTesting()
            );
        }
    }

    /**
     * <b>P-1, acquire half.</b> A second node must be refused the lease while the first still holds a
     * live one -- refused on node identity, since for a gated index a term comparison can never tell
     * two nodes apart.
     */
    public void testASecondWriterCannotActivateWhileTheFirstHoldsALiveLease() throws Exception {
        BlobContainer shardContainer = newContainer();
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(shardContainer);
        ObjectStoreCommitPublisher publisher = commitPublisher(shardContainer);

        try (Opened first = openEngine(shardStateStore, publisher, null)) {
            assertFalse(first.engine().isEngineFailedForTesting());

            ObjectStoreCommitHeadPublisher other = new ObjectStoreCommitHeadPublisher(publisher, shardStateStore);
            assertTrue(
                "the second node must be refused while the first node's lease is live",
                other.acquireOrRenewLease(
                    shardId.getIndex().getUUID(),
                    shardId.getId(),
                    primaryTerm.get(),
                    "other-node",
                    System.currentTimeMillis() + 30_000,
                    System.currentTimeMillis()
                ).isEmpty()
            );
        }
    }

    /** Every blob write fails; registers still work, so only registry/marker writes are affected. */
    private static final class WriteRejectingBlobContainer extends org.opensearch.common.blobstore.support.FilterBlobContainer {
        private final BlobContainer raw;

        WriteRejectingBlobContainer(BlobContainer raw) {
            super(raw);
            this.raw = raw;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new WriteRejectingBlobContainer(child);
        }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
            throw new IOException("injected: this container refuses writes");
        }

        @Override
        public java.util.Optional<org.opensearch.common.blobstore.BlobRegister> readRegister(String blobName) throws IOException {
            return raw.readRegister(blobName);
        }

        @Override
        public org.opensearch.common.blobstore.BlobRegisterCasResult compareAndSwapRegister(
            String blobName,
            long expectedGeneration,
            BytesReference newValue
        ) throws IOException {
            return raw.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }

    /** Fails the first {@code failures} atomic writes (bundle and manifest uploads), then passes them through. */
    private static final class FailNAtomicWritesBlobContainer extends org.opensearch.common.blobstore.support.FilterBlobContainer {
        private final BlobContainer raw;
        private final int failures;
        final AtomicInteger attempts = new AtomicInteger();

        FailNAtomicWritesBlobContainer(BlobContainer raw, int failures) {
            super(raw);
            this.raw = raw;
            this.failures = failures;
        }

        @Override
        protected BlobContainer wrapChild(BlobContainer child) {
            return new FailNAtomicWritesBlobContainer(child, failures);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            if (attempts.incrementAndGet() <= failures) {
                throw new IOException("injected transient object-store failure writing " + blobName);
            }
            super.writeBlobAtomic(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override
        public java.util.Optional<org.opensearch.common.blobstore.BlobRegister> readRegister(String blobName) throws IOException {
            return raw.readRegister(blobName);
        }

        @Override
        public org.opensearch.common.blobstore.BlobRegisterCasResult compareAndSwapRegister(
            String blobName,
            long expectedGeneration,
            BytesReference newValue
        ) throws IOException {
            return raw.compareAndSwapRegister(blobName, expectedGeneration, newValue);
        }
    }
}
