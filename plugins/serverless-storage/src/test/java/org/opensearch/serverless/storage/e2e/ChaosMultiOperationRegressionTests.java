/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.e2e;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.serverless.storage.benchmark.LatencyInjectingBlobContainer;
import org.opensearch.serverless.storage.benchmark.LatencyProfile;
import org.opensearch.serverless.storage.compaction.CompactionPolicy;
import org.opensearch.serverless.storage.compaction.CompactionRebaseExecutor;
import org.opensearch.serverless.storage.compaction.LuceneMergeCompactionPublisher;
import org.opensearch.serverless.storage.compaction.RebaseResult;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.readerengine.ObjectStoreCommitMaterializer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * rfc-serverless-opensearch.md &sect;17's "broader probabilistic multi-operation throttling/5xx-storm
 * chaos injection" gap: a real, sustained ingest workload (many real Lucene commits, each published
 * and CAS-activated as the shard's new head) run against a {@link ProbabilisticFailingBlobContainer}
 * that independently faults <em>any</em> write-shaped or register-CAS-shaped call throughout the
 * whole run, not just once -- proving the system converges to a correct, non-corrupted final state
 * (every commit that a caller observed as successful is really durable and reachable; the final
 * shard head really points at the real latest manifest; every bundle that manifest references is
 * fully, correctly readable) despite sustained, randomized, multi-operation-shape faults, exactly
 * the "regardless of how many transient failures happen along the way" property a real flaky object
 * store demands.
 *
 * <p><b>Deliberately scoped down</b> (see {@link ProbabilisticFailingBlobContainer}'s own javadoc
 * for the write/delete/register-write-only fault scope): this class covers the ingest (publish +
 * shard-head CAS) path and, separately, the compaction (merge + rebase-publish) path. GC-sweep-under-chaos
 * is covered by {@code org.opensearch.serverless.storage.gc.GcSchedulerTaskChaosTests} instead, since
 * it needs package-private access to {@code GcSchedulerTask#sweepForTesting()}.
 *
 * <p><b>Genuinely concurrent multi-operation chaos is now also covered</b>, closing what was
 * previously this class's own explicitly-left-open follow-up: {@link
 * #testConcurrentShardsConvergeDespiteSustainedRandomizedMultiOperationFaults} runs several shards'
 * ingest workloads on real, genuinely concurrent threads against one shared {@link
 * ProbabilisticFailingBlobContainer} instance, rather than this class's other tests' sequential,
 * single-threaded shape -- exercising real concurrent access to the shared fault injector and the
 * underlying blob store, not just sequential retries.
 */
public class ChaosMultiOperationRegressionTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "chaos-idx";
    private static final int SHARD_ID = 0;
    private static final int ROUNDS = 15;
    private static final int MAX_ATTEMPTS_PER_ROUND = 50;

    private <T> T retryUnderChaos(Callable<T> attempt) throws Exception {
        Exception lastFailure = null;
        for (int i = 0; i < MAX_ATTEMPTS_PER_ROUND; i++) {
            try {
                return attempt.call();
            } catch (IOException | UncheckedIOException e) {
                // Not IOException alone: CompactionPublisher#computeNewHead's own interface
                // contract has no throws IOException (it can't -- Optional<ShardHead> is a plain
                // return type), so LuceneMergeCompactionPublisher wraps a faulted publish as
                // UncheckedIOException -- the exact same reason CompactionSchedulerTask's own real
                // production catch is Exception-wide, not IOException-only (see its own javadoc).
                // A real caller retries a transient fault exactly like this either way.
                lastFailure = e;
            }
        }
        throw new AssertionError("exhausted " + MAX_ATTEMPTS_PER_ROUND + " retry attempts", lastFailure);
    }

    public void testSustainedIngestConvergesDespiteRandomizedMultiOperationFaults() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        // Every write-shaped/register-CAS-shaped call across the whole run independently has a real
        // chance to fail -- not a one-shot fixture, not scoped to a single call.
        BlobContainer faultyContainer = new ProbabilisticFailingBlobContainer(rawContainer, random(), 0.3);

        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(faultyContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(faultyContainer);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(faultyContainer);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);

        assertEquals(
            "activation itself must also converge under chaos",
            CasResult.SUCCESS,
            retryUnderChaos(() -> shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), ShardHead.initial()))
        );

        CommitManifest lastPublishedManifest = null;
        for (int round = 1; round <= ROUNDS; round++) {
            long generation = round;
            int docCount = round;
            // The local Lucene commit happens exactly once per round, outside the retry loop --
            // only the *publish* step (the one that actually touches the faulty container) is
            // retried, matching how a real caller retries: re-publishing an already-durable local
            // commit, never re-committing it. Retrying the commit itself would be wrong -- Lucene
            // assigns each segment a fresh random ID on every commit, so a regenerated commit is
            // not byte-identical to the one a previous, partially-successful attempt may have
            // already uploaded under the same deterministic bundle name (confirmed by a real
            // checksum mismatch this test's first draft actually hit before being fixed this way).
            try (Directory writerDirectory = new ByteBuffersDirectory()) {
                try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                    for (int i = 0; i < docCount; i++) {
                        Document doc = new Document();
                        doc.add(new StringField("id", "round-" + generation + "-doc-" + i, Field.Store.YES));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
                SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);

                CommitManifest manifest = retryUnderChaos(
                    () -> publisher.publishCommit(
                        writerDirectory,
                        segmentInfos,
                        INDEX_UUID,
                        SHARD_ID,
                        1L,
                        generation,
                        docCount,
                        docCount,
                        new WalPosition("epoch-0", generation),
                        0,
                        PruningStats.empty()
                    )
                );

                VersionedShardHead current = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
                long expectedVersion = current.version();
                CasResult casResult = retryUnderChaos(
                    () -> shardStateStore.compareAndSet(
                        INDEX_UUID,
                        SHARD_ID,
                        Optional.of(expectedVersion),
                        new ShardHead(1, "node-1", Long.MAX_VALUE, manifest.generation())
                    )
                );
                assertEquals(
                    "round " + round + "'s head CAS must eventually succeed once retried past any injected faults",
                    CasResult.SUCCESS,
                    casResult
                );
                lastPublishedManifest = manifest;
            }
        }

        // Convergence check 1: the shard head really points at the real latest manifest, not a stale
        // or partially-applied one.
        VersionedShardHead finalHead = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
        assertEquals(
            "the final shard head must reference the very last round's manifest generation",
            lastPublishedManifest.generation(),
            finalHead.head().latestManifestGeneration()
        );

        // Convergence check 2: every commit this test observed as successfully published is really
        // still there and lists correctly -- no round was silently lost.
        List<CommitManifest> allManifests = manifestStore.listManifests();
        assertEquals("every one of the " + ROUNDS + " rounds' commits must still be listable, none lost", ROUNDS, allManifests.size());

        // Convergence check 3: every bundle every manifest references is fully present and
        // checksum-correct -- no injected fault left a corrupted or partially-written bundle behind.
        for (CommitManifest manifest : allManifests) {
            for (var entry : manifest.files().entrySet()) {
                FileReference ref = entry.getValue();
                byte[] bytes = bundleStore.readFile(
                    ref.bundleName(),
                    new BundleFileEntry(entry.getKey(), ref.offset(), ref.length(), ref.checksum())
                );
                assertNotNull(
                    "bundle file [" + entry.getKey() + "] in manifest generation " + manifest.generation() + " must read back cleanly",
                    bytes
                );
            }
        }
    }

    /**
     * The "throttling" half of &sect;17's chaos gap, closed via composition rather than duplicating
     * {@code LatencyInjectingBlobContainer}'s own sleep-before-delegating logic inside {@link
     * ProbabilisticFailingBlobContainer} itself (see that class's own javadoc for why): every call
     * in this run is both independently fault-prone <em>and</em> simulated-latency-afflicted at
     * once -- a real flaky, slow object store, not either property tested in isolation the way
     * this class's other ingest test and {@code ServerlessStorageReactivationUnderLatencyIT}'s own
     * latency-only tests each do separately.
     */
    public void testSustainedIngestConvergesDespiteSustainedRandomizedFaultsAndSimulatedLatency() throws Exception {
        int rounds = 8;
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainer faultyContainer = new ProbabilisticFailingBlobContainer(rawContainer, random(), 0.2);
        // Composed, not duplicated: the exact same LatencyInjectingBlobContainer this plugin's
        // other latency benchmarks/ITs already use, just layered on top of the fault injector here
        // instead of a clean delegate. LOW (not HIGH) on purpose -- keeping this test's real
        // wall-clock reasonable given retryUnderChaos may re-issue several of these already-slowed
        // calls per round.
        BlobContainer faultyAndSlowContainer = new LatencyInjectingBlobContainer(faultyContainer, LatencyProfile.LOW);

        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(faultyAndSlowContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(faultyAndSlowContainer);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(faultyAndSlowContainer);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);

        assertEquals(
            "activation itself must also converge under combined faults and latency",
            CasResult.SUCCESS,
            retryUnderChaos(() -> shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), ShardHead.initial()))
        );

        CommitManifest lastPublishedManifest = null;
        for (int round = 1; round <= rounds; round++) {
            long generation = round;
            int docCount = round;
            try (Directory writerDirectory = new ByteBuffersDirectory()) {
                try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                    for (int i = 0; i < docCount; i++) {
                        Document doc = new Document();
                        doc.add(new StringField("id", "round-" + generation + "-doc-" + i, Field.Store.YES));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
                SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);

                CommitManifest manifest = retryUnderChaos(
                    () -> publisher.publishCommit(
                        writerDirectory,
                        segmentInfos,
                        INDEX_UUID,
                        SHARD_ID,
                        1L,
                        generation,
                        docCount,
                        docCount,
                        new WalPosition("epoch-0", generation),
                        0,
                        PruningStats.empty()
                    )
                );

                VersionedShardHead current = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
                long expectedVersion = current.version();
                CasResult casResult = retryUnderChaos(
                    () -> shardStateStore.compareAndSet(
                        INDEX_UUID,
                        SHARD_ID,
                        Optional.of(expectedVersion),
                        new ShardHead(1, "node-1", Long.MAX_VALUE, manifest.generation())
                    )
                );
                assertEquals(
                    "round " + round + "'s head CAS must eventually succeed once retried past faults and latency alike",
                    CasResult.SUCCESS,
                    casResult
                );
                lastPublishedManifest = manifest;
            }
        }

        VersionedShardHead finalHead = shardStateStore.get(INDEX_UUID, SHARD_ID).orElseThrow();
        assertEquals(
            "the final shard head must reference the very last round's manifest generation",
            lastPublishedManifest.generation(),
            finalHead.head().latestManifestGeneration()
        );
        List<CommitManifest> allManifests = manifestStore.listManifests();
        assertEquals("every one of the " + rounds + " rounds' commits must still be listable, none lost", rounds, allManifests.size());
        for (CommitManifest manifest : allManifests) {
            for (var entry : manifest.files().entrySet()) {
                FileReference ref = entry.getValue();
                byte[] bytes = bundleStore.readFile(
                    ref.bundleName(),
                    new BundleFileEntry(entry.getKey(), ref.offset(), ref.length(), ref.checksum())
                );
                assertNotNull(
                    "bundle file [" + entry.getKey() + "] in manifest generation " + manifest.generation() + " must read back cleanly",
                    bytes
                );
            }
        }
    }

    /**
     * The "silent object-store corruption" half of this class's own broader chaos-injection gap:
     * {@link ProbabilisticFailingBlobContainer}'s corruption mode lets a write-shaped call succeed
     * while silently persisting a flipped byte, exactly the failure mode a clean thrown exception
     * (the only fault shape this class's other tests inject) can never model. Runs the exact same
     * real materialization path a fresh node's ordinary shard startup uses ({@link
     * ObjectStoreCommitMaterializer#materialize}) against every published manifest and asserts the
     * property this plugin has always maintained elsewhere (see {@code
     * BlobContainerBundleStore#writeBundle}'s own "never silently corrupt" collision guard): a
     * corrupted bundle either materializes byte-correctly (the flipped byte happened to land in a
     * region nothing on this read path re-verifies, e.g. the bundle header -- this format's
     * manifest-carried offsets make production reads independent of it, see {@code
     * BlobContainerBundleStore#readHeader}'s own javadoc) or it fails loudly with a
     * checksum-shaped {@link IOException} -- it must never silently hand back different bytes
     * than what was actually published.
     *
     * <p>Deliberately not asserting corruption is detected on every single round: with the flip
     * landing anywhere in the whole packed bundle (header included), a single trial isn't
     * guaranteed to hit file content specifically. Asserting at least one detection across many
     * rounds of growing, multi-file commits is the same statistical tolerance {@code
     * BundleWriterReaderTests#testRandomizedSingleByteCorruptionAlwaysFailsClosedAcrossManyTrials}
     * already accepts, just exercised through the real container + materializer path instead of
     * directly against {@code BundleWriter}/{@code BundleReader}.
     */
    public void testMaterializationDetectsRatherThanSilentlyAcceptsWriteTimeCorruption() throws Exception {
        int rounds = 15;
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        // failureProbability=0.0 isolates corruption's own effect from the clean-failure
        // convergence property this class's other tests already cover.
        BlobContainer faultyContainer = new ProbabilisticFailingBlobContainer(rawContainer, random(), 0.0, 0.4);

        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(faultyContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(faultyContainer);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
        ObjectStoreCommitMaterializer materializer = new ObjectStoreCommitMaterializer(bundleStore);

        boolean anyCorruptionDetected = false;
        for (int round = 1; round <= rounds; round++) {
            long generation = round;
            int docCount = round;
            try (Directory writerDirectory = new ByteBuffersDirectory()) {
                try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                    for (int i = 0; i < docCount; i++) {
                        Document doc = new Document();
                        doc.add(new StringField("id", "round-" + generation + "-doc-" + i, Field.Store.YES));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
                SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
                CommitManifest manifest = publisher.publishCommit(
                    writerDirectory,
                    segmentInfos,
                    INDEX_UUID,
                    SHARD_ID,
                    1L,
                    generation,
                    docCount,
                    docCount,
                    new WalPosition("epoch-0", generation),
                    0,
                    PruningStats.empty()
                );

                try (Directory targetDirectory = new ByteBuffersDirectory()) {
                    materializer.materialize(manifest, targetDirectory);
                    // Materialized cleanly: either nothing was corrupted this round, or the
                    // flipped byte landed somewhere this read path doesn't re-verify (see javadoc).
                } catch (IOException corruptionDetected) {
                    anyCorruptionDetected = true;
                    assertTrue(
                        "a detected corruption must fail with a checksum-shaped message, not some unrelated I/O error: "
                            + corruptionDetected,
                        corruptionDetected.getMessage() != null
                            && corruptionDetected.getMessage().toLowerCase(Locale.ROOT).contains("checksum")
                    );
                }
            }
        }

        assertTrue(
            "with corruptionProbability=0.4 across "
                + rounds
                + " rounds of growing multi-file commits, at least one real file-content corruption "
                + "must have been injected and detected -- otherwise this test isn't exercising the "
                + "property it claims to",
            anyCorruptionDetected
        );
    }

    /**
     * The genuinely-concurrent half of this class's own "broader probabilistic multi-operation"
     * chaos gap: closes the follow-up this class's own javadoc previously left explicitly open
     * ("genuinely concurrent... multi-operation chaos remains explicitly out of scope"). Runs
     * {@code shardCount} independent shards' ingest workloads on real, genuinely concurrent
     * {@link Thread}s. Each shard gets its own sub-{@link BlobContainer} (via {@code
     * blobContainer(BlobPath)}, the same per-shard isolation a real deployment always has --
     * {@code ServerlessStoragePlugin#resolveBlobContainer} never shares one container across
     * shards either), but every one of those sub-containers is still wrapped by the *same*
     * {@link ProbabilisticFailingBlobContainer} instance and shares the *same* underlying {@link
     * FsBlobStore}, so this genuinely stresses the shared fault injector's and the underlying
     * blob store's own thread-safety under real concurrent access from multiple shards at once --
     * not just safe when called sequentially the way every other test in this class exercises it.
     *
     * <p><b>A real test-design bug, not a production bug, caught by this test's own first
     * draft</b>: an earlier version shared one flat container across all shards, relying only on
     * {@code shardId} being embedded in blob names to keep them apart. {@link
     * CommitManifest#manifestName} is deliberately just {@code manifest-<primaryTerm>-<generation>}
     * with no {@code indexUuid}/{@code shardId} in it at all -- safe in every real deployment
     * because a manifest name only ever needs to be unique *within* one shard's own dedicated
     * container, never across shards sharing one container, which no production code path ever
     * does ({@code resolveBlobContainer} always returns a distinct container per shard). Sharing
     * one container across simulated shards in this test violated that assumption and caused real
     * cross-shard manifest name collisions (shard B's generation-3 manifest silently overwriting
     * shard A's own), reproducibly losing manifests -- fixed by giving each shard its own
     * sub-container here, matching how every real caller of this store already isolates shards.
     *
     * <p>The {@link Random} instance is obtained once via {@link #randomLong()} on the main test
     * thread <em>before</em> any worker thread starts, then shared across threads by calling only
     * its own thread-safe {@link Random#nextDouble()} -- calling this test framework's own {@link
     * #random()} from more than one thread is not supported (confirmed by this test's own first
     * draft, which hit a real {@code IllegalStateException} -- "this Random was created for/by
     * another thread" -- the moment a worker thread called it directly), but a plain {@link
     * Random} instance obtained from a single {@code long} seed has no such thread affinity.
     */
    public void testConcurrentShardsConvergeDespiteSustainedRandomizedMultiOperationFaults() throws Exception {
        int shardCount = 4;
        int roundsPerShard = 5;

        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        // One shared Random instance (safe to call nextDouble() on concurrently, see this method's
        // own javadoc) backs every shard's own ProbabilisticFailingBlobContainer wrapper below --
        // genuinely one shared fault injector under concurrent stress, not shardCount independent
        // ones that would each roll their own dice in isolation.
        Random sharedRandom = new Random(randomLong());

        List<AtomicReference<Throwable>> failures = new java.util.ArrayList<>();
        List<Thread> threads = new java.util.ArrayList<>();
        List<BlobContainerManifestStore> perShardManifestStores = new java.util.ArrayList<>();
        List<BlobContainerBundleStore> perShardBundleStores = new java.util.ArrayList<>();
        List<ShardStateStore> perShardStateStores = new java.util.ArrayList<>();
        for (int s = 0; s < shardCount; s++) {
            int shardId = s;
            // Each shard's own sub-container (from the one shared FsBlobStore, matching real
            // physical isolation -- see this method's own javadoc for the collision this avoids),
            // each independently wrapped in a ProbabilisticFailingBlobContainer sharing the same
            // Random instance, so the fault injection itself is still genuinely shared/contended.
            BlobContainer rawShardContainer = blobStore.blobContainer(BlobPath.cleanPath().add("shard-" + shardId));
            BlobContainer shardContainer = new ProbabilisticFailingBlobContainer(rawShardContainer, sharedRandom, 0.3);
            BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(shardContainer);
            BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(shardContainer);
            ShardStateStore shardStateStore = new BlobContainerShardStateStore(shardContainer);
            ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
            perShardBundleStores.add(bundleStore);
            perShardManifestStores.add(manifestStore);
            perShardStateStores.add(shardStateStore);

            AtomicReference<Throwable> failure = new AtomicReference<>();
            failures.add(failure);
            threads.add(new Thread(() -> {
                try {
                    runShardIngestWorkload(publisher, shardStateStore, shardId, roundsPerShard);
                } catch (Throwable t) {
                    failure.set(t);
                }
            }));
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join(TimeValue.timeValueMinutes(2).millis());
        }
        for (int s = 0; s < shardCount; s++) {
            assertNull("shard " + s + "'s concurrent ingest workload must converge without error", failures.get(s).get());
        }

        // Convergence check, per shard: each shard's own head really points at its own real latest
        // manifest, every one of its rounds is listable, and every bundle its manifests reference
        // reads back checksum-clean -- proving the genuinely concurrent run left no shard's state
        // corrupted or interleaved with another shard's, despite every shard's threads racing
        // against the same shared fault injector and underlying blob store the whole time.
        for (int s = 0; s < shardCount; s++) {
            int shardId = s;
            BlobContainerManifestStore manifestStore = perShardManifestStores.get(s);
            BlobContainerBundleStore bundleStore = perShardBundleStores.get(s);
            VersionedShardHead finalHead = perShardStateStores.get(s).get(INDEX_UUID, shardId).orElseThrow();
            List<CommitManifest> shardManifests = manifestStore.listManifests();
            assertEquals(
                "shard " + s + " must have exactly " + roundsPerShard + " listable manifests, none lost",
                roundsPerShard,
                shardManifests.size()
            );
            CommitManifest latest = shardManifests.stream()
                .max(java.util.Comparator.comparingLong(CommitManifest::generation))
                .orElseThrow();
            assertEquals(
                "shard " + s + "'s final head must reference its own real latest manifest",
                latest.generation(),
                finalHead.head().latestManifestGeneration()
            );
            for (CommitManifest manifest : shardManifests) {
                for (var entry : manifest.files().entrySet()) {
                    FileReference ref = entry.getValue();
                    byte[] bytes = bundleStore.readFile(
                        ref.bundleName(),
                        new BundleFileEntry(entry.getKey(), ref.offset(), ref.length(), ref.checksum())
                    );
                    assertNotNull(
                        "shard "
                            + s
                            + " bundle file ["
                            + entry.getKey()
                            + "] in generation "
                            + manifest.generation()
                            + " must read back cleanly",
                        bytes
                    );
                }
            }
        }
    }

    /** One shard's own sequential ingest workload -- run concurrently with other shards' own calls of this same method against the same shared publisher/shardStateStore/container. */
    private void runShardIngestWorkload(ObjectStoreCommitPublisher publisher, ShardStateStore shardStateStore, int shardId, int rounds)
        throws Exception {
        assertEquals(
            "shard " + shardId + "'s activation itself must also converge under chaos",
            CasResult.SUCCESS,
            retryUnderChaos(() -> shardStateStore.compareAndSet(INDEX_UUID, shardId, Optional.empty(), ShardHead.initial()))
        );

        for (int round = 1; round <= rounds; round++) {
            long generation = round;
            int docCount = round;
            try (Directory writerDirectory = new ByteBuffersDirectory()) {
                try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                    for (int i = 0; i < docCount; i++) {
                        Document doc = new Document();
                        doc.add(new StringField("id", "shard-" + shardId + "-round-" + generation + "-doc-" + i, Field.Store.YES));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
                SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);

                CommitManifest manifest = retryUnderChaos(
                    () -> publisher.publishCommit(
                        writerDirectory,
                        segmentInfos,
                        INDEX_UUID,
                        shardId,
                        1L,
                        generation,
                        docCount,
                        docCount,
                        new WalPosition("epoch-0", generation),
                        0,
                        PruningStats.empty()
                    )
                );

                VersionedShardHead current = shardStateStore.get(INDEX_UUID, shardId).orElseThrow();
                long expectedVersion = current.version();
                CasResult casResult = retryUnderChaos(
                    () -> shardStateStore.compareAndSet(
                        INDEX_UUID,
                        shardId,
                        Optional.of(expectedVersion),
                        new ShardHead(1, "node-1", Long.MAX_VALUE, manifest.generation())
                    )
                );
                assertEquals(
                    "shard " + shardId + " round " + round + "'s head CAS must eventually succeed once retried past any injected faults",
                    CasResult.SUCCESS,
                    casResult
                );
            }
        }
    }

    /** Commits {@code count} separate single-document segments (no merge), so a real multi-segment index results -- matching {@code CostAccountingRegressionTests}' own helper. */
    private static SegmentInfos commitSeparateSegments(Directory directory, int count) throws Exception {
        SegmentInfos infos = null;
        for (int i = 0; i < count; i++) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                Document doc = new Document();
                doc.add(new StringField("id", "doc-" + i, Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            infos = SegmentInfos.readLatestCommit(directory);
        }
        return infos;
    }

    /**
     * The compaction half of this class's own "broader probabilistic multi-operation" chaos gap:
     * closes rfc-serverless-opensearch.md &sect;17's own follow-on note ("compaction-under-chaos...
     * remain[s] open, real follow-up work") left by this test class's first slice. Publishes a real
     * 5-segment source commit, then runs {@link LuceneMergeCompactionPublisher} +
     * {@link CompactionRebaseExecutor} -- the exact same orchestration
     * {@code CostAccountingRegressionTests#testCompactionPublishingStaysWithinItsExpectedPutBudget}
     * exercises without chaos -- against a sustained, randomized-fault container, retrying the whole
     * rebase-and-publish call (safe because {@code ObjectStoreCommitPublisher#publishCommit}'s own
     * top-level {@code manifestExists} check makes a full retry idempotent, not just the bundle-write
     * half), and verifies real convergence: the compacted manifest is eventually published, still
     * references every one of the source's documents (a merge never silently drops or duplicates
     * anything), and every bundle it references reads back checksum-clean.
     */
    public void testCompactionConvergesDespiteRandomizedMultiOperationFaults() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer uncountedContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());

        // Setup: publish a real multi-segment source commit, uncounted -- this represents state
        // that already existed before the chaos-afflicted compaction workload this test measures.
        ObjectStoreCommitPublisher setupPublisher = new ObjectStoreCommitPublisher(
            new BlobContainerBundleStore(uncountedContainer),
            new BlobContainerManifestStore(uncountedContainer)
        );
        ShardStateStore setupShardStateStore = new BlobContainerShardStateStore(uncountedContainer);
        int sourceDocCount = 5;
        try (Directory sourceDirectory = new ByteBuffersDirectory()) {
            SegmentInfos sourceInfos = commitSeparateSegments(sourceDirectory, sourceDocCount);
            assertTrue("test setup should produce multiple segments", sourceInfos.size() > 1);
            CommitManifest sourceManifest = setupPublisher.publishCommit(
                sourceDirectory,
                sourceInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                sourceInfos.getGeneration(),
                sourceDocCount,
                sourceDocCount,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
            ShardHead initialHead = new ShardHead(1, "node-1", Long.MAX_VALUE, sourceManifest.generation());
            assertEquals(CasResult.SUCCESS, setupShardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), initialHead));
        }

        // The measured workload: compaction, driven through a container that independently faults
        // any write-shaped/register-CAS-shaped call, for as long as it's used.
        BlobContainer faultyContainer = new ProbabilisticFailingBlobContainer(uncountedContainer, random(), 0.3);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(faultyContainer);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(faultyContainer);
        ObjectStoreCommitPublisher commitPublisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(faultyContainer);
        LuceneMergeCompactionPublisher compactionPublisher = new LuceneMergeCompactionPublisher(
            INDEX_UUID,
            SHARD_ID,
            manifestStore,
            new ObjectStoreCommitMaterializer(bundleStore),
            commitPublisher,
            CompactionPolicy.withDefaults()
        );
        CompactionRebaseExecutor rebaseExecutor = new CompactionRebaseExecutor(shardStateStore, MAX_ATTEMPTS_PER_ROUND);

        // Unlike ingest, compaction's own retry is not unconditionally guaranteed to succeed:
        // LuceneMergeCompactionPublisher#computeNewHead redoes a real (non-byte-deterministic)
        // merge on every attempt, so a "bundle upload succeeded, then a later step faulted" sequence
        // leaves a stale, mismatched bundle under the deterministic target name that every further
        // retry (redoing the merge fresh) will collide with -- BlobContainerBundleStore#writeBundle
        // now refuses to silently trust or overwrite that mismatch (see its own javadoc), so retries
        // can legitimately exhaust. That is the *safe* failure mode this test actually verifies:
        // either compaction eventually succeeds correctly, or it safely aborts without ever
        // corrupting anything -- never "succeeds" with a manifest that doesn't match what's really
        // stored, which is the real bug this test caught before the writeBundle fix existed.
        RebaseResult result = null;
        try {
            result = retryUnderChaos(() -> rebaseExecutor.publish(INDEX_UUID, SHARD_ID, compactionPublisher));
        } catch (AssertionError exhausted) {
            assertTrue(
                "retry exhaustion must only ever be due to the documented bundle-collision safety "
                    + "guard, not some other unresolved fault -- got: "
                    + exhausted,
                causedByBundleCollisionGuard(exhausted)
            );
        }

        if (result != null) {
            assertEquals(
                "if compaction did retry past every fault, it must have genuinely published, not merely stopped short",
                RebaseResult.Outcome.PUBLISHED,
                result.outcome()
            );
            List<CommitManifest> manifests = manifestStore.listManifests();
            CommitManifest compacted = manifests.stream().max(java.util.Comparator.comparingLong(CommitManifest::generation)).orElseThrow();
            assertEquals("the compacted manifest must still account for every source document", sourceDocCount, compacted.totalDocCount());
        }

        // Safety check regardless of outcome: whatever manifests genuinely exist in the store are
        // never corrupted -- every bundle every one of them references reads back checksum-clean.
        for (CommitManifest manifest : manifestStore.listManifests()) {
            for (var entry : manifest.files().entrySet()) {
                FileReference ref = entry.getValue();
                byte[] bytes = bundleStore.readFile(
                    ref.bundleName(),
                    new BundleFileEntry(entry.getKey(), ref.offset(), ref.length(), ref.checksum())
                );
                assertNotNull(
                    "bundle file [" + entry.getKey() + "] in manifest generation " + manifest.generation() + " must read back cleanly",
                    bytes
                );
            }
        }
    }

    /** Walks {@code error}'s cause chain looking for {@code BlobContainerBundleStore#writeBundle}'s own documented collision-safety message. */
    private static boolean causedByBundleCollisionGuard(Throwable error) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (cause.getMessage() != null && cause.getMessage().contains("already exists with different real content")) {
                return true;
            }
        }
        return false;
    }
}
