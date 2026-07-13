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
import java.util.Optional;
import java.util.concurrent.Callable;

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
 * it needs package-private access to {@code GcSchedulerTask#sweepForTesting()}. Genuinely concurrent
 * (as opposed to this test's sequential, single-threaded) multi-operation chaos remains explicitly
 * out of scope, left as real follow-up work rather than silently assumed covered.
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
