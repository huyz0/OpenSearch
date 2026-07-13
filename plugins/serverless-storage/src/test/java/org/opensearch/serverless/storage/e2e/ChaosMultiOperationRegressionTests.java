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
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileEntry;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
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
 * for the write/delete/register-write-only fault scope): this test exercises the ingest
 * (publish + shard-head CAS) path specifically. Compaction-under-chaos, GC-sweep-under-chaos, and
 * genuinely concurrent (as opposed to this test's sequential, single-threaded) multi-operation
 * chaos all remain explicitly out of scope for this slice, left as real follow-up work rather than
 * silently assumed covered.
 */
public class ChaosMultiOperationRegressionTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "chaos-idx";
    private static final int SHARD_ID = 0;
    private static final int ROUNDS = 15;
    private static final int MAX_ATTEMPTS_PER_ROUND = 50;

    private <T> T retryUnderChaos(Callable<T> attempt) throws Exception {
        IOException lastFailure = null;
        for (int i = 0; i < MAX_ATTEMPTS_PER_ROUND; i++) {
            try {
                return attempt.call();
            } catch (IOException e) {
                lastFailure = e; // a real caller (e.g. WalMirroringTranslog#flushWithRetry) retries transient faults exactly like this.
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
}
