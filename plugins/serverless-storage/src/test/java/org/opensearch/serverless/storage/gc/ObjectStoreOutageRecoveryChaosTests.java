/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.serverless.storage.e2e.OutageInjectingBlobContainer;
import org.opensearch.serverless.storage.format.BlobContainerBundleStore;
import org.opensearch.serverless.storage.format.BundleFileContent;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.manifest.CommitManifest;
import org.opensearch.serverless.storage.manifest.FileReference;
import org.opensearch.serverless.storage.manifest.PruningStats;
import org.opensearch.serverless.storage.manifest.WalPosition;
import org.opensearch.serverless.storage.readerengine.lazydirectory.CleanerDaemonThreadLeakFilter;
import org.opensearch.serverless.storage.readerengine.lazydirectory.LazyBundleDirectory;
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.DurablePinRegistry;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.CasResult;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.writerengine.ObjectStoreCommitPublisher;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * rfc-serverless-opensearch.md &sect;13's own chaos-suite requirement: "a full object-store outage
 * with assertions on all three [degraded-mode] behaviors" (reads degrade to staleness, never
 * unavailability, for anything already cached; writes degrade to rejection, never silent
 * un-durability; GC/compaction/reconcilers halt rather than act on partial information), "plus the
 * recovery [after restoration] must drain... with jittered backoff, not synchronized thundering
 * herd" -- the jittered-backoff half of that requirement is {@link
 * org.opensearch.serverless.storage.scheduling.JitteredScheduling}, now wired into this class's own
 * {@link GcSchedulerTask} (and {@code WalGcSchedulerTask}/{@code CompactionSchedulerTask}) via a
 * one-time per-instance jitter -- see that class's own javadoc for why a one-time offset, not a
 * per-tick one, is what actually prevents lockstep ticking after a coordinated outage recovery.
 *
 * <p>Uses {@link OutageInjectingBlobContainer} -- a genuinely binary on/off switch, unlike {@code
 * ProbabilisticFailingBlobContainer}'s per-call dice roll -- to simulate a real "the whole object
 * store is down, then comes back" window, deliberately in the {@code gc} package (not {@code e2e},
 * where every other chaos test in this plugin otherwise lives) so it can call {@link
 * GcSchedulerTask#sweepForTesting()} directly, the same reason {@link GcSchedulerTaskChaosTests}
 * already lives here instead.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class ObjectStoreOutageRecoveryChaosTests extends OpenSearchTestCase {

    private static final String INDEX_UUID = "outage-chaos-idx";
    private static final int SHARD_ID = 0;

    public void testGcHaltsCleanlyDuringAFullOutageThenConvergesOnRecovery() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore setupBundleStore = new BlobContainerBundleStore(rawContainer);
        BlobContainerManifestStore setupManifestStore = new BlobContainerManifestStore(rawContainer);

        // Two real generations: one old enough to be deletable, one current -- exactly the shape a
        // real GC sweep needs to have any real work to do (or halt) at all. Written directly
        // (bypassing ObjectStoreCommitPublisher, which always stamps "now") so this test controls
        // createdAtMillis, the same technique GcSchedulerTaskChaosTests' own writeGeneration uses.
        long now = System.currentTimeMillis();
        long farInThePast = now - TimeValue.timeValueDays(1).millis();
        writeGeneration(setupBundleStore, setupManifestStore, 1, farInThePast);
        CommitManifest survivor = writeGeneration(setupBundleStore, setupManifestStore, 2, now);

        AtomicBoolean outage = new AtomicBoolean(false);
        BlobContainer outageContainer = new OutageInjectingBlobContainer(rawContainer, outage);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(outageContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(outageContainer);
        DurablePinRegistry pinRegistry = new BlobContainerDurablePinRegistry(outageContainer);
        long retentionWindowMillis = TimeValue.timeValueMinutes(1).millis();
        GcSchedulerConfig config = new GcSchedulerConfig(
            TimeValue.timeValueMinutes(5),
            retentionWindowMillis,
            manifestStore,
            bundleStore,
            pinRegistry
        );

        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            // Bundles now get their own sustained-observation safety window (GcSchedulerTask's own
            // javadoc explains why), so this test drives a controllable clock: one recovery sweep to
            // observe gen 1's bundle as newly orphaned, then advance well past the window and sweep
            // again to actually delete it -- otherwise "the bundle is gone" would never converge in
            // this test's single post-outage sweep.
            long[] clockMillis = { now };
            GcSchedulerTask task = new GcSchedulerTask(threadPool, config.interval(), INDEX_UUID, SHARD_ID, config, () -> clockMillis[0]);
            try {
                outage.set(true);
                expectThrows(IOException.class, task::sweepForTesting);

                // Halt, not partial action: the outage must never let a sweep delete only some of
                // what it found before faulting -- both the deletable manifest and its bundle must
                // still be completely present.
                assertEquals(
                    "a halted sweep during a full outage must never have deleted anything",
                    2,
                    setupManifestStore.listManifests().size()
                );
                assertEquals(
                    "a halted sweep during a full outage must never have deleted any bundle either",
                    2,
                    setupBundleStore.listBundleNames().size()
                );

                outage.set(false);
                task.sweepForTesting();
                clockMillis[0] = now + retentionWindowMillis + 1;
                task.sweepForTesting();
            } finally {
                task.close();
            }

            List<CommitManifest> remaining = setupManifestStore.listManifests();
            assertEquals("recovery must let the sweep converge normally: the deletable generation is now gone", 1, remaining.size());
            assertEquals(survivor.generation(), remaining.get(0).generation());
            assertEquals(
                "the deletable generation's own bundle must be gone too, not orphaned",
                1,
                setupBundleStore.listBundleNames().size()
            );
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    public void testWriterPublishRejectsDuringAFullOutageThenSucceedsOnRecovery() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        // A second, never-outage-wrapped view of the exact same underlying store, used only to
        // verify real on-disk state -- the outage-wrapped container itself is down during the
        // outage window and can't answer even a read-only listManifests() call, by design.
        BlobContainerManifestStore verificationManifestStore = new BlobContainerManifestStore(rawContainer);

        AtomicBoolean outage = new AtomicBoolean(false);
        BlobContainer outageContainer = new OutageInjectingBlobContainer(rawContainer, outage);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(outageContainer);
        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(outageContainer);
        ObjectStoreCommitPublisher publisher = new ObjectStoreCommitPublisher(bundleStore, manifestStore);
        ShardStateStore shardStateStore = new BlobContainerShardStateStore(outageContainer);

        publishOneDocumentGeneration(publisher, shardStateStore, 1);
        assertEquals(1, verificationManifestStore.listManifests().size());

        outage.set(true);
        expectThrows(IOException.class, () -> publishOneDocumentGeneration(publisher, shardStateStore, 2));
        // Rejected, not silently un-durable: the failed attempt must never have left a manifest
        // behind (a publish either genuinely lands or leaves no trace, never a partial one).
        assertEquals(
            "a rejected publish during a full outage must leave no trace behind",
            1,
            verificationManifestStore.listManifests().size()
        );

        outage.set(false);
        publishOneDocumentGeneration(publisher, shardStateStore, 2);
        assertEquals("recovery must let a fresh publish attempt succeed normally", 2, verificationManifestStore.listManifests().size());
    }

    public void testAlreadyCachedReadsSurviveAFullOutageWhileUncachedReadsFailFast() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer rawContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        BlobContainerBundleStore setupBundleStore = new BlobContainerBundleStore(rawContainer);
        BlobContainerManifestStore setupManifestStore = new BlobContainerManifestStore(rawContainer);
        ObjectStoreCommitPublisher setupPublisher = new ObjectStoreCommitPublisher(setupBundleStore, setupManifestStore);

        CommitManifest manifest;
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                for (int i = 0; i < 2; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", "doc-" + i, Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            manifest = setupPublisher.publishCommit(
                writerDirectory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                1,
                2,
                2,
                new WalPosition("epoch-0", 0),
                0,
                PruningStats.empty()
            );
        }
        assertTrue("test setup should produce at least two real files to distinguish cached-vs-uncached", manifest.files().size() >= 2);
        String cachedFileName = manifest.files().keySet().iterator().next();
        String uncachedFileName = manifest.files()
            .keySet()
            .stream()
            .filter(name -> name.equals(cachedFileName) == false)
            .findFirst()
            .orElseThrow();

        AtomicBoolean outage = new AtomicBoolean(false);
        BlobContainer outageContainer = new OutageInjectingBlobContainer(rawContainer, outage);
        BlobContainerBundleStore bundleStore = new BlobContainerBundleStore(outageContainer);

        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(64L * 1024 * 1024, 1);
            try (MMapDirectory cacheDirectory = new MMapDirectory(createTempDir(), SimpleFSLockFactory.INSTANCE)) {
                TransferManager transferManager = new TransferManager(bundleStore::openRange, fileCache, threadPool);
                LazyBundleDirectory directory = new LazyBundleDirectory(manifest, cacheDirectory, transferManager);

                // Warm the cache for exactly one file while the object store is still healthy.
                readFully(directory, cachedFileName);

                outage.set(true);

                // Already-cached: must keep serving, never degrade to unavailable.
                readFully(directory, cachedFileName);

                // Never touched before: this read genuinely needs the (now-down) object store, so
                // it must fail fast rather than hang or silently return wrong data.
                expectThrows(IOException.class, () -> readFully(directory, uncachedFileName));

                outage.set(false);
                // Recovery: the previously-failed file is now reachable again.
                readFully(directory, uncachedFileName);
            }
        } finally {
            ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private static void readFully(Directory directory, String fileName) throws IOException {
        try (var input = directory.openInput(fileName, org.apache.lucene.store.IOContext.READONCE)) {
            byte[] bytes = new byte[(int) input.length()];
            input.readBytes(bytes, 0, bytes.length);
        }
    }

    private static void publishOneDocumentGeneration(ObjectStoreCommitPublisher publisher, ShardStateStore shardStateStore, long generation)
        throws Exception {
        try (Directory writerDirectory = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(writerDirectory, new IndexWriterConfig())) {
                Document doc = new Document();
                doc.add(new StringField("id", "doc-" + generation, Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(writerDirectory);
            CommitManifest manifest = publisher.publishCommit(
                writerDirectory,
                segmentInfos,
                INDEX_UUID,
                SHARD_ID,
                1,
                generation,
                generation,
                generation,
                new WalPosition("epoch-0", generation),
                0,
                PruningStats.empty()
            );
            Optional<org.opensearch.serverless.storage.shardstate.VersionedShardHead> current = shardStateStore.get(INDEX_UUID, SHARD_ID);
            CasResult result = current.isEmpty()
                ? shardStateStore.compareAndSet(INDEX_UUID, SHARD_ID, Optional.empty(), new ShardHead(1, null, 0L, manifest.generation()))
                : shardStateStore.compareAndSet(
                    INDEX_UUID,
                    SHARD_ID,
                    Optional.of(current.get().version()),
                    new ShardHead(1, null, 0L, manifest.generation())
                );
            if (result != CasResult.SUCCESS) {
                throw new IllegalStateException("failed to activate head for generation " + generation + ": " + result);
            }
        }
    }

    /** Writes a real one-file bundle and its manifest directly, independent of ObjectStoreCommitPublisher so the test controls createdAtMillis -- mirrors GcSchedulerTaskChaosTests' own identically-purposed helper. */
    private static CommitManifest writeGeneration(
        BlobContainerBundleStore bundleStore,
        BlobContainerManifestStore manifestStore,
        long generation,
        long createdAtMillis
    ) throws Exception {
        String bundleName = BlobContainerBundleStore.NAME_PREFIX + INDEX_UUID + "-" + SHARD_ID + "-1-" + generation;
        byte[] content = ("content-" + generation).getBytes("UTF-8");
        var bundle = bundleStore.writeBundle(bundleName, List.of(new BundleFileContent("segments_" + generation, content)));
        var entry = bundle.entries().get("segments_" + generation);
        CommitManifest manifest = new CommitManifest(
            INDEX_UUID,
            SHARD_ID,
            1,
            generation,
            "segments_" + generation,
            java.util.Map.of("segments_" + generation, new FileReference(bundleName, entry.offset(), entry.length(), entry.checksum())),
            0,
            0,
            null,
            0,
            PruningStats.empty(),
            createdAtMillis
        );
        manifestStore.writeManifest(manifest);
        return manifest;
    }
}
