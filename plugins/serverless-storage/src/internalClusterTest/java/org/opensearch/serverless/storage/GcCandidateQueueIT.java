/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.gc.BlobGcCandidateLog;
import org.opensearch.serverless.storage.gc.BlobGcCandidateLog.LoggedCandidate;
import org.opensearch.serverless.storage.gc.GcCandidate;
import org.opensearch.serverless.storage.gc.GcCandidateTailer;
import org.opensearch.serverless.storage.manifest.BlobContainerManifestStore;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinRequest;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * The queue-driven front door onto manifest GC, end to end through a real cluster: a real write
 * superseding a real head appends a real entry to the object store, at the exact location the plugin
 * itself resolves, and a {@link GcCandidateTailer} pointed at that location correctly deletes what has
 * passed retention and is unpinned, while leaving what is pinned untouched. See {@link GcCandidate}'s own
 * class javadoc for the design; {@code BlobGcCandidateLogTests} and {@code GcCandidateTailerTests} already
 * cover the decision logic against fake containers and a fake clock -- what this proves instead is the
 * wiring: that {@code ObjectStoreCommitHeadPublisher}'s append and a tailer's read agree on where the log
 * lives, without either side being told directly by the other.
 *
 * <h2>Why this reads the log from a freshly built {@link FsBlobStore} rather than through the plugin</h2>
 *
 * {@code ServerlessStoragePlugin} resolves the candidate log's location (the {@code gc-candidates-root}
 * relative path) internally and does not expose the instance it built. Reconstructing one from the test
 * side, against the same {@link #basePath} the cluster itself was started with, is the same pattern this
 * plugin's own {@code BlobBackedDescriptorIT#testTheDescriptorReallyLivesInTheObjectStore} already
 * established for the descriptor change log: read through where the bytes actually are, not through a
 * seam built only for the plugin's own internal use.
 *
 * <h2>Why the tailer here is constructed directly rather than driven by the plugin's own schedule</h2>
 *
 * {@code SERVERLESS_STORAGE_GC_RETENTION_WINDOW_SETTING} enforces a minimum of ten minutes, which this
 * test cannot wait out. A directly constructed {@link GcCandidateTailer}, pointed at the real log and a
 * real per-shard container via {@link ServerlessStoragePlugin#blobContainerForDirectoryFactory}, exercises
 * the identical code path the plugin's own scheduled task would call -- only the retention window and
 * lookback are test-chosen rather than read from settings, which is exactly the seam
 * {@code GcCandidateTailer}'s own package-visible clock constructor already exists to support.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GcCandidateQueueIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "gc-candidate-queue-idx";
    private static final long RETENTION_WINDOW_MILLIS = 1; // effectively "any age qualifies" for this test
    private static final long LOOKBACK_MILLIS = 60_000;

    private Path basePath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private String startClusterAndCreateIndex() throws Exception {
        basePath = createTempDir("gc-candidate-queue");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            // The append inside ObjectStoreCommitHeadPublisher is itself gated on this being positive --
            // see ServerlessStoragePlugin#gcCandidateLogOrNull. The value only has to be positive: nothing
            // in this test waits on the plugin's own scheduled tailer tick, which is why it can be an
            // interval this test never has to sit through.
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_GC_CANDIDATE_TAIL_INTERVAL_SETTING.getKey(), "5m")
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String dataNode = internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);
        return dataNode;
    }

    private BlobGcCandidateLog candidateLog() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, basePath, false);
        return new BlobGcCandidateLog(blobStore::blobContainer, BlobPath.cleanPath().add("gc-candidates-root"));
    }

    /**
     * The write-side half: real flushed writes must supersede whatever generation came before them, and
     * every supersession must be discoverable from the object store alone -- proving {@code
     * ObjectStoreCommitHeadPublisher}'s append and a fresh reader agree on the log's location without
     * either being handed it by the other.
     *
     * <p>The first write establishes the shard's first real manifest and supersedes nothing -- there is no
     * prior head to name, only (at most) the lease-only placeholder {@code acquireOrRenewLease} may have
     * put in place at generation 0 ahead of it, which is not a real manifest and must not be named either
     * (a real bug this test caught: {@code currentHead != null} alone is not "something real was
     * superseded", only {@code currentHead.latestManifestGeneration() > 0} is). So two writes produce
     * exactly one candidate, not two, and asserting that count is the assertion that actually would have
     * caught either mistake -- checking only "more than zero" would not have.
     */
    public void testASupersedingWriteAppendsADiscoverableCandidate() throws Exception {
        startClusterAndCreateIndex();
        BlobGcCandidateLog log = candidateLog();
        assertEquals("a freshly created shard must start with nothing pending", 0, log.entriesSince(null).size());

        client().prepareIndex(INDEX_NAME).setId("1").setSource("f", "first").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().prepareIndex(INDEX_NAME).setId("2").setSource("f", "second").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();

        assertBusy(() -> {
            List<LoggedCandidate> pending = log.entriesSince(null);
            assertEquals("the second write must supersede the first; the first supersedes nothing", 1, pending.size());
            GcCandidate candidate = pending.get(0).candidate();
            assertEquals(indexUuid, candidate.indexUuid());
            assertEquals(0, candidate.shardId());
            assertTrue("no candidate may ever name generation 0 -- it is never a real manifest", candidate.generation() > 0);
        });
    }

    /**
     * The read side, chained onto the write side: what the tailer does with what the write side produced.
     * An eligible, unpinned candidate is deleted and retired; a pinned one is retired without touching the
     * manifest it names.
     */
    public void testTheTailerDeletesAnEligibleUnpinnedGenerationAndSparesAPinnedOne() throws Exception {
        String dataNode = startClusterAndCreateIndex();
        ServerlessStoragePlugin plugin = internalCluster().getInstance(ServerlessStoragePlugin.class, dataNode);
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();
        BlobContainer shardContainer = plugin.blobContainerForDirectoryFactory(indexUuid, 0);
        BlobGcCandidateLog log = candidateLog();
        // A freshly created shard's first-ever publish supersedes nothing (there is no prior head to name)
        // and so appends nothing -- asserted rather than assumed, because the arithmetic below only holds
        // if every candidate seen from here on was caused by one of this test's own three writes.
        assertEquals("a freshly created shard must start with nothing pending", 0, log.entriesSince(null).size());

        // Two writes before the pin (so there is at least one generation the pin is NOT taken on, which
        // is what makes this test able to tell "spared because pinned" apart from "spared because nothing
        // else was ever eligible"), then the pin, then a third write so the pinned generation is itself
        // superseded and becomes a candidate rather than staying the live head forever.
        client().prepareIndex(INDEX_NAME).setId("1").setSource("f", "v1").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().prepareIndex(INDEX_NAME).setId("2").setSource("f", "v2").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest(INDEX_NAME, "keep-this-one")).get();

        // The pin names a generation by its own live value at pin time, discovered rather than assumed --
        // this branch's own commit history is full of tests that broke by hardcoding a generation number
        // a hidden activation commit had already shifted by one.
        org.opensearch.serverless.storage.retention.DurablePinRegistry pinRegistry =
            new org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry(shardContainer);
        org.opensearch.serverless.storage.retention.PinRecord pinnedRecord = pinRegistry.getPins(indexUuid, 0)
            .stream()
            .filter(pin -> pin.pinId().equals("keep-this-one"))
            .findFirst()
            .orElseThrow();
        long pinnedGeneration = pinnedRecord.generation();

        client().prepareIndex(INDEX_NAME).setId("3").setSource("f", "v3").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        assertBusy(() -> assertTrue("at least one write must have superseded something", log.entriesSince(null).size() > 0));
        int pendingCandidateCount = log.entriesSince(null).size();

        GcCandidateTailer tailer = new GcCandidateTailer(
            log,
            plugin::blobContainerForDirectoryFactory,
            RETENTION_WINDOW_MILLIS,
            LOOKBACK_MILLIS
        );
        GcCandidateTailer.TailResult result = tailer.tailOnce();

        assertEquals("every pending candidate must be resolved one way or the other", pendingCandidateCount, result.resolved());
        assertEquals(
            "exactly the pinned generation must be spared -- everything else pending must actually be deleted",
            pendingCandidateCount - 1,
            result.manifestsDeleted()
        );
        assertTrue("every candidate must be retired from the queue either way", log.entriesSince(null).isEmpty());

        BlobContainerManifestStore manifestStore = new BlobContainerManifestStore(shardContainer);
        assertNotNull(
            "the pinned generation's manifest must still be readable",
            manifestStore.readManifest(pinnedRecord.primaryTerm(), pinnedGeneration)
        );
    }
}
