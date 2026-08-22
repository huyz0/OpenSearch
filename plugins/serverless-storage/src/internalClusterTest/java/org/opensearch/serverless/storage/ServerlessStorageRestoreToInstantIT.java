/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinRequest;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotRestoreAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotRestoreRequest;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Restoring an index to a point in time rather than to a name someone remembered to take.
 *
 * <h2>What this completes</h2>
 *
 * The retention half already existed: {@code PitrRetentionPolicy} keeps every manifest inside a window
 * pinned, so the data for any instant in the window survives. Nothing could ask for an instant — restore
 * took a pin id, and {@code CommitManifest.createdAtMillis} was read only by policies deciding what to keep.
 * So the state at 14:32 was on disk and unreachable. {@code restore_to} is the other half.
 *
 * <h2>Why the timeline is built from the index's own commits rather than from a clock</h2>
 *
 * The instant this restores to is read back from the manifest the flush produced, not from
 * {@code System.currentTimeMillis()} at the moment the test wrote a document. A test that names its own
 * instant is asserting that two clocks agree, which they do until the box is loaded; a test that names an
 * instant the index itself recorded is asserting what a user actually gets, which is "the state as of the
 * last commit at or before the time you named".
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageRestoreToInstantIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "restore-to-instant-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testAnIndexRestoresToTheStateItHadAtANamedInstant() throws Exception {
        Path basePath = createTempDir("serverless-storage-restore-to-instant");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        // Two published states, and a pin over each. The pin is what keeps a generation alive against GC,
        // and a point-in-time restore refuses a generation nothing holds -- which is the honest behaviour,
        // because the alternative is pointing the head at blobs GC is entitled to delete. In a deployment
        // the PITR window takes these pins automatically; here they are taken explicitly, so the test
        // exercises the resolution rather than the scheduler.
        client().prepareIndex(INDEX_NAME).setId("1").setSource("f", "first").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest(INDEX_NAME, "as-of-one-doc")).get();
        long afterFirstDoc = System.currentTimeMillis();

        // Far enough apart that the two commits cannot share a millisecond, since the instant asked for
        // below has to fall unambiguously between them.
        Thread.sleep(50);

        client().prepareIndex(INDEX_NAME).setId("2").setSource("f", "second").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest(INDEX_NAME, "as-of-two-docs")).get();

        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals("both documents must be live before the restore, or it has nothing to prove", 2, count());

        // Closed so the writer lease is released: a restore-in-place will not move the head out from under
        // a live writer.
        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());

        // The exception is converted inside the block rather than left to propagate: assertBusy only
        // retries on AssertionError, so an ExecutionException here would fail on the first attempt instead
        // of waiting -- the trap this suite's own sibling test documents. Forty seconds because releasing a
        // writer lease is what is being waited for, and that is the budget the sibling restore test uses.
        assertBusy(() -> {
            try {
                client().execute(IndexSnapshotRestoreAction.INSTANCE, IndexSnapshotRestoreRequest.toInstant(INDEX_NAME, afterFirstDoc))
                    .get();
            } catch (Exception e) {
                throw new AssertionError("restore not yet accepted: " + e.getMessage(), e);
            }
        }, 40, TimeUnit.SECONDS);

        // The assertion, and it is on the durable head rather than on a search, for a reason this test had
        // to find out. Restoring moves the head onto the *data* that was current at the instant asked for --
        // proven here against the manifest list itself, so the test asserts the resolution rather than
        // restating it.
        //
        // Asserted on the segments the head names rather than on its generation number, because a restore
        // publishes a new generation carrying the resolved one's segments rather than rewinding onto the
        // resolved generation itself (RestoreManifestSynthesis explains why: rewinding the head also rewinds
        // WAL replay's floor, and replay then reapplies exactly what the restore rolled back). Content
        // equality is the stronger claim in any case -- a generation number matching proves a number was
        // written, and this proves the shard is actually pointing at the right bytes.
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();
        org.opensearch.common.blobstore.BlobContainer container = internalCluster().getDataNodeInstance(ServerlessStoragePlugin.class)
            .blobContainerForDirectoryFactory(indexUuid, 0);
        org.opensearch.serverless.storage.manifest.BlobContainerManifestStore manifestStore =
            new org.opensearch.serverless.storage.manifest.BlobContainerManifestStore(container);
        java.util.List<org.opensearch.serverless.storage.manifest.CommitManifest> manifests = manifestStore.listManifests();
        org.opensearch.serverless.storage.manifest.CommitManifest resolved =
            org.opensearch.serverless.storage.retention.PitrRestoreResolution.newestAtOrBefore(manifests, afterFirstDoc).orElseThrow();
        org.opensearch.serverless.storage.shardstate.ShardHead head =
            new org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore(container).get(indexUuid, 0).orElseThrow().head();
        org.opensearch.serverless.storage.manifest.CommitManifest headManifest = manifestStore.readManifest(
            head.primaryTerm(),
            head.latestManifestGeneration()
        );

        assertEquals(
            "the head must name the segments that were current at the instant asked for, and not a later "
                + "commit's -- a restore that lands past the instant includes the writes the caller named a "
                + "time to be rid of",
            resolved.segmentsFileName(),
            headManifest.segmentsFileName()
        );
        assertEquals(
            "and the same file set, so it is the same commit rather than a same-named one",
            resolved.files(),
            headManifest.files()
        );
        assertTrue(
            "the restore must have published forward rather than rewound: everything else in this system "
                + "assumes head generations only increase, and recovery reads the head",
            headManifest.generation() > resolved.generation()
        );
        assertTrue(
            "and the resolved generation must genuinely predate the second write, or the timeline this test "
                + "built is too coarse to be asserting anything",
            manifests.stream().anyMatch(m -> m.generation() > resolved.generation() && m.createdAtMillis() > afterFirstDoc)
        );
    }

    /**
     * What a search says once the index is opened again, which is the only question that decides whether a
     * restore is real.
     *
     * <h4>What this caught</h4>
     *
     * A restore used to move the head and be undone by the next recovery. The cause turned out to be the
     * one this test's earlier form guessed at: WAL replay. {@code ObjectStoreCommitHeadPublisher
     * #readLatestManifest} reads <em>the head</em>, and {@code ObjectStoreWriterEngine#replayWalOperations}
     * takes that manifest's WAL position as its replay floor -- so rewinding the head onto an older
     * generation rewound the floor with it, and replay, which exists to reapply writes a manifest does not
     * yet carry and cannot tell those from writes deliberately rolled back, put every one of them back.
     *
     * <p>The head being what recovery reads is the good half of that finding: it means the head is
     * authoritative, and a restore only had to stop rewinding it. It now publishes a new generation carrying
     * the target's segments and the newest manifest's WAL position -- see {@code RestoreManifestSynthesis}.
     *
     * <p>This is the assertion the rest of the restore coverage could not make:
     * {@code ServerlessStorageIndexSnapshotActionIT} checks the head record and never reopens the index, so
     * a restore undone by recovery looked identical there to one that holds. Reopening is the whole test.
     */
    public void testARestoredIndexStillReadsAsRestoredAfterItIsReopened() throws Exception {
        Path basePath = createTempDir("serverless-storage-restore-survives-reopen");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("f", "first").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest(INDEX_NAME, "as-of-one-doc")).get();
        long afterFirstDoc = System.currentTimeMillis();
        Thread.sleep(50);

        client().prepareIndex(INDEX_NAME).setId("2").setSource("f", "second").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest(INDEX_NAME, "as-of-two-docs")).get();

        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());
        assertBusy(() -> {
            try {
                client().execute(IndexSnapshotRestoreAction.INSTANCE, IndexSnapshotRestoreRequest.toInstant(INDEX_NAME, afterFirstDoc))
                    .get();
            } catch (Exception e) {
                throw new AssertionError("restore not yet accepted: " + e.getMessage(), e);
            }
        }, 40, TimeUnit.SECONDS);
        assertTrue(client().admin().indices().prepareOpen(INDEX_NAME).get().isAcknowledged());

        assertBusy(() -> {
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
            assertEquals("the document written after the restore point must not come back", 1, count());
        });
    }

    /**
     * The same round trip with WAL mirroring on, because the test above runs with it off and a restore has
     * to hold in both configurations.
     *
     * <h4>What this does and does not establish, stated because the difference matters</h4>
     *
     * {@code serverless_storage.wal_mirroring.enabled} defaults to false, so the test above never reaches
     * {@code ObjectStoreWriterEngine#replayWalOperations} at all -- it short-circuits. This one turns
     * mirroring on and asserts, before anything else, that it actually took effect (a manifest carrying a
     * WAL position), so it cannot silently degrade into a second copy of the test above.
     *
     * <p>It does <b>not</b> isolate {@code RestoreManifestSynthesis}' choice to take its WAL position from
     * the newest manifest rather than the restored one. That was measured rather than assumed: reverting
     * that field by hand leaves this test green, and so does deliberately leaving the second write unflushed
     * so that it is durable in the WAL and named by no manifest at all. In this harness WAL replay
     * contributes nothing to what the reopened shard reads, either because the write never reaches a
     * replayed chunk or because the term floor excludes it -- which of those it is has not been
     * established. So the field is kept because it is correct by construction (a restore that has decided
     * not to replay a range must not advertise a floor beneath it) and not because a test proves it
     * necessary. What carries both configurations is the stale-local-store half, and that one does have a
     * test that fails without it.
     */
    public void testARestoreSurvivesReopeningWhenWalMirroringIsOn() throws Exception {
        Path basePath = createTempDir("serverless-storage-restore-survives-reopen-wal");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("f", "first").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest(INDEX_NAME, "as-of-one-doc")).get();
        long afterFirstDoc = System.currentTimeMillis();
        Thread.sleep(50);

        client().prepareIndex(INDEX_NAME).setId("2").setSource("f", "second").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // The guard that makes this test mean something. Everything below passes just as well with WAL
        // mirroring off, in which case replay short-circuits and the WAL half of the fix is never reached --
        // so assert first that the setting actually took effect, rather than trusting that putting it in the
        // node settings was enough. A manifest published by a WAL-mirroring writer carries a WAL position;
        // one published without mirroring carries null, deliberately (a placeholder position here is what
        // once pinned WAL garbage collection cluster-wide).
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();
        org.opensearch.common.blobstore.BlobContainer walCheckContainer = internalCluster().getDataNodeInstance(
            ServerlessStoragePlugin.class
        ).blobContainerForDirectoryFactory(indexUuid, 0);
        assertTrue(
            "no manifest carries a WAL position, so WAL mirroring is not actually on and this test is not "
                + "exercising WAL replay at all",
            new org.opensearch.serverless.storage.manifest.BlobContainerManifestStore(walCheckContainer).listManifests()
                .stream()
                .anyMatch(m -> m.walPosition() != null)
        );

        assertTrue(client().admin().indices().prepareClose(INDEX_NAME).get().isAcknowledged());
        assertBusy(() -> {
            try {
                client().execute(IndexSnapshotRestoreAction.INSTANCE, IndexSnapshotRestoreRequest.toInstant(INDEX_NAME, afterFirstDoc))
                    .get();
            } catch (Exception e) {
                throw new AssertionError("restore not yet accepted: " + e.getMessage(), e);
            }
        }, 40, TimeUnit.SECONDS);
        assertTrue(client().admin().indices().prepareOpen(INDEX_NAME).get().isAcknowledged());

        assertBusy(() -> {
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
            assertEquals(
                "the document written after the restore point must not come back, and with WAL mirroring on "
                    + "the WAL is where it would come back from",
                1,
                count()
            );
        });
    }

    /**
     * The refusal that matters more than the happy path, because its alternative is silent. An instant older
     * than anything the index has could be answered by restoring to the oldest surviving generation, which
     * would hand back a different point in time than the one asked for and say nothing about it.
     */
    public void testAnInstantOlderThanTheIndexIsRefusedRatherThanRoundedUp() throws Exception {
        Path basePath = createTempDir("serverless-storage-restore-to-instant-too-old");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);
        client().prepareIndex(INDEX_NAME).setId("1").setSource("f", "first").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        ExecutionException failure = expectThrows(
            ExecutionException.class,
            () -> client().execute(IndexSnapshotRestoreAction.INSTANCE, IndexSnapshotRestoreRequest.toInstant(INDEX_NAME, 1L)).get()
        );
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertTrue(
            "the refusal must say the index has nothing that old rather than quietly restoring to its "
                + "oldest generation: "
                + failure.getMessage(),
            failure.getMessage().contains("no generation at or before") || failure.getMessage().contains("cannot be restored to")
        );
        assertEquals("and nothing may have moved", 1, count());
    }

    private long count() {
        return client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value();
    }
}
