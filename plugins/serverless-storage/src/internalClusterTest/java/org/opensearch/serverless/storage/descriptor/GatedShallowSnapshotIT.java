/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.descriptor;

import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.serverless.storage.ServerlessStoragePlugin;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinRequest;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinResponse;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotReleaseAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotReleaseRequest;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotRestoreAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotRestoreRequest;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The shallow-snapshot surface against a gated index: pin, restore, release, end to end.
 *
 * <h2>What the plugin already has</h2>
 *
 * A shallow snapshot, in the sense the question means: {@code _snapshot_pin} writes a durable pin naming a
 * manifest generation and copies nothing, because the bundles that generation refers to are already in the
 * object store. {@code _snapshot_restore} compare-and-swaps the shard head back to a pinned generation, and
 * {@code _snapshot_release} drops the pin so GC can reclaim what it was holding. There is no data movement
 * anywhere in that, which is what makes it cheap enough to matter at this branch's target population.
 *
 * <h2>What this used to measure</h2>
 *
 * That none of it reached a gated index. The index-level orchestration resolved the index name through
 * {@code clusterService.state().metadata().index(name)} to get a uuid and a shard count, and that answers
 * null for a gated index -- so a pin failed {@code IndexNotFoundException} for an index that exists, is
 * serving traffic, and has manifests to pin. The shard-level actions underneath take a uuid and a shard id
 * directly and never consult cluster state, so the mechanism was always indifferent to gating; only the
 * name-to-uuid step in front of it was not. It resolves through {@code AbsentIndexDescriptorSuppliers} now.
 *
 * <h2>Why the round trip rather than the pin alone, and what it found</h2>
 *
 * A pin that can be taken and not restored from is not a snapshot, and the failure would look like success
 * at exactly the moment it matters least and costs most. So the round trip writes after pinning, restores,
 * and asserts the later write is gone -- the only assertion that distinguishes a restore which moved the
 * head from one that returned quietly having done nothing.
 *
 * <p>It did not pass at first, and what it found on the way to passing is worth more than the feature it was
 * written for. Restore-in-place refuses while a writer lease is held; the ordinary-index restore test
 * releases that lease by closing the index; and closing a gated index did not release it.
 *
 * <p>Pulling that thread found three layers, all now fixed. The only channel that tells other nodes about a
 * gated index's change is the descriptor change log. {@code DescriptorChange} carried
 * {@code (name, uuid, kind, atMillis)} with no state, so the tailer could not tell a close from a mapping
 * update and released shards only for deletions -- it now carries {@code CLOSED}, and the tailer asks
 * {@code releasesShard()} rather than {@code !live()}, because a closed index keeps its name and loses its
 * shard. That alone changed nothing, which is how the second layer surfaced: a close is written through
 * {@code ClaimedIndexLifecycle.updateIndex}, and that path recorded no change of any kind, so there was
 * never an entry for the new kind to travel in. It records one now, which also means a mapping update
 * republished that way stops leaving other nodes' caches stale.
 *
 * <p>The third layer was this harness: {@code installBlobBackedDescriptorPlane} never called {@code
 * DescriptorGate.setChangeFeed}, so no test using it had ever appended a change or tailed one, regardless of
 * what the first two layers fixed. {@link #startDescriptorChangeTail} closes it -- a real
 * {@code BlobDescriptorChangeLog} and a real {@code DescriptorChangeTailer} polling it on a background
 * thread, the same shape production runs, opted into only by the test that needs it. With that wired, the
 * round trip passes: closing appends a {@code CLOSED} change, the tailer applies it and releases the shard,
 * the lease goes with it, and restore-in-place becomes reachable. Confirmed the other direction too --
 * reverting to no change feed reproduces the original failure exactly, the writer-lease
 * {@code IllegalStateException} the AwaitsFix this class used to carry named -- so the fix is in the wiring
 * this test drives, not in loosening what it asserts.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class GatedShallowSnapshotIT extends org.opensearch.serverless.storage.ServerlessStorageIntegTestCase {

    private volatile java.nio.file.Path sharedBasePath;

    private java.nio.file.Path basePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ServerlessStoragePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath().toString())
            .put(ServerlessStoragePlugin.COMPUTED_PLACEMENT_ENABLED_SETTING.getKey(), true)
            .build();
    }

    private volatile Scheduler.Cancellable descriptorChangeTail;

    @After
    public void clearGate() throws Exception {
        Scheduler.Cancellable tail = descriptorChangeTail;
        descriptorChangeTail = null;
        if (tail != null) {
            tail.cancel();
        }
        DescriptorGate.uninstall();
    }

    /**
     * Starts the third layer the round trip below needs: an appender was already wired ({@code
     * ClaimedIndexLifecycle.updateIndex} records a change on close) and a consumer already existed
     * ({@code DescriptorChangeTailer} calls {@code GatedIndexRelease.release} for a change that {@code
     * releasesShard()}), but nothing in this suite had ever connected them. {@code
     * installBlobBackedDescriptorPlane} installs the descriptor store and the mapping store; it never calls
     * {@code DescriptorGate.setChangeFeed}, so every test using it runs with an appender writing to nothing
     * and no tailer reading anything.
     *
     * <p>A fresh {@code FsBlobStore} is enough: the only two parties that need to agree on where the log
     * lives are the appender ({@code DescriptorGate}, wired here) and the tailer polling it, and nothing else
     * reads this store. Scheduled at the same {@code ThreadPool.Names.GENERIC} production uses, at the
     * setting's own floor (100ms) rather than its 5s default, so the {@code assertBusy} windows below do not
     * need to be five times as generous just to give a background poll room to run.
     *
     * <p>Deliberately not folded into {@code installBlobBackedDescriptorPlane} itself: forty-odd other
     * classes use that method and do not need a background thread walking an object store on every test run,
     * and several exercise the descriptor cache's own TTL, which a tailer invalidating aggressively would
     * change the timing of. Opt in per test, the same way {@code installOverFailableContainer} is a variant
     * rather than a default.
     */
    private void startDescriptorChangeTail(DescriptorBackend backend) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobDescriptorChangeLog changeLog = new BlobDescriptorChangeLog(blobStore::blobContainer, BlobPath.cleanPath());
        DescriptorGate.setChangeFeed(changeLog);
        DescriptorChangeTailer tailer = new DescriptorChangeTailer(changeLog, backend);
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class);
        descriptorChangeTail = threadPool.scheduleWithFixedDelay(
            tailer::tailOnce,
            TimeValue.timeValueMillis(100),
            ThreadPool.Names.GENERIC
        );
    }

    /**
     * The round trip the plan asked for: pin, write past it, close (which must release the lease), restore,
     * and confirm the later write is gone.
     *
     * <p>This is the test that found the three layers {@link DescriptorChange}'s own javadoc and this class's
     * history describe. The first two -- {@code CLOSED} as a kind, and {@code updateGated} recording one at
     * all -- were fixed without this test passing, because nothing in the suite drove a change from append
     * through to a shard actually being released. {@link #startDescriptorChangeTail} is that drive: with a
     * real change log installed and a real tailer polling it, closing the index appends a {@code CLOSED}
     * change, the tailer applies it and calls {@code GatedIndexRelease.release}, the data node's {@code
     * IndicesClusterStateService} closes the shard it opened on demand, the writer lease goes with it, and
     * restore-in-place -- which refuses while a lease is held -- becomes reachable. Proven rather than
     * inferred: the assertion is the later write disappearing from a real search, not a log line saying the
     * shard closed.
     */
    public void testPinRestoreAndReleaseAllWorkOnAGatedIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        InstalledDescriptorPlane plane = installBlobBackedDescriptorPlane();
        startDescriptorChangeTail(plane.points());

        assertTrue(
            client().admin().indices().create(new CreateIndexRequest("serverless_pin-me").settings(gated())).actionGet().isAcknowledged()
        );
        assertBusy(() -> {
            try {
                client().prepareIndex("serverless_pin-me").setId("before").setSource("f", "v").get();
            } catch (Exception e) {
                throw new AssertionError("write not yet servable: " + e.getMessage(), e);
            }
        }, 60, TimeUnit.SECONDS);
        // Flushed, so there is a manifest generation to pin rather than only unpublished translog.
        client().admin().indices().prepareFlush("serverless_pin-me").get();

        IndexSnapshotPinResponse pinned = client().execute(
            IndexSnapshotPinAction.INSTANCE,
            new IndexSnapshotPinRequest("serverless_pin-me", "pin-1")
        ).get();
        assertEquals("every shard of the gated index must be pinned, with nothing copied anywhere", 1, pinned.shardCount());

        // Write past the pin, and publish it, so the restore below has something to undo. Without the
        // flush this asserts nothing: an unpublished write is not in any manifest, so a head that never
        // moved would look exactly like a head that moved back.
        client().prepareIndex("serverless_pin-me").setId("after").setSource("f", "v").get();
        client().admin().indices().prepareFlush("serverless_pin-me").get();
        client().admin().indices().prepareRefresh("serverless_pin-me").get();
        assertEquals(
            "both documents must be visible before the restore, or the restore has nothing to prove",
            2,
            client().prepareSearch("serverless_pin-me").get().getHits().getTotalHits().value()
        );

        // Closed before restoring, because a restore-in-place refuses while a writer lease is held -- it
        // will not move the head out from under a live writer. This is the same step the ordinary-index
        // restore test takes, and it is available here only because close is expressible for a gated index:
        // IndexDescriptor carries State, so the gated close path writes descriptor.withState(CLOSE) and
        // means it. Were that not so, restore-in-place would be unreachable for a gated index and this test
        // would be recording that instead.
        assertTrue(client().admin().indices().prepareClose("serverless_pin-me").get().isAcknowledged());

        // assertBusy around the restore itself: closing releases the lease asynchronously, so the first
        // attempt can still meet it.
        assertBusy(() -> {
            try {
                client().execute(IndexSnapshotRestoreAction.INSTANCE, new IndexSnapshotRestoreRequest("serverless_pin-me", "pin-1")).get();
            } catch (Exception e) {
                throw new AssertionError("restore not yet accepted: " + e.getMessage(), e);
            }
        }, 60, TimeUnit.SECONDS);

        assertTrue(client().admin().indices().prepareOpen("serverless_pin-me").get().isAcknowledged());

        // The head is back at the pinned generation. Asserted through a real search rather than through the
        // head record, because what a restore is for is what the index answers afterwards.
        assertBusy(() -> {
            client().admin().indices().prepareRefresh("serverless_pin-me").get();
            assertEquals(
                "the write taken after the pin must be gone once the head is restored to it",
                1,
                client().prepareSearch("serverless_pin-me").get().getHits().getTotalHits().value()
            );
        }, 60, TimeUnit.SECONDS);

        // And the pin releases, which is what lets GC reclaim what it was holding.
        assertEquals(
            "release must cover every shard the pin covered",
            1,
            client().execute(IndexSnapshotReleaseAction.INSTANCE, new IndexSnapshotReleaseRequest("serverless_pin-me", "pin-1"))
                .get()
                .shardCount()
        );
    }

    /**
     * The control, and the reason the gap is narrow rather than deep: the identical call against an index
     * that uses the same storage engine and is <em>not</em> gated succeeds. Nothing about the pin mechanism
     * cares; only the name resolution in front of it does.
     */
    public void testPinningAnUngatedServerlessIndexSucceeds() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        assertTrue(
            client().admin().indices().create(new CreateIndexRequest("ungated-pin-me").settings(gated())).actionGet().isAcknowledged()
        );
        ensureGreen("ungated-pin-me");
        client().prepareIndex("ungated-pin-me").setId("1").setSource("f", "v").get();
        client().admin().indices().prepareFlush("ungated-pin-me").get();

        assertEquals(
            "one shard pinned, with no data copied anywhere -- the pin names a manifest generation that is "
                + "already in the object store, which is what makes this shallow",
            1,
            client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest("ungated-pin-me", "pin-1")).get().shardCount()
        );
    }

    private static Settings gated() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.serverless_storage.enabled", true)
            .build();
    }

    /**
     * Close, which the restore above needs and which turned out not to close anything.
     *
     * <p>Found by needing it: a restore-in-place refuses while a writer lease is held, the ordinary-index
     * restore test releases that lease by closing the index, and closing a gated index left it serving. The
     * cause was two places dropping the descriptor's state on the floor. {@code IndexDescriptor
     * #toIndexMetadata} never set it, so every reader that synthesised metadata saw {@code OPEN} whatever
     * the descriptor said; and {@code IndexNameExpressionResolver}'s gated branch returns a concrete index
     * before reaching {@code shouldTrackConcreteIndex}, the one place that refuses a closed index. So
     * {@code MetadataIndexStateService} wrote {@code descriptor.withState(CLOSE)}, answered acknowledged,
     * and the index went on accepting writes and answering searches.
     *
     * <p><b>One difference from an ordinary close survives the fix and is asserted as such.</b> An ordinary
     * close is acknowledged once its cluster state update has been applied; a gated close writes the
     * descriptor and acknowledges, and other nodes learn of it by tailing the change log. So a gated close
     * converges rather than arriving, and a client that closes and immediately writes can still be served.
     * This test found that by passing alone and failing inside a loaded full suite.
     *
     * <p>Compared against an ordinary index rather than against a written-down expectation, for the same
     * reason the document lifecycle test does it: "closed" is defined by what a closed index does, and the
     * ordinary one is the definition. The test that covered gated close before this asserted the
     * descriptor's own state -- the label it had just written -- which is why a label was all it was.
     */
    public void testAClosedGatedIndexRefusesWhatAClosedOrdinaryIndexRefuses() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(2);
        installBlobBackedDescriptorPlane();

        client().admin().indices().create(new CreateIndexRequest("serverless_close-me").settings(gated())).actionGet();
        assertBusy(() -> {
            try {
                client().prepareIndex("serverless_close-me").setId("1").setSource("f", "v").get();
            } catch (Exception e) {
                throw new AssertionError("not servable yet: " + e.getMessage(), e);
            }
        }, 60, TimeUnit.SECONDS);

        client().admin().indices().create(new CreateIndexRequest("ordinary-close-me").settings(plainSettings())).actionGet();
        client().prepareIndex("ordinary-close-me").setId("1").setSource("f", "v").get();

        java.util.List<String> ordinaryBehaviour = closeAndObserve("ordinary-close-me");
        assertEquals(
            "the definition this is measured against: a closed ordinary index refuses both, immediately, "
                + "because close is acknowledged only once its cluster state update has been applied",
            java.util.List.of("write=IndexClosedException", "search=IndexClosedException"),
            ordinaryBehaviour
        );

        // The gated side converges rather than arriving, and that is a real difference rather than test
        // slack. An ordinary close is acknowledged after its cluster state update is applied; a gated close
        // writes descriptor.withState(CLOSE) and acknowledges, and other nodes learn of it by tailing the
        // descriptor change log. So there is a window in which a closed gated index still answers -- this
        // test passed alone and failed inside a loaded full suite, which is the window, not flakiness.
        // Bounded, because a window that does not close is the defect this test exists for.
        assertTrue(client().admin().indices().prepareClose("serverless_close-me").get().isAcknowledged());
        assertBusy(
            () -> assertEquals(
                "a closed gated index must end up refusing exactly what a closed ordinary index refuses",
                ordinaryBehaviour,
                observe("serverless_close-me")
            ),
            60,
            TimeUnit.SECONDS
        );
    }

    private java.util.List<String> closeAndObserve(String name) {
        assertTrue(client().admin().indices().prepareClose(name).get().isAcknowledged());
        return observe(name);
    }

    private java.util.List<String> observe(String name) {
        java.util.List<String> observed = new java.util.ArrayList<>();
        try {
            client().prepareIndex(name).setId("2").setSource("f", "v").get();
            observed.add("write=ACCEPTED");
        } catch (Exception e) {
            observed.add("write=" + e.getClass().getSimpleName());
        }
        try {
            client().prepareSearch(name).get();
            observed.add("search=ACCEPTED");
        } catch (Exception e) {
            observed.add("search=" + e.getClass().getSimpleName());
        }
        return observed;
    }

    private static Settings plainSettings() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
    }
}
