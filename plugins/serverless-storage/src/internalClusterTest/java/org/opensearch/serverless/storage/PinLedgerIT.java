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
import org.opensearch.serverless.storage.retention.BlobContainerDurablePinRegistry;
import org.opensearch.serverless.storage.retention.BlobContainerPinLedgerStore;
import org.opensearch.serverless.storage.retention.PinLedger;
import org.opensearch.serverless.storage.retention.PinRecord;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotPinRequest;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotReleaseAction;
import org.opensearch.serverless.storage.retention.action.IndexSnapshotReleaseRequest;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * The ledger that makes releasing a pin retryable.
 *
 * <h2>What it is for</h2>
 *
 * A pin is written per shard, so an index-wide pin is N pins under one id, and until this existed nothing
 * recorded that they were one thing. Release walked the index's <em>current</em> shard count, which is not
 * necessarily the count the pin was taken across; a release that failed halfway left pins behind with
 * nothing naming them; and a coordinator that died mid-pin left pins nobody could enumerate. All three are
 * the same gap: no record of what a pin covered.
 *
 * <p>Core's remote-store shallow copy solved this from the other direction and it is worth stating, because
 * this is deliberately the same shape: there, every lock on a remote segment file is paired with a
 * {@code shallow-snap-<uuid>} blob in the repository, and snapshot deletion reads that blob to learn which
 * locks to release -- releasing first and deleting the record only afterwards, so a failed release leaves
 * the record for the next attempt. Their repository entry is the ledger. A pin taken through
 * {@code _snapshot_pin} never goes near a repository, which is what makes it cheap, so the ledger has to be
 * written on purpose.
 *
 * <h2>What is asserted</h2>
 *
 * The retryability itself, driven the only way that proves it: interrupt a release halfway by hand, then
 * run release again and require it to finish the job. A test that merely pinned and released would pass
 * without a ledger at all.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PinLedgerIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "pin-ledger-idx";
    private static final int SHARDS = 3;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testAPartlyDoneReleaseIsFinishedByRunningItAgain() throws Exception {
        String dataNode = startClusterAndIndex();
        ServerlessStoragePlugin plugin = internalCluster().getInstance(ServerlessStoragePlugin.class, dataNode);
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();
        BlobContainerPinLedgerStore ledgerStore = new BlobContainerPinLedgerStore(plugin.blobContainerForDirectoryFactory(indexUuid, 0));

        client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest(INDEX_NAME, "pin-1")).get();

        Optional<PinLedger> ledger = ledgerStore.read("pin-1");
        assertTrue("pinning must record what it covered, or a later release has nothing to work from", ledger.isPresent());
        assertEquals("and must record the shard count the pin was actually taken across", SHARDS, ledger.get().shardCount());
        assertEquals(indexUuid, ledger.get().indexUuid());
        for (int shard = 0; shard < SHARDS; shard++) {
            assertTrue("shard " + shard + " must be pinned", pinnedShards(plugin, indexUuid, shard).contains("pin-1"));
        }

        // A release interrupted halfway, which is what a coordinator dying in the middle of one looks like
        // from the outside: some shards released, the record still there because it is deleted last.
        new BlobContainerDurablePinRegistry(plugin.blobContainerForDirectoryFactory(indexUuid, 0)).removePin(indexUuid, 0, "pin-1");
        assertFalse("shard 0 is now released and shards 1..n are not", pinnedShards(plugin, indexUuid, 0).contains("pin-1"));
        assertTrue(
            "shard 1 must still hold the pin, or this is not a half-done release",
            pinnedShards(plugin, indexUuid, 1).contains("pin-1")
        );
        assertTrue("and the record must survive the interruption -- that is the whole point", ledgerStore.read("pin-1").isPresent());

        // Run it again. It must finish rather than fail on the shard that is already released.
        client().execute(IndexSnapshotReleaseAction.INSTANCE, new IndexSnapshotReleaseRequest(INDEX_NAME, "pin-1")).get();

        for (int shard = 0; shard < SHARDS; shard++) {
            assertFalse(
                "shard " + shard + " must hold no pin after the retry, including the one released by hand",
                pinnedShards(plugin, indexUuid, shard).contains("pin-1")
            );
        }
        assertTrue(
            "and only now may the record go: deleted after the pins, so a failure before this point leaves "
                + "something for the next attempt to find",
            ledgerStore.read("pin-1").isEmpty()
        );
    }

    /**
     * The other half of retryable: releasing something already fully released must succeed rather than
     * fail. An operator retrying after an ambiguous failure has no way to know which case they are in, so
     * the two must be indistinguishable from the outside.
     */
    public void testReleasingTwiceSucceedsBothTimes() throws Exception {
        String dataNode = startClusterAndIndex();
        ServerlessStoragePlugin plugin = internalCluster().getInstance(ServerlessStoragePlugin.class, dataNode);
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();

        client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest(INDEX_NAME, "pin-2")).get();
        assertEquals(
            SHARDS,
            client().execute(IndexSnapshotReleaseAction.INSTANCE, new IndexSnapshotReleaseRequest(INDEX_NAME, "pin-2")).get().shardCount()
        );
        assertEquals(
            "a second release must be a no-op that succeeds, not a failure -- the ledger is gone, so this "
                + "also exercises the fallback to the index's current shard count",
            SHARDS,
            client().execute(IndexSnapshotReleaseAction.INSTANCE, new IndexSnapshotReleaseRequest(INDEX_NAME, "pin-2")).get().shardCount()
        );
        for (int shard = 0; shard < SHARDS; shard++) {
            assertFalse(pinnedShards(plugin, indexUuid, shard).contains("pin-2"));
        }
    }

    /**
     * That the ledger, and not the index's current shard count, decides what a release covers.
     *
     * <p>Without this the other tests here pass just as well with the ledger ignored, because a pin and its
     * release see the same shard count in a test that does not reshard. So the ledger is rewritten to name
     * fewer shards than were pinned, and release must honour it -- which is the behaviour that matters for
     * an index whose shard count changed between the pin and the release, where walking the current count
     * would release a set the pin was never taken across and report success.
     */
    public void testTheLedgerDecidesWhatIsReleasedRatherThanTheCurrentShardCount() throws Exception {
        String dataNode = startClusterAndIndex();
        ServerlessStoragePlugin plugin = internalCluster().getInstance(ServerlessStoragePlugin.class, dataNode);
        String indexUuid = client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME).getIndexUUID();
        BlobContainerPinLedgerStore ledgerStore = new BlobContainerPinLedgerStore(plugin.blobContainerForDirectoryFactory(indexUuid, 0));

        client().execute(IndexSnapshotPinAction.INSTANCE, new IndexSnapshotPinRequest(INDEX_NAME, "pin-3")).get();

        // Rewritten to name two of the three shards. Standing in for a shard count that changed between the
        // pin and the release, which is the case this record exists to survive.
        ledgerStore.write(new PinLedger("pin-3", INDEX_NAME, indexUuid, SHARDS - 1, System.currentTimeMillis()));

        assertEquals(
            "the release must cover what the record names",
            SHARDS - 1,
            client().execute(IndexSnapshotReleaseAction.INSTANCE, new IndexSnapshotReleaseRequest(INDEX_NAME, "pin-3")).get().shardCount()
        );
        assertFalse(pinnedShards(plugin, indexUuid, 0).contains("pin-3"));
        assertTrue(
            "the shard outside the record must still be pinned -- if it is not, the release walked the "
                + "index's current shard count and the record was never consulted",
            pinnedShards(plugin, indexUuid, SHARDS - 1).contains("pin-3")
        );
    }

    private Set<String> pinnedShards(ServerlessStoragePlugin plugin, String indexUuid, int shardId) throws Exception {
        Set<PinRecord> pins = new BlobContainerDurablePinRegistry(plugin.blobContainerForDirectoryFactory(indexUuid, shardId)).getPins(
            indexUuid,
            shardId
        );
        return pins.stream().map(PinRecord::pinId).collect(java.util.stream.Collectors.toSet());
    }

    private String startClusterAndIndex() throws Exception {
        Path basePath = createTempDir("pin-ledger");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .build();
        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        String dataNode = internalCluster().startDataOnlyNode(nodeSettings);

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARDS)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);
        // Enough docs, unrouted, that every shard publishes a manifest and therefore has a head to pin.
        for (int i = 0; i < 30; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("f", "v" + i).get();
        }
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        return dataNode;
    }
}
