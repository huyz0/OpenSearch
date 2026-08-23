/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * The end-to-end proof rfc-serverless-opensearch.md's whole WAL/crash-recovery effort (&sect;6.4,
 * &sect;7.1.2) has been building toward: a real multi-node cluster, a real writer node killed, and
 * the shard's data still there afterward -- not a unit-level proof of one piece in isolation, but
 * the actual failure this plugin exists to survive.
 *
 * <p>All nodes share one {@code serverless_storage.base_path} directory, standing in for the object
 * store every node in a real deployment would share -- that's what lets the survivor node see the
 * dead node's published manifest at all.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageWriterFailoverIT extends ServerlessStorageIntegTestCase {

    private static final String INDEX_NAME = "serverless-failover-idx";

    /**
     * Set by {@link #testWriterShardSurvivesItsNodeBeingKilledWithEncryptionEnabled} before
     * starting any node, and merged into every node's settings by {@link #nodeSettings}. Secure
     * settings can only ever be attached once per {@link Settings} object -- composing two
     * already-secure-settings-bearing {@link Settings} via {@code Settings.Builder#put(Settings)}
     * throws -- so this has to be threaded in at the one place ({@link #nodeSettings}) that already
     * owns building each node's final settings from scratch, rather than pre-built and passed to
     * {@code startClusterManagerOnlyNode}/{@code startDataOnlyNode} directly.
     */
    private volatile MockSecureSettings encryptionSecureSettingsOverride;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        // OpenSearchIntegTestCase randomly injects its own MockEngineFactory otherwise, which
        // collides with WriterEngineFactory ("multiple engine factories provided for [...]") --
        // this plugin's own EngineFactory is the whole point of the test.
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        if (encryptionSecureSettingsOverride != null) {
            // Each node gets its own clone -- MockSecureSettings is consumed per Settings build,
            // and this same override is reused across every node (cluster-manager + both data
            // nodes) in the encrypted test.
            builder.setSecureSettings((MockSecureSettings) encryptionSecureSettingsOverride.clone());
        }
        return builder.build();
    }

    private Settings sharedNodeSettings(Path basePath) {
        return Settings.builder()
            .putList("path.repo", basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), basePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
            .build();
    }

    public void testWriterShardSurvivesItsNodeBeingKilled() throws Exception {
        Path sharedBasePath = createTempDir("serverless-storage-shared");
        Settings nodeSettings = sharedNodeSettings(sharedBasePath);

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
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

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        // Publishing (rfc-serverless-opensearch.md &sect;7.1) only happens on flush, not on refresh
        // -- an explicit flush is what makes this document durable in a manifest the survivor node
        // can actually recover from independent of WAL replay at all.
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Doc 2 is never flushed -- durable only in the WAL (WalMirroringTranslog#add flushes each
        // operation's WAL chunk synchronously, so this write is already WAL-durable by the time
        // .get() returns) and in the dying node's own local translog, which the survivor never sees.
        // Recovering it is WalReplayRecovery's job, not manifest materialization's -- this is the
        // gap the RFC's own &sect;16 status note flagged as still needing IT coverage, not unit
        // tests alone.
        client().prepareIndex(INDEX_NAME).setId("2").setSource("field", "value2").get();

        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 2);

        // Whichever node actually holds the primary is the one that must die -- not necessarily
        // the first data node started, since ServerlessStorageExistingShardsAllocator (like any
        // allocator) is free to have placed it on either data node.
        ShardRouting primaryShard = internalCluster().clusterService().state().routingTable().index(INDEX_NAME).shard(0).primaryShard();
        String primaryNodeId = primaryShard.currentNodeId();
        String primaryNodeName = internalCluster().clusterService().state().nodes().get(primaryNodeId).getName();

        internalCluster().stopRandomNode(settings -> primaryNodeName.equals(settings.get("node.name")));

        // The remaining data node is the only place this shard can go -- and it starts with a
        // completely empty local Store, exactly the scenario
        // ObjectStoreShardRecoveryStrategy's EXISTING_STORE case and ServerlessStorageExistingShardsAllocator
        // exist for (rfc-serverless-opensearch.md &sect;7.1.2). Without either, this would either
        // never leave UNASSIGNED (no allocator willing to place it) or fail recovery outright (no
        // local commit to read) -- ensureGreen succeeding at all is most of what this test proves.
        ensureGreen(INDEX_NAME);

        refresh(INDEX_NAME);
        // Doc 1 (materialized from the manifest) and doc 2 (recovered via WAL replay past
        // activationWalPosition, ObjectStoreWriterEngine#engineRecoveryOperations) must both be
        // present -- proving the full chain (fencing, fetch/filter/decode, apply-to-shard) under a
        // real cluster, not just the manifest-materialization half.
        SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).get();
        assertHitCount(response, 2);
    }

    /**
     * Same crash-recovery proof as {@link #testWriterShardSurvivesItsNodeBeingKilled}, but with
     * encryption enabled on every node (rfc-serverless-opensearch.md &sect;16 Phase 2's own status
     * note: "further broadening the IT (encryption enabled, multiple shards)" was the one
     * documented remaining gap once the base crash-recovery scenario was proven). Every store this
     * plugin writes -- bundles, manifests, shard-state registers, and WAL chunks -- is encrypted
     * with the same shared key every node in the cluster is configured with, so the survivor node
     * genuinely has to decrypt the dying node's data to recover it, not merely re-read plaintext
     * that happened to already be readable.
     */
    public void testWriterShardSurvivesItsNodeBeingKilledWithEncryptionEnabled() throws Exception {
        Path sharedBasePath = createTempDir("serverless-storage-shared-encrypted");
        byte[] rawKeyBytes = new byte[32];
        random().nextBytes(rawKeyBytes);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(
            ServerlessStoragePlugin.SERVERLESS_STORAGE_ENCRYPTION_KEY_SETTING.getKey(),
            Base64.getEncoder().encodeToString(rawKeyBytes)
        );
        encryptionSecureSettingsOverride = secureSettings;
        Settings nodeSettings = sharedNodeSettings(sharedBasePath);

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
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

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().prepareIndex(INDEX_NAME).setId("2").setSource("field", "value2").get();

        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 2);

        ShardRouting primaryShard = internalCluster().clusterService().state().routingTable().index(INDEX_NAME).shard(0).primaryShard();
        String primaryNodeId = primaryShard.currentNodeId();
        String primaryNodeName = internalCluster().clusterService().state().nodes().get(primaryNodeId).getName();

        internalCluster().stopRandomNode(settings -> primaryNodeName.equals(settings.get("node.name")));

        // The survivor node was started with the SAME shared key -- if this ever regresses to each
        // node generating its own key, or the key not actually being applied to every store this
        // plugin writes, this recovery would fail loudly (decrypt failure) rather than silently
        // succeed with corrupted data, per EncryptingBlobContainer/WalRecordCrypto's own
        // fail-loudly-on-tamper-or-wrong-key design.
        ensureGreen(INDEX_NAME);

        refresh(INDEX_NAME);
        SearchResponse response = client().prepareSearch(INDEX_NAME).setSize(0).get();
        assertHitCount(response, 2);
    }

    /**
     * Closes the remaining half of &sect;16 Phase 2's own "further broadening the IT" note: every
     * other crash-recovery test in this class covers exactly one shard's worth of recovery, which
     * leaves open the question of whether killing one node correctly recovers only the shard(s)
     * whose primary actually lived there, while a shard whose primary survived is left completely
     * undisturbed -- not just "did the whole index still respond." A 3-shard, 3-data-node cluster
     * with a real writer-shard-aware allocator (unlike a plain even-split assumption) makes no
     * guarantee about which node hosts how many primaries, so this test kills whichever node hosts
     * shard 0's primary specifically (deterministic, unlike "some node") and separately asserts
     * shard 0's own document survives, not merely that the index-wide total is still correct (which
     * a bug that silently dropped a healthy, undisturbed shard's data could still coincidentally
     * satisfy if it happened to also lose the same number of documents elsewhere).
     */
    public void testMultipleShardsEachIndependentlySurviveTheirOwnPrimarysNodeBeingKilled() throws Exception {
        Path sharedBasePath = createTempDir("serverless-storage-shared-multi-shard");
        Settings nodeSettings = sharedNodeSettings(sharedBasePath);

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        int numberOfShards = 3;
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        // "routing" pins each doc to a specific shard via the standard hash-of-routing-value
        // mechanism -- deterministic and portable across OpenSearch versions, unlike depending on
        // the id-hash default distribution. shard0Routing is confirmed below to actually land on
        // shard 0 before it's relied on for the rest of the test.
        String shard0Routing = findRoutingValueForShard(numberOfShards, 0);
        String shard1Routing = findRoutingValueForShard(numberOfShards, 1);

        client().prepareIndex(INDEX_NAME).setId("shard0-doc1").setSource("field", "value1").setRouting(shard0Routing).get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        // WAL-only durable on shard 0, and a completely untouched shard 1 doc, both flushed AND
        // WAL-durable so shard 1's recovery path (which never loses its primary) is exercised too,
        // just via the normal in-place-open path rather than crash recovery.
        client().prepareIndex(INDEX_NAME).setId("shard0-doc2").setSource("field", "value2").setRouting(shard0Routing).get();
        client().prepareIndex(INDEX_NAME).setId("shard1-doc1").setSource("field", "value1").setRouting(shard1Routing).get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        refresh(INDEX_NAME);
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 3);

        ShardRouting shard0Primary = internalCluster().clusterService().state().routingTable().index(INDEX_NAME).shard(0).primaryShard();
        String shard0PrimaryNodeId = shard0Primary.currentNodeId();
        String shard0PrimaryNodeName = internalCluster().clusterService().state().nodes().get(shard0PrimaryNodeId).getName();

        internalCluster().stopRandomNode(settings -> shard0PrimaryNodeName.equals(settings.get("node.name")));

        ensureGreen(INDEX_NAME);
        refresh(INDEX_NAME);

        // The index-wide total proves nothing was silently lost anywhere...
        assertHitCount(client().prepareSearch(INDEX_NAME).setSize(0).get(), 3);
        // ...and these two per-shard-routed gets prove specifically that shard 0's own two
        // documents (one manifest-durable, one WAL-only-durable) both survived its primary's node
        // being killed, while shard 1's document -- whose primary was never touched -- was of
        // course also never at risk, but is checked anyway so a bug that accidentally routed both
        // docs onto the same shard would not silently pass this test.
        assertHitCount(client().prepareSearch(INDEX_NAME).setRouting(shard0Routing).setSize(0).get(), 2);
        assertHitCount(client().prepareSearch(INDEX_NAME).setRouting(shard1Routing).setSize(0).get(), 1);
    }

    /**
     * Brute-forces a routing value that {@code OperationRouting}'s standard hash-of-routing-value
     * mechanism maps to {@code targetShardId} for an index with {@code numberOfShards} shards --
     * simpler and more portable than depending on the hashing algorithm's exact internals, and
     * self-verifying: if no candidate in the search space maps to the target shard (should never
     * happen for a reasonable shard count), this fails loudly rather than silently testing the
     * wrong shard.
     */
    private String findRoutingValueForShard(int numberOfShards, int targetShardId) {
        for (int candidate = 0; candidate < 10_000; candidate++) {
            String routingValue = "route-" + candidate;
            int shardId = internalCluster().clusterService()
                .operationRouting()
                .indexShards(internalCluster().clusterService().state(), INDEX_NAME, null, routingValue)
                .shardId()
                .id();
            if (shardId == targetShardId) {
                return routingValue;
            }
        }
        throw new AssertionError("could not find a routing value mapping to shard " + targetShardId + " within the search space");
    }
}
