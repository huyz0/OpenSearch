/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage;

import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.serverless.storage.allocation.ReaderShardPlacementAllocationDecider;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * The real experiment rfc-serverless-opensearch.md &sect;18 risk #10 called for: a serverless-storage
 * index with {@code index.remote_store.enabled: true} (required by core's own {@code
 * MetadataCreateIndexService#validateSearchOnlyReplicasSettings} before it will accept {@code
 * index.number_of_search_replicas > 0} at all) and a real search-only shard copy, observing whether
 * this plugin's own reader path ({@link org.opensearch.serverless.storage.readerengine.ReaderEngineFactory},
 * chosen purely from {@link ShardRouting#isSearchOnly()} in {@link ServerlessStoragePlugin#getEngineFactory},
 * independent of core's remote-store machinery) actually serves reads from that shard copy.
 *
 * <p>Three real, load-bearing findings came out of building this, all now fixed or documented
 * rather than left as inference from reading code:
 *
 * <ul>
 *   <li>The pre-existing risk #7 {@code SEGMENT}-replication rejection in {@link
 *       ServerlessStorageIndexSettingProvider} blocked index creation outright for this scenario --
 *       core's own prerequisite chain for search-only replicas requires {@code remote_store.enabled},
 *       which itself requires an explicit {@code replication.type: SEGMENT}, so there was no way to
 *       ask for a search-only replica without tripping the rejection. Fixed by scoping that
 *       rejection to {@code index.number_of_replicas > 0} (writer replicas) -- a search-only shard
 *       copy never engages core's peer-to-peer segment-copy protocol regardless of this setting.
 *   <li>The search-only shard copy then sat {@code UNASSIGNED} until this test explicitly set
 *       {@code node.attr.serverless_storage_reader: "true"} on the search-only node -- confirmed,
 *       by temporarily instrumenting {@code ServerlessStorageExistingShardsAllocator#allocateUnassigned}
 *       with debug logging, that {@link ReaderShardPlacementAllocationDecider} was working exactly
 *       as designed (rfc-serverless-opensearch.md &sect;10): it requires that attribute on any node
 *       before it will host a reader shard, and a plain {@code startSearchOnlyNode()} node doesn't
 *       carry it. Not a bug -- an undocumented-until-now operational prerequisite for reader
 *       placement to work at all, which this test (and this javadoc) now records.
 *   <li>Core's own remote segment-store upload machinery genuinely did run in the background on
 *       the writer shard, uploading real segment bytes nothing in this plugin ever reads back --
 *       confirmed (not merely inferred) via this test's own assertion on {@code
 *       IndicesStatsResponse}'s {@code getRemoteSegmentStats().getUploadBytesStarted()}, reliably
 *       {@code > 0} before this was fixed. Not a correctness bug, but a real, measured cost/efficiency
 *       gap (rfc-serverless-opensearch.md &sect;18 risk #10) -- fixed by a new core seam,
 *       {@code ShardRecoveryStrategy#ownsRemoteSegmentDurability}, that {@link
 *       org.opensearch.serverless.storage.writerengine.ObjectStoreShardRecoveryStrategy} answers to tell
 *       {@code IndexShard} its own manifest publication already is this shard's durable remote
 *       copy. This test now asserts the upload count is exactly {@code 0}.
 * </ul>
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageSearchOnlyReplicaIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "serverless-search-only-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // super.nodePlugins() -- not a fresh list -- conditionally adds the mock repository
        // plugin(s) (MockFsRepositoryPlugin / MockFsMetadataSupportedRepositoryPlugin) that
        // RemoteStoreBaseIntegTestCase's own nodeSettings() cluster settings already assume are
        // present; dropping them makes every node fail to start with "repository type ... does not
        // exist" the moment it tries to create its remote-store repository.
        return Stream.concat(super.nodePlugins().stream(), Stream.of(ServerlessStoragePlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected boolean addMockInternalEngine() {
        // Same reasoning as ServerlessStorageWriterFailoverIT: OpenSearchIntegTestCase's randomly
        // injected MockEngineFactory collides with this plugin's own EngineFactory selection, which
        // is the entire thing under test here.
        return false;
    }

    private volatile Path sharedBasePath;

    private Path serverlessStorageBasePath() {
        if (sharedBasePath == null) {
            synchronized (this) {
                if (sharedBasePath == null) {
                    // randomRepoPath(), not createTempDir(): it resolves under the cluster's
                    // already-configured path.repo root, so this plugin's own base path is an
                    // allowed repository location too -- a fresh unrelated temp directory
                    // (createTempDir()) is not, and every node fails to start with "doesn't match
                    // any of the locations specified by path.repo" instead.
                    sharedBasePath = randomRepoPath();
                }
            }
        }
        return sharedBasePath;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Path sharedPath = serverlessStorageBasePath();
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), sharedPath.toString())
            .build();
    }

    public void testSearchOnlyReplicaServesReadsForAServerlessStorageIndex() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, 1)
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_ENABLED_SETTING.getKey(), true)
                .build()
        );
        ensureYellow(INDEX_NAME);

        // The reader-designation attribute ReaderShardPlacementAllocationDecider requires (see
        // class javadoc above) -- without it, this node is a perfectly ordinary search-only node
        // as far as core is concerned, but this plugin's own decider refuses to place a reader
        // shard copy on it and the shard sits UNASSIGNED forever.
        internalCluster().startNode(
            Settings.builder()
                .put(nodeSettings(0))
                .put("node.roles", "search")
                .put("node.attr." + ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE, "true")
                .build()
        );
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        // Serverless storage's reader path materializes from a published manifest (rfc-serverless-opensearch.md
        // &sect;8), not from segment-copy -- an explicit flush is what publishes one, exactly as
        // ServerlessStorageWriterFailoverIT's own crash-recovery test requires for the same reason.
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        refresh(INDEX_NAME);

        // Routing straight at the search-only copy -- if ReaderEngineFactory never got wired up, or
        // wired up but unable to materialize from the manifest this writer just published, this
        // fails outright rather than silently falling back to the primary. A plain client-side
        // refresh doesn't force this: ObjectStoreReaderEngine#refresh only reopens against whatever
        // is already materialized locally -- pulling a *newer* manifest happens on its own
        // background poll (ObjectStoreReaderEngine#pollForNewerManifest, every
        // DIRECTORY_ENTRY_TTL_MILLIS/3 = 20s), so this has to poll rather than assert once.
        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setPreference(Preference.SEARCH_REPLICA.type()).setSize(0).get();
            assertHitCount(response, 1);
        }, 30, TimeUnit.SECONDS);

        // The other, previously-open half of risk #10: does core's own remote segment-store upload
        // machinery run pointlessly in the background alongside this plugin's manifest/bundle
        // publication? It used to -- confirmed with real, non-zero uploadBytesStarted before
        // ObjectStoreShardRecoveryStrategy#ownsRemoteSegmentDurability existed -- but not anymore:
        // that answer tells core this index already keeps every segment durable via its
        // own manifest publication, so IndexShard#createEngineConfig now skips wiring in
        // RemoteStoreRefreshListener entirely for this shard. Asserting exactly 0 here, not just
        // "no exception," is what actually proves the new core seam is taking effect end-to-end,
        // not merely compiling.
        IndicesStatsResponse stats = client().admin().indices().prepareStats(INDEX_NAME).setSegments(true).get();
        boolean sawPrimary = false;
        for (ShardStats shardStats : stats.getShards()) {
            if (shardStats.getShardRouting().primary()) {
                sawPrimary = true;
                assertEquals(
                    "core's remote segment-store upload machinery must never fire for a serverless-storage "
                        + "index's writer shard now that ObjectStoreShardRecoveryStrategy#ownsRemoteSegmentDurability "
                        + "returns true (rfc-serverless-opensearch.md §18 risk #10)",
                    0L,
                    shardStats.getStats().getSegments().getRemoteSegmentStats().getUploadBytesStarted()
                );
            }
        }
        assertTrue("expected to find the primary shard in the stats response", sawPrimary);
    }
}
