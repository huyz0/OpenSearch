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
import org.opensearch.cluster.routing.Preference;
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
 * Proves rfc-serverless-opensearch.md &sect;8's writer-side publication notification actually
 * delivers, not just that it compiles: {@link ServerlessStorageSearchOnlyReplicaIT} already showed
 * a search-only copy eventually sees a new write, but had to poll for up to 30 real seconds because
 * a reader otherwise only picks up a newer manifest on its own background schedule, every {@code
 * ObjectStoreReaderEngine#MANIFEST_POLL_INTERVAL} = 5s. {@link
 * org.opensearch.serverless.storage.writerengine.WriterPublicationNotifier} exists precisely to
 * beat that wait: after a successful publish, the writer notifies every search-only replica
 * directly via {@code PollNowAction}, so the same read becomes visible almost immediately --
 * bounded by real transport RPC latency, not the poll interval.
 *
 * <p>This is what makes the test discriminating rather than a restatement of the one above: a
 * {@code assertBusy} timeout of 2 seconds, well under the 5s background-poll period, so this test
 * fails if the notification path is ever broken (the search replica would then only catch up on its
 * own schedule, past this timeout) and passes only when the writer's post-publish notification
 * genuinely reaches the reader.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStoragePublicationNotificationIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "serverless-publication-notification-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(ServerlessStoragePlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    private volatile Path sharedBasePath;

    private Path serverlessStorageBasePath() {
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
    protected Settings nodeSettings(int nodeOrdinal) {
        Path sharedPath = serverlessStorageBasePath();
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), sharedPath.toString())
            .build();
    }

    public void testSearchOnlyReplicaSeesAWriteWithoutWaitingOutTheBackgroundPoll() throws Exception {
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

        internalCluster().startNode(
            Settings.builder()
                .put(nodeSettings(0))
                .put("node.roles", "search")
                .put("node.attr." + ReaderShardPlacementAllocationDecider.READER_NODE_ATTRIBUTE, "true")
                .build()
        );
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        refresh(INDEX_NAME);

        assertBusy(() -> {
            SearchResponse response = client().prepareSearch(INDEX_NAME).setPreference(Preference.SEARCH_REPLICA.type()).setSize(0).get();
            assertHitCount(response, 1);
        }, 2, TimeUnit.SECONDS);
    }
}
