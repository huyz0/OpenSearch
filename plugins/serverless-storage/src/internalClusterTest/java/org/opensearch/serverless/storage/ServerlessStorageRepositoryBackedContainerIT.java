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
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Stream;

/**
 * Proves {@code serverless_storage.repository} (rfc-serverless-opensearch.md's own top-of-file
 * status note: "this plugin doesn't yet construct one of those concrete containers instead of the
 * local-filesystem one, which is the remaining piece of wiring") genuinely resolves a shard's
 * container through a real, independently-registered {@code BlobStoreRepository} rather than the
 * plugin's own {@code serverless_storage.base_path} -- using the built-in {@code fs} repository
 * type as a stand-in for {@code repository-s3}/{@code repository-gcs}/{@code repository-azure},
 * since all four share the exact same {@code BlobStoreRepository#blobStore()} seam this plugin
 * reads through; the concrete repository type is deliberately irrelevant to what's being proven.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ServerlessStorageRepositoryBackedContainerIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "serverless-repo-backed-it-idx";
    private static final String REPO_NAME = "serverless-repo-backed-it-repo";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ServerlessStoragePlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testAShardsBytesLandInTheRegisteredRepositoryNotTheLocalBasePath() throws Exception {
        Path repoPath = createTempDir("serverless-storage-repo-backed-it-repo");
        Path unusedLocalBasePath = createTempDir("serverless-storage-repo-backed-it-unused-local");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", repoPath.toString())
            // Deliberately still configured: proves the repository setting takes precedence over
            // this, not merely that things work when this is left unset.
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), unusedLocalBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey(), REPO_NAME)
            .build();

        internalCluster().startClusterManagerOnlyNode(nodeSettings);
        internalCluster().startDataOnlyNode(nodeSettings);

        assertTrue(
            "repository registration must be acknowledged before it's usable",
            client().admin()
                .cluster()
                .preparePutRepository(REPO_NAME)
                .setType(FsRepository.TYPE)
                .setSettings(Settings.builder().put(FsRepository.LOCATION_SETTING.getKey(), repoPath.toString()))
                .get()
                .isAcknowledged()
        );

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
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        assertEquals(
            "the doc must be genuinely searchable back out of the repository-backed container, " + "not just written somewhere",
            1,
            client().prepareSearch(INDEX_NAME)
                .setQuery(org.opensearch.index.query.QueryBuilders.matchAllQuery())
                .get()
                .getHits()
                .getHits().length
        );

        assertTrue("real manifest/bundle bytes must exist somewhere under the registered repository's own path", hasAnyFileUnder(repoPath));
        assertFalse(
            "nothing must have been written under the configured-but-superseded local base_path -- "
                + "the repository setting must take precedence, not merely also work",
            hasAnyFileUnder(unusedLocalBasePath)
        );
    }

    private static boolean hasAnyFileUnder(Path root) throws Exception {
        if (Files.exists(root) == false) {
            return false;
        }
        try (Stream<Path> files = Files.walk(root)) {
            return files.anyMatch(Files::isRegularFile);
        }
    }
}
