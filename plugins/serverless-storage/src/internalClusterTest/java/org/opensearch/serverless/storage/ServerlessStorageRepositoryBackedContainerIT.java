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
public class ServerlessStorageRepositoryBackedContainerIT extends ServerlessStorageIntegTestCase {

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

    /**
     * Closes part of rfc-serverless-opensearch.md &sect;16's own "shared/dedicated WAL containers
     * remain local-filesystem-only, a natural follow-up" note: {@code resolveDedicatedWalContainer}
     * now goes through the exact same {@code resolveContainer} seam a shard's own regular container
     * does -- resolved lazily per shard, at real shard-open time, the same timing the regular
     * container already has, so it doesn't hit the eager-resolution-at-node-startup problem that
     * keeps the *shared* WAL container ({@code createComponents}'s own, node-scoped, spanning every
     * writer shard) local-filesystem-only for now (see that method's own comment for why). With
     * {@code serverless_storage.repository} configured and a dedicated WAL stream opted into via
     * {@code index.serverless_storage.wal.dedicated_stream}, real dedicated WAL chunk bytes must
     * land under the registered repository, not the local {@code base_path}.
     */
    public void testDedicatedWalChunksLandInTheRegisteredRepositoryNotTheLocalBasePath() throws Exception {
        Path repoPath = createTempDir("serverless-storage-repo-backed-wal-it-repo");
        Path unusedLocalBasePath = createTempDir("serverless-storage-repo-backed-wal-it-unused-local");
        Settings nodeSettings = Settings.builder()
            // Both paths must be registered path.repo roots: Environment#resolveRepoFile refuses
            // to resolve SERVERLESS_STORAGE_BASE_PATH_SETTING otherwise, which would leave basePath
            // null -- and the shared WAL container (createComponents' own, still local-filesystem-
            // only, see that method's own comment) gates its entire construction, including the
            // dedicated-stream machinery that depends on it existing at all, on basePath != null.
            .putList("path.repo", repoPath.toString(), unusedLocalBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), unusedLocalBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey(), REPO_NAME)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
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
                .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_DEDICATED_STREAM_SETTING.getKey(), true)
                .build()
        );
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        assertTrue(
            "real dedicated WAL chunk bytes must exist under the registered repository's own " + "wal-dedicated/ prefix",
            hasAnyFileUnder(repoPath.resolve("serverless_storage").resolve("wal-dedicated"))
        );
        assertFalse(
            "nothing must have been written under the local base_path's own wal-dedicated/ prefix -- "
                + "the repository setting must take precedence for the dedicated WAL container too, "
                + "not just the regular shard container (the shared, node-scoped WAL container's own "
                + "wal/ prefix is now covered too, by "
                + "testSharedWalContainerLandsInTheRegisteredRepositoryNotTheLocalBasePath below)",
            hasAnyFileUnder(unusedLocalBasePath.resolve("wal-dedicated"))
        );
    }

    /**
     * Closes rfc-serverless-opensearch.md &sect;16's own remaining "shared WAL container... remains
     * local-filesystem-only" note: {@code ServerlessStoragePlugin#resolveSharedWalChunkService} now
     * resolves the node-shared {@code wal/} container through the exact same {@code resolveContainer}
     * seam the dedicated-stream and regular per-shard containers already use, deferred to first real
     * writer-shard use (see that method's own javadoc for why eager, {@code createComponents}-time
     * resolution couldn't do this safely). With {@code serverless_storage.repository} configured and
     * WAL mirroring enabled (no dedicated stream opted into -- this is the shared-container path,
     * not the one {@link #testDedicatedWalChunksLandInTheRegisteredRepositoryNotTheLocalBasePath}
     * already covers), real shared WAL chunk bytes must land under the registered repository, not
     * the local {@code base_path}.
     */
    public void testSharedWalContainerLandsInTheRegisteredRepositoryNotTheLocalBasePath() throws Exception {
        Path repoPath = createTempDir("serverless-storage-repo-backed-shared-wal-it-repo");
        Path unusedLocalBasePath = createTempDir("serverless-storage-repo-backed-shared-wal-it-unused-local");
        Settings nodeSettings = Settings.builder()
            .putList("path.repo", repoPath.toString(), unusedLocalBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_BASE_PATH_SETTING.getKey(), unusedLocalBasePath.toString())
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_REPOSITORY_SETTING.getKey(), REPO_NAME)
            .put(ServerlessStoragePlugin.SERVERLESS_STORAGE_WAL_MIRRORING_ENABLED_SETTING.getKey(), true)
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

        // WalMirroringTranslog#add flushes its WAL mirror after every single operation, so an
        // ordinary indexed document is enough to force a real chunk write through the shared
        // container -- no need to bypass the translog path the way ServerlessStorageNodeWalBacklogActionIT
        // does to observe unflushed backlog.
        client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value1").get();

        assertTrue(
            "real shared WAL chunk bytes must exist under the registered repository's own wal/ prefix",
            hasAnyFileUnder(repoPath.resolve("serverless_storage").resolve("wal"))
        );
        assertFalse(
            "nothing must have been written under the local base_path's own wal/ prefix -- the "
                + "repository setting must take precedence for the shared WAL container too, now that "
                + "its resolution is deferred to first real writer-shard use instead of happening "
                + "eagerly (and unavoidably local-filesystem-only) at node startup",
            hasAnyFileUnder(unusedLocalBasePath.resolve("wal"))
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
