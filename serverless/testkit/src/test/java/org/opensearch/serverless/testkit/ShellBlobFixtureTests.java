/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.reconcile.BackgroundReconciler;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.serverless.store.CommitManifest;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.stream.Stream;

/**
 * Writes a small index through the shell and copies every blob it left to {@code tests.shell_blobs.out}, so another
 * implementation of the blob formats can decode bytes this one wrote. The first node publishes into a node pack; a
 * successor with node packs off adds a second segment as a segment blob in its own term container. Skipped unless the
 * property is set.
 */
public class ShellBlobFixtureTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"},\"n\":{\"type\":\"long\"}}}";

    private Settings nodeSettings(String name, boolean nodePacks) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "shell-blob-fixtures")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.segments.node_pack.enabled", nodePacks)
            .put("serverless.segments.node_pack.linger_millis", 5)
            .put("serverless.wal.node_log.window_millis", 20)
            .build();
    }

    private static void write(ServerlessNode node, String prefix, int count) throws Exception {
        final ShardId shardId = node.reconciler().openShards().iterator().next();
        for (int i = 0; i < count; i++) {
            node.writes().index(shardId, prefix + i, "{\"msg\":\"shell blob fixture\",\"n\":" + i + "}");
        }
    }

    public void testWriteFixtures() throws Exception {
        final String out = System.getProperty("tests.shell_blobs.out");
        assumeTrue("set tests.shell_blobs.out to write the fixtures", out != null && out.isEmpty() == false);
        final Path root = createTempDir();
        final FsBlobStore store = new FsBlobStore(8192, root, false);
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), System::currentTimeMillis, TTL);
        plane.createIndex(new IndexDescriptor("fixture", "fixture-uuid-0000000", 1, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("fixture-packs", true))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("fixture", 0);
            loop.tick(System.currentTimeMillis());
            write(node, "first-", 3);
            loop.publishAll();
            loop.setIdleAfterMillis(1);
            loop.stopWanting("fixture", 0);
            loop.releaseIdle(System.currentTimeMillis() + 60_000L);
        }
        try (ServerlessNode node = new ServerlessNode(nodeSettings("fixture-segment-blobs", false))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            loop.want("fixture", 0);
            loop.tick(System.currentTimeMillis());
            write(node, "second-", 2);
            loop.publishAll();
            final CommitManifest manifest = plane.segmentPublisher("fixture", 0).readManifest().orElseThrow();
            logger.info("fixture manifest: {}", manifest.files());
        }
        final Path target = Path.of(out);
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path file : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator) {
                final String relative = root.relativize(file).toString();
                if (relative.contains("extra") || relative.contains(".opensearch-register-locks#")) {
                    continue;
                }
                final Path copy = target.resolve(relative);
                Files.createDirectories(copy.getParent());
                Files.copy(file, copy, StandardCopyOption.REPLACE_EXISTING);
            }
        }
    }
}
