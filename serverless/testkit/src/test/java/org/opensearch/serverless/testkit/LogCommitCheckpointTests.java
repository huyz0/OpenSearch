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
import org.opensearch.serverless.store.CommitStaging;
import org.opensearch.serverless.store.NodePackWriter;
import org.opensearch.test.OpenSearchTestCase;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The checkpoint age of log commits: bounded by the commit period below and the pack deadline above, set on the node
 * and changed at runtime for the cluster.
 */
public class LogCommitCheckpointTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final int SHARDS = 2;
    private static final String SETTING = "serverless.wal.node_log.commits.checkpoint_millis";
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private Settings nodeSettings(String name, long checkpointMillis) {
        return Settings.builder()
            .put("node.name", name)
            .put("cluster.name", "serverless-checkpoint-age")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .put("serverless.wal.node_log.enabled", true)
            .put("serverless.wal.node_log.window_millis", 20)
            .put("serverless.wal.node_log.commits", true)
            .put(SETTING, checkpointMillis)
            .put("serverless.publish.min_interval_millis", 0L)
            .build();
    }

    private static HttpResponse<String> call(ServerlessNode node, String method, String path, String body) throws Exception {
        final int port = node.boundHttpAddress().publishAddress().getPort();
        try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build()) {
            return http.send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                    .header("Content-Type", "application/json")
                    .method(method, HttpRequest.BodyPublishers.ofString(body))
                    .timeout(Duration.ofSeconds(60))
                    .build(),
                HttpResponse.BodyHandlers.ofString()
            );
        }
    }

    public void testTheAgeIsBoundedByTheCommitPeriodAndThePackDeadline() {
        assertEquals(60_000L, CommitStaging.DEFAULT_MAX_STAGED_MILLIS);
        assertEquals(NodePackWriter.SWAP_DEADLINE_MILLIS / 2, CommitStaging.MAX_STAGED_MILLIS);
        assertNull("the default suits the default five-second spacing", CommitStaging.check(60_000L, 5_000L));
        assertNull("two commit periods", CommitStaging.check(10_000L, 5_000L));
        assertNotNull("under two commit periods", CommitStaging.check(9_999L, 5_000L));
        assertNotNull("under a second", CommitStaging.check(999L, 0L));
        assertNull("half the pack deadline", CommitStaging.check(CommitStaging.MAX_STAGED_MILLIS, 5_000L));
        assertNotNull("past half the pack deadline", CommitStaging.check(CommitStaging.MAX_STAGED_MILLIS + 1, 5_000L));
        final String noRoom = CommitStaging.check(60_000L, 100_000L);
        assertNotNull("a commit period that leaves no age", noRoom);
        assertTrue(noRoom, noRoom.contains("commit period"));
    }

    public void testANodeRefusesAnAgeItsCommitPeriodRules() {
        final IllegalArgumentException refused = expectThrows(
            IllegalArgumentException.class,
            () -> new ServerlessNode(nodeSettings("checkpoint-refused", 500L)).close()
        );
        assertTrue(refused.getMessage(), refused.getMessage().contains(SETTING));
    }

    public void testTheClusterSettingIsValidatedAndChangesTheAgeAtRuntime() throws Exception {
        final AtomicLong clock = new AtomicLong(System.currentTimeMillis());
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(8192, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("alpha", "uuid-alpha-00000000", SHARDS, MAPPING, null));
        try (ServerlessNode node = new ServerlessNode(nodeSettings("checkpoint-runtime", CommitStaging.MAX_STAGED_MILLIS))) {
            node.startup().start();
            node.setMetadataPlane(plane);
            node.leases().renewLease(plane);
            plane.membership().refresh();
            final BackgroundReconciler loop = new BackgroundReconciler(node, plane);
            for (int shard = 0; shard < SHARDS; shard++) {
                loop.want("alpha", shard);
            }
            loop.tick(clock.get());
            for (int round = 0; round < 2; round++) {
                for (ShardId shardId : node.reconciler().openShards()) {
                    node.writes().index(shardId, "r" + round + "-" + shardId.id(), "{\"msg\":\"staged\"}");
                }
                assertEquals(SHARDS, loop.publishAll().size());
            }
            assertEquals("two and a half minutes not yet passed", 0, loop.checkpointDue().size());

            for (String bad : new String[] { "500", "150001", "soon" }) {
                final HttpResponse<String> refused = call(
                    node,
                    "PUT",
                    "/_cluster/settings",
                    "{\"persistent\":{\"" + SETTING + "\":\"" + bad + "\"}}"
                );
                assertEquals(refused.body(), 400, refused.statusCode());
                assertTrue(refused.body(), refused.body().contains(SETTING));
            }
            assertEquals("a refused value changes nothing", 0, loop.checkpointDue().size());

            final HttpResponse<String> accepted = call(node, "PUT", "/_cluster/settings", "{\"persistent\":{\"" + SETTING + "\":2000}}");
            assertEquals(accepted.body(), 200, accepted.statusCode());
            assertBusy(() -> assertEquals("the shorter age applies to commits already staged", SHARDS, loop.checkpointDue().size()));
        }
    }
}
