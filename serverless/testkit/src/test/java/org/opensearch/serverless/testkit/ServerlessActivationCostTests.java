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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.shell.ServerlessNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

/**
 * What taking one more shard costs a node that already holds many: the fleet's survivors of a dead node take hundreds of
 * its shards while holding close to a thousand of their own, and their opens slowed to seconds.
 *
 * <p>One index per shard, as the fleet's load has, opened one after another in batches, the cost of each batch read from
 * the node's own counters: the local view projected and applied, and the open by step. Logged for the record; asserted
 * only on the trend, generously, as {@link ServerlessShardMemoryTests} does -- what would fail it is a cost per open
 * that grows with the shards held, which is the shape the fleet suggested.
 */
public class ServerlessActivationCostTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";
    private static final int BATCH = 100;
    private static final int BATCHES = 6;

    public void testTakingAShardDoesNotCostMoreTheMoreAreHeld() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = new MetadataPlane(new FsBlobStore(1024, createTempDir(), false), BlobPath.cleanPath(), clock::get, TTL);
        final int total = BATCH * BATCHES;
        for (int i = 0; i < total; i++) {
            plane.createIndex(new IndexDescriptor(name(i), "uuid-" + name(i), 1, MAPPING, null));
        }
        final Settings settings = Settings.builder()
            .put("node.name", "cost")
            .put("cluster.name", "serverless-activation-cost")
            .put("path.home", createTempDir())
            .put("network.host", "127.0.0.1")
            .put("http.port", "0")
            .put("transport.port", "0")
            .put("serverless.roles", "ingest")
            .build();
        try (ServerlessNode node = new ServerlessNode(settings)) {
            node.start();
            node.setMetadataPlane(plane);
            double first = -1;
            double last = -1;
            for (int batch = 0; batch < BATCHES; batch++) {
                final long[] viewsBefore = node.localViewMillis();
                final long[] stepsBefore = node.reconciler().writerOpenPhaseMillis();
                final long startedAt = System.nanoTime();
                for (int i = batch * BATCH; i < (batch + 1) * BATCH; i++) {
                    node.activateWriter(plane, name(i), 0).orElseThrow();
                }
                final double perShard = (System.nanoTime() - startedAt) / 1e6 / BATCH;
                final long[] views = node.localViewMillis();
                final long[] steps = node.reconciler().writerOpenPhaseMillis();
                final StringBuilder byStep = new StringBuilder();
                for (int s = 0; s < org.opensearch.serverless.shard.ShardReconciler.WRITER_OPEN_PHASES.size(); s++) {
                    byStep.append(' ')
                        .append(org.opensearch.serverless.shard.ShardReconciler.WRITER_OPEN_PHASES.get(s))
                        .append('=')
                        .append(String.format(Locale.ROOT, "%.1f", (steps[s] - stepsBefore[s]) / (double) BATCH));
                }
                logger.info(
                    "activation cost: holding {}..{}: {} ms a shard; view project {} ms, apply {} ms;{}",
                    batch * BATCH,
                    (batch + 1) * BATCH,
                    String.format(Locale.ROOT, "%.1f", perShard),
                    String.format(Locale.ROOT, "%.1f", (views[0] - viewsBefore[0]) / (double) BATCH),
                    String.format(Locale.ROOT, "%.1f", (views[1] - viewsBefore[1]) / (double) BATCH),
                    byStep
                );
                if (batch == 1) {
                    first = perShard;
                }
                last = perShard;
            }
            assertEquals(total, node.reconciler().openShards().size());
            // From the second batch, not the first: the first pays class loading and JIT warm-up.
            assertTrue(
                "taking a shard at "
                    + total
                    + " held must not cost dramatically more than at "
                    + BATCH
                    + " ("
                    + first
                    + " ms against "
                    + last
                    + " ms)",
                last < first * 5
            );
        }
    }

    private static String name(int i) {
        return String.format(Locale.ROOT, "logs-%07d", i);
    }
}
