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
import org.opensearch.serverless.store.WalRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;

/** Run with -Poptions.forkOptions.memoryMaximumSize=512m: durable history must not need an equally large heap to recover. */
public class WalRecoveryMemoryTests extends OpenSearchTestCase {
    public void testRecoveryStreamsHistoryLargerThanTheHeap() throws Exception {
        assumeTrue("run with -Poptions.forkOptions.memoryMaximumSize=512m", Runtime.getRuntime().maxMemory() <= 768L * 1024 * 1024);
        try (var store = new FsBlobStore(1024, createTempDir(), false)) {
            final var plane = new MetadataPlane(store, BlobPath.cleanPath(), () -> 1000L, 30_000L);
            final var descriptor = new IndexDescriptor(
                "recovery-memory",
                "uuid-recovery-memory",
                1,
                "{\"properties\":{\"payload\":{\"type\":\"keyword\",\"index\":false,\"doc_values\":false}}}",
                null
            );
            plane.createIndex(descriptor);
            final var wal = plane.walStore(descriptor.name(), descriptor.uuid(), 0);
            final String source = "{\"payload\":\"" + "x".repeat(128 * 1024) + "\"}";
            for (int offset = 0; offset < 5120; offset += 32) {
                final var batch = new ArrayList<WalRecord>();
                for (int i = offset; i < offset + 32; i++) {
                    batch.add(new WalRecord("doc-" + i, source, i, 1L, 1L));
                }
                wal.append(1L, batch);
            }
            final var settings = Settings.builder()
                .put("node.name", "wal-recovery-memory")
                .put("path.home", createTempDir())
                .put("network.host", "127.0.0.1")
                .put("http.port", "0")
                .put("transport.port", "0")
                .put("serverless.roles", "ingest")
                .build();
            try (var node = new ServerlessNode(settings)) {
                node.start();
                node.setMetadataPlane(plane);
                final var shard = node.activateWriter(plane, descriptor.name(), 0).orElseThrow();
                assertTrue(node.get(shard, "doc-0").found());
                assertTrue(node.get(shard, "doc-5119").found());
            }
        }
    }
}
