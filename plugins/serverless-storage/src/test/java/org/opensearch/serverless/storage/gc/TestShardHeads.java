/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.gc;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.serverless.storage.shardstate.VersionedShardHead;

import java.io.IOException;
import java.util.Optional;

/**
 * Publishes a shard head the way a real writer's commit does, for tests that write manifest blobs directly.
 *
 * <p>Needed because writing a manifest blob is no longer the same thing as publishing it: {@link
 * GcSchedulerTask} anchors "latest" to the head register, so a test that only writes manifests has, from the
 * sweep's point of view, published nothing at all -- which is exactly the distinction that stops a writer
 * killed between its manifest write and its head CAS from making the live head look superseded.
 */
final class TestShardHeads {

    private TestShardHeads() {}

    /** Moves the head forward to {@code generation} under {@code primaryTerm}, creating it if the shard has none. */
    static void publish(BlobContainer container, String indexUuid, int shardId, long primaryTerm, long generation) throws IOException {
        ShardStateStore store = new BlobContainerShardStateStore(container);
        Optional<VersionedShardHead> current = store.get(indexUuid, shardId);
        if (current.isEmpty()) {
            store.compareAndSet(indexUuid, shardId, Optional.empty(), new ShardHead(primaryTerm, null, 0L, generation));
            return;
        }
        if (current.get().head().latestManifestGeneration() < generation) {
            store.compareAndSet(
                indexUuid,
                shardId,
                Optional.of(current.get().version()),
                current.get().head().withPublishedGeneration(primaryTerm, generation)
            );
        }
    }
}
