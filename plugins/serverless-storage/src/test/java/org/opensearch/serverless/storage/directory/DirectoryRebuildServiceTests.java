/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.directory;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.storage.shardstate.BlobContainerShardStateStore;
import org.opensearch.serverless.storage.shardstate.ShardHead;
import org.opensearch.serverless.storage.shardstate.ShardStateStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Optional;

public class DirectoryRebuildServiceTests extends OpenSearchTestCase {

    private BlobContainer rootContainerAt(java.nio.file.Path root) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, root, false);
        return new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
    }

    private ShardStateStore shardStateStoreFor(java.nio.file.Path root, String indexUuid, int shardId) throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, root, false);
        BlobPath shardPath = BlobPath.cleanPath().add(indexUuid).add(String.valueOf(shardId));
        BlobContainer shardContainer = blobStore.blobContainer(shardPath);
        return new BlobContainerShardStateStore(shardContainer);
    }

    public void testRebuildRecoversAWriterHintForAShardWithAnActiveLease() throws Exception {
        java.nio.file.Path root = createTempDir();
        ShardStateStore shardStateStore = shardStateStoreFor(root, "idx-a", 3);
        shardStateStore.compareAndSet("idx-a", 3, Optional.empty(), new ShardHead(1, "node-1", System.currentTimeMillis() + 60_000, 5));

        ShardDirectory shardDirectory = new InMemoryShardDirectory();
        DirectoryRebuildService rebuildService = new DirectoryRebuildService(shardDirectory, 60_000L);

        int recovered = rebuildService.rebuildFrom(rootContainerAt(root));

        assertEquals(1, recovered);
        ShardDirectoryEntry entry = shardDirectory.lookup("idx-a", 3).orElseThrow();
        assertEquals("node-1", entry.nodeId());
        assertEquals(ShardRole.WRITER, entry.role());
        assertEquals(1, entry.primaryTerm());
        assertEquals(5, entry.generation());
    }

    public void testRebuildSkipsAShardWhoseLeaseHasExpired() throws Exception {
        java.nio.file.Path root = createTempDir();
        ShardStateStore shardStateStore = shardStateStoreFor(root, "idx-a", 0);
        shardStateStore.compareAndSet("idx-a", 0, Optional.empty(), new ShardHead(1, "node-1", 1L, 0));

        ShardDirectory shardDirectory = new InMemoryShardDirectory();
        DirectoryRebuildService rebuildService = new DirectoryRebuildService(shardDirectory, 60_000L, () -> Long.MAX_VALUE / 2);

        int recovered = rebuildService.rebuildFrom(rootContainerAt(root));

        assertEquals(0, recovered);
        assertTrue(shardDirectory.lookup("idx-a", 0).isEmpty());
    }

    public void testRebuildSkipsAShardThatHasNeverBeenActivated() throws Exception {
        // A shard directory can exist on disk (e.g. a stray empty dir) without ever having had a
        // head written to it -- the walk must not choke on that, just skip it.
        java.nio.file.Path root = createTempDir();
        java.nio.file.Files.createDirectories(root.resolve("idx-a").resolve("0"));

        ShardDirectory shardDirectory = new InMemoryShardDirectory();
        DirectoryRebuildService rebuildService = new DirectoryRebuildService(shardDirectory, 60_000L);

        int recovered = rebuildService.rebuildFrom(rootContainerAt(root));

        assertEquals(0, recovered);
    }

    public void testRebuildRecoversMultipleShardsAcrossMultipleIndices() throws Exception {
        java.nio.file.Path root = createTempDir();
        long farFuture = System.currentTimeMillis() + 60_000;
        shardStateStoreFor(root, "idx-a", 0).compareAndSet("idx-a", 0, Optional.empty(), new ShardHead(1, "node-1", farFuture, 0));
        shardStateStoreFor(root, "idx-a", 1).compareAndSet("idx-a", 1, Optional.empty(), new ShardHead(1, "node-2", farFuture, 0));
        shardStateStoreFor(root, "idx-b", 0).compareAndSet("idx-b", 0, Optional.empty(), new ShardHead(1, "node-3", farFuture, 0));

        ShardDirectory shardDirectory = new InMemoryShardDirectory();
        DirectoryRebuildService rebuildService = new DirectoryRebuildService(shardDirectory, 60_000L);

        int recovered = rebuildService.rebuildFrom(rootContainerAt(root));

        assertEquals(3, recovered);
        assertEquals("node-1", shardDirectory.lookup("idx-a", 0).orElseThrow().nodeId());
        assertEquals("node-2", shardDirectory.lookup("idx-a", 1).orElseThrow().nodeId());
        assertEquals("node-3", shardDirectory.lookup("idx-b", 0).orElseThrow().nodeId());
    }

    public void testRebuildOnAnEmptyRootRecoversNothing() throws Exception {
        java.nio.file.Path root = createTempDir();
        ShardDirectory shardDirectory = new InMemoryShardDirectory();
        DirectoryRebuildService rebuildService = new DirectoryRebuildService(shardDirectory, 60_000L);

        assertEquals(0, rebuildService.rebuildFrom(rootContainerAt(root)));
    }
}
