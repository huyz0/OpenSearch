/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.wal;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class WalShardRegistryTests extends OpenSearchTestCase {

    private WalShardRegistry newRegistry() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        return new WalShardRegistry(blobContainer);
    }

    public void testRegisteredShardsIsEmptyBeforeAnyRegistration() throws Exception {
        assertEquals(Set.of(), newRegistry().registeredShards());
    }

    public void testRegisterAddsAShard() throws Exception {
        WalShardRegistry registry = newRegistry();
        registry.register("idx", 0);
        assertEquals(Set.of(new RegisteredShard("idx", 0)), registry.registeredShards());
    }

    public void testRegisterIsIdempotent() throws Exception {
        WalShardRegistry registry = newRegistry();
        registry.register("idx", 0);
        registry.register("idx", 0);
        assertEquals(Set.of(new RegisteredShard("idx", 0)), registry.registeredShards());
    }

    public void testRegisterAccumulatesMultipleDistinctShards() throws Exception {
        WalShardRegistry registry = newRegistry();
        registry.register("idx", 0);
        registry.register("idx", 1);
        registry.register("other-idx", 0);
        assertEquals(
            Set.of(new RegisteredShard("idx", 0), new RegisteredShard("idx", 1), new RegisteredShard("other-idx", 0)),
            registry.registeredShards()
        );
    }

    public void testDeregisterRemovesExactlyOneShard() throws Exception {
        WalShardRegistry registry = newRegistry();
        registry.register("idx", 0);
        registry.register("idx", 1);
        registry.deregister("idx", 0);
        assertEquals(Set.of(new RegisteredShard("idx", 1)), registry.registeredShards());
    }

    public void testDeregisterOfAnUnregisteredShardIsANoOp() throws Exception {
        WalShardRegistry registry = newRegistry();
        registry.register("idx", 0);
        registry.deregister("idx", 999);
        assertEquals(Set.of(new RegisteredShard("idx", 0)), registry.registeredShards());
    }

    /**
     * The registry is one marker blob per shard rather than one shared set, which is what makes
     * registration O(1) instead of "read the whole set, deserialize it, CAS the whole set back" -- roughly
     * ten megabytes moved per writer-engine activation at a hundred thousand shards, against a single
     * register every concurrently-activating shard in the cluster contended.
     */
    public void testRegistrationWritesOneMarkerBlobPerShardAndNoSharedSet() throws Exception {
        FsBlobStore blobStore = new FsBlobStore(1024, createTempDir(), false);
        BlobContainer blobContainer = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        WalShardRegistry registry = new WalShardRegistry(blobContainer);

        registry.register("idx", 0);
        registry.register("idx", 1);
        registry.register("other-idx", 0);

        assertEquals("one blob per registered shard", 3, blobContainer.listBlobsByPrefix("registered-shard-").size());
        assertTrue("registration must not write the shared set at all", blobContainer.readRegister("registered-shards").isEmpty());

        registry.deregister("idx", 0);
        assertEquals(2, blobContainer.listBlobsByPrefix("registered-shard-").size());
    }

    /**
     * A marker blob's name carries the index uuid, so the encoding has to survive every byte an index uuid
     * can hold and come back out unchanged -- an index uuid is URL-safe base64, whose alphabet includes the
     * {@code _} and {@code -} that a naive name format would use as separators.
     */
    public void testShardIdentityRoundTripsThroughTheMarkerBlobName() throws Exception {
        WalShardRegistry registry = newRegistry();
        String awkwardUuid = "a_b-c__d-";
        registry.register(awkwardUuid, 7);
        registry.register("plain", 7);

        assertEquals(Set.of(new RegisteredShard(awkwardUuid, 7), new RegisteredShard("plain", 7)), registry.registeredShards());

        registry.deregister(awkwardUuid, 7);
        assertEquals(Set.of(new RegisteredShard("plain", 7)), registry.registeredShards());
    }

    public void testConcurrentRegistrationsFromManyThreadsAreAllCaptured() throws Exception {
        WalShardRegistry registry = newRegistry();
        int shardCount = 30;
        ExecutorService executor = Executors.newFixedThreadPool(shardCount);
        CountDownLatch startLine = new CountDownLatch(1);
        try {
            java.util.List<Future<?>> futures = new java.util.ArrayList<>();
            for (int i = 0; i < shardCount; i++) {
                final int shardId = i;
                futures.add(executor.submit(() -> {
                    startLine.await();
                    registry.register("idx", shardId);
                    return null;
                }));
            }
            startLine.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }

        assertEquals(shardCount, registry.registeredShards().size());
    }
}
