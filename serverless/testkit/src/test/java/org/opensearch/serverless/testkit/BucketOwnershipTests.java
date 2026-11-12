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
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.metadata.Acquisition;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ShardHead;
import org.opensearch.serverless.metadata.ShardHeadStore;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Members of a bucket are owned through one head: joining a bucket a node holds costs no write, a member given back
 * keeps the head while another is open, and a takeover moves the whole bucket with one swap.
 */
public class BucketOwnershipTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final String MAPPING = "{\"properties\":{\"msg\":{\"type\":\"text\"}}}";

    private final AtomicLong clock = new AtomicLong(1_000_000L);
    private final Set<ShardId> openOnA = ConcurrentHashMap.newKeySet();

    private MetadataPlane plane(CountingBlobStore store) throws Exception {
        final MetadataPlane plane = new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
        plane.createIndex(new IndexDescriptor("logs-a", "uuid-a", 1, MAPPING, null).withBucket("b-1"));
        plane.createIndex(new IndexDescriptor("logs-b", "uuid-b", 1, MAPPING, null).withBucket("b-1"));
        plane.createIndex(new IndexDescriptor("solo", "uuid-solo", 1, MAPPING, null));
        plane.enableBuckets().register("node-a", () -> openOnA);
        return plane;
    }

    private static ShardId shard(String index, String uuid) {
        return new ShardId(new Index(index, uuid), 0);
    }

    public void testJoiningAHeldBucketCostsNoWrite() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final ShardHeadStore heads = plane(store).heads();
            final Acquisition first = heads.acquire("logs-a", 0, "node-a", "eph-a", "uuid-a", true);
            assertTrue(first.acquired());
            assertEquals("a member's head is shown as its own", "logs-a", first.head().indexName());
            assertEquals("uuid-a", first.head().indexUuid());
            openOnA.add(shard("logs-a", "uuid-a"));

            store.reset();
            final Acquisition second = heads.acquire("logs-b", 0, "node-a", "eph-a", "uuid-b", true);
            assertTrue(second.acquired());
            assertEquals("joining must not move the bucket's term", first.head().term(), second.head().term());
            assertEquals("joining a held bucket costs no write", 0, store.registerWrites() + store.blobWrites());
            assertEquals("uuid-b", second.head().indexUuid());

            final Acquisition solo = heads.acquire("solo", 0, "node-a", "eph-a", "uuid-solo", true);
            assertTrue("an index outside buckets keeps its own head", solo.acquired());
            assertEquals("solo", heads.read("solo", 0).orElseThrow().indexName());
        }
    }

    public void testAnotherNodeIsRefusedWhileTheBucketIsHeld() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final ShardHeadStore heads = plane(store).heads();
            assertTrue(heads.acquire("logs-a", 0, "node-a", "eph-a", "uuid-a", true).acquired());
            final Acquisition other = heads.acquire("logs-b", 0, "node-b", "eph-b", "uuid-b", true);
            assertFalse("a held bucket refuses every member to another node", other.acquired());
            assertEquals("node-a", other.head().ownerNodeId());
            assertEquals("logs-b", other.head().indexName());
        }
    }

    public void testOnlyTheLastOpenMemberGivesTheHeadBack() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final ShardHeadStore heads = plane(store).heads();
            final long term = heads.acquire("logs-a", 0, "node-a", "eph-a", "uuid-a", true).head().term();
            heads.acquire("logs-b", 0, "node-a", "eph-a", "uuid-b", true);
            openOnA.add(shard("logs-a", "uuid-a"));
            openOnA.add(shard("logs-b", "uuid-b"));

            store.reset();
            assertTrue(heads.release("logs-a", 0, "node-a", term, "uuid-a", "eph-a", "node-a"));
            assertEquals("giving back a member while another is open costs no write", 0, store.registerWrites());
            assertEquals("node-a", heads.read("logs-b", 0).orElseThrow().ownerNodeId());
            openOnA.remove(shard("logs-a", "uuid-a"));

            assertTrue(heads.release("logs-b", 0, "node-a", term, "uuid-b", "eph-a", "node-a"));
            final ShardHead released = heads.read("logs-a", 0).orElseThrow();
            assertNull("the last member gives the bucket back", released.ownerNodeId());
            assertEquals("a release keeps the term", term, released.term());
            assertEquals(List.of("node-a"), released.writers());
        }
    }

    public void testATakeoverMovesTheWholeBucket() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final ShardHeadStore heads = plane(store).heads();
            final long term = heads.acquire("logs-a", 0, "node-a", "eph-a", "uuid-a", true).head().term();
            clock.addAndGet(TTL + 1);

            store.reset();
            final Acquisition taken = heads.acquire("logs-a", 0, "node-b", "eph-b", "uuid-a", true);
            assertTrue("a lapsed owner's bucket can be taken", taken.acquired());
            assertEquals(term + 1, taken.head().term());
            assertEquals("node-a", taken.previousOwner());
            assertTrue("the dead owner's log is named for replay", taken.head().writers().contains("node-a"));
            assertEquals("one swap moves the bucket", 1, store.registerWrites());

            final Acquisition sibling = heads.acquire("logs-b", 0, "node-b", "eph-b", "uuid-b", true);
            assertTrue(sibling.acquired());
            assertEquals("the sibling came along at the same term", term + 1, sibling.head().term());
            assertEquals("node-b", heads.read("logs-b", 0).orElseThrow().ownerNodeId());
            assertFalse("the old owner holds nothing of it", heads.acquire("logs-b", 0, "node-a", "eph-a2", "uuid-b", true).acquired());
        }
    }

    public void testADeadOwnersBucketIsGivenBackOnce() throws Exception {
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final ShardHeadStore heads = plane(store).heads();
            final long term = heads.acquire("logs-a", 0, "node-a", "eph-a", "uuid-a", true).head().term();
            clock.addAndGet(TTL + 1);
            final ShardHeadStore.Abandoned abandoned = heads.releaseAbandoned("logs-b", 0, "node-a").orElseThrow();
            assertEquals("logs-b", abandoned.released().indexName());
            assertEquals(term + 1, abandoned.released().term());
            assertNull(abandoned.released().ownerNodeId());
            assertTrue("the bucket went with it", heads.releaseAbandoned("logs-a", 0, "node-a").isEmpty());
        }
    }
}
