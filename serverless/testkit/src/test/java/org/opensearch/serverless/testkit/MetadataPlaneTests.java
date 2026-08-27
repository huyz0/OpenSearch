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
import org.opensearch.serverless.cluster.IndexDescriptor;
import org.opensearch.serverless.cluster.ShardAssignment;
import org.opensearch.serverless.metadata.Acquisition;
import org.opensearch.serverless.metadata.IndexAlreadyExistsException;
import org.opensearch.serverless.metadata.MetadataPlane;
import org.opensearch.serverless.metadata.ShardHead;
import org.opensearch.serverless.metadata.Truth;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Phase 3: descriptors and shard-heads as CAS registers.
 *
 * <p><b>D5 applies to every test here.</b> These run against {@code FsBlobContainer}, whose
 * compare-and-swap is real local-filesystem atomicity. That says nothing about whether S3's
 * {@code If-Match} or GCS's generation preconditions behave the same way under contention, which is R11
 * and remains open. A green run here is not evidence about a cloud provider.
 */
public class MetadataPlaneTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private MetadataPlane plane(AtomicLong clock) throws Exception {
        final FsBlobStore store = new FsBlobStore(1024, createTempDir(), false);
        return new MetadataPlane(store, BlobPath.cleanPath(), clock::get, TTL);
    }

    private static IndexDescriptor alpha() {
        return new IndexDescriptor("alpha", "uuid-alpha-00000000", 2, "{\"properties\":{\"msg\":{\"type\":\"text\"}}}", null);
    }

    public void testCreateIndexIsPutIfAbsentAndNamesAreUnique() throws Exception {
        final MetadataPlane plane = plane(new AtomicLong(1_000L));
        final long generation = plane.createIndex(alpha());
        assertTrue("creation must report a real generation", generation > 0);

        // Name uniqueness with no cluster-manager and no lock: the object store arbitrates.
        final IndexAlreadyExistsException e = expectThrows(IndexAlreadyExistsException.class, () -> plane.createIndex(alpha()));
        assertTrue(e.getMessage().contains("alpha"));

        final Optional<IndexDescriptor> read = plane.describe("alpha");
        assertTrue("descriptor must be readable back", read.isPresent());
        assertEquals(alpha(), read.get());
        assertEquals(2, read.get().numberOfShards());
    }

    public void testDescribeDistinguishesAbsentFromPresent() throws Exception {
        final MetadataPlane plane = plane(new AtomicLong(1_000L));
        assertTrue("an index that was never created must read as absent", plane.describe("ghost").isEmpty());
        plane.createIndex(alpha());
        assertTrue(plane.describe("alpha").isPresent());
    }

    public void testActivationTakesOwnershipAndBumpsTheTerm() throws Exception {
        final MetadataPlane plane = plane(new AtomicLong(1_000L));
        plane.createIndex(alpha());

        final Acquisition first = plane.activate("alpha", 0, "node-a", "eph-a");
        assertTrue("first activation of a never-activated shard must win", first.acquired());
        assertEquals("a fresh head starts at term 1", 1L, first.head().term());
        assertEquals("node-a", first.head().ownerNodeId());

        // The same node re-acquiring bumps the term: a new claim, not a renewal.
        final Acquisition again = plane.activate("alpha", 0, "node-a", "eph-a2");
        assertTrue(again.acquired());
        assertEquals(2L, again.head().term());
    }

    public void testALiveLeaseIsNotStolenAndTheLoserLearnsTheWinner() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(alpha());

        assertTrue(plane.activate("alpha", 0, "node-a", "eph-a").acquired());

        final Acquisition contender = plane.activate("alpha", 0, "node-b", "eph-b");
        assertFalse("a live lease must not be stolen", contender.acquired());
        // The loser is told who won, so it routes there instead of retrying elsewhere and
        // ping-ponging ownership -- the failure rfc-serverless-metadata-plane.md section 6 warns about.
        assertEquals("node-a", contender.head().ownerNodeId());
        assertEquals(1L, contender.head().term());
    }

    public void testAnExpiredLeaseIsAcquirableAndBumpsTheTerm() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(alpha());
        assertTrue(plane.activate("alpha", 0, "node-a", "eph-a").acquired());

        clock.set(1_000L + TTL);   // node-a's lease has lapsed

        final Acquisition failover = plane.activate("alpha", 0, "node-b", "eph-b");
        assertTrue("an expired lease must be acquirable", failover.acquired());
        assertEquals("failover must bump the term", 2L, failover.head().term());
        assertEquals("node-b", failover.head().ownerNodeId());
    }

    public void testRenewExtendsTheLeaseWithoutBumpingTheTerm() throws Exception {
        final AtomicLong clock = new AtomicLong(1_000L);
        final MetadataPlane plane = plane(clock);
        plane.createIndex(alpha());
        plane.activate("alpha", 0, "node-a", "eph-a");

        clock.set(20_000L);
        final Optional<ShardHead> renewed = plane.heads().renew("alpha", 0, "node-a");
        assertTrue("the owner must be able to renew", renewed.isPresent());
        assertEquals("renewal must not bump the term; nothing changed hands", 1L, renewed.get().term());

        // Past the original expiry, still held.
        clock.set(1_000L + TTL);
        assertFalse("renewal did not extend the lease", plane.activate("alpha", 0, "node-b", "eph-b").acquired());

        // A non-owner cannot renew.
        assertTrue(plane.heads().renew("alpha", 0, "node-b").isEmpty());
    }

    /** Concurrent contenders for one never-activated shard: exactly one may win. */
    public void testConcurrentActivationHasExactlyOneWinner() throws Exception {
        final MetadataPlane plane = plane(new AtomicLong(1_000L));
        plane.createIndex(alpha());

        final int contenders = 8;
        final CountDownLatch startLine = new CountDownLatch(1);
        final AtomicInteger winners = new AtomicInteger();
        final AtomicInteger errors = new AtomicInteger();
        final Thread[] threads = new Thread[contenders];
        for (int i = 0; i < contenders; i++) {
            final String nodeId = "node-" + i;
            threads[i] = new Thread(() -> {
                try {
                    startLine.await();
                    if (plane.activate("alpha", 1, nodeId, nodeId + "-eph").acquired()) {
                        winners.incrementAndGet();
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                }
            });
            threads[i].start();
        }
        startLine.countDown();
        for (Thread t : threads) {
            t.join(30_000L);
        }
        assertEquals("no contender should have errored", 0, errors.get());
        assertEquals("compare-and-swap must admit exactly one winner", 1, winners.get());
        assertTrue(plane.heads().read("alpha", 1).isPresent());
    }

    public void testTruthForReturnsOnlyWhatThisNodeHosts() throws Exception {
        final MetadataPlane plane = plane(new AtomicLong(1_000L));
        plane.createIndex(alpha());
        plane.createIndex(new IndexDescriptor("beta", "uuid-beta-000000000", 1, null, null));

        plane.activate("alpha", 0, "node-a", "eph-a");
        plane.activate("alpha", 1, "node-b", "eph-b");
        plane.activate("beta", 0, "node-b", "eph-b");

        final Truth a = plane.truthFor("node-a");
        assertEquals("node-a hosts one shard", 1, a.assignments().size());
        assertEquals(Set.of("alpha"), a.descriptors().stream().map(IndexDescriptor::name).collect(Collectors.toSet()));

        final Truth b = plane.truthFor("node-b");
        assertEquals("node-b hosts two shards", 2, b.assignments().size());
        assertEquals(Set.of("alpha", "beta"), b.descriptors().stream().map(IndexDescriptor::name).collect(Collectors.toSet()));

        // A node that owns nothing gets nothing -- residency tracks the working set, not the world.
        final Truth c = plane.truthFor("node-c");
        assertTrue(c.assignments().isEmpty());
        assertTrue(c.descriptors().isEmpty());
    }

    public void testTruthCarriesTheShardHeadTermNotACounter() throws Exception {
        final MetadataPlane plane = plane(new AtomicLong(1_000L));
        plane.createIndex(alpha());
        plane.activate("alpha", 0, "node-a", "eph-a");
        plane.activate("alpha", 0, "node-a", "eph-a");   // term 2
        plane.activate("alpha", 0, "node-a", "eph-a");   // term 3

        final List<ShardAssignment> owned = List.copyOf(plane.truthFor("node-a").assignments());
        assertEquals(1, owned.size());
        assertEquals("the assignment must carry the head's term, which s1-findings.md showed is load-bearing", 3L, owned.get(0).term());
    }

    public void testDeleteIndexRemovesHeadsAndDescriptor() throws Exception {
        final MetadataPlane plane = plane(new AtomicLong(1_000L));
        plane.createIndex(alpha());
        plane.activate("alpha", 0, "node-a", "eph-a");
        plane.activate("alpha", 1, "node-a", "eph-a");
        assertEquals(2, plane.truthFor("node-a").assignments().size());

        assertTrue(plane.deleteIndex("alpha"));

        assertTrue("the descriptor must be gone", plane.describe("alpha").isEmpty());
        assertTrue("shard head 0 must be gone", plane.heads().read("alpha", 0).isEmpty());
        assertTrue("shard head 1 must be gone", plane.heads().read("alpha", 1).isEmpty());
        assertTrue("truth must no longer mention it", plane.truthFor("node-a").assignments().isEmpty());

        assertFalse("deleting a missing index reports false rather than throwing", plane.deleteIndex("alpha"));

        // The name is free again, which it would not be if the descriptor register lingered.
        assertTrue(plane.createIndex(alpha()) > 0);
    }

    public void testHeadAndDescriptorSurviveSerializationRoundTrips() throws Exception {
        final IndexDescriptor descriptor = alpha();
        assertEquals(
            descriptor,
            IndexDescriptor.fromStream(
                new ByteArrayInputStream(org.opensearch.core.common.bytes.BytesReference.toBytes(descriptor.toBytes()))
            )
        );

        final ShardHead head = new ShardHead("alpha", 3, 9L, "node-a", "eph-a", 12_345L);
        final ShardHead parsed = ShardHead.fromStream(
            new ByteArrayInputStream(org.opensearch.core.common.bytes.BytesReference.toBytes(head.toBytes()))
        );
        assertEquals(head.indexName(), parsed.indexName());
        assertEquals(head.shardId(), parsed.shardId());
        assertEquals(head.term(), parsed.term());
        assertEquals(head.ownerNodeId(), parsed.ownerNodeId());
        assertEquals(head.leaseExpiresAtMillis(), parsed.leaseExpiresAtMillis());
    }
}
