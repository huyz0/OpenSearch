/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.storage.shardstate;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Whether two nodes that both believe they own one writer shard can both publish to it.
 *
 * <h2>Why this suite exists</h2>
 *
 * {@link BlobContainerShardStateStoreTests} races the register and proves it is a correct
 * compare-and-swap: twenty threads activating, exactly one winner; eight publishers with retries, an
 * exact final generation. All of that was already true and none of it was the problem. <b>The register
 * serialises writes; it says nothing about who is entitled to make them</b>, and the policy layered on
 * top of it -- which writer is the current one -- had no test at all.
 *
 * <p>It also had no implementation. Fencing was written entirely in terms of the primary term
 * ({@code currentHead.leaseTerm() > primaryTerm} refuses, anything else proceeds), and for a gated index
 * the primary term is the compile-time constant {@code IndexDescriptor.FIRST_PRIMARY_TERM = 1}: a
 * published index's term is bumped by the cluster manager on each primary promotion and a gated index has
 * no cluster manager step to bump it in. So the comparison was {@code 1 > 1}, false forever, the refusal
 * branch was unreachable, and {@link ShardHead#leaseHolderNodeId()} was overwritten by whoever asked last
 * and never compared against the asker. Two nodes both acquired, both published, and the head alternated
 * between two complete manifest lineages derived from two different local Lucene commits -- each
 * generation missing the documents the other had acknowledged, with the CAS reporting success to both.
 *
 * <h2>What the helpers below are</h2>
 *
 * {@link #acquireOrRenewLease} and {@link #publish} are the protocol {@code
 * ObjectStoreCommitHeadPublisher} is to run, transcribed here against the same primitives the engine
 * uses -- a real {@link FsBlobContainer} through {@link BlobContainerShardStateStore}. They are
 * deliberately not a mock: every property asserted below is a property of {@link ShardHead}'s own
 * predicates and of the store's CAS, so it holds for the engine exactly as it holds here.
 */
public class ShardHeadWriterFencingTests extends OpenSearchTestCase {

    private static final String UUID = "idx-uuid";
    private static final int SHARD = 0;

    /** The one term every gated shard is opened at, which is the whole difficulty. */
    private static final long GATED_PRIMARY_TERM = 1L;

    private static final long LEASE_TTL_MILLIS = 30_000L;

    private ShardStateStore newStore() throws Exception {
        Path dir = createTempDir();
        FsBlobStore blobStore = new FsBlobStore(1024, dir, false);
        BlobContainer container = new FsBlobContainer(blobStore, BlobPath.cleanPath(), blobStore.path());
        return new BlobContainerShardStateStore(container);
    }

    /**
     * One node's attempt to acquire or renew this shard's writer lease.
     *
     * @return the lease term the successful acquisition installed -- the node's fencing token, to be held
     *         on the engine and presented to every later publish -- or empty when the lease is held by
     *         somebody else and this node has been fenced out.
     */
    private static OptionalLong acquireOrRenewLease(ShardStateStore store, String nodeId, long primaryTerm, long nowMillis)
        throws Exception {
        for (int attempt = 0; attempt < 16; attempt++) {
            Optional<VersionedShardHead> current = store.get(UUID, SHARD);
            ShardHead head = current.map(VersionedShardHead::head).orElse(null);
            if (head != null && head.isLeaseHeldByAnotherNodeAt(nodeId, nowMillis)) {
                // The refusal that did not exist. Not a term comparison: an unexpired lease held by a
                // different node is the observation, and it is available whatever the terms say.
                return OptionalLong.empty();
            }
            ShardHead next;
            if (head == null) {
                next = new ShardHead(primaryTerm, nodeId, nowMillis + LEASE_TTL_MILLIS, 0L);
            } else if (nodeId.equals(head.leaseHolderNodeId())) {
                // A heartbeat by the node that already holds it. Must not advance the lease term, or a
                // writer would fence itself out against the token it is holding.
                next = head.withRenewedLease(nodeId, nowMillis + LEASE_TTL_MILLIS, primaryTerm);
            } else {
                // A genuine takeover of a free or lapsed lease, which is the only thing that advances the
                // fencing token.
                next = head.withTakenOverLease(nodeId, nowMillis + LEASE_TTL_MILLIS, primaryTerm);
            }
            if (store.compareAndSet(UUID, SHARD, current.map(VersionedShardHead::version), next) == CasResult.SUCCESS) {
                return OptionalLong.of(next.leaseTerm());
            }
        }
        return OptionalLong.empty();
    }

    /** One node's attempt to publish the next generation, presenting the token its acquisition returned. */
    private static boolean publish(ShardStateStore store, String nodeId, long acquiredLeaseTerm, long primaryTerm) throws Exception {
        Optional<VersionedShardHead> current = store.get(UUID, SHARD);
        ShardHead head = current.map(VersionedShardHead::head).orElse(null);
        if (head == null || head.isStillHeldBy(nodeId, acquiredLeaseTerm) == false) {
            return false;
        }
        ShardHead published = head.withPublishedGeneration(primaryTerm, head.latestManifestGeneration() + 1);
        return store.compareAndSet(UUID, SHARD, current.map(VersionedShardHead::version), published) == CasResult.SUCCESS;
    }

    /**
     * The finding, at its narrowest: same shard, same primary term, two node ids, one live lease.
     *
     * <p>Before the identity check this passed for both nodes -- {@code 1 > 1} is false, so B's acquire
     * fell through to an unconditional overwrite of the lease holder and returned true.
     */
    public void testASecondNodeCannotAcquireALeaseTheFirstStillHolds() throws Exception {
        ShardStateStore store = newStore();
        long now = 1_000L;

        OptionalLong a = acquireOrRenewLease(store, "node-A", GATED_PRIMARY_TERM, now);
        assertTrue("the first acquisition must succeed", a.isPresent());

        OptionalLong b = acquireOrRenewLease(store, "node-B", GATED_PRIMARY_TERM, now + 1);
        assertFalse(
            "a second node at the same primary term must be refused while the first node's lease is live; "
                + "if it is not, both publish and the head alternates between two divergent lineages",
            b.isPresent()
        );

        ShardHead head = store.get(UUID, SHARD).orElseThrow().head();
        assertEquals("node-A", head.leaseHolderNodeId());
    }

    /**
     * Eight nodes, one primary term, one instant: exactly one may come away holding the lease.
     *
     * <p>The concurrent form of the test above, and the one that would have caught the defect in the shape
     * it actually occurs in -- a membership epoch changing under two nodes that have not yet agreed about
     * it. The losers must lose at the fencing check or at the CAS; which one is not the point, and both
     * are exercised here because the interleaving decides.
     */
    public void testConcurrentAcquisitionAtEqualTermsHasExactlyOneWinner() throws Exception {
        ShardStateStore store = newStore();
        int nodes = 8;
        ExecutorService pool = Executors.newFixedThreadPool(nodes);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(nodes);
        List<String> winners = new CopyOnWriteArrayList<>();
        List<Exception> failures = new CopyOnWriteArrayList<>();
        try {
            for (int i = 0; i < nodes; i++) {
                String nodeId = "node-" + i;
                pool.execute(() -> {
                    try {
                        start.await();
                        if (acquireOrRenewLease(store, nodeId, GATED_PRIMARY_TERM, 1_000L).isPresent()) {
                            winners.add(nodeId);
                        }
                    } catch (Exception e) {
                        failures.add(e);
                    } finally {
                        done.countDown();
                    }
                });
            }
            start.countDown();
            assertTrue(done.await(30, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        assertTrue("no writer should have failed outright: " + failures, failures.isEmpty());
        assertEquals("exactly one node may hold a writer lease: " + winners, 1, winners.size());
        assertEquals(winners.get(0), store.get(UUID, SHARD).orElseThrow().head().leaseHolderNodeId());
    }

    /**
     * The other half of the protocol: after a legitimate takeover, the displaced writer's publish fails.
     *
     * <p>This is what makes the lease term a fencing token rather than a label. The takeover advances it
     * strictly, so the displaced writer's token no longer matches -- <em>immediately</em>, before the new
     * writer has published anything, which is the window a publication-driven bump leaves open and the one
     * a writer partitioned from the cluster manager sits in indefinitely.
     */
    public void testAPublishAfterALegitimateTakeoverIsRefused() throws Exception {
        ShardStateStore store = newStore();
        long now = 1_000L;

        long aToken = acquireOrRenewLease(store, "node-A", GATED_PRIMARY_TERM, now).orElseThrow();
        assertTrue("A holds the lease, so A may publish", publish(store, "node-A", aToken, GATED_PRIMARY_TERM));

        // A's lease lapses -- A is partitioned, or paused, and has not renewed.
        long afterExpiry = now + LEASE_TTL_MILLIS + 1;
        long bToken = acquireOrRenewLease(store, "node-B", GATED_PRIMARY_TERM, afterExpiry).orElseThrow();
        assertTrue("a takeover must advance the fencing token strictly: " + aToken + " -> " + bToken, bToken > aToken);

        assertFalse(
            "A was superseded and must not be able to publish, even though its primary term is identical "
                + "to B's and its own lease looked fine to it",
            publish(store, "node-A", aToken, GATED_PRIMARY_TERM)
        );
        assertTrue("B holds the lease and may publish", publish(store, "node-B", bToken, GATED_PRIMARY_TERM));

        ShardHead head = store.get(UUID, SHARD).orElseThrow().head();
        assertEquals("node-B", head.leaseHolderNodeId());
        assertEquals("A's refused publish must not have consumed a generation", 2L, head.latestManifestGeneration());
    }

    /**
     * A's renewal after B has taken over is refused too, rather than silently taking the lease back.
     *
     * <p>The engine renews on a timer, so without this a displaced writer reclaims the shard on its next
     * heartbeat and the two nodes trade the lease back and forth for as long as both stay up.
     */
    public void testTheDisplacedWritersNextHeartbeatIsRefused() throws Exception {
        ShardStateStore store = newStore();
        long now = 1_000L;
        long aToken = acquireOrRenewLease(store, "node-A", GATED_PRIMARY_TERM, now).orElseThrow();
        acquireOrRenewLease(store, "node-B", GATED_PRIMARY_TERM, now + LEASE_TTL_MILLIS + 1).orElseThrow();

        OptionalLong renewed = acquireOrRenewLease(store, "node-A", GATED_PRIMARY_TERM, now + LEASE_TTL_MILLIS + 2);
        assertFalse("A must be told it has been superseded, not handed the lease back", renewed.isPresent());
        assertEquals("node-B", store.get(UUID, SHARD).orElseThrow().head().leaseHolderNodeId());
        assertFalse(publish(store, "node-A", aToken, GATED_PRIMARY_TERM));
    }

    /**
     * A heartbeat by the node already holding the lease must leave the fencing token where it is.
     *
     * <p>The inverse failure of the one above, and easy to introduce while fixing it: bump on every
     * acquisition rather than on every takeover and the holder invalidates its own token ten seconds after
     * activating, so the shard fences its only legitimate writer out and stops accepting writes entirely.
     */
    public void testRenewalByTheHolderDoesNotAdvanceTheFencingToken() throws Exception {
        ShardStateStore store = newStore();
        long now = 1_000L;
        long token = acquireOrRenewLease(store, "node-A", GATED_PRIMARY_TERM, now).orElseThrow();

        for (int heartbeat = 1; heartbeat <= 5; heartbeat++) {
            long renewed = acquireOrRenewLease(store, "node-A", GATED_PRIMARY_TERM, now + heartbeat * 10_000L).orElseThrow();
            assertEquals("renewal is not takeover", token, renewed);
            assertTrue("the holder must still be able to publish after renewing", publish(store, "node-A", token, GATED_PRIMARY_TERM));
        }
        assertEquals(5L, store.get(UUID, SHARD).orElseThrow().head().latestManifestGeneration());
    }

    /**
     * The token advances across every takeover and never repeats, which is what "monotonic" has to mean
     * for it to stand in for a primary term nothing advances.
     */
    public void testTheLeaseTermIsMonotonicAcrossRepeatedTakeovers() throws Exception {
        ShardStateStore store = newStore();
        long now = 1_000L;
        long previous = Long.MIN_VALUE;
        for (int round = 0; round < 6; round++) {
            String nodeId = "node-" + (round % 2);
            long token = acquireOrRenewLease(store, nodeId, GATED_PRIMARY_TERM, now).orElseThrow();
            assertTrue("round " + round + " token " + token + " must exceed " + previous, token > previous);
            previous = token;
            now += LEASE_TTL_MILLIS + 1;
        }
    }

    /** The predicates themselves, stated directly so a change to either is caught here rather than above. */
    public void testTheHeadsOwnFencingPredicates() {
        ShardHead free = ShardHead.initial();
        assertFalse("nobody holds an initial head's lease", free.isLeaseHeldByAnotherNodeAt("node-A", 1_000L));

        ShardHead held = free.withTakenOverLease("node-A", 5_000L, 1L);
        assertTrue(held.isLeaseHeldByAnotherNodeAt("node-B", 1_000L));
        assertFalse("the holder is not 'another node'", held.isLeaseHeldByAnotherNodeAt("node-A", 1_000L));
        assertFalse("an expired lease does not block a takeover", held.isLeaseHeldByAnotherNodeAt("node-B", 9_000L));

        assertTrue(held.isStillHeldBy("node-A", held.leaseTerm()));
        assertFalse("the right token on the wrong node is not this tenancy", held.isStillHeldBy("node-B", held.leaseTerm()));
        assertFalse("the wrong token on the right node is a previous tenancy", held.isStillHeldBy("node-A", held.leaseTerm() - 1));
        assertFalse("a null node id can never hold a lease", held.isStillHeldBy(null, held.leaseTerm()));
    }
}
