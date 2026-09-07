/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.serverless.membership.BlobLeaseMembership;
import org.opensearch.serverless.membership.MembershipDelta;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Membership as a derived view over leases — {@code rfc-serverless-shell.md} §10.1, §10.2.
 *
 * <p>The clock is injected rather than slept on: a test that sleeps for a TTL is slow and flaky, and
 * these assertions are about ordering against a clock, not about real time.
 */
public class BlobLeaseMembershipTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    private FsBlobContainer container(Path dir) throws Exception {
        final FsBlobStore store = new FsBlobStore(1024, dir, false);
        return new FsBlobContainer(store, BlobPath.cleanPath(), dir);
    }

    private static NodeLease lease(String id, String... roles) {
        return new NodeLease(id, id + "-eph", "127.0.0.1:9300", Set.of(roles), 0L);
    }

    public void testRenewMakesANodeVisible() throws Exception {
        final AtomicLong now = new AtomicLong(1_000L);
        final BlobLeaseMembership membership = new BlobLeaseMembership(container(createTempDir()), now::get, TTL);

        assertEquals("nothing should be visible before any renewal", Set.of(), membership.refresh());

        membership.renew(lease("node-a", "ingest"));
        final Set<NodeLease> live = membership.refresh();

        assertEquals("exactly one node should be visible", 1, live.size());
        final NodeLease seen = live.iterator().next();
        assertEquals("node-a", seen.nodeId());
        assertEquals(Set.of("ingest"), seen.roles());
        assertEquals("expiry must be stamped at now + ttl", 31_000L, seen.expiresAtMillis());
    }

    public void testAnExpiredLeaseDisappearsWithoutAnyDeletion() throws Exception {
        final AtomicLong now = new AtomicLong(1_000L);
        final BlobLeaseMembership membership = new BlobLeaseMembership(container(createTempDir()), now::get, TTL);
        membership.renew(lease("node-a", "search"));
        assertEquals(1, membership.refresh().size());

        // One millisecond before expiry: still alive.
        now.set(30_999L);
        assertEquals("a lease must not expire early", 1, membership.refresh().size());

        // At expiry: gone. Nothing deleted the blob — liveness is a function of the clock.
        now.set(31_000L);
        assertEquals("an expired lease must not be reported live", 0, membership.refresh().size());
    }

    public void testRenewalExtendsLifeAndAdvancesTheRegisterGeneration() throws Exception {
        final AtomicLong now = new AtomicLong(1_000L);
        final FsBlobContainer c = container(createTempDir());
        final BlobLeaseMembership membership = new BlobLeaseMembership(c, now::get, TTL);

        membership.renew(lease("node-a", "ingest"));
        final long firstGeneration = c.readRegister(BlobLeaseMembership.LEASE_PREFIX + "node-a").orElseThrow().generation();

        now.set(20_000L);
        membership.renew(lease("node-a", "ingest"));
        final long secondGeneration = c.readRegister(BlobLeaseMembership.LEASE_PREFIX + "node-a").orElseThrow().generation();

        assertTrue("CAS generation must advance on renewal, or a stale writer could not be detected", secondGeneration > firstGeneration);

        // Past the ORIGINAL expiry but within the renewed one.
        now.set(31_000L);
        assertEquals("renewal did not extend the lease", 1, membership.refresh().size());
    }

    public void testTwoNodesAreBothVisibleAndReleaseIsImmediate() throws Exception {
        final AtomicLong now = new AtomicLong(1_000L);
        final BlobLeaseMembership membership = new BlobLeaseMembership(container(createTempDir()), now::get, TTL);

        membership.renew(lease("node-a", "ingest"));
        membership.renew(lease("node-b", "search"));

        final Set<String> ids = membership.refresh().stream().map(NodeLease::nodeId).collect(Collectors.toSet());
        assertEquals(Set.of("node-a", "node-b"), ids);

        // A clean shutdown does not make peers wait out a TTL.
        membership.release("node-b");
        assertEquals(Set.of("node-a"), membership.refresh().stream().map(NodeLease::nodeId).collect(Collectors.toSet()));
    }

    public void testSubscribersSeeJoinsAndLeaves() throws Exception {
        final AtomicLong now = new AtomicLong(1_000L);
        final BlobLeaseMembership membership = new BlobLeaseMembership(container(createTempDir()), now::get, TTL);
        final List<MembershipDelta> deltas = new ArrayList<>();
        membership.subscribe(deltas::add);

        membership.renew(lease("node-a", "ingest"));
        membership.refresh();
        assertEquals("a join should have been reported", 1, deltas.size());
        assertEquals(1, deltas.get(0).joined().size());
        assertEquals(0, deltas.get(0).left().size());

        // An unchanged refresh must not produce a delta — otherwise polling is pure noise.
        membership.refresh();
        assertEquals("an unchanged refresh must not notify", 1, deltas.size());

        now.set(31_000L);
        membership.refresh();
        assertEquals("an expiry should have been reported", 2, deltas.size());
        assertEquals(1, deltas.get(1).left().size());
        assertEquals("node-a", deltas.get(1).left().iterator().next().nodeId());
    }

    /**
     * A refresh does not re-read a lease whose own stamp says it cannot have run out.
     *
     * <p><b>The cost this pins.</b> The loop read one register per member on every refresh, and a request
     * path refreshes at half a lease. So each node spent one read per other node every few seconds, which
     * across the fleet is quadratic in its size — paid to be told that leases renewed every third of a
     * lifetime had not expired within half of one. An expiry only ever moves forward, so a stamp still in
     * the future cannot have become false, and the read is skipped.
     *
     * <p>Counted through a store that records requests, because the claim is a number.
     */
    public void testARefreshDoesNotRereadLeasesThatCannotHaveExpired() throws Exception {
        final AtomicLong now = new AtomicLong(1_000L);
        final CountingBlobStore store = new CountingBlobStore(new FsBlobStore(1024, createTempDir(), false));
        final BlobLeaseMembership membership = new BlobLeaseMembership(store.blobContainer(BlobPath.cleanPath()), now::get, TTL);
        for (int i = 0; i < 5; i++) {
            membership.renew(lease("node-" + i));
        }

        // The first refresh has nothing remembered and reads every one of them.
        store.reset();
        assertEquals(5, membership.refresh().size());
        final long cold = store.registerReads();

        // The second, a moment later, reads only the members index.
        store.reset();
        assertEquals(5, membership.refresh().size());
        final long warm = store.registerReads();

        logger.info("membership: a cold refresh over 5 members read {} registers, a warm one read {}", cold, warm);
        assertTrue("a cold refresh must read a lease per member: " + cold, cold >= 5);
        assertEquals("a warm refresh must read the members index and nothing else", 1, warm);

        // And once the leases have run out by their own stamps, it reads them again rather than trusting
        // what it remembered -- which is what lets a node that renewed come back, and one that did not go.
        now.addAndGet(TTL + 1);
        store.reset();
        assertTrue("every lease has expired, so none is live", membership.refresh().isEmpty());
        assertTrue("and each must have been read again: " + store.registerReads(), store.registerReads() >= 5);
    }

    /**
     * A node that stops renewing still leaves the snapshot, on the schedule its own lease describes.
     *
     * <p>The saving is that an unexpired lease is not re-read. What must not follow is that a dead node
     * lives forever in the snapshot because nobody looks again — so the memory is bounded by the stamp it
     * came with, and expiry is still expiry.
     */
    public void testANodeThatStopsRenewingStillLeaves() throws Exception {
        final AtomicLong now = new AtomicLong(1_000L);
        final BlobLeaseMembership membership = new BlobLeaseMembership(container(createTempDir()), now::get, TTL);
        membership.renew(lease("stays"));
        membership.renew(lease("goes"));
        assertEquals(2, membership.refresh().size());

        // Half a lifetime on, both are still believed live without a read.
        now.addAndGet(TTL / 2);
        assertEquals(2, membership.refresh().size());

        // Past the lifetime, one renews and the other does not.
        now.addAndGet(TTL);
        membership.renew(lease("stays"));
        final Set<String> live = membership.refresh().stream().map(NodeLease::nodeId).collect(Collectors.toSet());
        assertEquals("only the node still renewing may remain", Set.of("stays"), live);
    }

    public void testLeaseSurvivesASerializationRoundTrip() throws Exception {
        final NodeLease original = new NodeLease("node-x", "eph-7", "10.0.0.4:9300", Set.of("ingest", "search"), 12_345L);
        final NodeLease parsed = NodeLease.fromStream(new ByteArrayInputStream(BytesToArray(original)));
        assertEquals(original, parsed);
        assertEquals(Set.of("ingest", "search"), parsed.roles());
    }

    private static byte[] BytesToArray(NodeLease lease) throws Exception {
        return org.opensearch.core.common.bytes.BytesReference.toBytes(lease.toBytes());
    }
}
