/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.testkit;

import org.opensearch.serverless.bucket.BucketPlacement;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Every node places a bucket alike from the leases, a join moves about one bucket in N, and churn is damped.
 */
public class BucketPlacementTests extends OpenSearchTestCase {

    private static final long HOUR = 3_600_000L;
    private static final Predicate<NodeLease> WRITER = lease -> lease.roles().contains("ingest");

    private static NodeLease lease(String node, String eph, long now) {
        return new NodeLease(node, eph, "127.0.0.1:9300", Set.of("ingest"), now + HOUR);
    }

    private static List<NodeLease> fleet(int n, long now) {
        final List<NodeLease> leases = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            leases.add(lease("node-" + i, "e" + i, now));
        }
        return leases;
    }

    private static String bucket(int i) {
        return String.format(Locale.ROOT, "b-%05d", i);
    }

    public void testEveryNodeAgreesAndBucketsSpreadEvenly() {
        final long now = 1_000_000L;
        final List<NodeLease> leases = fleet(10, now);
        final BucketPlacement one = new BucketPlacement();
        final BucketPlacement two = new BucketPlacement();
        final Map<String, Integer> perNode = new HashMap<>();
        for (int b = 0; b < 10_000; b++) {
            final String owner = one.ownerFor(bucket(b), leases, now, WRITER).orElseThrow();
            assertEquals("two nodes must place a bucket alike", owner, two.ownerFor(bucket(b), leases, now, WRITER).orElseThrow());
            perNode.merge(owner, 1, Integer::sum);
        }
        for (int count : perNode.values()) {
            assertTrue("about a tenth of the buckets per node, got " + perNode, count > 800 && count < 1200);
        }
    }

    public void testAJoinerWaitsOutItsHoldDownThenTakesAboutOneInN() {
        long now = 1_000_000L;
        final BucketPlacement placement = new BucketPlacement();
        final List<NodeLease> before = fleet(9, now);
        placement.observe(before, now);
        final Map<String, String> owners = new HashMap<>();
        for (int b = 0; b < 9_000; b++) {
            owners.put(bucket(b), placement.ownerFor(bucket(b), before, now, WRITER).orElseThrow());
        }
        final List<NodeLease> after = new ArrayList<>(before);
        after.add(lease("node-new", "en", now));
        for (int b = 0; b < 9_000; b++) {
            assertEquals(
                "a joiner in its hold-down takes nothing",
                owners.get(bucket(b)),
                placement.ownerFor(bucket(b), after, now, WRITER).orElseThrow()
            );
        }
        now += BucketPlacement.DEFAULT_HOLD_DOWN_MILLIS;
        int moved = 0;
        for (int b = 0; b < 9_000; b++) {
            final String owner = placement.ownerFor(bucket(b), after, now, WRITER).orElseThrow();
            if (owner.equals(owners.get(bucket(b))) == false) {
                assertEquals("only the joiner gains buckets", "node-new", owner);
                moved++;
            }
        }
        assertTrue("about one in ten moves, got " + moved, moved > 650 && moved < 1150);
    }

    public void testAFlappingNodeWaitsLongerEachTime() {
        long now = 1_000_000L;
        final BucketPlacement placement = new BucketPlacement();
        final List<NodeLease> fleet = fleet(3, now);
        placement.observe(fleet, now);
        final String bucket = findBucketWonBy(placement, fleet, "node-0", now);
        // node-0 restarts: a new incarnation, then again soon after.
        fleet.set(0, lease("node-0", "e0-b", now));
        placement.observe(fleet, now);
        now += 60_000L;
        fleet.set(0, lease("node-0", "e0-c", now));
        placement.observe(fleet, now);
        now += BucketPlacement.DEFAULT_HOLD_DOWN_MILLIS;
        assertNotEquals(
            "a flapping node waits twice the hold-down",
            "node-0",
            placement.ownerFor(bucket, fleet, now, WRITER).orElseThrow()
        );
        now += BucketPlacement.DEFAULT_HOLD_DOWN_MILLIS;
        assertEquals("node-0", placement.ownerFor(bucket, fleet, now, WRITER).orElseThrow());
    }

    public void testAnOverloadedNodeIsPassedOverAndAHolderInTheTopTwoKeepsItsBucket() {
        final long now = 1_000_000L;
        final BucketPlacement placement = new BucketPlacement();
        final List<NodeLease> fleet = fleet(4, now);
        final String bucket = findBucketWonBy(placement, fleet, "node-0", now);
        final List<String> ranked = placement.rank(bucket, fleet, now, WRITER);
        assertTrue(placement.keeps(bucket, ranked.get(1), fleet, now, WRITER));
        assertFalse(placement.keeps(bucket, ranked.get(2), fleet, now, WRITER));

        final List<NodeLease> loaded = new ArrayList<>();
        for (NodeLease lease : fleet) {
            loaded.add(lease.nodeId().equals("node-0") ? lease.withLoad(100, 1000) : lease.withLoad(10, 1000));
        }
        assertNotEquals(
            "past 1.25x the mean, a node is passed over",
            "node-0",
            placement.ownerFor(bucket, loaded, now, WRITER).orElseThrow()
        );
    }

    public void testOnlyLiveWritersAreRanked() {
        final long now = 1_000_000L;
        final BucketPlacement placement = new BucketPlacement();
        final List<NodeLease> fleet = new ArrayList<>(fleet(2, now));
        fleet.add(new NodeLease("searcher", "es", "127.0.0.1:9300", Set.of("search"), now + HOUR));
        fleet.add(new NodeLease("expired", "ex", "127.0.0.1:9300", Set.of("ingest"), now - 1));
        for (int b = 0; b < 100; b++) {
            final String owner = placement.ownerFor(bucket(b), fleet, now, WRITER).orElseThrow();
            assertTrue(owner, owner.startsWith("node-"));
        }
        assertTrue(placement.ownerFor("b-1", List.of(), now, WRITER).isEmpty());
    }

    private static String findBucketWonBy(BucketPlacement placement, List<NodeLease> fleet, String node, long now) {
        for (int b = 0;; b++) {
            if (placement.ownerFor(bucket(b), fleet, now, WRITER).orElseThrow().equals(node)) {
                return bucket(b);
            }
        }
    }
}
