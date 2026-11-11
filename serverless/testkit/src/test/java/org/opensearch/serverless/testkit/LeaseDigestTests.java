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
import org.opensearch.serverless.membership.BlobLeaseMembership;
import org.opensearch.serverless.membership.NodeLease;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The lease digest saves reads and changes no answer: a lease taken from it is never live past its own stamp, and a
 * digest nobody writes any more leaves every node reading leases one by one, as before.
 */
public class LeaseDigestTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;
    private static final long STEP = 1_000L;

    private AtomicLong clock;
    private CountingBlobStore store;
    private final List<BlobLeaseMembership> members = new ArrayList<>();
    private final List<NodeLease> leases = new ArrayList<>();
    private final List<Boolean> running = new ArrayList<>();

    @Override
    public void setUp() throws Exception {
        super.setUp();
        clock = new AtomicLong(1_000_000L);
        store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false));
        for (int i = 0; i < 6; i++) {
            members.add(new BlobLeaseMembership(store.blobContainer(BlobPath.cleanPath().add("members")), clock::get, TTL));
            leases.add(new NodeLease("node-" + i, "e" + i, "10.0.0." + i + ":9300", Set.of(), 0L));
            running.add(true);
        }
    }

    @Override
    public void tearDown() throws Exception {
        store.close();
        super.tearDown();
    }

    /** Runs every running node: a renewal and a refresh each third of a lifetime, spread across it. */
    private void run(long millis) throws Exception {
        for (long t = 0; t < millis; t += STEP) {
            for (int i = 0; i < members.size(); i++) {
                if (running.get(i) && (clock.get() / STEP + i) % (TTL / 3 / STEP) == 0) {
                    leases.set(i, members.get(i).renew(leases.get(i)));
                    members.get(i).refresh();
                }
            }
            clock.addAndGet(STEP);
        }
    }

    private boolean anyRunningNodeSees(String nodeId) {
        for (int i = 0; i < members.size(); i++) {
            if (running.get(i) && members.get(i).current().stream().anyMatch(l -> l.nodeId().equals(nodeId))) {
                return true;
            }
        }
        return false;
    }

    public void testAStoppedNodeLeavesEveryViewByItsOwnStamp() throws Exception {
        run(2 * 60_000L);
        for (int i = 0; i < members.size(); i++) {
            assertEquals(members.size(), members.get(i).current().size());
        }
        // Not the digest writer: that is the lowest id.
        running.set(3, false);
        final long stamp = leases.get(3).expiresAtMillis();
        while (clock.get() < stamp) {
            run(STEP);
        }
        run(STEP);
        for (int i = 0; i < members.size(); i++) {
            if (running.get(i)) {
                assertFalse(
                    "node " + i + " still sees the stopped node after its stamp",
                    members.get(i).current().stream().anyMatch(l -> l.nodeId().equals("node-3") && l.isExpiredAt(clock.get()) == false)
                );
            }
        }
        run(TTL);
        assertFalse("a refresh past its stamp drops it from every view", anyRunningNodeSees("node-3"));
    }

    public void testWhenTheDigestWriterStopsEveryNodeStillSeesTheRest() throws Exception {
        run(2 * 60_000L);
        running.set(0, false);
        run(3 * TTL);
        for (int i = 1; i < members.size(); i++) {
            assertEquals("node " + i + " sees every running node", members.size() - 1, members.get(i).current().size());
        }
        assertFalse(anyRunningNodeSees("node-0"));
        // And the next lowest id takes the digest over: a steady minute reads about one digest per node per period.
        run(60_000L);
        store.reset();
        run(60_000L);
        final long periods = 60_000L / (TTL / 3);
        long leaseReads = 0L;
        for (String line : store.byPlace().split("\n")) {
            if (line.endsWith("REGISTER_GET members/lease-node-#")) {
                leaseReads = Long.parseLong(line.substring(0, line.indexOf(' ')));
            }
        }
        // The writer reads every lease once a period, and every node reads the stopped one until it is pruned.
        final long running = members.size() - 1;
        assertTrue(
            "lease reads are the writer's and the stopped node's only: " + store.byPlace(),
            leaseReads <= periods * (members.size() + running)
        );
    }
}
