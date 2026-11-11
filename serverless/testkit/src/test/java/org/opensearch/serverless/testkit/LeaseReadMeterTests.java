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
 * What keeping every node's view of the fleet costs in reads, as the fleet grows: each node renews every third of
 * a lease's life and refreshes its view as often, which is what the reconcile loop does.
 */
public class LeaseReadMeterTests extends OpenSearchTestCase {

    private static final long TTL = 30_000L;

    /** Register reads per node per second over five simulated minutes, after a minute of warm-up. */
    private double readsPerNodeSecond(int nodes) throws Exception {
        final AtomicLong clock = new AtomicLong(1_000_000L);
        try (CountingBlobStore store = new CountingBlobStore(new FsBlobStore(8192, createTempDir(), false))) {
            final List<BlobLeaseMembership> members = new ArrayList<>();
            final List<NodeLease> leases = new ArrayList<>();
            for (int i = 0; i < nodes; i++) {
                members.add(new BlobLeaseMembership(store.blobContainer(BlobPath.cleanPath().add("members")), clock::get, TTL));
                leases.add(
                    new NodeLease(String.format(java.util.Locale.ROOT, "node-%04d", i), "e" + i, "10.0.0." + i + ":9300", Set.of(), 0L)
                );
            }
            final long step = 1_000L;
            final long renewEvery = TTL / 3;
            long reads = 0L;
            long writes = 0L;
            for (long t = 0; t < 6 * 60_000L; t += step) {
                if (t == 60_000L) {
                    store.reset();
                }
                for (int i = 0; i < nodes; i++) {
                    // Spread across the period, as real nodes are.
                    final long phase = ((renewEvery * i) / nodes) / step * step;
                    if ((t - phase) % renewEvery == 0 || (t == 0 && i == 0)) {
                        members.get(i).renew(leases.get(i));
                    }
                    if ((t - phase) % renewEvery == 0) {
                        members.get(i).refresh();
                    }
                }
                clock.addAndGet(step);
            }
            reads = store.registerReads();
            writes = store.registerWrites();
            for (BlobLeaseMembership member : members) {
                assertEquals("every node sees the whole fleet", nodes, member.current().size());
            }
            final double seconds = 5 * 60.0;
            logger.info(
                "LEASEMETER|nodes={}|reads/node-s={}|writes/node-s={}|fleet reads/s={}",
                nodes,
                reads / (nodes * seconds),
                writes / (nodes * seconds),
                reads / seconds
            );
            return reads / (nodes * seconds);
        }
    }

    public void testReadsPerNodeAsTheFleetGrows() throws Exception {
        final double ten = readsPerNodeSecond(10);
        final double forty = readsPerNodeSecond(40);
        final double hundred = readsPerNodeSecond(100);
        logger.info("LEASEMETER|growth 40 -> 100 nodes: per-node reads x{}", hundred / forty);
        logger.info("LEASEMETER|growth 10 -> 40 nodes: per-node reads x{}", forty / ten);
    }
}
