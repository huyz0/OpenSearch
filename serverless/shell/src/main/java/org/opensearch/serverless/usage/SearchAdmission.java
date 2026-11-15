/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.serverless.usage;

import org.opensearch.common.lease.Releasable;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.IntSupplier;

/**
 * Which searches a node starts now and which wait, by tenant.
 *
 * <p><b>Fair share.</b> While the node runs as many searches as it has slots, a tenant already holding its share of them
 * waits for one of its own to end, or for the node to fall under its slots, and others go ahead. Below that nothing
 * waits, so a lone tenant on a quiet node is never held back. A wait is bounded: past the longest wait the search starts
 * anyway, since fairness orders searches and never refuses one.
 *
 * <p><b>Low priority.</b> A search marked so waits while half the slots are taken. It is refused only when also marked
 * refusable and still waiting at the deadline.
 */
final class SearchAdmission {

    private final double share;
    private final long maxWaitMillis;
    private final IntSupplier slots;
    private final Object lock = new Object();
    private final Map<String, Integer> byTenant = new HashMap<>();
    private int inFlight;

    SearchAdmission(double share, long maxWaitMillis, IntSupplier slots) {
        this.share = share;
        this.maxWaitMillis = maxWaitMillis;
        this.slots = slots;
    }

    Releasable admit(Set<String> tenants, boolean lowPriority, boolean refusable) throws IOException {
        final int slotCount = Math.max(1, slots.getAsInt());
        final int perTenant = share <= 0 ? Integer.MAX_VALUE : Math.max(1, (int) Math.ceil(slotCount * share));
        synchronized (lock) {
            final long deadline = System.nanoTime() + maxWaitMillis * 1_000_000L;
            while (mustWait(tenants, slotCount, perTenant, lowPriority)) {
                final long left = (deadline - System.nanoTime()) / 1_000_000L;
                if (left <= 0) {
                    if (refusable) {
                        throw new OpenSearchRejectedExecutionException("tenant " + tenants + " is over its cost budget; retry later");
                    }
                    break;
                }
                try {
                    lock.wait(Math.min(left, 50L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted waiting to start a search", e);
                }
            }
            inFlight++;
            for (String tenant : tenants) {
                byTenant.merge(tenant, 1, Integer::sum);
            }
        }
        return () -> {
            synchronized (lock) {
                inFlight--;
                for (String tenant : tenants) {
                    byTenant.computeIfPresent(tenant, (k, n) -> n <= 1 ? null : n - 1);
                }
                lock.notifyAll();
            }
        };
    }

    private boolean mustWait(Set<String> tenants, int slotCount, int perTenant, boolean lowPriority) {
        if (lowPriority && inFlight >= Math.max(1, slotCount / 2)) {
            return true;
        }
        if (inFlight < slotCount) {
            return false;
        }
        for (String tenant : tenants) {
            if (byTenant.getOrDefault(tenant, 0) >= perTenant) {
                return true;
            }
        }
        return false;
    }

    int inFlight() {
        synchronized (lock) {
            return inFlight;
        }
    }
}
