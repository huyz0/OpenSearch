/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.core.action.ActionListener;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Resolves the descriptors a request will need before the request starts needing them.
 *
 * <h2>The problem this exists for</h2>
 *
 * {@link AbsentIndexDescriptorSuppliers} is synchronous, and deliberately so: it is called from eleven
 * sites on the write path, one of them once per document in a bulk request, and rewriting those into
 * continuations would be a large invasive change to the hottest code in the product.
 *
 * <p>Behind a system index a synchronous lookup is a sub-millisecond local read. Behind an object store it
 * is a network round trip, and doing that inline on a transport thread turns a latency problem into an
 * availability problem: a burst of cold tenants blocks every thread that could have served them.
 *
 * <p>So the resolution moves earlier rather than the call sites moving to callbacks. A coordinator that
 * knows which indices a request touches resolves them all at once, asynchronously, before entering the
 * synchronous path. The eleven sites then find what they need already in memory and do not change at all.
 *
 * <h2>Why batching is the point, not just the timing</h2>
 *
 * A bulk request touching M distinct indices is M lookups. Issued one at a time from inside the document
 * loop they are M sequential round trips, which at a hundred tenants is seconds of metadata resolution
 * before a single document is written. Issued together they are one batch. The measured hit rate makes
 * this load-bearing rather than a nicety: at a cache bound of a tenth of the population under realistic
 * skew, roughly three lookups in ten still reach the store.
 *
 * <h2>It is a hint, and a request must not fail because it did not land</h2>
 *
 * Prefetching is an optimisation and is written to stay one. A failure completes the listener normally, so
 * the request proceeds and the synchronous sites resolve what they need the way they always did, slowly.
 * The alternative, failing a request because a speculative read failed, would make the optimisation able
 * to break the thing it optimises.
 *
 * <p>Unregistered, {@link #prefetch} completes immediately and touches nothing, so an ordinary cluster
 * behaves exactly as it did before this existed.
 */
public final class DescriptorPrefetch {

    private static final Logger logger = LogManager.getLogger(DescriptorPrefetch.class);

    public static final int DEFAULT_MAX_PREFETCH_BATCH_SIZE = 500;

    /** Calculates the optimal prefetch chunk size for a target population size (T84). */
    public static int optimalBatchSize(int targetCount) {
        if (targetCount <= 50) {
            return targetCount;
        }
        return Math.min(DEFAULT_MAX_PREFETCH_BATCH_SIZE, Math.max(50, targetCount / 4));
    }

    /** Resolves a batch of names into whatever cache the synchronous suppliers read from. */
    @FunctionalInterface
    public interface Prefetcher {
        void prefetch(Collection<String> indexNames, ActionListener<Void> listener);
    }

    private static final AtomicReference<Prefetcher> PREFETCHER = new AtomicReference<>();

    private DescriptorPrefetch() {}

    /** Installs the prefetcher. Registering null clears it, so a test can restore the default. */
    public static void register(Prefetcher prefetcher) {
        PREFETCHER.set(prefetcher);
    }

    /** Whether anything is installed, so a caller can skip assembling a name set that would be discarded. */
    public static boolean isRegistered() {
        return PREFETCHER.get() != null;
    }

    /**
     * Warms the descriptors for these names, then calls the listener.
     *
     * <p>The listener is always completed successfully, on this thread when there is nothing to do and on
     * whatever thread the prefetcher finishes on otherwise. A caller can therefore treat this as "carry on
     * when ready" without a failure branch, which is what keeps it from becoming a new way for a request
     * to fail.
     */
    public static void prefetch(Collection<String> indexNames, ActionListener<Void> listener) {
        Prefetcher prefetcher = PREFETCHER.get();
        if (prefetcher == null || indexNames == null || indexNames.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        try {
            prefetcher.prefetch(indexNames, ActionListener.wrap(ignored -> listener.onResponse(null), e -> {
                // Degrading to the slow path is the whole contract. A prefetch that failed leaves the
                // synchronous sites to resolve what they need, which is what they did before this class.
                logger.debug("descriptor prefetch failed for {}; falling back to resolving inline", indexNames.size(), e);
                listener.onResponse(null);
            }));
        } catch (Exception e) {
            logger.debug("descriptor prefetch threw for {}; falling back to resolving inline", indexNames.size(), e);
            listener.onResponse(null);
        }
    }

    /** Convenient fire-and-forget prefetch when no completion callback is required. */
    public static void prefetchAsync(Collection<String> indexNames) {
        prefetch(indexNames, ActionListener.wrap(() -> {}));
    }
}
