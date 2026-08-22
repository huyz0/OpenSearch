/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.cluster;

import org.opensearch.common.unit.TimeValue;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The node-level holder for the single {@link
 * IndexResidencyPolicy} a {@link org.opensearch.plugins.ClusterPlugin} may supply, mirroring {@code
 * IndexCreationStrategyRegistry}'s own shape deliberately -- see that class's javadoc for why this
 * "structurally identical to a mechanism already proven in this exact codebase" approach is the chosen one.
 *
 * <p>Registered once, at node startup, by {@code Node.java} from whichever single {@code ClusterPlugin}
 * supplies {@link org.opensearch.plugins.ClusterPlugin#getIndexResidencyPolicy()}.
 *
 * <p>Unlike {@code IndexCreationStrategyRegistry}, whose predicates default to "unclaimed" when nothing is
 * registered, this registry's accessors default to {@link IndexResidencyPolicy}'s own default methods --
 * the exact numeric defaults {@code IndicesClusterStateService} hardcoded before this phase -- so a
 * plugin-free node's sweep/eviction behavior is unchanged, not merely equivalent. See {@link
 * IndexResidencyPolicy}'s own javadoc for the full reasoning.
 */
public final class IndexResidencyPolicyRegistry {

    private static final IndexResidencyPolicy DEFAULT = new IndexResidencyPolicy() {
    };

    private static final AtomicReference<IndexResidencyPolicy> POLICY = new AtomicReference<>();

    private IndexResidencyPolicyRegistry() {}

    /** Installs the policy. Registering {@code null} clears it, which is how a test restores the default. */
    public static void register(IndexResidencyPolicy policy) {
        POLICY.set(policy);
    }

    public static boolean isRegistered() {
        return POLICY.get() != null;
    }

    /**
     * The registered policy's sweep interval, or the built-in default if nothing is registered or the
     * registered policy throws.
     */
    public static TimeValue sweepInterval() {
        try {
            return current().sweepInterval();
        } catch (Exception e) {
            return DEFAULT.sweepInterval();
        }
    }

    /**
     * The registered policy's idle-eviction threshold, or the built-in default if nothing is registered or
     * the registered policy throws.
     */
    public static TimeValue idleEvictionAfter() {
        try {
            return current().idleEvictionAfter();
        } catch (Exception e) {
            return DEFAULT.idleEvictionAfter();
        }
    }

    /**
     * The registered policy's max-open ceiling (zero meaning "derive one"), or the built-in default if
     * nothing is registered or the registered policy throws.
     */
    public static int maxOpen() {
        try {
            return current().maxOpen();
        } catch (Exception e) {
            return DEFAULT.maxOpen();
        }
    }

    /**
     * The registered policy's per-open-index heap cost, or {@link IndexResidencyPolicy#bytesPerOpenIndex()}'s
     * own conservative default if nothing is registered, the registered policy throws, or it answers a
     * non-positive number -- the last guard because this value is a divisor, and a broken policy must cost
     * a wrong ceiling rather than an {@code ArithmeticException} at node start.
     */
    public static long bytesPerOpenIndex() {
        try {
            long bytes = current().bytesPerOpenIndex();
            return bytes > 0 ? bytes : DEFAULT.bytesPerOpenIndex();
        } catch (Exception e) {
            return DEFAULT.bytesPerOpenIndex();
        }
    }

    private static IndexResidencyPolicy current() {
        IndexResidencyPolicy policy = POLICY.get();
        return policy == null ? DEFAULT : policy;
    }
}
